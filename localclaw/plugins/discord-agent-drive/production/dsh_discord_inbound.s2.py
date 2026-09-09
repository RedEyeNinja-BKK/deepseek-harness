#!/usr/bin/env python3
"""dsh_discord_inbound.py — minimal Discord Gateway listener for DSH-Edge.

One narrow job: receive Discord inbound messages for the existing Phase-4 bot
(1543715940741546115 / DSH-Edge), normalize them, and feed them into DSH via
the public session RPC (session.create / session.prompt). PRIMARY reply path =
DSH calls the EXISTING Phase-4 Discord REST MCP (mcp__discord__send_message).
v1.1 (2026-09-07) adds: (1) image-attachment ingest into the private media seam
(mirrors LINE v1.4.8 slots) so same-message edit instructions have an exact
ref; (2) a DETERMINISTIC emission backstop — if a DSH turn ends with final text
and DSH did NOT call the send tool, the listener delivers it (never duplicate:
backstop is suppressed when the send tool was observed).
v1.2 (2026-09-07) — backstop finalization delta (Hermes REQUEST-CHANGES
incorporated): per-turn accumulators finalize at EVERY turn boundary (queued
turns never merge) with exactly-once decisions; send OUTCOME tracked per
callId at tool/result level (only verified callIds are attributed); strict
gate-contract classification ('ok' only on status==200 + message_id for every
reported message, malformed => unknown never failed); confirmed-success send +
later empty model tail (EMPTY_RESPONSE class) = TERMINAL (no duplicate, no
fallback, conversation usable); FAILED send = exactly one honest fallback
(model final text if present, else a bounded failure notice) with retry only
on zero-chunk transport failure and honest terminal logging for partial/
undelivered fallbacks; send attempted with unknown outcome + final text = one
bounded text delivery. Tool-send + backstop never coexist on a confirmed
success.
v1.3 (2026-09-08) — MEDIA-SUCCESS ARTIFACT BACKSTOP (SUMO music MVP; Hermes
review round pending). Closes the first-turn delivery gap proven by SUMO
Case C: generate_music completes successfully (expensive GPU render) but the
model turn terminates abnormally (provider/stream error such as PI_AI_ERROR,
empty/error tail) BEFORE DSH's own mcp__discord__send_message fires, so the
user receives no artifact unless they ask again. Rule (deterministic, exactly
once): IF a generation tool produced a valid deliverable audio artifact this
turn AND no confirmed outbound Discord send of that artifact occurred AND the
turn ended abnormally or with an empty final response THEN the backstop
attaches that preserved artifact once to the originating channel. Narrow
scope: audio artifacts under the approved NFS multimedia root returned by a
music-generation tool; a failed/foreign generation NEVER triggers delivery;
no invented caption (artifact sent bare); per-conversation delivered-artifact
ledger reuses the existing persistent state file (bounded; no new
daemon/queue/database). Exactly-once guarantee is enforced per watcher/process
lifetime at BOTH enqueue and delivery time plus a durable ledger for restart
suppression; if the durable ledger write itself fails after a confirmed send
(CRITICAL logged, in-memory mark kept), restart suppression is BEST-EFFORT and
mirrors the existing processed-window residual posture in dispatch_to_dsh.
Also fixes a turn-boundary accounting bug
observed 2026-09-08 (plugin/snapshot user/message between model steps was
mistaken for a turn boundary, splitting one turn and defeating confirmed-send
text suppression -> a second plain-text message): boundaries now finalize only
on real turn/end and genuine user messages (source.kind == user).
Design constraints (operator GO 2026-09-02):
- discord.py (mature library) for Gateway protocol; no hand-rolled protocol.
- No DSH core patch, no webhook, no SaaS, no event bus, no Phase-4 redesign.
- Token ONLY via systemd LoadCredential (discord-bot-token). Never logged.
- Identity gate: READY user.id must equal EXPECTED_BOT_ID or abort.
- Event policy (initial): DMs, direct @mentions, replies to our messages.
- Dedupe: persisted last-processed message ids (replay-safe).
- Session mapping: DM -> per-user session; guild channel -> per-channel
  session; thread -> per-thread session. Persisted (restart-safe).
- Failure isolation: any error logs structured evidence and never propagates;
  this process dying must not affect dsh.service / REST gate / webgate.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import stat
import sys
import threading
import time
from pathlib import Path

import discord

# --- constants ----------------------------------------------------------------

EXPECTED_BOT_ID = 1543715940741546115
# Channels where DSH replies to EVERY message (no mention needed) — operator
# directive 2026-09-02: category "Kaem" (1486789784855908372) and all children.
# Implemented as a CATEGORY check (not a fixed channel list): every current and
# future channel under Kaem is covered (channel 1523892165703635075, moved
# under Kaem after the original list was staged, was silently dropped until
# this fix — matched operator policy "Kaem category only", not the 7-ID set).
ALWAYS_RESPOND_CATEGORY_ID = 1486789784855908372  # Kaem
DSH_API = "http://127.0.0.1:3080/api/"
# v1.1 (2026-09-07): Discord attachment ingest + deterministic emission
# backstop. BRIDGE_BASE mirrors the LINE v1.4.8 slot-ingest seam (private 5C
# input namespace; bridge owns validation/storage). IMAGE_SLOT_* mirror LINE.
BRIDGE_BASE = "http://127.0.0.1:8620"
BRIDGE_INGEST_TIMEOUT_S = 30
IMAGE_SLOT_TTL_S = 30 * 60          # association window: 30 minutes
IMAGE_SLOT_MAX = 3                  # bounded slots per conversation
DISCORD_IMAGE_MAX_BYTES = 20 * 1024 * 1024
REPLY_POLL_TIMEOUT_S = 840          # backstop watch bound (DSH queue turn)
REPLY_POLL_INTERVAL_S = 4.0
# v1.3 media-success artifact backstop (SUMO music MVP): names that produce a
# deliverable audio artifact; approved NFS multimedia artifact root; bounded
# durable delivered-artifact ledger per conversation (exactly-once across
# turns/restarts; stored in the existing persistent inbound-state.json).
MEDIA_GEN_NAME_MARKERS = ("generate_music",)
MEDIA_ROOT = "/mnt/off-vm-nfs/comfyui-media"
MEDIA_DELIVERED_MAX = 20            # ledger entries kept per conversation
_WATCHERS: dict[str, asyncio.Task] = {}   # one emission backstop per conversation
_WATCH_GUARD = threading.Lock()
# Persistent state: systemd StateDirectory (STATE_DIRECTORY env is set by the
# unit; /var/lib/dsh-discord-inbound) — survives listener restart, service
# restart, and reboots. Local fallback for manual (non-systemd) test runs.
_state_dir_env = os.environ.get("STATE_DIRECTORY")
STATE_DIR = Path(_state_dir_env.split(":")[0]) if _state_dir_env else Path("/var/lib/dsh-discord-inbound")
STATE_PATH = STATE_DIR / "inbound-state.json"
LIBS = "/opt/dsh-inbound"

# --- S2 pilot consumer (production candidate; DEFAULT-OFF) ------------------
# S2_PILOT_CONV empty (default) => byte-for-byte historical path: the S2 seam is
# never connected and no pilot routing occurs. When set to an EXACT conversation
# key (channel:<id> / thread:<id> / dm:<id>), that conversation routes through
# the native discord-agent-drive plugin seam while its route control says
# S2_ACTIVE or QUIESCING_TO_OLD; OLD returns it to the historical path.
S2_PILOT_CONV = os.environ.get("S2_PILOT_CONV", "") or ""
S2_SOCK_PATH = os.environ.get("S2_SOCK_PATH", "/run/dsh-discord-pilot/dsh.sock")
S2_ROUTE_FILE = STATE_DIR / "s2-route.json"     # gate-owned control; read only
S2_ADMIT_TIMEOUT_S = float(os.environ.get("S2_ADMIT_TIMEOUT_S", "60"))
# Explicit old-path tripwires for the live gate: PILOT stays 0 while S2 owns the
# pilot; SIBLING counts old-path work for every other conversation. Live gate
# greps journal lines 'old-path ... conv=<key>' inside a bounded time window.
OLD_PATH_COUNTERS = {"pilot": 0, "sibling": 0}
_S2_SEAM = None                # one async seam client per process (lazy)
_S2_SEAM_SID = None
_S2_CLIENT = None              # discord client (set on_ready)
_S2_CHANNEL_IDS: dict = {}     # pilot conv_key -> channel id (gateway-derived)
_S2_NEED_RECONNECT = False     # set when a finalization lacked its channel

# make vendored libs importable BEFORE discord import
sys.path.insert(0, LIBS)

import discord  # noqa: E402
import urllib.request  # noqa: E402
import s2_seam as s2  # noqa: E402  (S2 pilot helper; default-off)

# --- logging ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("dsh-discord-inbound")

# --- state --------------------------------------------------------------------


def load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"sessions": {}, "processed": []}


def save_state(state: dict) -> bool:
    """Atomic + durable state write (Hermes P2, 2026-09-07): unique tmp,
    fsync file + dir, os.replace. Returns False on ANY persistence failure so
    callers can keep the message retryable instead of marking it processed."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_name(f"inbound-state.tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(state))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, STATE_PATH)
        dfd = os.open(STATE_DIR, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        return True
    except Exception:
        log.error("state save FAILED (durable state may be stale)")
        return False


def remember_message(state: dict, message_id: int) -> bool:
    """Advance the processed window ONLY on durable save (replay-safe)."""
    state.setdefault("processed", [])
    if str(message_id) in state["processed"]:
        return True
    state["processed"].append(str(message_id))
    state["processed"] = state["processed"][-500:]
    if not save_state(state):
        # roll back the in-memory append so a replay can retry this message
        try:
            state["processed"].remove(str(message_id))
        except ValueError:
            pass
        return False
    return True


def _remember_durable(state: dict, message_id: int, attempts: int = 3) -> bool:
    """Bounded-retry durable processed-mark. On final failure the in-memory
    window is KEPT (no duplicate within this process on gateway replay); the
    caller logs CRITICAL for the restart-replay residual."""
    for i in range(attempts):
        if remember_message(state, message_id):
            return True
        time.sleep(0.5 * (i + 1))
    # keep the in-memory mark despite failed durable save (prevents duplicate
    # dispatch in THIS process); return False so the caller fails closed on the
    # backstop and logs the residual.
    state.setdefault("processed", [])
    if str(message_id) not in state["processed"]:
        state["processed"].append(str(message_id))
        state["processed"] = state["processed"][-500:]
    return False


def already_processed(state: dict, message_id: int) -> bool:
    return str(message_id) in state.get("processed", [])


def _mark_old_call(conv_key: str, what: str) -> None:
    """Tripwire: count + log when the HISTORICAL DSH-driving path acts for a
    conversation. While S2 owns the pilot this never fires for the pilot conv
    (structural: the branch returns before the old body); the live gate asserts
    zero pilot lines inside the active window and >=1 sibling lines."""
    if S2_PILOT_CONV and conv_key == S2_PILOT_CONV:
        OLD_PATH_COUNTERS["pilot"] += 1
    else:
        OLD_PATH_COUNTERS["sibling"] += 1
    log.info("old-path %s conv=%s (counters pilot=%d sibling=%d)",
             what, conv_key, OLD_PATH_COUNTERS["pilot"],
             OLD_PATH_COUNTERS["sibling"])


def _pilot_dsh_message_id(conv_key: str, discord_id) -> str:
    """Deterministic DSH identity EXACTLY as the plugin builds it (no second
    identity generated here; mirrors dshMessageIdFor)."""
    return f"discord:{conv_key}:{discord_id}"


# --- DSH session RPC ----------------------------------------------------------


def dsh_rpc(endpoint: str, payload: dict, timeout: float = 60.0) -> dict:
    import urllib.request as _ur
    body = json.dumps({"type": "client-request", "rpcId": f"inb-{time.time_ns()}",
                       "method": endpoint, "payload": payload}).encode()
    # ALWAYS direct: never route loopback DSH calls through any configured proxy
    # (proxy env in a service context silently redirects and breaks the API).
    opener = _ur.build_opener(_ur.ProxyHandler({}))
    req = urllib.request.Request(
        DSH_API + endpoint, data=body,
        headers={"Content-Type": "application/json", "Host": "127.0.0.1:3080"})
    try:
        with opener.open(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read(300).decode(errors="replace")
        except Exception:
            pass
        log.error("dsh_rpc %s -> HTTP %s body=%r proxies=%r url=%s host_hdr=%r",
                  endpoint, e.code, detail, _ur.getproxies(), req.full_url,
                  req.get_header("Host"))
        raise


def repr_dsh_error() -> str:
    """Capture last dsh_rpc failure detail for logging (structured evidence)."""
    return "see traceback above"


def conversation_key(message: discord.Message) -> tuple[str, str]:
    """(scope, label) — one DSH session per real conversation context."""
    if message.guild is None:
        return ("dm", f"dm-{message.author.id}")
    if isinstance(message.channel, discord.Thread):
        return ("thread", str(message.channel.id))
    return ("channel", str(message.channel.id))


def get_or_create_session(state: dict, key: tuple[str, str]) -> str | None:
    map_key = f"{key[0]}:{key[1]}"
    sid = state.get("sessions", {}).get(map_key)
    if sid:
        return sid
    try:
        resp = dsh_rpc("session.create", {"cwd": "/opt/dsh/workspace"})
        new_sid = resp["result"]["value"]["sessionId"]
    except Exception:
        log.exception("session.create failed: %s", repr_dsh_error())
        return None
    state.setdefault("sessions", {})[map_key] = new_sid
    if not save_state(state):
        # mapping not durable -> a restart would lose continuity; leave the
        # message retryable (fail-closed) rather than use an un-durable session
        del state["sessions"][map_key]
        log.error("session map NOT durable for %s - session create rolled back",
                  map_key)
        return None
    log.info("created session %s for %s", new_sid, map_key)
    _mark_old_call(map_key, "session.create")
    return new_sid


# --- attachment ingest (v1.1; mirrors LINE v1.4.8 slot-ingest) ----------------


def prune_image_slots(state: dict, conv_key: str | None = None) -> None:
    cutoff = time.time() - IMAGE_SLOT_TTL_S
    slots = state.setdefault("imageSlots", {})
    if conv_key:
        lst = slots.get(conv_key)
        if lst:
            slots[conv_key] = [s for s in lst if s.get("at", 0) >= cutoff]
        return
    for k in list(slots):
        slots[k] = [s for s in slots[k] if s.get("at", 0) >= cutoff]
        if not slots[k]:
            del slots[k]


def bridge_ingest_bytes(state: dict, conv_key: str, message_id: str,
                        user_id: str, data: bytes, filename: str) -> dict | None:
    """POST normalized image bytes to the media bridge; record a bounded
    per-conversation slot on success. Returns the artifact dict or None."""
    try:
        body = json.dumps({"filename": filename,
                           "b64": base64.b64encode(data).decode()}).encode()
        req = urllib.request.Request(
            BRIDGE_BASE + "/ingest_input", data=body,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=BRIDGE_INGEST_TIMEOUT_S) as r:
            out = json.loads(r.read() or b"{}")
    except Exception as e:
        log.error("discord image ingest failed for msg %s (%s): %s",
                  message_id, conv_key, e.__class__.__name__)
        return None
    if not isinstance(out, dict) or out.get("ok") is not True \
            or not isinstance(out.get("artifact"), str):
        log.error("discord image ingest rejected for msg %s (%s): %s",
                  message_id, conv_key, (out or {}).get("error", "malformed"))
        return None
    lst = state.setdefault("imageSlots", {}).setdefault(conv_key, [])
    lst.append({"message_id": str(message_id), "artifact": out["artifact"],
                "user": str(user_id), "at": time.time()})
    state["imageSlots"][conv_key] = lst[-IMAGE_SLOT_MAX:]
    prune_image_slots(state, conv_key)
    if not save_state(state):
        log.error("discord image slot NOT durable (save failed) for msg %s "
                  "(%s) - slot rolled back", message_id, conv_key)
        lst = state.get("imageSlots", {}).get(conv_key) or []
        if lst and lst[-1].get("message_id") == str(message_id):
            lst.pop()
        return None
    log.info("discord image slot stored for %s (msg %s, artifact %.24s...)",
             conv_key, message_id, out["artifact"])
    return out


def image_context_block(state: dict, conv_key: str) -> str:
    """Bounded [dsh image context] block listing editable source refs for this
    conversation (newest last). Mirrors LINE v1.4.8 so the model can call
    edit_image with an exact ref. Empty when no live slots."""
    prune_image_slots(state, conv_key)
    slots = state.get("imageSlots", {}).get(conv_key) or []
    if not slots:
        return ""
    lines = []
    for s in slots:
        lines.append(json.dumps({
            "artifact": s["artifact"],
            "from_message_id": s["message_id"],
            "sent_by": s["user"],
        }, ensure_ascii=False))
    return ("[dsh image context] editable source images for this conversation "
            "(newest last):\n" + "\n".join(lines) + "\n")


async def ingest_attachments(state: dict, conv_key: str,
                             message: discord.Message) -> str:
    """Fetch + privately ingest image attachments (bounded). Returns a context
    block for the envelope (or empty). Never exposes source CDN URLs as the
    durable input artifact; the bridge owns the private 5C input namespace."""
    refs = []
    for att in list(message.attachments)[:IMAGE_SLOT_MAX]:
        if not (att.content_type or "").lower().startswith("image/"):
            continue
        if att.size > DISCORD_IMAGE_MAX_BYTES:
            log.warning("discord attachment %s too large (%s B) - skipped",
                        att.id, att.size)
            continue
        try:
            data = await att.read()
        except Exception as e:
            log.error("discord attachment fetch failed msg %s: %s",
                      message.id, e.__class__.__name__)
            continue
        if not data:
            continue
        fn = Path(att.filename or "image.png").name
        try:
            out = await asyncio.to_thread(
                bridge_ingest_bytes, state, conv_key, str(message.id),
                str(message.author.id), data, fn)
        except Exception as e:
            out = None
            log.error("discord ingest task failed msg %s: %s",
                      message.id, e.__class__.__name__)
        if out:
            refs.append(out.get("artifact"))
    if not refs:
        return ""
    return image_context_block(state, conv_key)


def build_envelope(message: discord.Message) -> str:
    """Structured platform context + content. NOT prose; explicit metadata."""
    ch = message.channel
    guild = message.guild
    ctx = {
        "platform": "discord",
        "discord_user_id": str(message.author.id),
        "discord_user_name": message.author.display_name,
        "guild_id": str(guild.id) if guild else None,
        "guild_name": guild.name if guild else None,
        "channel_id": str(ch.id),
        "channel_name": getattr(ch, "name", None),
        "is_dm": guild is None,
        "is_thread": isinstance(ch, discord.Thread),
        "thread_id": str(ch.id) if isinstance(ch, discord.Thread) else None,
        "message_id": str(message.id),
        "reply_to_message_id": (str(message.reference.message_id)
                                if message.reference and message.reference.message_id else None),
        "attachment_count": len(message.attachments),
        "content_length": len(message.content or ""),
    }
    lines = [f"[discord message] {json.dumps(ctx, ensure_ascii=False)}"]
    content = message.content or ""
    if message.attachments:
        lines.append("[attachments] (ingested privately; see dsh image context "
                     "for editable refs)" if content else
                     "[attachments] (ingested privately; see dsh image context "
                     "for editable refs)")
    if not content and message.attachments:
        lines.append("(no text content; image ingested for editing)")
    lines.append(content)
    return "\n".join(lines)


async def dispatch_to_dsh(state: dict, message: discord.Message) -> None:
    key = conversation_key(message)
    conv_key = f"{key[0]}:{key[1]}"
    # S2 pilot consumer (default-off): if this is THE pilot conversation and S2
    # authority is ACTIVE/QUIESCING_TO_OLD, the event is consumed by the native
    # seam path (or fail-closed) and the historical DSH-driving/backstop body
    # below is NEVER reached (structural zero old-path calls for the pilot while
    # S2 owns it). OLD returns the pilot to the historical path below, but never
    # resubmits a message that was already S2-attempted (no double-submit).
    if S2_PILOT_CONV and conv_key == S2_PILOT_CONV:
        route = s2.read_route_file(S2_ROUTE_FILE, conv_key)
        if route in (s2.S2_ROUTE_ACTIVE, s2.S2_ROUTE_QUIESCING):
            await _s2_dispatch_pilot(state, conv_key, message, route)
            return
        if s2.s2_attempted(state, conv_key, str(message.id)):
            st = s2.s2_attempted(state, conv_key, str(message.id)).get("state")
            log.warning("s2: pilot msg %s was S2-attempted (%s); OLD path will "
                        "NOT resubmit (single DSH-driving authority)",
                        message.id, st)
            return
        # fall through to the historical path (OLD authority for the pilot)
    # v1.1: ingest image attachments into the private media seam BEFORE the
    # prompt so an edit instruction in the SAME message has an exact editable
    # ref (case: one Discord message with image + edit text).
    image_ctx = await ingest_attachments(state, conv_key, message)
    sid = get_or_create_session(state, key)
    if not sid:
        log.error("no DSH session for %s; message %s left UNMARKED (retryable "
                  "on gateway replay)", key, message.id)
        return
    # Watermark BEFORE prompt acceptance so the backstop observes every event of
    # the turn we are about to start (never misses a fast reply, never replays
    # pre-existing history) — Hermes REQUEST-CHANGES 2026-09-07.
    watermark = await asyncio.to_thread(_session_max_seq, sid)
    envelope = build_envelope(message)
    content_parts = [{"type": "text", "text": envelope}]
    if image_ctx:
        content_parts.append({"type": "text", "text": image_ctx})
    try:
        # queue mode: if DSH is mid-turn, the prompt splices into next-turn inbox
        resp = dsh_rpc("session.prompt", {
            "sessionId": sid, "mode": "queue",
            "content": content_parts,
            "clientTimeZone": "Asia/Bangkok"})
        if not resp.get("result", {}).get("ok"):
            raise RuntimeError(json.dumps(resp)[:300])
        log.info("dispatched msg %s to %s%s", message.id, sid,
                 " (with dsh image context)" if image_ctx else "")
    except Exception:
        log.exception("session.prompt failed for message %s (session %s); "
                      "left UNMARKED (retryable on gateway replay)",
                      message.id, sid)
        # dedupe advances only on confirmed acceptance (below): a failed RPC
        # leaves the message retryable if Discord replays the event.
        return
    _mark_old_call(conv_key, "session.prompt")
    # Advance processed ONLY when durably saved (replay-safe). A transient
    # failure is retried a bounded number of times; if it still fails the
    # in-memory window is kept (no duplicate within THIS process) and the
    # restart-replay residual is logged CRITICAL (LINE-adapter-equivalent
    # posture). Backstop never starts on a non-durable acceptance.
    if not _remember_durable(state, message.id):
        log.critical("msg %s dispatched but durable processed-state write "
                     "FAILED after retries — replay after a restart may "
                     "double-dispatch (documented residual); backstop NOT "
                     "started for this message", message.id)
        return
    # v1.1 deterministic emission backstop: watch this conversation's turn(s);
    # if DSH ends a turn with final text but did NOT call the Discord send tool,
    # deliver it here. One watcher per conversation; a watcher already running
    # has an EARLIER watermark so it observes every subsequent queued turn.
    _spawn_backstop(state, key, sid, message.channel, watermark)


def _spawn_backstop(state: dict, key: tuple[str, str], sid: str,
                    channel, watermark: int = 0) -> None:
    map_key = f"{key[0]}:{key[1]}"
    _mark_old_call(map_key, "backstop.spawn")
    with _WATCH_GUARD:
        existing = _WATCHERS.get(map_key)
        if existing and not existing.done():
            return  # one backstop already watching this conversation
    task = asyncio.create_task(
        _backstop_loop(state, map_key, sid, channel, watermark))
    with _WATCH_GUARD:
        _WATCHERS[map_key] = task


def _session_max_seq(sid: str) -> int:
    try:
        evs = _history_events(sid, 120)
        return max((e.get("event", {}).get("seq") or 0) for e in evs) if evs else 0
    except Exception:
        return 0


_FAILURE_NOTICE = ("⚠️ Delivery failed — I couldn't post that image/message "
                    "to Discord. Please try again in a moment.")

# --- S2 pilot consumer (native seam path; default-off) -----------------------
# These functions run ONLY for the exact S2_PILOT_CONV conversation while route
# is S2_ACTIVE/QUIESCING_TO_OLD. They reuse the retained external authority
# (attachment ingest, envelope normalization, session map, external ledger,
# _deliver_channel/_deliver_media_file) and suppress the historical
# session.create / session.prompt / session.history / _WATCHERS / _backstop_loop
# / _flush_turn decision path for the pilot (structural: the dispatch branch
# returns before the old body). Finalization delivery uses the exact proven seam
# protocol and the honest at-most-once + INDETERMINATE ledger semantics.


def _pilot_seam(state: dict, conv_key: str):
    global _S2_SEAM, _S2_SEAM_SID
    sid = state.get("sessions", {}).get(conv_key)
    if not sid:
        return None
    if _S2_SEAM is not None and _S2_SEAM_SID == sid:
        return _S2_SEAM
    seam = s2.S2Seam(
        sock_path=S2_SOCK_PATH, conv_key=conv_key, pilot_session_id=sid,
        delivered_provider=lambda: s2.s2_delivered_fids(state, conv_key),
        save_fn=save_state,
        on_finalization=lambda frame: _on_s2_finalization(state, conv_key, frame),
        log_fn=log)
    _S2_SEAM = seam
    _S2_SEAM_SID = sid
    return seam


def _resolve_s2_channel(conv_key: str):
    cid = _S2_CHANNEL_IDS.get(conv_key)
    if cid is None or _S2_CLIENT is None:
        return None
    try:
        return _S2_CLIENT.get_channel(int(cid))
    except Exception:
        return None


async def _on_s2_finalization(state: dict, conv_key: str, frame: dict) -> None:
    """Consumer-side finalization delivery. When the Discord channel object is
    not yet known (e.g. right after a restart before any pilot inbound), the
    frame is NOT recorded and a reconnect is requested so the plugin re-emits it
    (the plugin replays owed finalizations on every hello; nothing was sent so
    there is no duplicate risk). Delivery then goes through the EXISTING
    _deliver_channel/_deliver_media_file machinery with the existing ledger."""
    global _S2_NEED_RECONNECT
    channel = _resolve_s2_channel(conv_key)
    if channel is None:
        log.warning("s2: finalization %s for %s has no known channel yet - "
                    "receive after reconnect", frame.get("finalizationId"),
                    conv_key)
        _S2_NEED_RECONNECT = True
        return
    text_pieces = lambda t: [p for p in (t[i:i + _FALLBACK_CHUNK]
                                         for i in range(0, len(t), _FALLBACK_CHUNK))
                             if p.strip()]  # noqa: E731

    async def deliver_text(text: str) -> str:
        sent = await _deliver_channel(channel, text)
        expected = len(text_pieces(text))
        if sent == 0:
            return "zero"
        if sent < expected:
            return "partial"
        return "ok"

    async def deliver_media(item: dict) -> bool:
        return await _deliver_media_file(channel, item, conv_key)

    await s2.handle_finalization(state, conv_key, frame, save_state,
                                 deliver_text, deliver_media)


async def _s2_dispatch_pilot(state: dict, conv_key: str, message,
                             route: str) -> None:
    """Admit one pilot message over the native seam. Fail-closed and visible:
    no session.create, no session.prompt, no old backstop, no double-submit.
    ACKed durable => processed advanced; anything else => message left
    unprocessed + recorded as S2-attempted so a route flip to OLD never
    resubmits it on the historical path."""
    global _S2_NEED_RECONNECT
    sid = state.get("sessions", {}).get(conv_key)
    if not sid:
        log.error("s2: pilot %s has no mapped DSH session - FAIL-CLOSED (no "
                  "session.create on the S2 path); msg %s not admitted",
                  conv_key, message.id)
        s2.s2_remember_attempted(state, conv_key, str(message.id), "no-session",
                                 save_state)
        return
    if route == s2.S2_ROUTE_QUIESCING:
        log.warning("s2: pilot msg %s NOT admitted (QUIESCING_TO_OLD fence)",
                    message.id)
        s2.s2_remember_attempted(state, conv_key, str(message.id), "quiesced",
                                 save_state)
        return
    # gateway-derived channel knowledge for finalization delivery
    _S2_CHANNEL_IDS[conv_key] = message.channel.id
    seam = _pilot_seam(state, conv_key)
    if seam is None:
        log.error("s2: seam unavailable for %s - msg %s not admitted (visible "
                  "failure; no fallback double-submit)", conv_key, message.id)
        s2.s2_remember_attempted(state, conv_key, str(message.id), "no-seam",
                                 save_state)
        return
    try:
        await seam.start()
    except Exception as e:
        log.error("s2: seam start failed for %s: %s", conv_key, e)
    for _ in range(10):
        if seam.hello_acked:
            break
        await asyncio.sleep(0.3)
    if not seam.hello_acked:
        log.error("s2: seam not ready for %s - msg %s not admitted (visible "
                  "failure; deterministic retry on a later event/reconnect)",
                  conv_key, message.id)
        s2.s2_remember_attempted(state, conv_key, str(message.id), "no-hello",
                                 save_state)
        return
    # Push the current route-control state to the plugin (it boots OLD and only
    # accepts admissions under S2_ACTIVE/QUIESCING). Best-effort: authority is
    # enforced consumer-side regardless.
    try:
        cur = s2.read_route_file(S2_ROUTE_FILE, conv_key)
        if cur in (s2.S2_ROUTE_ACTIVE, s2.S2_ROUTE_QUIESCING):
            await seam.set_route(cur)
            log.info("s2: route pushed to plugin: %s", cur)
    except Exception as e:
        log.warning("s2: route push failed: %s", e)
    if _S2_NEED_RECONNECT:
        # A finalization previously lacked a channel; now that we have one,
        # reconnect so the plugin re-emits it for delivery.
        _S2_NEED_RECONNECT = False
        try:
            await seam.stop()
        except Exception:
            pass
        seam = _pilot_seam(state, conv_key)
        try:
            await seam.start()
        except Exception as e:
            log.error("s2: seam restart failed for %s: %s", conv_key, e)
        for _ in range(10):
            if seam.hello_acked:
                break
            await asyncio.sleep(0.3)
    # retained external authority: attachments + envelope + image context
    image_ctx = await ingest_attachments(state, conv_key, message)
    envelope = build_envelope(message)
    content = envelope + (("\n" + image_ctx) if image_ctx else "")
    refs = []
    if image_ctx:
        for s in (state.get("imageSlots", {}).get(conv_key) or [])[-IMAGE_SLOT_MAX:]:
            if str(s.get("message_id")) == str(message.id):
                refs.append(s.get("artifact"))
    discord_id = str(message.id)
    frame = {
        "conversationKey": conv_key, "sessionId": sid,
        "discordMessageId": discord_id,
        "dshMessageId": _pilot_dsh_message_id(conv_key, discord_id),
        "authorId": str(message.author.id), "content": content,
        "attachmentRefs": refs, "ts": int(time.time()),
    }
    try:
        ack = await seam.admit(frame, timeout=S2_ADMIT_TIMEOUT_S)
    except Exception as e:
        log.warning("s2: admit for msg %s raised %s - ambiguous (no resend, "
                    "deterministic dedupe on replay)", discord_id,
                    e.__class__.__name__)
        s2.s2_remember_attempted(state, conv_key, discord_id, "ambiguous",
                                 save_state)
        return
    if ack.get("accepted") is True:
        kind = "claimed" if ack.get("durable") else "ambiguous"
        s2.s2_remember_attempted(state, conv_key, discord_id, kind, save_state)
        if ack.get("durable") and ack.get("observed") == "claimed":
            if not _remember_durable(state, int(discord_id)):
                log.critical("s2: msg %s admitted durable but processed-state "
                             "write FAILED - restart-replay residual (in-process "
                             "dedupe by deterministic id still holds)", discord_id)
        log.info("s2: msg %s admitted to %s (durable=%s deduped=%s)",
                 discord_id, sid, ack.get("durable"), ack.get("deduped"))
    else:
        code = ack.get("code", "unknown")
        log.warning("s2: msg %s NOT accepted (code=%s) - visible failure, no "
                    "old-path fallback", discord_id, code)
        s2.s2_remember_attempted(state, conv_key, discord_id,
                                 "rejected:" + str(code), save_state)


# Delivery retry bound for fallback attempts that send ZERO chunks (transport/
# network error): retried once on a later poll, then terminal undelivered.
_FALLBACK_MAX_ATTEMPTS = 2
_FALLBACK_CHUNK = 1900


def _is_send_name(name: str) -> bool:
    nm = (name or "").lower()
    return "send_message" in nm or "discord" in nm


def _extract_tool_result(ev: dict):
    """Pull (call_id, text, is_error) from a tool/result event message.

    Accepts the real DSH event shape (data.message.source.callId) plus the
    common envelope alternatives data.message.callId and data.callId, so a
    confirmed send is never silently unattributed (Hermes P1-6)."""
    try:
        data = ev.get("data") or {}
        msg = data.get("message") or {}
        src = msg.get("source") or {}
        call_id = None
        if isinstance(src, dict) and src.get("callId"):
            call_id = src["callId"]
        elif isinstance(msg, dict) and msg.get("callId"):
            call_id = msg["callId"]
        elif data.get("callId"):
            call_id = data["callId"]
        content = msg.get("content") if isinstance(msg, dict) else []
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") == "tool-result":
                    text = ""
                    for piece in item.get("content") or []:
                        if isinstance(piece, dict) and piece.get("type") == "text":
                            text += piece.get("text") or ""
                    return call_id, text.strip(), bool(item.get("isError"))
    except Exception:
        pass
    return None, None, False


def _send_outcome_from_result(text: str | None, is_error: bool) -> str | None:
    """Classify ONE discord send tool/result: 'ok' | 'failed' | None (unknown).

    Real adapter contract (dsh_discord_adapter.py): a failed gate call returns
    text 'error: <code>: <message>' WITH isError=True; a success returns the
    gate result JSON {"sent":N,"messages":[{status,message_id},...]} with
    isError=False. Therefore 'failed' iff is_error is set, or a gate-shaped
    JSON reply whose entries are NOT all confirmed. 'ok' ONLY on the verified
    gate contract (sent>=1, len(messages)==sent, every entry status==200 with
    a non-empty message_id). Any other shape (free-form text, malformed JSON,
    unrecognized object) is None = unknown — never invented (Hermes P1-7).
    """
    if is_error:
        return "failed"
    if not text:
        return None
    try:
        obj = json.loads(text)
    except Exception:
        return None  # unrecognized representation -> unknown
    if not isinstance(obj, dict):
        return None
    sent = obj.get("sent")
    msgs = obj.get("messages")
    if isinstance(sent, int) and isinstance(msgs, list):
        if sent >= 1 and len(msgs) == sent and all(
                isinstance(m, dict)
                and m.get("status") == 200
                and isinstance(m.get("message_id"), str)
                and m["message_id"]
                for m in msgs):
            return "ok"
        # gate-shaped reply that did NOT fully confirm => genuinely failed
        return "failed"
    return None  # not gate shape -> unknown


def _reduce_send_outcomes(calls: dict) -> str | None:
    """Per-call outcomes -> overall turn outcome (Hermes P2-3). Any confirmed
    failure wins (bounded fallback applies). Confirmed success for ALL
    attempted calls -> 'ok'. Any unresolved call keeps the turn 'unknown' so we
    neither suppress a needed fallback on incomplete evidence nor duplicate a
    confirmed success."""
    if not calls:
        return None
    outs = set(calls.values())
    if "failed" in outs:
        return "failed"
    if outs == {"ok"}:
        return "ok"
    return None


def _is_real_user_message(ev: dict) -> bool:
    """True only for a genuine inbound user message. DSH injects plugin/
    snapshot messages (runtime context, time-context, approvals) between model
    steps as user/message events with source.kind == 'plugin'; those are NOT
    turn boundaries and MUST NOT finalize the accumulator (observed 2026-09-08:
    a mid-turn time-context snapshot split one model turn and defeated
    confirmed-send text suppression, emitting the final text as a second
    message). Real inbound dispatches carry source.kind == 'user'."""
    try:
        data = ev.get("data") or {}
        return (data.get("source") or {}).get("kind") == "user"
    except Exception:
        return False


def _is_media_gen_name(name: str) -> bool:
    """True for the DSH tool that returns a deliverable audio artifact
    (mcp__image__generate_music and any equivalent future alias)."""
    nm = (name or "").lower()
    return any(m in nm for m in MEDIA_GEN_NAME_MARKERS)


def _media_from_result(text: str | None, is_error: bool):
    """Strict parse of a generation tool/result into a media artifact record.

    Mirrors the adapter strict-success envelope (no fabricated success): only
    an ok:true object with a real artifact under the approved NFS multimedia
    root, a non-empty sha256, a positive byte count and an audio mime yields a
    record. An error, malformed JSON, failed generation, or a non-deliverable
    artifact returns None so the backstop NEVER invents delivery."""
    if is_error or not text:
        return None
    try:
        obj = json.loads(text)
    except Exception:
        return None
    if not isinstance(obj, dict) or obj.get("ok") is not True:
        return None
    path = obj.get("artifact")
    sha = obj.get("sha256")
    mime = obj.get("mime")
    nbytes = obj.get("bytes")
    # sha256 must be a real 64-hex SHA-256 digest (not any non-empty string):
    # identity collisions in the dedupe ledger must not be possible via a
    # malformed producer value (Hermes review finding 4).
    sha_ok = isinstance(sha, str) and bool(re.fullmatch(r"[0-9a-fA-F]{64}", sha))
    if not (isinstance(path, str) and path and sha_ok
            and isinstance(mime, str) and mime.lower().startswith("audio/")
            and isinstance(nbytes, int) and nbytes > 0):
        return None
    try:
        real = os.path.realpath(path)
        root = os.path.realpath(MEDIA_ROOT)
    except Exception:
        return None
    if not real.startswith(root + os.sep):
        return None
    return {"path": real, "sha256": sha.lower(), "mime": mime.lower(),
            "bytes": nbytes, "tx_id": obj.get("tx_id"),
            "ref": obj.get("ref")}


def _media_identity(media: dict) -> str:
    """Stable identity for exactly-once dedupe (content sha256 + path)."""
    return f"{media.get('sha256')}|{media.get('path')}"


def _media_delivered(state: dict, map_key: str, identity: str) -> bool:
    try:
        conv = (state.get("media_delivered", {}) or {}).get(map_key, {}) or {}
        return identity in conv
    except Exception:
        return False


def _media_mark_durable(state: dict, map_key: str, identity: str,
                        attempts: int = 3) -> bool:
    """Record a confirmed backstop artifact delivery in the persistent state
    (bounded per-conversation ledger inside the existing inbound-state.json).
    Exactly-once within this watcher/process lifetime is guaranteed by the
    in-memory mark regardless of persistence. The durable write makes restart
    suppression best-effort: if persistence fails after retries the mark is
    kept in memory and a CRITICAL residual is logged (a restart could then
    re-deliver only after a NEW turn reproduces an IDENTICAL artifact identity
    - music renders are non-deterministic, so this is a documented residual,
    mirroring the processed-window posture in dispatch_to_dsh)."""
    try:
        state.setdefault("media_delivered", {})
        conv = state["media_delivered"].setdefault(map_key, {})
        conv[identity] = int(time.time())
        # Bounded ledger. Eviction is safe because dedupe evidence only needs to
        # span the active watch window + any replayable queued turn inside it:
        # in-process duplicates are impossible (queue items are dropped at
        # delivery via _media_delivered re-check) and a post-eviction replay
        # would require a NEW turn reproducing an IDENTICAL artifact identity,
        # which non-deterministic renders cannot do (Hermes review finding 2).
        while len(conv) > MEDIA_DELIVERED_MAX:
            oldest = min(conv, key=conv.get)
            del conv[oldest]
        for i in range(attempts):
            if save_state(state):
                return True
            time.sleep(0.5 * (i + 1))
        log.critical("media delivered mark NOT durable for %s (identity %s) - "
                     "restart-replay residual documented", map_key, identity)
        return False
    except Exception:
        log.exception("media durable mark failed for %s", map_key)
        return False


def _send_files_from_args(arguments) -> list:
    """Absolute paths a Discord send_message call asked to attach (used to
    detect that a confirmed send already delivered the generated artifact)."""
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except Exception:
            return []
    if not isinstance(arguments, dict):
        return []
    files = arguments.get("files")
    if not isinstance(files, list):
        return []
    out = []
    for f in files:
        if isinstance(f, str) and f:
            try:
                out.append(os.path.realpath(f))
            except Exception:
                pass
    return out


def _media_pending(cur: dict) -> list:
    """Artifacts produced this turn whose exact path was NOT confirmed-delivered
    by a DSH send call (send outcome ok for a call that attached the file)."""
    delivered = set()
    for cid, outc in (cur.get("send_calls") or {}).items():
        if outc == "ok":
            for p in (cur.get("send_files") or {}).get(cid, []) or []:
                try:
                    delivered.add(os.path.realpath(p))
                except Exception:
                    pass
    pending = []
    for cid, m in (cur.get("media") or {}).items():
        try:
            if os.path.realpath(m["path"]) not in delivered:
                pending.append(m)
        except Exception:
            pending.append(m)
    return pending


def _fresh_turn_state() -> dict:
    """One accumulator per DSH turn. 'active' becomes True the moment any
    turn-owned content is observed, so a turn/start never finalizes an empty
    or not-yet-started accumulator (Hermes P1-4). v1.3 adds per-turn media
    artifact + send-file tracking and the turn/end error flag."""
    return {"buf": [], "send_calls": {}, "send_seen": False,
            "send_files": {}, "media_calls": set(), "media": {},
            "end_error": False, "active": False}


async def _deliver_channel(channel, text: str) -> int:
    """Send text in bounded chunks. Returns the number of chunks fully sent.
    0 == nothing sent (retryable later); 0 < sent < expected == partial
    (terminal, never resend from the start). Never raises."""
    sent = 0
    pieces = [text[i:i + _FALLBACK_CHUNK]
              for i in range(0, len(text), _FALLBACK_CHUNK)]
    pieces = [p for p in pieces if p.strip()]
    for piece in pieces:
        try:
            await channel.send(piece)
        except Exception as e:
            log.error("backstop channel send failed (chunk %d/%d): %s",
                      sent + 1, len(pieces), e.__class__.__name__)
            return sent
        sent += 1
        await asyncio.sleep(0.4)
    return sent


async def _deliver_media_file(channel, item: dict, map_key: str) -> bool:
    """Send one preserved artifact attachment (bare, no invented caption) to
    the originating channel. Returns True only when Discord accepted the
    message (send returned without exception). Never raises.

    Provenance is revalidated AT OPEN (Hermes review finding 3): the stored
    path is re-resolved and must still sit under realpath(MEDIA_ROOT); the
    final component is opened O_NOFOLLOW and must be a regular file, so a
    parse-time-to-delivery-time replacement or symlink swap cannot smuggle an
    out-of-root path to Discord."""
    path = item.get("file")
    if not path:
        return False
    try:
        root = os.path.realpath(MEDIA_ROOT)
        real = os.path.realpath(path)
        if not real.startswith(root + os.sep):
            log.error("backstop media path left approved root at delivery "
                      "(%s): refused", map_key)
            return False
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(real, os.O_RDONLY | nofollow)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                os.close(fd)
                log.error("backstop media path not a regular file at delivery "
                          "(%s): refused", map_key)
                return False
        except Exception:
            os.close(fd)
            raise
        with os.fdopen(fd, "rb") as fh:
            f = discord.File(fh, filename=os.path.basename(real))
            await channel.send(file=f)
        return True
    except Exception as e:
        log.error("backstop media send failed for %s (%s): %s",
                  map_key, os.path.basename(path), e.__class__.__name__)
        return False


async def _flush_turn(state: dict, cur: dict, map_key: str, channel,
                      queue: list) -> bool:
    """Make exactly ONE decision for a completed turn and enqueue at most one
    fallback text delivery AND at most one media-artifact delivery (v1.3).
    Suppress on confirmed success (no duplicate). Returns True when something
    was enqueued."""
    text = "".join(cur["buf"]).strip() if cur["buf"] else ""
    outcome = _reduce_send_outcomes(cur["send_calls"])
    # v1.3 media-success artifact backstop: a deliverable audio artifact was
    # produced this turn (generate_music ok) but was NOT confirmed-delivered by
    # a DSH send call. If the turn ended abnormally (error) or with an empty
    # final response, deterministically deliver the preserved artifact once;
    # otherwise the expensive successful generation silently produces nothing.
    pending = []
    if cur.get("media"):
        pending = _media_pending(cur)
        # Skip media auto-delivery when the model completed normally WITH final
        # text and made no send attempt: that is a live conversational reply
        # (recovery/nudge path exists); only abnormal/empty endings trigger.
        if pending and not cur.get("end_error") and text:
            pending = []
        # Cross-turn/restart exactly-once: drop identities the durable ledger
        # already records as delivered for this conversation.
        pending = [m for m in pending
                   if not _media_delivered(state, map_key, _media_identity(m))]
    if pending:
        if outcome == "failed":
            log.warning("backstop: DSH send FAILED with artifact pending for %s; "
                        "enqueue artifact once (honest retry)", map_key)
        else:
            log.info("backstop: artifact pending with %s for %s; enqueue "
                     "artifact once",
                     "abnormal turn end" if cur.get("end_error")
                     else "empty final response", map_key)
        for m in pending:
            queue.append({"file": m["path"],
                          "identity": _media_identity(m), "attempts": 0})
        # In a failed/unknown-send turn the model's final text (if any) is still
        # honest to deliver alongside the retried artifact (v1.2 semantics kept:
        # failed => text fallback; unknown + send_seen => one bounded text).
        if text and (outcome == "failed"
                     or (outcome is None and cur.get("send_seen"))):
            queue.append({"text": text, "attempts": 0})
        return True
    if text:
        if outcome == "ok":
            log.info("backstop: DSH send confirmed ok for %s - final text "
                     "suppressed (no duplicate)", map_key)
        elif outcome == "failed":
            log.info("backstop: DSH send FAILED for %s; enqueue final text once "
                     "(honest fallback)", map_key)
            queue.append({"text": text, "attempts": 0})
            return True
        elif cur["send_seen"]:
            log.warning("backstop: send attempted but outcome unknown for %s; "
                        "enqueue final text once (bounded)", map_key)
            queue.append({"text": text, "attempts": 0})
            return True
        else:
            log.info("backstop: enqueue final text for %s (DSH did not call "
                     "send tool)", map_key)
            queue.append({"text": text, "attempts": 0})
            return True
    else:
        if outcome == "failed":
            log.warning("backstop: DSH send FAILED with no final text for %s; "
                        "enqueue ONE honest failure notice", map_key)
            queue.append({"text": _FAILURE_NOTICE, "attempts": 0})
            return True
        elif outcome == "ok":
            log.info("backstop: DSH send confirmed ok with empty model tail for "
                     "%s - terminal, no action", map_key)
        elif cur["send_seen"]:
            log.warning("backstop: send attempted with no final text/outcome "
                        "for %s - nothing honest to deliver", map_key)
    return False


async def _drain_delivery_queue(state: dict, queue: list, channel,
                                map_key: str) -> None:
    """Attempt queued fallback deliveries. Exactly-once per finalized turn: a
    zero-chunk transport failure is retried up to _FALLBACK_MAX_ATTEMPTS; a
    partial send is terminal (never resend from the start); success clears.
    v1.3 media items (file attachments) follow the same bounded retry and are
    recorded in the durable per-conversation delivered ledger on confirmed
    success. Per-item exception-safe: an unexpected item error or cancellation
    preserves the untouched current+suffix so nothing is silently lost (Hermes
    P1-5)."""
    remaining = []
    i = 0
    try:
        while i < len(queue):
            item = queue[i]
            i += 1
            item["attempts"] += 1
            if "file" in item:
                # Exactly-once at DELIVERY time (not only enqueue time): two
                # queued turns in the same poll can both enqueue the same
                # artifact before the first delivery marks the ledger; re-check
                # before every send attempt so a later duplicate item is
                # dropped, never re-sent (design G).
                if item.get("identity") and _media_delivered(
                        state, map_key, item["identity"]):
                    continue
                try:
                    ok = await _deliver_media_file(channel, item, map_key)
                except asyncio.CancelledError:
                    remaining.append(item)
                    remaining.extend(queue[i:])
                    raise
                except Exception as e:
                    log.error("backstop media drain error for %s: %s",
                              map_key, e.__class__.__name__)
                    ok = False
                if ok:
                    if item.get("identity"):
                        _media_mark_durable(state, map_key, item["identity"])
                elif item["attempts"] < _FALLBACK_MAX_ATTEMPTS:
                    remaining.append(item)  # retry on a later poll
                else:
                    log.critical("backstop: media undelivered after %d attempts "
                                 "for %s - terminal (user sees nothing)",
                                 item["attempts"], map_key)
                continue
            try:
                sent = await _deliver_channel(channel, item["text"])
            except asyncio.CancelledError:
                # preserve current item + untouched suffix for an orderly end
                remaining.append(item)
                remaining.extend(queue[i:])
                raise
            except Exception as e:
                log.error("backstop fallback drain error for %s: %s",
                          map_key, e.__class__.__name__)
                sent = 0  # transport-ish failure: treat as zero-chunk retryable
            pieces = len([p for p in (item["text"][i2:i2 + _FALLBACK_CHUNK]
                                      for i2 in range(0, len(item["text"]),
                                                     _FALLBACK_CHUNK)) if p.strip()])
            if sent == 0 and item["attempts"] < _FALLBACK_MAX_ATTEMPTS:
                remaining.append(item)  # retry on a later poll
            elif sent == 0:
                log.critical("backstop: fallback undelivered after %d attempts "
                             "for %s - terminal (user sees nothing)",
                             item["attempts"], map_key)
            elif sent < pieces:
                log.critical("backstop: fallback PARTIAL (%d/%d chunks) for %s "
                             "- terminal, no resend from start", sent, pieces,
                             map_key)
    finally:
        queue[:] = remaining


async def _backstop_loop(state: dict, map_key: str, sid: str, channel,
                         watermark: int = 0) -> None:
    """Watch the DSH session for assistant final text that was NOT delivered by
    DSH's own mcp__discord__send_message call, and deliver it once per turn.
    Bounded; logs on give-up (never wedges the listener). v1.2 finalization
    delta: per-turn outcome tracking (ok/failed/unknown) with exactly-once
    honest fallback; a confirmed send + empty model tail stays terminal.
    v1.3: per-turn media-artifact tracking + artifact backstop (see module
    header); turn boundaries finalize on real turn/end and genuine user
    messages only (plugin/snapshot user/message events never split a turn)."""
    try:
        deadline = time.time() + REPLY_POLL_TIMEOUT_S
        last_seq = watermark
        cur = _fresh_turn_state()
        queue: list = []
        idle_cycles = 0
        while time.time() < deadline:
            try:
                evs = await asyncio.to_thread(_history_events, sid, 600)
            except Exception as e:
                log.error("backstop history read failed (%s): %s", map_key, e)
                await asyncio.sleep(REPLY_POLL_INTERVAL_S)
                continue
            max_seq = last_seq
            need_drain = False
            for e in evs:
                ev = e.get("event", {})
                seq = ev.get("seq") or 0
                if seq <= last_seq:
                    continue
                max_seq = max(max_seq, seq)
                typ = ev.get("type", "")
                data = ev.get("data", {}) or {}
                real_user = typ == "user/message" and _is_real_user_message(ev)
                if typ in ("assistant/chunk", "tool/call", "tool/result") or real_user:
                    # Any turn-owned content marks the accumulator active so a
                    # later boundary finalizes it (Hermes P1-4). Plugin/snapshot
                    # user/message events are NOT turn-owned content.
                    cur["active"] = True
                if typ == "assistant/chunk":
                    chunk = data.get("chunk", {}) or {}
                    if chunk.get("type") == "text-delta" and chunk.get("text"):
                        cur["buf"].append(chunk["text"])
                    elif chunk.get("type") == "tool-call-delta" and chunk.get("name"):
                        if _is_send_name(chunk["name"]):
                            cur["send_seen"] = True
                    elif chunk.get("type") == "block-end":
                        block = chunk.get("block") or {}
                        if block.get("type") == "tool-call":
                            if _is_send_name(block.get("name")):
                                cur["send_seen"] = True
                                if block.get("id"):
                                    cur["send_calls"].setdefault(block["id"])
                            elif _is_media_gen_name(block.get("name")) \
                                    and block.get("id"):
                                cur["media_calls"].add(block["id"])
                elif typ == "tool/call":
                    cid = data.get("callId")
                    if _is_send_name(data.get("name")):
                        cur["send_seen"] = True
                        if cid:
                            cur["send_calls"].setdefault(cid)
                            cur["send_files"][cid] = _send_files_from_args(
                                data.get("arguments"))
                    elif _is_media_gen_name(data.get("name")) and cid:
                        cur["media_calls"].add(cid)
                elif typ == "tool/result":
                    call_id, rtext, rerr = _extract_tool_result(ev)
                    # Only attribute a result whose callId we observed (Hermes
                    # P1-3 / P1-6 shape normalization).
                    if call_id and call_id in cur["send_calls"] \
                            and cur["send_calls"][call_id] is None:
                        cur["send_calls"][call_id] = _send_outcome_from_result(
                            rtext, rerr)
                    if call_id and call_id in cur["media_calls"] \
                            and call_id not in cur["media"]:
                        rec = _media_from_result(rtext, rerr)
                        if rec:
                            cur["media"][call_id] = rec
                elif typ == "turn/start":
                    # A new turn begins: finalize a prior turn ONLY if it was
                    # actually active (content seen since the last boundary).
                    if cur["active"]:
                        if await _flush_turn(state, cur, map_key, channel, queue):
                            need_drain = True
                        cur = _fresh_turn_state()
                elif typ == "turn/end":
                    # Record how the turn ended BEFORE finalizing (v1.3 media
                    # backstop needs abnormal vs normal completion).
                    reason = (data.get("reason") or {}).get("kind", "")
                    if reason and reason != "completed":
                        cur["end_error"] = True
                    if cur["active"]:
                        if await _flush_turn(state, cur, map_key, channel, queue):
                            need_drain = True
                        cur = _fresh_turn_state()
                elif real_user:
                    # A genuine queued user message finalizes the prior turn
                    # now; later events start a fresh accumulator. Plugin/
                    # snapshot user/message events never reach this branch.
                    if cur["active"]:
                        if await _flush_turn(state, cur, map_key, channel, queue):
                            need_drain = True
                        cur = _fresh_turn_state()
            if max_seq > last_seq:
                last_seq = max_seq
                idle_cycles = 0
            else:
                idle_cycles += 1
            if need_drain:
                await _drain_delivery_queue(state, queue, channel, map_key)
            elif idle_cycles >= 3:
                # Retry pending zero-chunk fallback on a later poll. Finalize a
                # quiet turn only when it has NO in-flight tool call awaiting
                # its result (v1.3): a music generation runs ~100s with no new
                # history events between tool/call and tool/result — an idle
                # flush must NOT reset the accumulator mid-turn or the later
                # tool/result could never be attributed to the media backstop.
                in_flight = bool(cur.get("send_calls")) or bool(
                    cur.get("media_calls"))
                if cur["active"] and not in_flight:
                    if await _flush_turn(state, cur, map_key, channel, queue):
                        need_drain = True
                    cur = _fresh_turn_state()
                if need_drain or queue:
                    await _drain_delivery_queue(state, queue, channel, map_key)
                idle_cycles = 0
            await asyncio.sleep(REPLY_POLL_INTERVAL_S)
    except Exception:
        log.exception("backstop loop crashed for %s", map_key)
    finally:
        with _WATCH_GUARD:
            _WATCHERS.pop(map_key, None)
        log.info("backstop watcher ended for %s", map_key)


def _history_events(sid: str, limit: int) -> list:
    resp = dsh_rpc("session.history",
                   {"sessionId": sid, "limit": limit}, timeout=90)
    return (resp.get("result", {}).get("value", {}) or {}).get("events", [])


def in_always_respond(message: discord.Message) -> bool:
    """True for any channel (or thread) inside the Kaem category."""
    ch = message.channel
    cat_id = getattr(ch, "category_id", None)
    if cat_id is None:  # threads: category lives on the parent channel
        parent = getattr(ch, "parent", None)
        cat_id = getattr(parent, "category_id", None) if parent is not None else None
    return cat_id == ALWAYS_RESPOND_CATEGORY_ID


def should_process(state: dict, message: discord.Message, bot_user_id: int) -> bool:
    if message.author.bot:
        return False  # ignore ALL bots incl. ourselves -> no loops
    if already_processed(state, message.id):
        return False
    is_dm = message.guild is None
    mentions_me = bot_user_id in (m.id for m in message.mentions)
    replies_to_me = bool(
        message.reference and message.reference.resolved
        and getattr(message.reference.resolved, "author", None)
        and message.reference.resolved.author.id == bot_user_id)
    return is_dm or mentions_me or replies_to_me or in_always_respond(message)


# --- discord client -----------------------------------------------------------


class DshInboundClient(discord.Client):
    def __init__(self, state: dict):
        global _S2_CLIENT
        intents = discord.Intents.none()
        intents.guilds = True            # guild/channel context
        intents.guild_messages = True    # guild MESSAGE_CREATE
        intents.dm_messages = True       # DMs
        intents.message_content = True   # request; auto-degrades if not granted
        super().__init__(intents=intents)
        self.state = state
        _S2_CLIENT = self

    async def setup_hook(self) -> None:
        # keep our own presence explicit; do not auto-sync anything
        log.info("gateway client initializing")

    async def on_ready(self):
        if self.user is None or self.user.id != EXPECTED_BOT_ID:
            log.critical("READY identity mismatch: %s — ABORTING (expected %s)",
                         getattr(self.user, "id", None), EXPECTED_BOT_ID)
            await self.close()
            os._exit(2)
        log.info("READY as %s (%s) — ONLINE; guilds=%d (dsh-discord-inbound v1.3)",
                 self.user, self.user.id, len(self.guilds))

    async def on_message(self, message: discord.Message):
        try:
            if not should_process(self.state, message, EXPECTED_BOT_ID):
                return
            log.info("inbound msg %s from %s in %s (dm=%s)",
                     message.id, message.author.id,
                     getattr(message.channel, "name", "dm"), message.guild is None)
            # dedupe advances ONLY on confirmed dispatch (inside dispatch_to_dsh):
            # a transient DSH/RPC failure must leave the message retryable.
            await dispatch_to_dsh(self.state, message)
        except Exception:
            log.exception("inbound handling failed for message %s", message.id)


def main() -> None:
    candidates = [
        Path("/run/credentials/dsh-discord-inbound.service/discord-bot-token"),
        Path("/run/credentials/dsh-discord.service/discord-bot-token"),
    ]
    token_file = next((c for c in candidates if candidate_exists(c)), None)
    if token_file is None:
        log.critical("no discord-bot-token credential found in %s", [str(c) for c in candidates])
        sys.exit(1)
    token = token_file.read_text().strip()
    if not token:
        log.critical("empty bot token credential")
        sys.exit(1)

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = load_state()
    client = DshInboundClient(state)
    try:
        client.run(token, log_handler=None)  # token never printed
    except discord.LoginFailure:
        log.critical("Discord rejected the bot token (401) — check staged credential")
        sys.exit(1)



def candidate_exists(p: Path) -> bool:
    try:
        return p.is_file()
    except Exception:
        return False


if __name__ == "__main__":
    main()
