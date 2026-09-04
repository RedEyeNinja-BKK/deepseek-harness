#!/usr/bin/env python3
"""dsh_line_inbound.py (v3) — minimal LINE Messaging API webhook for DSH.

Mirrors the proven dsh_discord_inbound.py architecture (operator GO 2026-09-03):
one narrow job — receive LINE inbound messages, normalize them, feed them into
DSH via the public session RPC (session.create / session.prompt), then send
DSH's reply back over LINE. DSH owns conversation, reasoning, and tools.

Platform facts (evidence, fetched 2026-09-03):
- Webhook signature: x-line-signature = base64(HMAC-SHA256(channel_secret,
  raw request body)) — LINE docs "Verify webhook signature".
- Reply tokens are short-lived (seconds); replies go via the Push Message API
  (POST /v2/bot/message/push, max 5 messages, X-Line-Retry-Key idempotent) —
  line-openapi messaging-api.yml.
- Text message limit 5000 chars; this adapter chunks at 4500 for margin.

Design constraints (operator directive 2026-09-03):
- stdlib only; no new runtime dependencies.
- No DSH core patch, no event bus, no second agent, no RBAC layer.
- Credentials ONLY via systemd LoadCredential; never logged or echoed.
- Trust gate: valid signature required; 1:1 chats always dispatch; group/room
  chats dispatch ONLY when the bot is mentioned.
- Failure isolation: this process dying must not affect dsh.service, the
  Discord lane, webgate, or schedules.

v3 (Hermes review round 2, run_93d4c4ad6bd44d76b34d50b5155b982c, all findings
applied): state persistence failure is fail-closed (STATE_UNAVAILABLE blocks
new dispatch; dir fsync); delivery reservation protocol (claim/finish under
STATE_LOCK, no network I/O under the lock) shared by webhook and retry-loop
paths; pending entries are compare-and-set (attempts never reset; eviction
archives full payloads for operator recovery); CredError during delivery
records pending with credential status; strict history/event schema validation
(single unambiguous marker match, contiguity required); duplicate/conflicting
Content-Length and any Transfer-Encoding rejected; queue capacity check +
reservation + executor submission atomic BEFORE the 200 ack (no post-ack
drop); bounded shutdown drain enforcing SHUTDOWN_GRACE_S; chunker hardened
(str input, boundary-tested 4500/22500/22501).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.server
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# --- constants ----------------------------------------------------------------

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 3087
WEBHOOK_PATH = "/line/webhook"
HEALTH_PATH = "/line/health"

LINE_API = "https://api.line.me/v2/bot"
DSH_API = "http://127.0.0.1:3080/api/"
DSH_CWD = "/opt/dsh/workspace"
CLIENT_TZ = "Asia/Bangkok"

TEXT_CHUNK_MAX = 4500          # LINE text limit 5000; margin for envelope safety
MAX_PUSH_MESSAGES = 5          # LINE push API hard limit
PUSH_TIMEOUT_S = 30
PUSH_MAX_ATTEMPTS = 2          # one 429 Retry-After retry within one delivery
PUSH_RETRY_BACKOFF_CAP_S = 30.0
REPLY_POLL_TIMEOUT_S = 240     # bounded DSH-turn wait (family chat scale)
REPLY_POLL_INTERVAL_S = 4.0
MAX_BODY_BYTES = 1 * 1024 * 1024
PROCESSED_WINDOW = 1000        # bounded dedupe window (replay-safe)
PENDING_RETRY_INTERVAL_S = 30
PENDING_MAX_ATTEMPTS = 3
MAX_QUEUE_DEPTH = 64           # bounded work queue; overflow drops with evidence
SHUTDOWN_GRACE_S = 300
INFLIGHT_STALE_S = 600         # crashed claim recovery threshold

STATE_DIR_ENV = os.environ.get("STATE_DIRECTORY")
STATE_DIR = Path(STATE_DIR_ENV.split(":")[0]) if STATE_DIR_ENV else Path("/var/lib/dsh-line-inbound")
STATE_PATH = STATE_DIR / "line-state.json"

PROFILE_TTL_S = 7 * 24 * 3600

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("dsh-line-inbound")

# --- credentials (LoadCredential only) ----------------------------------------


class CredError(Exception):
    pass


def _read_credential(name: str) -> str:
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if not cred_dir:
        raise CredError(f"CREDENTIALS_DIRECTORY not set ({name} unavailable)")
    path = Path(cred_dir) / name
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise CredError(f"credential unreadable: {name}") from exc
    if not value:
        raise CredError(f"credential empty: {name}")
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in value):
        raise CredError(f"credential contains control characters: {name}")
    return value


def channel_secret() -> str:
    return _read_credential("channel-secret")


def channel_token() -> str:
    return _read_credential("channel-access-token")


# --- state (persisted, restart-safe; ALL access under STATE_LOCK) --------------

STATE_LOCK = threading.RLock()
_CONV_LOCKS_GUARD = threading.Lock()
_CONV_LOCKS: dict[str, threading.Lock] = {}
# fail-closed flag: once a state persistence write fails, NEW dispatches stop
# (LINE redelivery replays safely); in-flight work may finish.
STATE_UNAVAILABLE = threading.Event()


def conversation_lock(key: str) -> threading.Lock:
    """One lock per LINE conversation: serializes prompt->reply->push so
    messages in one conversation are handled in order and never share a turn."""
    with _CONV_LOCKS_GUARD:
        if key not in _CONV_LOCKS:
            if len(_CONV_LOCKS) > 512:  # bounded; family scale
                _CONV_LOCKS.clear()
            _CONV_LOCKS[key] = threading.Lock()
        return _CONV_LOCKS[key]


def load_state() -> dict:
    try:
        state = json.loads(STATE_PATH.read_text())
        if isinstance(state, dict):
            state.setdefault("sessions", {})
            state.setdefault("accepted", [])
            state.setdefault("delivered", [])
            state.setdefault("failed", [])
            state.setdefault("pending", {})
            state.setdefault("profiles", {})
            # schema-normalize pending entries (malformed claim fields would
            # otherwise strand a claim: treated as not-in-flight, re-durable)
            for mid, e in list(state.get("pending", {}).items()):
                if not isinstance(e, dict):
                    state["pending"].pop(mid, None)
                    continue
                if not isinstance(e.get("inFlight"), bool):
                    e["inFlight"] = False
                if not isinstance(e.get("inFlightAt"), (int, float)):
                    e["inFlightAt"] = 0
                if not isinstance(e.get("attempts"), int):
                    e["attempts"] = 0
            return state
    except Exception:
        pass
    return {"sessions": {}, "accepted": [], "delivered": [], "failed": [],
            "pending": {}, "profiles": {}}


def save_state(state: dict) -> bool:
    """Caller must hold STATE_LOCK. Atomic tmp+fsync+dir-fsync+replace.
    Returns False on ANY persistence failure (transition callers treat that
    as a hard failure — see STATE_UNAVAILABLE)."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_DIR / f"line-state.tmp.{os.getpid()}.{threading.get_ident()}"
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
    except OSError:
        log.error("state save FAILED — durable state may be stale")
        return False


def mark_accepted(state: dict, message_id: str) -> bool:
    """Dedupe advances ONLY on confirmed DSH acceptance (prompt accepted).
    Persistence failure -> STATE_UNAVAILABLE set (fail-closed for new work)."""
    state.setdefault("accepted", []).append(str(message_id))
    state["accepted"] = state["accepted"][-PROCESSED_WINDOW:]
    ok = save_state(state)
    if not ok:
        STATE_UNAVAILABLE.set()
        log.critical("state persistence failed at mark_accepted — new dispatches "
                     "blocked until restart (fail-closed)")
    return ok


def has_pending(state: dict, message_id: str) -> dict | None:
    return state.get("pending", {}).get(str(message_id))


def record_pending(state: dict, message_id: str, target_id: str, reply: str) -> bool:
    """Durable pending-delivery entry. Compare-and-set semantics: an existing
    entry is preserved (attempts never reset); conflicting target/reply for
    the same message id is logged and ignored. Returns False when the entry
    could NOT be persisted (STATE_UNAVAILABLE set; caller must not push)."""
    pending = state.setdefault("pending", {})
    existing = pending.get(str(message_id))
    if existing:
        if existing.get("to") != target_id or existing.get("reply") != reply:
            log.error("pending conflict for msg %s — keeping existing entry "
                      "(attempts=%d)", message_id, existing.get("attempts", 0))
        return save_state(state)
    pending[str(message_id)] = {"to": target_id, "reply": reply,
                                "attempts": 0, "inFlight": False,
                                "inFlightAt": 0, "queuedAt": time.time()}
    ids = list(pending.keys())
    if len(ids) > 100:  # bounded: evict oldest INTO durable payload archive
        archive = state.setdefault("failed", [])
        for old in ids[:-100]:
            entry = pending.pop(old)
            entry["evictedAt"] = time.time()
            archive.append({"message_id": old, **entry})
            log.critical("pending overflow: msg %s archived UNDELIVERED to "
                         "failed list — operator recovery required", old)
    state["failed"] = state.get("failed", [])[-PROCESSED_WINDOW:]
    if not save_state(state):
        pending.pop(str(message_id), None)  # do not leave un-durable state
        STATE_UNAVAILABLE.set()
        log.critical("pending record for msg %s NOT persisted — delivery "
                     "refused (fail-closed)", message_id)
        return False
    return True


def claim_delivery(state: dict, message_id: str) -> dict | None:
    """Atomically reserve a pending entry for delivery. Returns a COPY of the
    entry if claimed, None if absent / claimed-elsewhere / claim not
    persistable. Persistence failure -> fail-closed (STATE_UNAVAILABLE set,
    no push): an un-durable claim would allow a duplicate push after restart.
    A claim persisted but then crashed-over is recovered after
    INFLIGHT_STALE_S by recover_stale_claims()."""
    with STATE_LOCK:
        entry = state.get("pending", {}).get(str(message_id))
        if not entry:
            return None
        if entry.get("inFlight") and \
                time.time() - entry.get("inFlightAt", 0) <= INFLIGHT_STALE_S:
            return None
        entry["inFlight"] = True
        entry["inFlightAt"] = time.time()
        if not save_state(state):
            entry["inFlight"] = False
            entry["inFlightAt"] = 0
            STATE_UNAVAILABLE.set()
            log.critical("claim for msg %s NOT persisted — delivery refused "
                         "(fail-closed)", message_id)
            return None
        return dict(entry)


def finish_delivery(state: dict, message_id: str, success: bool) -> bool:
    """Release the claim; on success move to delivered, else bump attempts.
    Returns True only when the transition is durably persisted. On save
    failure: STATE_UNAVAILABLE is set, memory is restored to the last known
    durable claim state, and recovery evidence is logged. Success-path note:
    the push DID happen; LINE's X-Line-Retry-Key (deterministic per message
    id) dedupes any post-restart re-push."""
    with STATE_LOCK:
        entry = state.get("pending", {}).get(str(message_id))
        if success:
            if entry is not None:
                state.get("pending", {}).pop(str(message_id), None)
            state.setdefault("delivered", []).append(str(message_id))
            state["delivered"] = state["delivered"][-PROCESSED_WINDOW:]
            if not save_state(state):
                # restore memory to the last known durable claim state
                if entry is not None:
                    state["pending"][str(message_id)] = entry
                STATE_UNAVAILABLE.set()
                log.critical("delivery of msg %s succeeded but state NOT "
                             "persisted — OUTCOME UNCERTAIN, recovery "
                             "required (LINE retry-key dedupes re-push)",
                             message_id)
                return False
            return True
        elif entry:
            prior_attempts = entry.get("attempts", 0)
            entry["inFlight"] = False
            entry["inFlightAt"] = 0
            entry["attempts"] = prior_attempts + 1
            if not save_state(state):
                # restore memory to the last known durable claim state
                entry["inFlight"] = True
                entry["attempts"] = prior_attempts
                STATE_UNAVAILABLE.set()
                log.critical("failure state for msg %s NOT persisted — "
                             "claim restored to durable in-flight state, "
                             "recovery required", message_id)
                return False
            return True
        return True


def recover_stale_claims(state: dict) -> int:
    """Clear inFlight claims older than INFLIGHT_STALE_S (crash after claim,
    before finish). Called at startup and periodically by the retry loop.
    Returns the number of recovered entries."""
    now = time.time()
    with STATE_LOCK:
        recovered = 0
        for mid, e in state.get("pending", {}).items():
            if e.get("inFlight") and now - e.get("inFlightAt", 0) > INFLIGHT_STALE_S:
                e["inFlight"] = False
                e["inFlightAt"] = 0
                recovered += 1
                log.warning("recovered stale delivery claim for msg %s", mid)
        if recovered:
            if not save_state(state):
                STATE_UNAVAILABLE.set()
        return recovered


# --- LINE API client (sanitized logging: class + label + status only) ----------


class LineHttpError(Exception):
    def __init__(self, label: str, status: int | None, retry_after: float | None = None):
        super().__init__(f"LINE {label} failed ({status if status is not None else 'transport'})")
        self.label = label
        self.status = status
        self.retry_after = retry_after


def line_request(method: str, path: str, *, json_body=None, extra_headers=None,
                 label: str = "", timeout: float = PUSH_TIMEOUT_S):
    """One LINE API call. Returns parsed JSON on 2xx. Non-2xx/transport ->
    LineHttpError with sanitized info only (never headers/body/token)."""
    label = label or path
    headers = {"Authorization": f"Bearer {channel_token()}",
               "Content-Type": "application/json",
               "User-Agent": "dsh-line-inbound (localclaw-vm, 1.0)"}
    body = json.dumps(json_body).encode() if json_body is not None else None
    req = urllib.request.Request(LINE_API + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        # headers/body deliberately discarded except the sanitized Retry-After
        # value used for 429 backoff
        retry_after = None
        try:
            retry_after = min(60.0, max(1.0, float(e.headers.get("Retry-After", ""))))
        except (ValueError, TypeError, AttributeError):
            retry_after = None
        raise LineHttpError(label, e.code, retry_after) from None
    except OSError as e:
        raise LineHttpError(label, None) from e


def push_messages(to_id: str, texts: list[str], retry_key_seed: str):
    """Send text messages via Push API with 429 Retry-After-aware bounded retry.
    texts must already be chunked via chunk_reply_text(). Idempotent per
    message via X-Line-Retry-Key (LINE dedupes redelivered pushes)."""
    msgs = [{"type": "text", "text": t} for t in texts]
    retry_key = str(uuid.uuid5(uuid.NAMESPACE_URL, f"dsh-line:{retry_key_seed}"))
    for attempt in range(1, PUSH_MAX_ATTEMPTS + 1):
        try:
            return line_request("POST", "/message/push", label="push",
                                json_body={"to": to_id, "messages": msgs},
                                extra_headers={"X-Line-Retry-Key": retry_key})
        except LineHttpError as e:
            if e.status == 429 and attempt < PUSH_MAX_ATTEMPTS:
                delay = e.retry_after if e.retry_after else 5.0 * attempt
                time.sleep(min(PUSH_RETRY_BACKOFF_CAP_S, delay))
                continue
            raise


def build_profile_path(source_type: str, group_id: str | None, user_id: str) -> str:
    """1:1 -> /profile/{userId}; group -> /group/{groupId}/member/{userId}."""
    if source_type == "group" and group_id:
        return f"/group/{group_id}/member/{user_id}"
    return f"/profile/{user_id}"


def get_display_name(source_type: str, group_id: str | None, user_id: str,
                     state: dict) -> str | None:
    """Best-effort display name, cached per (group,user) with 7-day TTL.
    Failures are non-fatal — the envelope simply omits the name."""
    cache = state.setdefault("profiles", {})
    cache_key = f"{group_id}:{user_id}" if group_id else user_id
    entry = cache.get(cache_key)
    now = time.time()
    if entry and now - entry.get("fetchedAt", 0) < PROFILE_TTL_S:
        return entry.get("displayName")
    try:
        data = line_request("GET", build_profile_path(source_type, group_id, user_id),
                            label="profile")
        name = (data or {}).get("displayName")
        if name:
            # profile cache mutation+save under STATE_LOCK (shared state file;
            # non-durable semantics: save failure just reverts the entry)
            with STATE_LOCK:
                cache[cache_key] = {"displayName": name, "fetchedAt": now}
                if not save_state(state):
                    log.warning("profile cache not persisted (non-durable cache)")
                    cache.pop(cache_key, None)
        return name
    except (LineHttpError, CredError) as e:
        log.info("profile fetch failed (%s) — non-fatal", e)
        return entry.get("displayName") if entry else None


# --- DSH RPC -------------------------------------------------------------------


def dsh_rpc(method: str, payload: dict, timeout: float = 60.0) -> dict:
    body = json.dumps({"type": "client-request",
                       "rpcId": f"line-{time.time_ns()}",
                       "method": method, "payload": payload}).encode()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(DSH_API + method, data=body,
                                 headers={"Content-Type": "application/json",
                                          "Host": "127.0.0.1:3080"})
    with opener.open(req, timeout=timeout) as r:
        resp = json.loads(r.read())
    if resp.get("result", {}).get("ok") is False:
        raise RuntimeError("dsh rpc error: "
                           + json.dumps(resp.get("result", {}).get("error") or {}, default=str)[:200])
    return resp


def get_session_entry(session_id: str) -> dict | None:
    resp = dsh_rpc("session.list", {})
    value = resp.get("result", {}).get("value") or {}
    items = value.get("items", []) if isinstance(value, dict) else (value or [])
    if not isinstance(items, list):
        raise RuntimeError("session.list schema drift: items not a list")
    for it in items:
        if not isinstance(it, dict):
            continue
        if (it.get("sessionId") or it.get("id")) == session_id:
            # field-type validation (schema drift -> explicit failure)
            if not isinstance(it.get("running", False), bool):
                raise RuntimeError("session.list schema drift: running not bool")
            return it
    return None


def get_or_create_session(state: dict, map_key: str) -> str | None:
    with STATE_LOCK:
        sid = state.get("sessions", {}).get(map_key)
    if sid:
        try:
            if get_session_entry(sid) is not None:
                return sid
        except Exception:
            log.error("session.list failed checking %s — treating as absent", sid)
    try:
        resp = dsh_rpc("session.create", {"cwd": DSH_CWD})
        new_sid = resp["result"]["value"]["sessionId"]
        if not isinstance(new_sid, str) or not new_sid:
            raise RuntimeError("session.create schema drift: sessionId not string")
    except Exception:
        log.exception("session.create failed for %s", map_key)
        return None
    with STATE_LOCK:
        state.setdefault("sessions", {})[map_key] = new_sid
        if not save_state(state):
            # un-durable session map breaks conversation continuity -> fail
            # the event (redelivery-safe) rather than prompt an unmapped session
            state.get("sessions", {}).pop(map_key, None)
            STATE_UNAVAILABLE.set()
            log.critical("session map for %s NOT persisted — event refused "
                         "(fail-closed)", map_key)
            return None
    log.info("created DSH session %s for %s", new_sid, map_key)
    return new_sid


# --- reply extraction (dispatch-marker identified, strictly validated) ----------


TOOL_BLOCK_KINDS = {"tool-call"}   # observed DSH scaffold block kind (live-proven)

def extract_reply_for_dispatch(events: list, dispatch_id: str) -> str | None:
    """Locate the user/message event carrying our dispatch marker; require
    exactly one match; require its turn's turn/end; collect only assistant
    text blocks of that turn appearing AFTER the user event. Schema drift or
    ambiguity -> None (explicitly logged by the caller)."""
    if not isinstance(events, list):
        return None
    matches: list[tuple[int, int]] = []  # (index, turn)
    for idx, e in enumerate(events):
        if not isinstance(e, dict):
            continue
        ev = e.get("event")
        if not isinstance(ev, dict) or ev.get("type") != "user/message":
            continue
        data = ev.get("data") or {}
        for block in (data.get("content") or []):
            if (isinstance(block, dict) and block.get("type") == "text"
                    and isinstance(block.get("text"), str)
                    and dispatch_id in block["text"]):
                matches.append((idx, data.get("turn")))
                break
    if len(matches) != 1:
        return None
    user_idx, our_turn = matches[0]
    if our_turn is None:
        # Live DSH history omits "turn" on user/message events. Infer it from
        # the governing turn/start: prefer the last turn/start at or before
        # the user event (live ordering), else the first turn/start after it
        # before any turn/end. Still unknown -> fail closed (None).
        for e in reversed(events[:user_idx + 1]):
            if isinstance(e, dict) and isinstance(e.get("event"), dict) \
                    and e["event"].get("type") == "turn/start":
                our_turn = (e["event"].get("data") or {}).get("turn")
                break
        if our_turn is None:
            for e in events[user_idx + 1:]:
                if not isinstance(e, dict) or not isinstance(e.get("event"), dict):
                    continue
                etype = e["event"].get("type")
                if etype == "turn/end":
                    break
                if etype == "turn/start":
                    our_turn = (e["event"].get("data") or {}).get("turn")
                    break
        if our_turn is None:
            return None
    finished = False
    final_blocks = None
    for e in events[user_idx + 1:]:
        if not isinstance(e, dict):
            continue
        ev = e.get("event")
        if not isinstance(ev, dict):
            continue
        data = ev.get("data") or {}
        etype = ev.get("type")
        if etype == "assistant/message" and data.get("turn") == our_turn:
            # F3: only the LAST assistant/message of the turn is the canonical
            # final response (live-proven contract; intermediates are scaffold).
            final_blocks = (data.get("message") or {}).get("content")
        elif etype == "turn/end" and data.get("turn") == our_turn:
            finished = True
            break
    if not finished:
        return None
    if not isinstance(final_blocks, list):
        return None  # no assistant message in turn -> fail closed
    if any(isinstance(b, dict) and b.get("type") in TOOL_BLOCK_KINDS
           for b in final_blocks):
        return None  # turn ended on tool-call scaffold -> fail closed
    texts = [b["text"] for b in final_blocks
             if isinstance(b, dict) and b.get("type") == "text"
             and isinstance(b.get("text"), str) and b["text"].strip()]
    reply = "\n".join(texts)
    return reply.strip() or None


def wait_for_reply(session_id: str, dispatch_id: str,
                   dispatched_at_ms: int) -> str | None:
    """Bounded poll: wait until the session is no longer running AND has been
    updated after our dispatch, then fetch history ONCE and extract our turn.
    Schema drift at any step -> None (evidence logged)."""
    deadline = time.time() + REPLY_POLL_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(REPLY_POLL_INTERVAL_S)
        try:
            entry = get_session_entry(session_id)
        except Exception as e:
            log.error("session.list failed while polling %s: %s",
                      session_id, e.__class__.__name__)
            continue
        if entry is None:
            log.error("session %s vanished during poll", session_id)
            return None
        if entry.get("running"):
            continue
        if entry.get("updatedAt", 0) < dispatched_at_ms:
            continue
        try:
            resp = dsh_rpc("session.history", {"sessionId": session_id})
            value = resp.get("result", {}).get("value")
            events = (value or {}).get("events") if isinstance(value, dict) else None
            if not isinstance(events, list):
                log.error("history schema drift for %s (events not a list)", session_id)
                return None
            return extract_reply_for_dispatch(events, dispatch_id)
        except Exception as e:
            log.error("history fetch failed for %s: %s", session_id, e.__class__.__name__)
            return None
    log.warning("reply poll timed out after %ss (session %s, dispatch %s)",
                REPLY_POLL_TIMEOUT_S, session_id, dispatch_id)
    return None


# --- delivery -------------------------------------------------------------------


def chunk_reply_text(reply: str) -> list[str]:
    """Bounded chunker: <=MAX_PUSH_MESSAGES pieces, each <=TEXT_CHUNK_MAX;
    overflow truncates the final piece with an explicit notice. Non-string
    input returns []."""
    if not isinstance(reply, str) or not reply:
        return []
    n = max(1, -(-len(reply) // TEXT_CHUNK_MAX))
    if n > MAX_PUSH_MESSAGES:
        n = MAX_PUSH_MESSAGES
    base = min(-(-len(reply) // n), TEXT_CHUNK_MAX)
    out = [reply[i * base:(i + 1) * base] for i in range(n)]
    out = [c for c in out if c]
    if not out:
        return []
    if len(reply) > n * base:
        suffix = "\n\n…(reply truncated)"
        out[-1] = out[-1][:TEXT_CHUNK_MAX - len(suffix)] + suffix
    return out


def _push_claimed(claim: dict, message_id: str) -> None:
    """Perform the push for a claimed delivery. Raises on failure."""
    push_messages(claim["to"], chunk_reply_text(claim["reply"]),
                  retry_key_seed=message_id)


def deliver_reply(state: dict, message_id: str, target_id: str, reply: str) -> bool:
    """Push reply over LINE via the reservation protocol. Returns True on
    success. On failure (HTTP or credential) records/keeps a durable pending
    entry for bounded background retry. Persistence failures anywhere in the
    chain fail closed (no push; STATE_UNAVAILABLE)."""
    with STATE_LOCK:
        if not record_pending(state, message_id, target_id, reply):
            return False  # fail-closed (already logged)
    claim = claim_delivery(state, message_id)  # lock-free; no I/O under lock
    if not claim:
        return False
    try:
        _push_claimed(claim, message_id)
    except (LineHttpError, CredError) as e:
        log.error("push failed for msg %s: %s (queued for bounded retry)",
                  message_id, e)
        if not finish_delivery(state, message_id, success=False):
            log.critical("msg %s delivery outcome UNCERTAIN — recovery required",
                         message_id)
        return False
    if not finish_delivery(state, message_id, success=True):
        log.critical("msg %s pushed but delivered-state NOT persisted — "
                     "OUTCOME UNCERTAIN (LINE retry-key dedupes re-push)",
                     message_id)
        return False
    log.info("pushed reply for msg %s -> %s", message_id, target_id)
    return True


def pending_retry_loop(state: dict, stop_event: threading.Event) -> None:
    """Bounded background retry of undelivered replies (no re-prompt of DSH;
    X-Line-Retry-Key makes redelivered pushes idempotent at LINE). Uses the
    same claim/finish reservation as the webhook path (claim_delivery itself
    recovers claims stale past INFLIGHT_STALE_S) — never concurrent."""
    while not stop_event.wait(PENDING_RETRY_INTERVAL_S):
        if stop_event.is_set() or STATE_UNAVAILABLE.is_set():
            return
        # sweep: archive exhausted entries (durable; restore + fail-closed on
        # persistence failure rather than claiming a false archive)
        with STATE_LOCK:
            for mid, e in list(state.get("pending", {}).items()):
                if e.get("attempts", 0) < PENDING_MAX_ATTEMPTS:
                    continue
                state.get("pending", {}).pop(mid, None)
                archive = state.setdefault("failed", [])
                e["exhaustedAt"] = time.time()
                archive.append({"message_id": mid, **e})
                state["failed"] = state.get("failed", [])[-PROCESSED_WINDOW:]
                if not save_state(state):
                    state.get("pending", {})[mid] = e  # restore (memory=durable)
                    STATE_UNAVAILABLE.set()
                    log.critical("exhaustion archive for msg %s NOT persisted "
                                 "— OUTCOME UNCERTAIN, recovery required", mid)
                else:
                    log.critical("pending reply for msg %s exhausted %d "
                                 "attempts — ARCHIVED UNDELIVERED, operator "
                                 "recovery required", mid, PENDING_MAX_ATTEMPTS)
        # retry remaining entries via the shared reservation protocol
        with STATE_LOCK:
            ids = list(state.get("pending", {}).keys())
        for message_id in ids:
            if stop_event.is_set() or STATE_UNAVAILABLE.is_set():
                return
            claim = claim_delivery(state, message_id)  # no I/O under lock
            if not claim:
                continue
            try:
                _push_claimed(claim, message_id)
            except (LineHttpError, CredError) as e:
                log.warning("pending retry failed for msg %s: %s", message_id, e)
                if not finish_delivery(state, message_id, success=False):
                    log.critical("msg %s retry outcome UNCERTAIN — recovery "
                                 "required", message_id)
                continue
            if not finish_delivery(state, message_id, success=True):
                log.critical("msg %s pushed by retry but state NOT persisted — "
                             "OUTCOME UNCERTAIN", message_id)
                continue
            log.info("pending retry delivered msg %s", message_id)


# --- dispatch -------------------------------------------------------------------


def build_envelope(source_type: str, target_id: str, display_name: str | None,
                   message: dict, event: dict, dispatch_id: str) -> str:
    ctx = {
        "platform": "line",
        "line_source_type": source_type,
        "line_source_id": target_id,
        "line_user_id": (event.get("source") or {}).get("userId"),
        "line_user_name": display_name,
        "message_id": message.get("id"),
        "message_type": message.get("type"),
        "content_length": len(message.get("text") or ""),
        "is_redelivery": bool((event.get("deliveryContext") or {}).get("isRedelivery")),
        "dispatch_id": dispatch_id,
    }
    text = message.get("text") or ""
    if message.get("type") == "sticker":
        text = "(sent a sticker)"
    elif message.get("type") not in ("text", "sticker"):
        text = f"(sent a {message.get('type')} — media handling not enabled yet)"
    return f"[line message] {json.dumps(ctx, ensure_ascii=False)}\n{text}"


def should_dispatch(event: dict) -> tuple[bool, str | None, str]:
    """Trust/interest gate. Returns (dispatch, target_id, source_type)."""
    if event.get("mode") != "active" or event.get("type") != "message":
        return False, None, ""
    source = event.get("source") or {}
    source_type = source.get("type")
    if source_type == "user":
        target = source.get("userId")
        return (bool(target), target, source_type) if target else (False, None, source_type)
    if source_type in ("group", "room"):
        target = source.get("groupId") or source.get("roomId")
        if not target:
            return False, None, source_type
        message = event.get("message") or {}
        mentionees = ((message.get("mention") or {}).get("mentionees")) or []
        mentioned = any(m.get("isSelf") or m.get("type") == "all" for m in mentionees)
        return (mentioned, target, source_type)
    return False, None, source_type or ""


def handle_event(state: dict, event: dict) -> None:
    if STATE_UNAVAILABLE.is_set():
        log.error("state persistence unavailable — event DROPPED for redelivery "
                  "(fail-closed)")
        return
    dispatch, target_id, source_type = should_dispatch(event)
    if not dispatch:
        log.info("skipped event (type=%s mode=%s source=%s mention-gated)",
                 event.get("type"), event.get("mode"), source_type)
        return
    message = event.get("message") or {}
    message_id = str(message.get("id") or "")
    if not message_id:
        return

    conv_key = f"{source_type}:{target_id}"
    with conversation_lock(conv_key):
        with STATE_LOCK:
            if message_id in state.get("delivered", []):
                log.info("dedupe skip (delivered): message %s", message_id)
                return
            if message_id in state.get("accepted", []):
                log.info("dedupe skip (accepted): message %s", message_id)
                return

        # accepted-but-undelivered message on redelivery: retry delivery only
        with STATE_LOCK:
            pending = has_pending(state, message_id)
        if pending and not pending.get("inFlight"):
            log.info("redelivery of undelivered msg %s — retrying push", message_id)
            deliver_reply(state, message_id, target_id, pending["reply"])
            return
        if pending:  # in flight elsewhere
            return

        user_id = (event.get("source") or {}).get("userId") or target_id
        group_id = (event.get("source") or {}).get("groupId") \
            if source_type == "group" else None
        display_name = get_display_name(source_type, group_id, user_id, state)

        sid = get_or_create_session(state, conv_key)
        if not sid:
            log.error("no DSH session for %s; message %s left UNMARKED",
                      conv_key, message_id)
            return

        dispatch_id = uuid.uuid4().hex
        envelope = build_envelope(source_type, target_id, display_name,
                                  message, event, dispatch_id)
        dispatched_at_ms = int(time.time() * 1000)
        try:
            dsh_rpc("session.prompt", {
                "sessionId": sid, "mode": "queue",
                "content": [{"type": "text", "text": envelope}],
                "clientTimeZone": CLIENT_TZ})
        except Exception as e:
            log.error("session.prompt failed for message %s (session %s): %s — "
                      "left UNMARKED (retryable on redelivery)",
                      message_id, sid, e.__class__.__name__)
            return
        if not mark_accepted(state, message_id):
            log.critical("msg %s prompted but acceptance state NOT persisted — "
                         "fail-closed engaged; reply delivery aborted for safety",
                         message_id)
            return
        log.info("dispatched line msg %s to session %s (dispatch %s)",
                 message_id, sid, dispatch_id)

        reply = wait_for_reply(sid, dispatch_id, dispatched_at_ms)
        if not reply:
            log.error("no reply extracted for line msg %s (session %s, "
                      "dispatch %s) — NOT retrying automatically",
                      message_id, sid, dispatch_id)
            return
        deliver_reply(state, message_id, target_id, reply)


# --- signature verification -----------------------------------------------------


def verify_signature(raw_body: bytes, header_signature: str | None, secret: str) -> bool:
    if not header_signature:
        return False
    mac = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256)
    expected = base64.b64encode(mac.digest()).decode("utf-8")
    return hmac.compare_digest(expected, header_signature)


# --- HTTP server -----------------------------------------------------------------


class WebhookHandler(http.server.BaseHTTPRequestHandler):
    server_version = "dsh-line-inbound/1.2"
    state: dict = {}
    executor: ThreadPoolExecutor | None = None
    queue_depth = 0
    shutting_down = False
    queue_guard = threading.Lock()

    def log_message(self, fmt, *args):
        log.info("http %s", fmt % args)

    def _reject(self, code: int):
        self.send_response(code)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def do_GET(self):
        split = urllib.parse.urlsplit(self.path)
        if split.query:
            self._reject(400)
            return
        if split.path == HEALTH_PATH:
            body = json.dumps({"ok": True, "service": "dsh-line-inbound"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._reject(404)

    def do_POST(self):
        split = urllib.parse.urlsplit(self.path)
        if split.query or split.path != WEBHOOK_PATH:
            self._reject(404)
            return
        if self.headers.get("Transfer-Encoding") is not None:
            # ANY transfer-encoding header is rejected (framing ambiguity)
            self._reject(411)
            return
        lengths = self.headers.get_all("Content-Length") or []
        if len(set(lengths)) != 1:
            # missing, duplicated-with-conflict, or duplicated-nonidentical
            self._reject(400)
            return
        try:
            length = int(lengths[0])
            assert length >= 0
        except (ValueError, AssertionError):
            self._reject(400)
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self._reject(413 if length > MAX_BODY_BYTES else 400)
            return
        raw_body = self.rfile.read(length)
        if len(raw_body) != length:  # short read: framing broken
            self._reject(400)
            return
        # reject BEFORE processing so LINE redelivers later
        with type(self).queue_guard:
            if type(self).shutting_down:
                self._reject(503)
                return
        try:
            secret = channel_secret()
        except CredError as exc:
            log.critical("credential unavailable: %s", exc)
            self._reject(500)
            return
        if not verify_signature(raw_body, self.headers.get("x-line-signature"), secret):
            log.warning("signature verification FAILED (path=%s)", split.path)
            self._reject(403)
            return
        try:
            payload = json.loads(raw_body)
            events = payload.get("events", []) if isinstance(payload, dict) else []
        except ValueError:
            log.warning("invalid JSON after signature pass (should not happen)")
            self._reject(400)
            return
        # reserve capacity + submit atomically BEFORE the 200 ack
        # NOTE: counter lives on the CLASS — `self.queue_depth += 1` would
        # create a per-instance shadow (proven bug: worker decremented the
        # class counter to -1 while the drain loop watched it forever)
        with self.queue_guard:
            cls = type(self)
            if cls.shutting_down or cls.executor is None:
                self._reject(503)
                return
            if cls.queue_depth >= MAX_QUEUE_DEPTH:
                log.error("work queue full (%d) — batch REJECTED for redelivery",
                          cls.queue_depth)
                self._reject(503)
                return
            # reserve FIRST (worker may finish an empty batch faster than this
            # handler thread increments — decrement-before-increment race)
            cls.queue_depth += 1
            try:
                cls.executor.submit(self._drain_batch, events)
            except RuntimeError:
                cls.queue_depth -= 1
                self._reject(503)
                log.error("executor rejecting work (draining) — batch rejected 503")
                return
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    @classmethod
    def _drain_batch(cls, events: list):
        try:
            for event in events:
                if isinstance(event, dict):
                    try:
                        handle_event(cls.state, event)
                    except Exception as e:
                        log.error("event handling failed (isolated): %s",
                                  e.__class__.__name__)
        finally:
            with cls.queue_guard:
                cls.queue_depth -= 1


_stop_event = threading.Event()


def _shutdown(signum, frame):
    log.info("stop signal received (%s)", signum)
    _stop_event.set()


def main() -> None:
    if "--selftest" in sys.argv:
        raise SystemExit(selftest())
    try:
        channel_secret()
        channel_token()
    except CredError as exc:
        log.critical("startup aborted: %s", exc)
        sys.exit(1)
    # fail fast if the state dir is not writable (persistence is contractual)
    probe = dict(load_state())
    if not save_state(probe):
        log.critical("startup aborted: state persistence self-check failed")
        sys.exit(1)
    recover_stale_claims(probe)  # crashed-claim recovery at startup
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    WebhookHandler.state = probe
    WebhookHandler.executor = ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="line-worker")
    server = http.server.ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), WebhookHandler)
    log.info("dsh-line-inbound v1.2 listening on %s:%d%s",
             LISTEN_HOST, LISTEN_PORT, WEBHOOK_PATH)
    import threading as _t
    accept_thread = _t.Thread(target=server.serve_forever, daemon=True)
    accept_thread.start()
    retry = _t.Thread(target=pending_retry_loop,
                      args=(WebhookHandler.state, _stop_event), daemon=True)
    retry.start()
    try:
        while not _stop_event.wait(1.0):
            pass
    finally:
        # bounded drain: stop accepting, wait up to SHUTDOWN_GRACE_S for the
        # work queue to empty, then abandon anything still running (LINE
        # redelivery makes abandoned work safe; each handler persists state
        # synchronously, so no state transition is lost).
        with WebhookHandler.queue_guard:
            WebhookHandler.shutting_down = True
        log.info("draining in-flight work (grace %ss)", SHUTDOWN_GRACE_S)
        server.shutdown()
        depth = 1
        deadline = time.time() + SHUTDOWN_GRACE_S
        while time.time() < deadline:
            with WebhookHandler.queue_guard:
                depth = WebhookHandler.queue_depth
            if depth == 0:
                break
            time.sleep(0.5)
        log.info("drain %s — exiting", "complete" if depth == 0 else "TIMEOUT")
        server.server_close()
        # release any durable in-flight claims so a restart can retry them
        # immediately instead of waiting out INFLIGHT_STALE_S (best effort;
        # stale-claim recovery covers a failed save anyway)
        with STATE_LOCK:
            released = 0
            for e in WebhookHandler.state.get("pending", {}).values():
                if e.get("inFlight"):
                    e["inFlight"] = False
                    e["inFlightAt"] = 0
                    released += 1
            if released:
                if not save_state(WebhookHandler.state):
                    log.warning("claim release not persisted (recovery covers it)")
            log.info("released %d in-flight claim(s)", released)
            # durable abandonment record: hard exit can kill a handler mid-
            # transition, so an already-acked event may lack durable state.
            # ACCEPTED OPERATIONAL RISK (bounded): abandoned batches re-enter
            # only via LINE redelivery (which we may have 200-acked — a lost
            # reply is possible) and stale-claim recovery re-arms any durable
            # pending entry. Recorded here so recovery is evidence-driven.
            WebhookHandler.state["lastShutdown"] = {
                "at": time.time(), "abandonedBatches": depth,
                "pendingUndelivered": len(WebhookHandler.state.get("pending", {}))}
            if not save_state(WebhookHandler.state):
                log.critical("shutdown-abandonment record NOT persisted — "
                             "recovery required on next start")
        # executor worker threads are non-daemon and would block interpreter
        # exit past the bounded grace; os._exit is the bounded-shutdown
        # implementation (see abandonment record above for the accepted
        # data-loss boundary).
        os._exit(0)


# --- selftest (local validation, no network, no real credentials) ----------------


def selftest() -> int:
    import tempfile
    global STATE_DIR, STATE_PATH
    STATE_DIR = Path(tempfile.mkdtemp(prefix="dsh-line-selftest"))
    STATE_PATH = STATE_DIR / "line-state.json"
    failures = []

    def check(name: str, cond: bool):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    print("dsh_line_inbound selftest:")

    # 1. signature roundtrip (canonical method per LINE docs, dummy secret)
    secret = "selftest-secret"
    body = json.dumps({"destination": "Utest", "events": []}).encode()
    sig = base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()
    check("signature roundtrip", verify_signature(body, sig, secret))
    check("signature rejects tampered body", not verify_signature(body + b" ", sig, secret))
    check("signature rejects missing header", not verify_signature(body, None, secret))
    check("wrong signature rejected", not verify_signature(body, sig[:-2] + "aa", secret))

    # 2. bounded chunker (live path) incl. boundary sizes
    for size, expect_trunc in ((1, False), (100, False), (TEXT_CHUNK_MAX, False),
                               (TEXT_CHUNK_MAX * 5, False),
                               (TEXT_CHUNK_MAX * 5 + 1, True)):
        chunks = chunk_reply_text("x" * size)
        ok = (len(chunks) <= MAX_PUSH_MESSAGES
              and all(len(c) <= TEXT_CHUNK_MAX for c in chunks)
              and all(c for c in chunks))
        if expect_trunc:
            ok = ok and chunks[-1].endswith("(reply truncated)")
        else:
            ok = ok and sum(len(c) for c in chunks) == size
        check(f"chunking size={size}", ok)
    check("empty reply -> no messages", chunk_reply_text("") == [])
    check("non-string reply -> no messages", chunk_reply_text(None) == [])

    # 3. dispatch-marker reply extraction
    def ev(etype, seq, data):
        return {"event": {"type": etype, "seq": seq, "time": 1, "data": data}}

    did = "dispatch-abc123"
    events = [
        ev("user/message", 1, {"turn": 1, "content": [{"type": "text", "text": "older"}],
            "source": {"kind": "user"}, "role": "user"}),
        ev("assistant/message", 2, {"turn": 1, "message": {"role": "assistant",
            "content": [{"type": "text", "text": "in-flight prior reply"}]}}),
        ev("turn/end", 3, {"turn": 1, "reason": {"kind": "completed"}}),
        ev("user/message", 4, {"turn": 2,
            "content": [{"type": "text", "text": f'[line message] {{"dispatch_id": "{did}"}}\nhello'}],
            "source": {"kind": "user"}, "role": "user"}),
        ev("assistant/message", 4, {"turn": 2, "message": {"role": "assistant",
            "content": [{"type": "text", "text": "สวัสดีครับ"},
                        {"type": "text", "text": "ทองคำแท่ง 67,850 บาท"}]}}),
        ev("turn/end", 5, {"turn": 2, "reason": {"kind": "completed"}}),
    ]
    reply = extract_reply_for_dispatch(events, did)
    check("marker identifies OUR turn (not prior turn)",
          reply == "สวัสดีครับ\nทองคำแท่ง 67,850 บาท")
    check("unknown marker -> None", extract_reply_for_dispatch(events, "nope") is None)
    check("empty events -> None", extract_reply_for_dispatch([], did) is None)
    events_unfinished = [e for e in events if e["event"]["type"] != "turn/end"
                         or e["event"]["data"].get("turn") != 2]
    check("unfinished turn -> None", extract_reply_for_dispatch(events_unfinished, did) is None)
    # ambiguity: two marker-bearing user events -> None
    events_ambiguous = events + [events[3]]
    check("ambiguous markers -> None",
          extract_reply_for_dispatch(events_ambiguous, did) is None)
    check("non-list events -> None", extract_reply_for_dispatch(None, did) is None)

    # 3b. live-schema regression (2026-09-03 real DM): user/message carries NO
    # "turn"; turn/start precedes the user message of that turn.
    did2 = "dispatch-live1"
    events_live = [
        ev("agent/inbox/spliced", 3, {"inserted": [{"content": [{"type": "text",
            "text": f'[line message] {{"dispatch_id": "{did2}"}}\nHello'}]}]}),
        ev("turn/start", 4, {"turn": 1}),
        ev("user/message", 7, {"content": [{"type": "text",
            "text": f'[line message] {{"dispatch_id": "{did2}"}}\nHello'}],
            "source": {"kind": "user"}, "role": "user"}),
        ev("assistant/chunk", 24, {"turn": 1, "step": 1,
            "chunk": {"type": "block-end", "index": 0,
                      "block": {"type": "text", "text": "Hello Vincent!"}}}),
        ev("assistant/message", 27, {"turn": 1, "step": 1, "message": {"role": "assistant",
            "content": [{"type": "text", "text": "Hello Vincent!"}],
            "source": {"kind": "model"}}}),
        ev("turn/end", 29, {"turn": 1, "reason": {"kind": "completed"}}),
    ]
    check("live schema (no turn on user/message) extracts reply",
          extract_reply_for_dispatch(events_live, did2) == "Hello Vincent!")
    live_no_end = [e for e in events_live if e["event"]["type"] != "turn/end"]
    check("live schema unfinished turn -> None",
          extract_reply_for_dispatch(live_no_end, did2) is None)
    check("live schema unknown marker -> None",
          extract_reply_for_dispatch(events_live, "nope") is None)
    events_orphan = [e for e in events_live
                     if e["event"]["type"] not in ("turn/start", "turn/end")]
    check("live schema no turn/start anywhere -> fail closed (None)",
          extract_reply_for_dispatch(events_orphan, did2) is None)

    # 4. envelope
    msg = {"id": "m1", "type": "text", "text": "hello"}
    event = {"source": {"type": "user", "userId": "U123"}, "message": msg,
             "deliveryContext": {"isRedelivery": False}, "mode": "active"}
    env = build_envelope("user", "U123", "Somchai", msg, event, "d1")
    check("envelope has platform + marker + text",
          '"platform": "line"' in env and "d1" in env and "hello" in env)
    env2 = build_envelope("user", "U123", None, {"id": "m2", "type": "image"},
                          {"source": {"type": "user", "userId": "U123"}}, "d2")
    check("envelope notes unsupported media", "media handling not enabled" in env2)

    # 5. trust gate
    group_evt = {"mode": "active", "type": "message",
                 "source": {"type": "group", "groupId": "G1", "userId": "U1"},
                 "message": {"id": "m3", "type": "text", "text": "hi",
                             "mention": {"mentionees": [{"isSelf": True}]}}}
    ok, tgt, stype = should_dispatch(group_evt)
    check("group mention dispatch", ok and tgt == "G1" and stype == "group")
    group_evt2 = json.loads(json.dumps(group_evt))
    group_evt2["message"]["mention"] = {"mentionees": [{"userId": "U9", "isSelf": False}]}
    check("group without mention skipped", not should_dispatch(group_evt2)[0])
    standby_evt = {"mode": "standby", "type": "message", "source": {"type": "user",
                   "userId": "U1"}, "message": {"id": "m4", "type": "text", "text": "x"}}
    check("standby mode skipped", not should_dispatch(standby_evt)[0])

    # 6. group profile path
    check("group profile endpoint",
          build_profile_path("group", "G1", "U1") == "/group/G1/member/U1")
    check("user profile endpoint",
          build_profile_path("user", None, "U1") == "/profile/U1")

    # 7. pending CAS: existing entry preserved (attempts not reset)
    st = {"sessions": {}, "accepted": [], "delivered": [], "failed": [],
          "pending": {}, "profiles": {}}
    check("record_pending persisted", record_pending(st, "m1", "T1", "r1"))
    st["pending"]["m1"]["attempts"] = 2
    check("pending CAS preserves attempts",
          record_pending(st, "m1", "T1", "r1")
          and st["pending"]["m1"]["attempts"] == 2)
    record_pending(st, "m1", "T2", "r2")  # conflicting -> keep existing
    check("pending conflict keeps existing", st["pending"]["m1"]["to"] == "T1")

    # 8. claim/finish reservation
    claim = claim_delivery(st, "m1")
    check("claim reserves in-flight", claim is not None
          and st["pending"]["m1"]["inFlight"])
    check("second claim rejected", claim_delivery(st, "m1") is None)
    finish_delivery(st, "m1", success=False)
    check("failed finish releases + bumps attempts",
          not st["pending"]["m1"]["inFlight"] and st["pending"]["m1"]["attempts"] == 3)
    finish_delivery(st, "m1", success=True)
    check("success finish moves to delivered",
          "m1" in st["delivered"] and "m1" not in st["pending"])

    # 8b. durable claim for m5 (real persistence) for the monkeypatched tests
    record_pending(st, "m5", "T1", "r5")
    claim5 = claim_delivery(st, "m5")
    check("claim m5 (durable)", claim5 is not None
          and st["pending"]["m5"]["inFlight"] is True)

    # 9. stale-claim recovery
    record_pending(st, "m2", "T1", "r2")
    c2 = claim_delivery(st, "m2")
    check("claim m2", c2 is not None)
    st["pending"]["m2"]["inFlightAt"] = time.time() - INFLIGHT_STALE_S - 1
    check("stale claim recovered", recover_stale_claims(st) == 1
          and not st["pending"]["m2"]["inFlight"])

    # 10. persistence failure -> fail-closed (monkeypatch save_state)
    real_save = save_state
    try:
        globals()["save_state"] = lambda s: False
        check("record_pending refuses on save failure",
              record_pending(st, "m3", "T1", "r3") is False
              and "m3" not in st["pending"])
        st["pending"]["m4"] = {"to": "T1", "reply": "r4", "attempts": 0,
                               "inFlight": False, "inFlightAt": 0}
        check("claim refuses on save failure", claim_delivery(st, "m4") is None)
        check("fail-closed flag set", STATE_UNAVAILABLE.is_set())
        st["pending"].pop("m4", None)
        STATE_UNAVAILABLE.clear()
        # finish success + save failure -> False, memory restored to durable state
        check("finish-success save failure -> False",
              finish_delivery(st, "m5", success=True) is False
              and "m5" in st["pending"]                       # memory restored
              and st["pending"]["m5"]["inFlight"] is True
              and STATE_UNAVAILABLE.is_set())
        STATE_UNAVAILABLE.clear()
        # finish failure + save failure -> inFlight restored, attempts unchanged
        check("finish-failure save failure -> False",
              finish_delivery(st, "m5", success=False) is False
              and st["pending"]["m5"]["inFlight"] is True
              and st["pending"]["m5"]["attempts"] == 0)
        STATE_UNAVAILABLE.clear()
        st["pending"].pop("m5", None)
    finally:
        globals()["save_state"] = real_save

    # 11. load_state normalization of malformed pending entries
    st["pending"]["m6"] = {"to": "T1", "reply": "r6", "attempts": "x",
                           "inFlight": "yes", "inFlightAt": "soon"}
    tmp_norm = STATE_DIR / "line-state.json"
    tmp_norm.write_text(json.dumps(st))
    loaded = load_state()
    e6 = loaded["pending"]["m6"]
    check("malformed pending normalized",
          e6["inFlight"] is False and e6["inFlightAt"] == 0 and e6["attempts"] == 0)

    print(f"  result: {'ALL PASS' if not failures else f'FAILURES: {failures}'}")
    return 0 if not failures else 1


if __name__ == "__main__":
    main()
