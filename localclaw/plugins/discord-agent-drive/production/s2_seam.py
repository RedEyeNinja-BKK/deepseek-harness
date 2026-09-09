"""s2_seam.py — S2 pilot seam helper for dsh_discord_inbound (PRODUCTION CANDIDATE).

NON-PRODUCTION until the S2 cutover GO. Default-off: the listener only imports
this module and calls into it when the S2_PILOT_CONV environment flag is
non-empty AND equals an incoming conversation key; with the flag empty this
module is never exercised and the historical listener path is unchanged.

Scope (one tiny helper; NOT a new service/daemon/socket/ledger/framework):
- AF_UNIX JSONL client (async) to the REAL discord-agent-drive plugin seam.
- Route-control file reader (gate-owned s2-route.json under STATE_DIRECTORY).
- Minimal external-delivery ledger helpers INSIDE the existing inbound-state.json
  (state['s2']['delivered_finalizations'] + state['s2']['attempted']; media
  identity suppression reuses the existing media_delivered ledger).
- Finalization handling with the honest guarantee: AT-MOST-ONCE automatic
  external send with durable fail-visible INDETERMINATE state across an
  ambiguous send/commit crash window. No unconditional "exactly once" claim.

The plugin protocol is used EXACTLY as reviewed (no redesign):
  hello{deliveredFinalizations} / route{state} / admitted{conversationKey,
  sessionId, discordMessageId, dshMessageId, authorId, content, attachmentRefs,
  ts} -> hello-ack / route-ack / ack{for:admitted|finalization,...} /
  finalization{finalizationId, kind, turn, facts}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("dsh-s2-seam")

S2_ROUTE_OLD = "OLD"
S2_ROUTE_ACTIVE = "S2_ACTIVE"
S2_ROUTE_QUIESCING = "QUIESCING_TO_OLD"
_VALID_ROUTES = {S2_ROUTE_OLD, S2_ROUTE_ACTIVE, S2_ROUTE_QUIESCING}

DEFAULT_ADMIT_TIMEOUT_S = 60.0
HELLO_TIMEOUT_S = 30.0
ROUTE_TIMEOUT_S = 15.0
RECONNECT_DELAY_S = 2.0
MAX_FRAME_BYTES = 1024 * 1024
MAX_PENDING_LEDGER = 5000        # bounded per-conversation fid ledger entries
ATTEMPTED_MAX = 500              # bounded per-conversation attempted window


# ---------------------------------------------------------------------------
# Pure helpers: pilot flag + route control file
# ---------------------------------------------------------------------------

def pilot_active(pilot_conv: str) -> bool:
    """S2 authority is only ever on when S2_PILOT_CONV is set to an exact key."""
    return bool(pilot_conv)


def is_pilot_conv(conv_key: str, pilot_conv: str) -> bool:
    return bool(pilot_conv) and conv_key == pilot_conv


def read_route_file(route_path: Path, conv_key: str) -> str:
    """Gate-owned route control (default OLD). Returns OLD on any error so a
    missing/corrupt control file fails closed to the historical path."""
    try:
        if not route_path.is_file():
            return S2_ROUTE_OLD
        data = json.loads(route_path.read_text())
        route = (data or {}).get(conv_key)
        if route in _VALID_ROUTES:
            return route
        route = (data or {}).get("route")
        if route in _VALID_ROUTES:
            return route
        return S2_ROUTE_OLD
    except Exception:
        return S2_ROUTE_OLD


# ---------------------------------------------------------------------------
# External delivery ledger helpers (inside the existing inbound-state.json)
# ---------------------------------------------------------------------------

def _s2_branch(state: dict) -> dict:
    return state.setdefault("s2", {})


def s2_delivered_fids(state: dict, conv_key: str) -> list:
    conv = _s2_branch(state).setdefault("delivered_finalizations", {}).get(conv_key, {})
    return [fid for fid, e in conv.items()
            if isinstance(e, dict) and e.get("state") == "delivered"]


def s2_fid_state(state: dict, conv_key: str, fid: str) -> str | None:
    e = _s2_branch(state).setdefault("delivered_finalizations", {}).get(conv_key, {}).get(fid)
    return e.get("state") if isinstance(e, dict) else None


def s2_begin_pending(state: dict, conv_key: str, fid: str, save_fn) -> bool:
    """Durable pending BEFORE any external send (crash-safe at-most-once)."""
    conv = _s2_branch(state).setdefault("delivered_finalizations", {}).setdefault(conv_key, {})
    conv[fid] = {"state": "pending", "at": int(time.time())}
    while len(conv) > MAX_PENDING_LEDGER:
        oldest = min(conv, key=lambda k: (conv[k].get("at") or 0, k))
        del conv[oldest]
    return save_fn(state)


def s2_settle_delivered(state: dict, conv_key: str, fid: str, save_fn, kind: str | None = None) -> bool:
    conv = _s2_branch(state).setdefault("delivered_finalizations", {}).setdefault(conv_key, {})
    conv[fid] = {"state": "delivered", "at": int(time.time())}
    if kind:
        conv[fid]["kind"] = kind
    return save_fn(state)


def s2_mark_indeterminate(state: dict, conv_key: str, fid: str, save_fn, reason: str) -> bool:
    """Fail-visible INDETERMINATE evidence (pending replay / ambiguous window).
    Never auto-resends. Returns durable-save success for the evidence."""
    conv = _s2_branch(state).setdefault("delivered_finalizations", {}).setdefault(conv_key, {})
    conv[fid] = {"state": "indeterminate", "reason": reason, "at": int(time.time())}
    return save_fn(state)


def s2_attempted(state: dict, conv_key: str, discord_id: str) -> dict | None:
    e = _s2_branch(state).setdefault("attempted", {}).get(conv_key, {}).get(str(discord_id))
    return e if isinstance(e, dict) else None


def s2_remember_attempted(state: dict, conv_key: str, discord_id: str, kind: str, save_fn) -> bool:
    """Record that an S2 admission was ATTEMPTED for this Discord message so a
    route flip to OLD never lets the historical path resubmit an ambiguous S2
    message. kind: 'claimed' (durably accepted) or 'ambiguous' (no durable claim)."""
    conv = _s2_branch(state).setdefault("attempted", {}).setdefault(conv_key, {})
    conv[str(discord_id)] = {"state": kind, "at": int(time.time())}
    while len(conv) > ATTEMPTED_MAX:
        oldest = min(conv, key=lambda k: (conv[k].get("at") or 0, k))
        del conv[oldest]
    return save_fn(state)


# ---------------------------------------------------------------------------
# Finalization frame handling (mirror of the isolated shim, on real state)
# ---------------------------------------------------------------------------

async def handle_finalization(state: dict, conv_key: str, frame: dict,
                              save_fn, deliver_text, deliver_media) -> None:
    """Exactly one decision per finalization fid with the honest at-most-once +
    indeterminate semantics. deliver_text(text) -> 'ok'|'partial'|'zero'|'error';
    deliver_media(item) -> bool (mirror of the listener's _deliver_media_file)."""
    fid = frame.get("finalizationId")
    kind = frame.get("kind")
    facts = frame.get("facts") or {}
    if not fid or kind not in ("noop", "text-fallback", "artifact", "failure-notice"):
        log.error("s2: unhandled finalization frame kind=%s fid=%s", kind, fid)
        return
    existing = s2_fid_state(state, conv_key, fid)
    if existing == "delivered":
        log.info("s2: finalization delivered-suppressed (fid=%s)", fid)
        return
    if existing == "pending":
        log.warning("s2: finalization PENDING prior attempt (fid=%s) - "
                    "INDETERMINATE, no auto-resend", fid)
        s2_mark_indeterminate(state, conv_key, fid, save_fn, "pending-replay-no-resend")
        return
    if existing == "indeterminate":
        log.info("s2: finalization already INDETERMINATE (fid=%s) - no resend", fid)
        return
    if kind == "noop":
        # Confirmed-send/terminal-empty suppression: nothing external to send.
        s2_settle_delivered(state, conv_key, fid, save_fn, kind="noop")
        return
    # media identity suppression (existing media_delivered ledger)
    remaining = []
    for a in facts.get("artifacts") or []:
        ident = a.get("identity")
        if not ident:
            continue
        med = state.setdefault("media_delivered", {}).setdefault(conv_key, {})
        if ident in med:
            log.info("s2: media identity already delivered; suppress (%s)", ident)
            continue
        remaining.append({"file": a.get("path"), "identity": ident})
    if kind == "artifact" and (facts.get("artifacts") or []) and not remaining:
        s2_settle_delivered(state, conv_key, fid, save_fn, kind="artifact-suppressed")
        return
    if kind == "text-fallback":
        text = facts.get("text")
        if not text:
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="empty-text")
            return
        if not s2_begin_pending(state, conv_key, fid, save_fn):
            log.critical("s2: begin_pending durable FAILED for %s - no external "
                         "send (fail closed)", fid)
            return
        result = await deliver_text(text)
        if result == "ok":
            s2_settle_delivered(state, conv_key, fid, save_fn, kind=kind)
        elif result == "partial":
            # terminal partial: never resend from the start; ledger delivered
            # matches the old-path posture (partial is terminal).
            log.critical("s2: fallback PARTIAL for %s - terminal, no resend", fid)
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="partial")
        else:
            s2_mark_indeterminate(state, conv_key, fid, save_fn,
                                  "zero-or-error-send-" + str(result))
        return
    if kind == "artifact":
        if not remaining:
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="artifact-none")
            return
        if not s2_begin_pending(state, conv_key, fid, save_fn):
            log.critical("s2: begin_pending durable FAILED for %s - no external "
                         "send (fail closed)", fid)
            return
        all_ok = True
        for item in remaining:
            ok = await deliver_media(item)
            if ok:
                ident = item.get("identity")
                if ident:
                    state.setdefault("media_delivered", {}).setdefault(conv_key, {})[ident] = int(time.time())
                    save_fn(state)
            else:
                all_ok = False
        if all_ok:
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="artifact")
        else:
            s2_mark_indeterminate(state, conv_key, fid, save_fn,
                                  "media-send-incomplete")
        return
    if kind == "failure-notice":
        text = facts.get("text") or "(delivery failure notice)"
        if not s2_begin_pending(state, conv_key, fid, save_fn):
            log.critical("s2: begin_pending durable FAILED for %s - no external "
                         "send (fail closed)", fid)
            return
        result = await deliver_text(text)
        if result == "ok":
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="failure-notice")
        elif result == "partial":
            log.critical("s2: failure-notice PARTIAL for %s - terminal", fid)
            s2_settle_delivered(state, conv_key, fid, save_fn, kind="partial")
        else:
            s2_mark_indeterminate(state, conv_key, fid, save_fn,
                                  "zero-or-error-send-" + str(result))
        return


# ---------------------------------------------------------------------------
# Async AF_UNIX JSONL seam client
# ---------------------------------------------------------------------------

class S2Seam:
    """One async AF_UNIX client to the plugin seam. Reconnects with hello on
    every connect (the plugin rebuilds admission state + reconciles owed
    finalizations from its authoritative log, so reconnect is crash-safe)."""

    def __init__(self, sock_path: str, conv_key: str, pilot_session_id: str,
                 delivered_provider, save_fn, on_finalization, log_fn=None):
        self.sock_path = sock_path
        self.conv_key = conv_key
        self.pilot_session_id = pilot_session_id
        self.delivered_provider = delivered_provider   # callable() -> list[str]
        self.save_fn = save_fn
        self.on_finalization = on_finalization         # async callable(frame)
        self.log_fn = log_fn or log
        self.enabled = False
        self._reader: asyncio.Task | None = None
        self._writer = None
        self._reader_task: asyncio.Task | None = None
        self._loop = None
        self._pending_acks: dict = {}
        self.connected = False
        self.hello_acked = False

    # -- lifecycle ----------------------------------------------------------
    async def start(self):
        if self._reader_task is not None:
            return
        self.enabled = True
        self._loop = asyncio.get_running_loop()
        self._reader_task = asyncio.create_task(self._run())

    async def stop(self):
        self.enabled = False
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None
        self._close_writer()

    def _close_writer(self):
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None
        self.connected = False
        self.hello_acked = False
        for fut in self._pending_acks.values():
            if not fut.done():
                fut.set_exception(ConnectionError("seam closed"))
        self._pending_acks.clear()

    # -- transport ----------------------------------------------------------
    async def _run(self):
        while self.enabled:
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log_fn.warning("s2 seam connect failed: %s", e)
            if not self.enabled:
                break
            await asyncio.sleep(RECONNECT_DELAY_S)

    async def _connect_once(self):
        try:
            reader, writer = await asyncio.open_unix_connection(self.sock_path)
        except FileNotFoundError:
            self.log_fn.warning("s2 seam socket not present (%s)", self.sock_path)
            return
        self._writer = writer
        self.connected = True
        self.log_fn.info("s2 seam connected %s", self.sock_path)
        delivered = self.delivered_provider()
        await self._send({"type": "hello", "deliveredFinalizations": delivered})
        try:
            raw = await asyncio.wait_for(reader.readline(), HELLO_TIMEOUT_S)
            ack = json.loads(raw.decode("utf-8", errors="replace").strip())
        except Exception as e:
            self.log_fn.warning("s2 hello failed: %s", e)
            self._close_writer()
            return
        if ack.get("type") != "hello-ack":
            self.log_fn.warning("s2 hello-ack missing (got %s)", ack.get("type"))
            self._close_writer()
            return
        self.hello_acked = True
        self.log_fn.info("s2 hello-ack route=%s pilot=%s sid=%s",
                         ack.get("route"), ack.get("pilotConversationKey"),
                         ack.get("pilotSessionId"))
        try:
            while self.enabled:
                raw = await reader.readline()
                if not raw:
                    break
                if len(raw) > MAX_FRAME_BYTES:
                    self.log_fn.error("s2 oversize inbound frame dropped")
                    continue
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    frame = json.loads(line)
                except Exception:
                    self.log_fn.error("s2 malformed frame ignored")
                    continue
                await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log_fn.warning("s2 read error: %s", e)
        finally:
            self._close_writer()

    async def _send(self, frame: dict) -> None:
        if self._writer is None:
            raise ConnectionError("s2 seam not connected")
        line = (json.dumps(frame) + "\n").encode("utf-8")
        self._writer.write(line)
        await self._writer.drain()

    def _ack_key(self, frame: dict):
        ftype = frame.get("type")
        if ftype == "ack":
            return ("ack", frame.get("for"), frame.get("discordMessageId"))
        if ftype == "hello-ack":
            return ("hello", None)
        if ftype == "route-ack":
            return ("route", None)
        return None

    async def _dispatch(self, frame: dict) -> None:
        ftype = frame.get("type")
        if ftype == "finalization":
            try:
                await self.on_finalization(frame)
            except Exception as e:
                self.log_fn.error("s2 finalization handler error: %s", e)
            return
        fut = self._pending_acks.pop(self._ack_key(frame), None)
        if fut is not None and not fut.done():
            fut.set_result(frame)

    async def _await_ack(self, key, timeout: float) -> dict:
        fut = self._loop.create_future()
        self._pending_acks[key] = fut
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending_acks.pop(key, None)

    async def set_route(self, route: str) -> dict:
        """Send a route transition to the plugin (authority lives in the
        listener; the plugin echoes the state). Returns route-ack."""
        await self._send({"type": "route", "state": route})
        return await self._await_ack(("route", None), ROUTE_TIMEOUT_S)

    async def admit(self, frame: dict, timeout: float = DEFAULT_ADMIT_TIMEOUT_S) -> dict:
        """Send one admitted frame and await the plugin ACK (matched by Discord
        message id). Raises on timeout / disconnect (caller decides retry)."""
        await self._send({"type": "admitted", **frame})
        return await self._await_ack(
            ("ack", "admitted", str(frame.get("discordMessageId"))), timeout)
