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

v4 (Increment 1, 2026-09-04): person identity + approved-family-group
admission + own-mention-only gating. New durable admission state file
(admission-state.json, flock-guarded, operator CLI dsh_line_admission.py).
Trust boundary: admission/mention classification runs BEFORE any DSH
session/model/tool work; unknown groups are captured as PENDING candidates
(no DSH access, no leave until the operator declines or the 14-day window
expires), unadmitted DM outsiders are denied, and group dispatch requires
LINE's own-mention metadata (isSelf==True) — @All and textual lookalikes
never summon DSH. F3 final-output filtering unchanged.

v5 (Increment 2, 2026-09-04): explicit EN<->TH translation command. Command form:
`translate <text>` in an admitted DM; `@DSH translate <text>` (real LINE
own-mention metadata) in the approved group; source text supplied in the SAME
message; nothing inferred from prior messages. Gating unchanged: the command is
recognized ONLY after the Increment-1 admission+own-mention trust gate passes, so
literal '@DSH' text and '@All' never reach translation. Dispatch goes to the
DSH-native Typhoon translation specialist (cordis subagent `translate`); the
specialist's reply is returned translation-only. Specialist failure returns a
concise fixed user-facing failure line — NEVER a fabricated/fallback normal-model
answer — under the unchanged F3 final-output filter.

v6 (Increment 2b, 2026-09-04): reply-to-DSH invocation. In the APPROVED family
group a LINE Reply (quotedMessageId) that POSITIVELY refers to a message DSH
itself sent now invokes DSH, same as a real own-mention. Authorship proof is
metadata-only: our own Push API `sentMessages[].id` responses are recorded
(bounded, prunable, no message bodies) and matched against the inbound
quotedMessageId within the same conversation. Replies to family members,
unknown/stale ids, other conversations, unapproved groups, '@All' and literal
typed '@DSH' never invoke. When the Push response exposes no sent-message ids
(legacy/empty body) the feature fail-opens to mention-only — never heuristic
author detection (no display text, names, timestamps, or model inference).
Translation composes with either invocation form: invoked DSH + text starting
with `translate ` = translation.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import http.server
import json
import logging
import os
import queue
import re
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
# Increment 2: only text messages carry the translation command; every other
# message type goes down the normal envelope path (media not enabled yet).
TRANSLATE_TARGET_TYPES = frozenset({"text"})
# Increment 2b: bounded authorship memory for reply-to-DSH recognition.
# sent-message IDs of our own replies, mapped to the conversation — NO bodies.
DSH_OUTBOUND_MAX = 500              # max tracked sent-message IDs
                                    # (oldest-first eviction; NO time TTL)

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
ADMISSION_PATH = STATE_DIR / "admission-state.json"
ADMISSION_LOCK_PATH = STATE_DIR / "admission.lock"

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
            state.setdefault("dshOutbound", {})
            # Increment 2b: normalize outbound-author memory (bounded
            # metadata; malformed/foreign entries dropped at load).
            ob = state.get("dshOutbound", {})
            for sid, e in list(ob.items()):
                if not isinstance(e, dict) \
                        or not isinstance(sid, str) or not sid \
                        or not isinstance(e.get("conv"), str) \
                        or not isinstance(e.get("at"), (int, float)):
                    ob.pop(sid, None)
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
            "pending": {}, "profiles": {}, "dshOutbound": {}}


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


# --- admission & identity (Increment 1; separate durable state file) ------------
#
# Identity model:
#   person identity        = DSH-owned record "p-<uuid>" — represents the human,
#                            never a transport identifier.
#   channel identity       = bindings inside a person record (line/discord);
#                            binding two channels to one person requires an
#                            EXPLICIT operator command — display names, profile
#                            names, similarity or model inference NEVER merge.
#   conversation identity  = LINE DM / LINE group|room (and later Discord
#                            channels) — membership is not identity.
#   admission state        = per-conversation state machine (UNKNOWN -> PENDING
#                            -> APPROVED | DECLINED) + per-person DM eligibility
#                            (observed in an APPROVED group's roster, or an
#                            explicit operator grant).
#
# Durable state: STATE_DIR/admission-state.json (schema v1) under flock on
# STATE_DIR/admission.lock so the operator CLI (dsh_line_admission.py, sudo)
# and this service never corrupt each other. Malformed/unreadable admission
# state is FAIL-CLOSED: every admission denies, nothing dispatches, CRITICAL
# logged. PENDING candidates never gain sessions, model calls, or tools; the
# LINE leave capability (already on this credential surface) is exercised on
# DECLINED/expired candidates by the maintenance loop, never on PENDING (the
# operator needs the candidate alive to approve it).

GROUP_ROSTER_MAX = 64                     # bounded roster (family scale)
LEAVE_MAX_ATTEMPTS = 5
ADM_STATES = ("PENDING", "APPROVED", "DECLINED")
DEFAULT_ADMISSION = {"version": 1, "persons": {}, "groups": {},
                     "dmGrants": {"line": {}}, "dmDenials": {"line": {}}}


def admission_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def _default_admission() -> dict:
    return json.loads(json.dumps(DEFAULT_ADMISSION))


def _validate_admission(adm) -> bool:
    if not isinstance(adm, dict) or adm.get("version") != 1:
        return False
    persons, groups = adm.get("persons"), adm.get("groups")
    grants = (adm.get("dmGrants") or {}).get("line")
    denials = (adm.get("dmDenials") or {}).get("line")
    if not isinstance(persons, dict) or not isinstance(groups, dict):
        return False
    if grants is not None and not isinstance(grants, dict):
        return False
    if denials is not None and not isinstance(denials, dict):
        return False
    for g in groups.values():
        if not isinstance(g, dict) or g.get("state") not in ADM_STATES:
            return False
        if not isinstance(g.get("roster", []), list):
            return False
    for p in persons.values():
        if not isinstance(p, dict) or not isinstance(p.get("bindings"), dict):
            return False
        if not isinstance((p.get("bindings") or {}).get("line", []), list):
            return False
    return True


def load_admission() -> tuple[dict, bool]:
    """Returns (admission, healthy). healthy=False -> deny-all (fail-closed)."""
    try:
        lock_fd = os.open(ADMISSION_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_SH)
            raw = (ADMISSION_PATH.read_text(encoding="utf-8")
                   if ADMISSION_PATH.exists() else "")
        finally:
            os.close(lock_fd)
        if not raw.strip():
            return json.loads(json.dumps(DEFAULT_ADMISSION)), True
        adm = json.loads(raw)
        if not _validate_admission(adm):
            log.critical("admission state MALFORMED — deny-all engaged (fail-closed)")
            return json.loads(json.dumps(DEFAULT_ADMISSION)), False
        return adm, True
    except Exception as exc:
        log.critical("admission state UNREADABLE (%s) — deny-all engaged",
                     exc.__class__.__name__)
        return json.loads(json.dumps(DEFAULT_ADMISSION)), False


def save_admission(adm: dict) -> bool:
    """flock-exclusive atomic write (tmp + fsync + replace + dir fsync)."""
    try:
        if not _validate_admission(adm):
            log.error("refusing to persist malformed admission state")
            return False
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(ADMISSION_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            tmp = STATE_DIR / f"admission.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(adm))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, ADMISSION_PATH)
            dfd = os.open(STATE_DIR, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            return True
        finally:
            os.close(lock_fd)
    except OSError as exc:
        log.error("admission save FAILED: %s", exc.__class__.__name__)
        return False


def mention_gate_self(mentionees) -> bool:
    """TRUE only for LINE's own-mention metadata (isSelf is True). @All
    (type == "all") and textual lookalikes are intentionally ignored — no
    regex/string matching exists by design."""
    if not isinstance(mentionees, list):
        return False
    return any(isinstance(m, dict) and m.get("isSelf") is True for m in mentionees)


def group_state(adm: dict, gid: str) -> str:
    g = adm.get("groups", {}).get(gid)
    return g.get("state", "UNKNOWN") if isinstance(g, dict) else "UNKNOWN"


def person_for_line(adm: dict, line_user: str) -> dict | None:
    """Person bound to a LINE user. None = unbound. The user appearing in more
    than one person, or twice inside one person, is corrupt state -> fail-closed
    marker dict (never guess)."""
    matches = []
    for p in adm.get("persons", {}).values():
        if not isinstance(p, dict):
            continue
        binds = (p.get("bindings") or {}).get("line") or []
        matches.extend(p for _ in range(sum(1 for x in binds if x == line_user)))
    if len(matches) > 1:
        log.critical("person bindings corrupt (%d match this LINE user) — "
                     "fail-closed", len(matches))
        return {"__corrupt__": True}
    return matches[0] if matches else None


def line_user_dm_eligible(adm: dict, line_user: str) -> bool:
    """DM eligibility precedence (operator directive 4): explicit operator
    DENY > approved-group roster eligibility > explicit grant. A deny is
    durable until an explicit grant-dm clears it; identity never confers
    capability; names are never consulted."""
    if line_user in (adm.get("dmDenials", {}).get("line") or {}):
        return False
    for g in adm.get("groups", {}).values():
        if isinstance(g, dict) and g.get("state") == "APPROVED" \
                and line_user in (g.get("roster") or []):
            return True
    return line_user in (adm.get("dmGrants", {}).get("line") or {})


def line_user_admitted(adm: dict, line_user: str) -> bool:
    """DM admission predicate (review fix): CURRENT eligibility only - an
    APPROVED-group roster entry or an active operator grant. A person RECORD
    is pure identity and NEVER confers capability, so revocation (grant
    removal + roster strip) is authoritative. Corrupt bindings deny."""
    p = person_for_line(adm, line_user)
    if p and p.get("__corrupt__"):
        return False
    return line_user_dm_eligible(adm, line_user)


def transact_admission(mutator):
    """One exclusive-lock read-modify-write transaction on the admission file
    (review fix: closes the service/CLI lost-update race). Load + validate +
    mutator(adm) -> (adm2, proceed) + post-validate + atomic write, all under
    a single flock. mutator does NO network I/O. mutator(adm) returns
    (adm2, result); a FALSY result aborts WITHOUT persisting; otherwise the
    post-validated state is persisted and (adm2, result, True) is returned
    (result = the mutator's payload, e.g. the bound person). (None, None,
    False) when the store is unhealthy (fail-closed)."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(ADMISSION_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            raw = (ADMISSION_PATH.read_text(encoding="utf-8")
                   if ADMISSION_PATH.exists() else "")
            try:
                adm = (json.loads(raw) if raw.strip()
                       else json.loads(json.dumps(DEFAULT_ADMISSION)))
            except Exception:
                log.critical("admission state UNREADABLE - transaction "
                             "refused (fail-closed)")
                return None, False
            if not _validate_admission(adm):
                log.critical("admission state MALFORMED - transaction "
                             "refused (fail-closed)")
                return None, False
            adm2, result = mutator(adm)
            if not result:
                return adm2, None, False
            if not _validate_admission(adm2):
                log.error("transaction produced invalid admission state - "
                          "refusing to persist (fail-closed)")
                return adm2, None, False
            tmp = STATE_DIR / f"admission.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(adm2))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, ADMISSION_PATH)
            dfd = os.open(STATE_DIR, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            return adm2, result, True
        finally:
            os.close(lock_fd)
    except OSError as exc:
        log.error("admission transaction FAILED: %s", exc.__class__.__name__)
        return None, None, False


def new_person(adm: dict, label: str | None) -> dict:
    pid = "p-" + uuid.uuid4().hex
    adm["persons"][pid] = {"personId": pid, "label": label,
                           "bindings": {"line": [], "discord": []},
                           "dmEligible": {"line": False, "discord": False},
                           "createdAt": admission_now(),
                           "updatedAt": admission_now()}
    return adm["persons"][pid]


def observe_group_message(adm: dict, source_type: str, gid: str,
                          user_id: str | None,
                          summary: dict | None = None) -> tuple[dict, str]:
    """State transition for one observation from a group|room. Pure dict
    transform (caller persists). Events: NEW_PENDING, PENDING_SEEN, ROSTER
    (roster grew), APPROVED_SEEN, DECLINED_SEEN."""
    groups = adm.setdefault("groups", {})
    g = groups.get(gid)
    if not isinstance(g, dict):
        # Single-family-group invariant (operator directive 1): at most ONE
        # PENDING candidate (while nothing is approved) and at most ONE
        # APPROVED group globally. A DIFFERENT unknown group arriving while a
        # candidate/approved group exists is recorded DECLINED with
        # leaveRequested (already-implemented LINE leave removes it); it never
        # becomes a second candidate and never gains DSH access.
        has_candidate = any(isinstance(x, dict) and x.get("state") in
                            ("PENDING", "APPROVED")
                            for x in groups.values())
        if has_candidate:
            groups[gid] = {"state": "DECLINED", "kind": source_type or "group",
                           "summary": summary, "firstSeen": admission_now(),
                           "decidedAt": admission_now(),
                           "decidedBy": "single-family-group-invariant",
                           "roster": [], "leaveRequested": True,
                           "left": False, "leaveAttempts": 0}
            return adm, "REJECTED_EXTRA_GROUP"
        groups[gid] = {"state": "PENDING", "kind": source_type or "group",
                       "summary": summary, "firstSeen": admission_now(),
                       "decidedAt": None, "decidedBy": None, "roster": [],
                       "leaveRequested": False, "left": False,
                       "leaveAttempts": 0}
        return adm, "NEW_PENDING"
    if summary and not g.get("summary"):
        g["summary"] = summary
    if g.get("state") == "APPROVED":
        # roster bootstrap (operator directive 3): ONLY the approved family
        # group accumulates a roster; denied users are not rostered.
        denied = user_id in (adm.get("dmDenials", {}).get("line") or {})
        if (user_id and not denied and user_id not in g["roster"]
                and len(g["roster"]) < GROUP_ROSTER_MAX):
            g["roster"].append(user_id)
            return adm, "ROSTER"
        return adm, "APPROVED_SEEN"
    if g.get("state") == "PENDING":
        # minimal candidate metadata only - NO roster accumulation
        return adm, "PENDING_SEEN"
    return adm, "DECLINED_SEEN"


def classify_event(event: dict, adm: dict) -> dict:
    """Admission + mention classifier — THE trust boundary. Runs BEFORE any
    DSH session/model/tool work; fails closed in every ambiguous case."""
    out = {"action": "SKIP", "source_type": None, "target_id": None,
           "user_id": None, "reason": ""}
    if event.get("mode") != "active":
        out["reason"] = "standby event"
        return out
    etype = event.get("type")
    source = event.get("source") or {}
    source_type = source.get("type")
    out["source_type"] = source_type
    if etype == "join" and source_type in ("group", "room"):
        target = source.get("groupId") or source.get("roomId")
        out.update(action="OBSERVE_PENDING", target_id=target,
                   reason="bot joined unverified conversation — candidate capture")
        return out
    if etype != "message":
        out["reason"] = f"event type {etype}"
        return out
    message = event.get("message") or {}
    user_id = source.get("userId")
    out["user_id"] = user_id
    if source_type == "user":
        target = source.get("userId")
        out["target_id"] = target
        if not target:
            out["reason"] = "DM without userId"
            return out
        if line_user_admitted(adm, target):
            out.update(action="DISPATCH_DM",
                       reason="admitted DM (mention not required)")
        else:
            out.update(action="DENIED_DM", reason="unadmitted outsider DM")
        return out
    if source_type in ("group", "room"):
        target = source.get("groupId") or source.get("roomId")
        out["target_id"] = target
        if not target:
            out["reason"] = "group/room without id"
            return out
        st = group_state(adm, target)
        mentionees = ((message.get("mention") or {}).get("mentionees")) or []
        if st == "APPROVED":
            out["self_mentioned"] = mention_gate_self(mentionees)
            out.update(action="APPROVED_GROUP_MSG",
                       reason="approved group message (roster bootstrap; "
                              "dispatch only on own-mention)")
        elif st == "PENDING":
            out.update(action="OBSERVE_PENDING",
                       reason="pending approval — no DSH access")
        elif st == "DECLINED":
            out.update(action="DENIED_GROUP", reason="declined/revoked group")
        else:
            out.update(action="OBSERVE_PENDING",
                       reason="unknown group — captured as PENDING candidate")
        return out
    out["reason"] = f"unknown source type {source_type}"
    return out


def admission_maintenance_once() -> None:
    """Perform requested leaves via the LINE API already available to this
    integration (rejected extra groups / revoked groups). PENDING candidates
    are NEVER expired automatically (operator-driven decisions only). Two-
    phase: state transitions in lock-held transactions; leave HTTP calls
    OUTSIDE the lock; outcomes recorded in a second transaction."""
    targets = []

    def _collect_leaves(adm):
        for gid, g in adm.get("groups", {}).items():
            if not isinstance(g, dict):
                continue
            if (g.get("leaveRequested") and not g.get("left")
                    and g.get("leaveAttempts", 0) < LEAVE_MAX_ATTEMPTS):
                g["leaveAttempts"] = g.get("leaveAttempts", 0) + 1
                targets.append((gid, g.get("kind") or "group"))
        return adm, bool(targets)
    adm, _payload, _persisted = transact_admission(_collect_leaves)
    if adm is None:
        return
    ok_ids = []
    for gid, kind in targets:
        try:
            line_request("POST", f"/{kind}/{gid}/leave", label="leave")
            ok_ids.append(gid)
            log.info("admission: left %s %s", kind, gid)
        except LineHttpError as exc:
            log.error("admission: leave failed (%s %s status=%s attempt=%d)",
                      kind, gid, exc.status,
                      (adm.get("groups", {}).get(gid) or {}).get("leaveAttempts"))
    if ok_ids:
        def _record(adm):
            for gid in ok_ids:
                g = adm.get("groups", {}).get(gid)
                if isinstance(g, dict):
                    g["left"] = True
                    g["leftAt"] = admission_now()
            return adm, True
        transact_admission(_record)  # noqa: result unused


def admission_maintenance_loop(stop_event: threading.Event) -> None:
    while not stop_event.wait(60.0):
        try:
            admission_maintenance_once()
        except CredError as exc:
            log.error("admission maintenance skipped (credential: %s)",
                      exc.__class__.__name__)
        except Exception as exc:
            log.error("admission maintenance error: %s", exc.__class__.__name__)


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


# --- translation (Increment 2) --------------------------------------------------
# Explicit EN<->TH translation command. The DSH-native Typhoon translation
# specialist (cordis subagent, toolName `translate`) owns direction detection,
# translation, and name/URL/number preservation; the adapter only parses the
# command and carries the payload. Nothing here selects or detects language.
TRANSLATE_CMD = "translate"
TRANSLATE_CMD_MAX = 4000                    # bounded source text (chars)
TRANSLATE_FAILURE_TEXT = ("Sorry — the translation service is unavailable "
                          "right now. Please try again in a moment.")
TRANSLATE_HELP_TEXT = ("Please send the text to translate in the same "
                       "message, e.g. `translate Good morning`.")
TRANSLATE_PERSONA = (
    "You are a translation engine. Translate the user's text automatically "
    "between English and Thai: predominantly English source -> natural Thai; "
    "predominantly Thai source -> natural English. For mixed text, translate "
    "the natural-language content and keep embedded names, URLs, numbers, "
    "emojis and formatting unchanged where practical. Output ONLY the "
    "translation — no preamble, no commentary, no alternatives, no "
    "transliteration, no explanation.")


def parse_translate_command(text: str) -> dict | None:
    """Smallest unambiguous parser for the explicit translation command:
    `translate <text>` must be the START of the text it is given.

    Callers contract: the DM path passes the RAW message text; the group path
    passes the own-mention-stripped remainder (strip_own_mention_text — the
    real mention span was already removed and the gate already passed). The
    parser itself performs no gating and no mention handling.

    Returns {"source": <text>} or None. Bare `translate` (empty source) is a
    VALID command match returning {"source": ""} — the caller renders a
    concise failure/help instead of invoking the specialist. Non-command text
    (including the word `translate` anywhere but the command position) -> None.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    s = text.lstrip()
    m = re.match(r"translate(?:\s+(.*))?$", s, re.DOTALL | re.IGNORECASE)
    if not m:
        return None
    source = (m.group(1) or "").strip()
    if len(source) > TRANSLATE_CMD_MAX:
        return None
    return {"source": source}


def translate_output_valid(reply: str, source: str) -> bool:
    """Fail-closed validity gate on the specialist's output. Probes proved
    the child chain can occasionally return a source ECHO or an error string
    instead of a translation; such output must NEVER reach the family. This
    is rejection (-> concise failure line), not post-generation editing of
    accepted translations."""
    if not isinstance(reply, str) or not reply.strip():
        return False
    r = reply.strip()
    if r.startswith("Error:") or r.startswith("error:"):
        return False
    s = (source or "").strip()
    if len(s) > 8 and r.casefold() == s.casefold():
        return False  # source echoed untranslated
    return True


def translate_via_dsh(session_id: str, dispatch_id: str,
                      source: str) -> str | None:
    """Send ONLY the source text to the DSH-native translation specialist
    (cordis subagent `translate`, model switchyard/thaillm/typhoon) through
    the SAME session.prompt -> history-extraction path as normal dispatches
    (F3 final-output filtering unchanged). Returns the specialist's reply, or
    None on any failure — callers must NEVER substitute a normal-model answer
    for a failed translation (fail-closed user-facing failure line instead)."""
    marker = f"[line translate {dispatch_id}]"
    envelope = (f"{marker}\n"
                "Use the translate tool ONCE with source_text set to EXACTLY "
                "the text between BEGIN and END below (copy it verbatim, no "
                "edits, no additions):\n"
                "BEGIN\n"
                f"{source}\n"
                "END\n"
                "After the tool returns, reply with the translation from the "
                "tool result and nothing else.")
    dispatched_at_ms = int(time.time() * 1000)
    try:
        dsh_rpc("session.prompt", {
            "sessionId": session_id, "mode": "queue",
            "content": [{"type": "text", "text": envelope}],
            "clientTimeZone": CLIENT_TZ})
    except Exception as e:
        log.error("translation session.prompt failed (session %s, dispatch %s): %s",
                  session_id, dispatch_id, e.__class__.__name__)
        return None
    reply = wait_for_reply(session_id, dispatch_id, dispatched_at_ms)
    if reply is None or not reply.strip():
        log.error("translation produced no reply (session %s, dispatch %s)",
                  session_id, dispatch_id)
        return None
    # The specialist's FINAL assistant text may arrive in two shapes:
    #   (a) the raw MCP tool-result envelope {"ok": true,
    #       "result": {"translation": "..."}} — a model that quotes the
    #       tool result verbatim, and
    #   (b) the translation itself, because the model followed the envelope
    #       instruction and replied with the translation text (observed live
    #       in production 2026-09-05 16:19-16:20 BKK).
    # Accept BOTH. A JSON object that is not a VALID ok:true envelope (or
    # relays ok:false) is a failure — never deliver a wrapped/partial
    # payload. Non-object text IS the translation; downstream gates
    # (error-shape, source-echo, failure-text) still apply fail-closed.
    r = reply.strip()
    if r.startswith("error:"):
        log.error("translate tool failed (dispatch %s): %.120s",
                  dispatch_id, r)
        return None
    translation: str | None = None
    if r.startswith("{"):
        try:
            parsed = json.loads(r)
        except json.JSONDecodeError:
            log.error("translate tool returned malformed envelope "
                      "(dispatch %s): %.120s", dispatch_id, r)
            return None
        if isinstance(parsed, dict):
            if parsed.get("ok") is True and isinstance(parsed.get("result"), dict):
                t = parsed["result"].get("translation")
                if isinstance(t, str) and t.strip():
                    translation = t
            else:
                log.error("translate tool returned invalid/failed envelope "
                          "(dispatch %s): %.120s", dispatch_id, r)
                return None
        else:
            translation = r  # JSON but not an object — treat as text
    else:
        translation = r  # plain final text = the translation itself
    if translation is None or not translation.strip():
        log.error("translate produced empty translation (dispatch %s)",
                  dispatch_id)
        return None
    if not translate_output_valid(translation, source):
        log.error("translate output invalid (echo shape) - treating as "
                  "failure (dispatch %s)", dispatch_id)
        return None
    return translation


# Increment 2b: outbound DSH-authorship memory (bounded metadata, no bodies).

def outbound_conv_key(target_id: str) -> str:
    """Canonical conversation key for the outbound-author store. Targets in
    this adapter appear both raw (LINE Push API 'to': U…/C…/R…, LINE's
    stable ID/type convention) and prefixed (internal keys: user:/group:/
    room:). Store and lookup MUST agree or reply-to-DSH can never match
    (INC2 live-acceptance finding 2026-09-05 18:00: capture stored raw,
    gate compared prefixed). Idempotent."""
    if not isinstance(target_id, str) or not target_id:
        return str(target_id)
    if target_id.startswith(("user:", "group:", "room:")):
        return target_id
    if target_id.startswith("U"):
        return f"user:{target_id}"
    if target_id.startswith("R"):
        return f"room:{target_id}"
    return f"group:{target_id}"


def record_outbound_ids(state: dict, target_id: str,
                        sent_ids: list) -> bool:
    """Record the sent-message IDs of OUR OWN push (authorship proof for
    reply-to-DSH). Minimum bounded metadata only: id -> (conversation key,
    epoch timestamp). NO message bodies, NO user content. Retention is a
    bounded OLDEST-FIRST store (DSH_OUTBOUND_MAX) - NO time-based TTL: a
    valid LINE Reply must never stop invoking DSH merely because the quoted
    message aged out (operator directive 2026-09-05); no LINE semantics
    require one. Persistence failure FAILS CLOSED (STATE_UNAVAILABLE): reply
    recognition becomes unavailable and dispatch is refused rather than
    running on unverifiable authorship memory. The real-self-mention path is
    metadata-only and unaffected."""
    with STATE_LOCK:
        ob = state.setdefault("dshOutbound", {})
        now = time.time()
        for sid in sent_ids:
            if isinstance(sid, str) and sid:
                ob[sid] = {"conv": outbound_conv_key(target_id), "at": now}
        # bounded oldest-first eviction (no TTL expiry)
        if len(ob) > DSH_OUTBOUND_MAX:
            for sid, _ in sorted(ob.items(),
                                 key=lambda kv: kv[1].get("at", 0))[
                                 :len(ob) - DSH_OUTBOUND_MAX]:
                ob.pop(sid, None)
        if not save_state(state):
            STATE_UNAVAILABLE.set()
            log.critical("outbound-author memory NOT persisted - reply-to-DSH "
                         "recognition fails closed (fail-closed)")
            return False
        return True


def dsh_sent_message_id(state: dict, quoted_id: str, conv_key: str) -> bool:
    """TRUE iff quoted_id refers to a message THIS adapter sent into the SAME
    conversation (metadata-only authorship proof). Unknown/stale/other-
    conversation ids -> False (reply stays silent). Both sides are passed
    through the canonical key form (INC2 live-acceptance fix 2026-09-05:
    capture previously stored raw ids while the gate compared prefixed
    keys — same-conversation match was structurally impossible; canonicaliz-
    ing the STORED side too rescues any raw entries written before v1.4.2)."""
    entry = state.get("dshOutbound", {}).get(str(quoted_id))
    if not isinstance(entry, dict):
        return False
    return outbound_conv_key(entry.get("conv", "")) == outbound_conv_key(conv_key)


def prune_outbound(state: dict) -> None:
    """Periodic retention bound for the outbound-author memory (main loop):
    bounded oldest-first only - no time-based expiry. Malformed entries are
    dropped as unmatchable metadata corruption."""
    with STATE_LOCK:
        ob = state.get("dshOutbound")
        if not ob:
            return
        changed = False
        for sid, e in list(ob.items()):
            if not isinstance(e, dict):
                ob.pop(sid, None)
                changed = True
        if len(ob) > DSH_OUTBOUND_MAX:
            for sid, _ in sorted(ob.items(),
                                 key=lambda kv: kv[1].get("at", 0))[
                                 :len(ob) - DSH_OUTBOUND_MAX]:
                ob.pop(sid, None)
                changed = True
        if changed:
            save_state(state)


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


def _push_claimed(state: dict, claim: dict, message_id: str) -> None:
    """Perform the push for a claimed delivery. Raises on failure. Captures
    the sent-message IDs from the Push response (sentMessages[].id) into the
    bounded outbound-author memory (reply-to-DSH proof; no bodies)."""
    resp = push_messages(claim["to"], chunk_reply_text(claim["reply"]),
                         retry_key_seed=message_id)
    sent = None
    if isinstance(resp, dict):
        sent = resp.get("sentMessages")
    if isinstance(sent, list) and sent:
        ids = [m.get("id") for m in sent if isinstance(m, dict)]
        log.info("push captured %d outbound id(s) for %s",
                 len([i for i in ids if i]), outbound_conv_key(claim["to"]))
        if not record_outbound_ids(state, claim["to"], [i for i in ids if i]):
            raise CredError("outbound-author memory not persisted")


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
        _push_claimed(state, claim, message_id)
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
                _push_claimed(state, claim, message_id)
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
                   message: dict, event: dict, dispatch_id: str,
                   person_id: str | None = None) -> str:
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
        "person_id": person_id,
        "dispatch_id": dispatch_id,
    }
    text = message.get("text") or ""
    if message.get("type") == "sticker":
        text = "(sent a sticker)"
    elif message.get("type") not in ("text", "sticker"):
        text = f"(sent a {message.get('type')} — media handling not enabled yet)"
    return f"[line message] {json.dumps(ctx, ensure_ascii=False)}\n{text}"


def handle_event(state: dict, event: dict) -> None:
    if STATE_UNAVAILABLE.is_set():
        log.error("state persistence unavailable - event DROPPED for redelivery "
                  "(fail-closed)")
        return
    # Trust boundary (Increment 1): (1) webhook signature/shape is verified in
    # the HTTP layer; (2) admission + mention classification happens here,
    # BEFORE any DSH session/model/tool work. Denied/unknown events never
    # reach get_or_create_session or session.prompt.
    adm, adm_ok = load_admission()
    if not adm_ok:
        log.critical("admission state unhealthy - event DROPPED for redelivery "
                     "(fail-closed, deny-all)")
        return
    decision = classify_event(event, adm)
    action = decision["action"]
    message = event.get("message") or {}
    message_id = str(message.get("id") or "")

    if action == "APPROVED_GROUP_MSG":
        # Operator directive 3: roster FIRST (identity bootstrap only - the
        # message body is never stored anywhere), then apply the mention gate
        # separately; ordinary chatter never creates/resumes a DSH session.
        target_id = decision["target_id"]
        source_type = decision["source_type"]
        user_id = decision["user_id"]
        if not target_id or not user_id:
            return

        def _roster_txn(adm):
            if group_state(adm, target_id) != "APPROVED":
                return adm, False
            adm, _ev = observe_group_message(adm, source_type, target_id,
                                             user_id)
            return adm, True

        adm_r, _roster_payload, roster_ok = transact_admission(_roster_txn)
        if adm_r is None or not roster_ok:
            log.critical("approved-group roster transaction failed/unhealthy "
                         "- message %s not rostered (fail-closed)", message_id)
            return
        conv_key = f"{source_type}:{target_id}"
        own_mentioned = bool(decision.get("self_mentioned"))
        # Increment 2b: second invocation form — a LINE Reply whose
        # quotedMessageId POSITIVELY refers to a message this adapter sent
        # into THIS conversation (bounded metadata memory, no bodies).
        # Operator correction (2026-09-05): the two signals are INDEPENDENT —
        # unrelated mention metadata (@All, other-member mentions) never
        # cancels a positively authenticated reply-to-DSH invocation.
        quoted = message.get("quotedMessageId")
        reply_invoked = bool(quoted) and dsh_sent_message_id(state, quoted,
                                                             conv_key)
        if reply_invoked:
            log.info("admission gate: approved-group reply-to-DSH dispatch "
                     "(sender %s rostered, quoted confirmed DSH outbound)",
                     user_id)
        elif own_mentioned:
            log.info("admission gate: approved-group own-mention dispatch "
                     "(sender %s rostered)", user_id)
        else:
            if quoted:
                log.info("admission gate: reply-to-DSH not invoked - "
                         "quotedMessageId %.8s not confirmed as DSH "
                         "outbound for %s", str(quoted), conv_key)
            log.info("admission gate: approved-group chatter rostered sender "
                     "%s; no own-mention/reply-to-DSH - silent (never "
                     "dispatched)", user_id)
            return
        # Increment 2: `translate <text>` in the SAME message as a DSH
        # invocation. Own-mention messages parse from the mention-stripped
        # remainder; reply-invoked messages parse from raw text (a literal
        # '@DSH' there is just text and will not parse — no string detection).
        if message.get("type") in TRANSLATE_TARGET_TYPES:
            stripped = strip_own_mention_text(message) if own_mentioned \
                else (message.get("text") or "")
            cmd = parse_translate_command(stripped)
            if cmd is not None:
                log.info("translation command in approved group "
                         "(sender %s, source_chars=%d)",
                         user_id, len(cmd["source"]))
                return handle_translate(state, message_id, target_id, user_id,
                                        source_type, cmd, event)
    elif action == "DISPATCH_DM":
        target_id = decision["target_id"]
        source_type = decision["source_type"]
        # Increment 2: admitted DMs need no mention; `translate <text>` must be
        # the START of the message. Non-command DMs stay on the normal path.
        if message.get("type") in TRANSLATE_TARGET_TYPES:
            cmd = parse_translate_command(message.get("text") or "")
            if cmd is not None:
                log.info("translation command in admitted DM "
                         "(sender %s, source_chars=%d)",
                         decision["user_id"], len(cmd["source"]))
                return handle_translate(state, message_id, target_id,
                                        decision["user_id"], source_type,
                                        cmd, event)
    else:
        # OBSERVE_PENDING (candidate capture / metadata refresh),
        # DENIED_DM, DENIED_GROUP, SKIP, SKIP_SILENT, standby, non-message.
        log.info("admission gate: %s (%s) src=%s target=%s user=%s",
                 action, decision["reason"], decision["source_type"],
                 decision["target_id"], decision["user_id"])
        if action == "OBSERVE_PENDING" and decision["target_id"]:
            stype = decision["source_type"] if decision["source_type"] in (
                "group", "room") else "group"
            summary = None
            if group_state(adm, decision["target_id"]) == "UNKNOWN":
                try:
                    raw = line_request("GET",
                                       f"/{stype}/{decision['target_id']}/summary",
                                       label="group-summary")
                    name = (raw or {}).get("groupName")
                    summary = {"name": name} if name else None
                except Exception:
                    summary = None
            captured = {}
            transact_admission(lambda a: _capture_txn(
                a, decision["source_type"], decision["target_id"],
                decision["user_id"], summary, captured))
            if captured.get("kind") == "REJECTED_EXTRA_GROUP":
                log.info("admission: extra group %s rejected "
                         "(single-family-group invariant) - leave requested",
                         decision["target_id"])
        return

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
            log.info("redelivery of undelivered msg %s - retrying push", message_id)
            deliver_reply(state, message_id, target_id, pending["reply"])
            return
        if pending:  # in flight elsewhere
            return

        user_id = decision["user_id"] or target_id
        group_id = target_id if source_type == "group" else None

        # admission revalidation + person binding in ONE transaction (review
        # fix): the dispatch decision is re-checked against the FINAL admission
        # state under the exclusive lock immediately BEFORE any DSH
        # session/model/tool work (rostering already happened above for
        # approved-group messages).
        def _dispatch_txn(adm):
            if source_type == "group" and group_state(adm, group_id) != "APPROVED":
                return adm, False
            if source_type == "user" and not line_user_dm_eligible(adm, user_id):
                log.critical("DM eligibility withdrawn before dispatch - "
                             "message %s aborted (fail-closed)", message_id)
                return adm, False
            p = person_for_line(adm, user_id)
            if p and p.get("__corrupt__"):
                log.critical("person binding corrupt - message %s NOT "
                             "dispatched (fail-closed)", message_id)
                return adm, False
            if p is None:
                np_ = new_person(adm, None)
                np_["bindings"]["line"].append(user_id)
                np_["dmEligible"]["line"] = True
                np_["updatedAt"] = admission_now()
                p = np_
            return adm, p

        adm_final, person, persisted = transact_admission(_dispatch_txn)
        if adm_final is None or not persisted or not person:
            log.critical("admission transaction failed/unhealthy or admission "
                         "withdrawn - message %s aborted (fail-closed)",
                         message_id)
            return

        display_name = get_display_name(source_type, group_id, user_id, state)

        sid = get_or_create_session(state, conv_key)
        if not sid:
            log.error("no DSH session for %s; message %s left UNMARKED",
                      conv_key, message_id)
            return

        dispatch_id = uuid.uuid4().hex
        envelope = build_envelope(source_type, target_id, display_name,
                                  message, event, dispatch_id,
                                  person_id=person.get("personId"))
        dispatched_at_ms = int(time.time() * 1000)
        try:
            dsh_rpc("session.prompt", {
                "sessionId": sid, "mode": "queue",
                "content": [{"type": "text", "text": envelope}],
                "clientTimeZone": CLIENT_TZ})
        except Exception as e:
            log.error("session.prompt failed for message %s (session %s): %s - "
                      "left UNMARKED (retryable on redelivery)",
                      message_id, sid, e.__class__.__name__)
            return
        if not mark_accepted(state, message_id):
            log.critical("msg %s prompted but acceptance state NOT persisted - "
                         "fail-closed engaged; reply delivery aborted for safety",
                         message_id)
            return
        log.info("dispatched line msg %s to session %s (dispatch %s)",
                 message_id, sid, dispatch_id)

        reply = wait_for_reply(sid, dispatch_id, dispatched_at_ms)
        if not reply:
            log.error("no reply extracted for line msg %s (session %s, "
                      "dispatch %s) - NOT retrying automatically",
                      message_id, sid, dispatch_id)
            return
        deliver_reply(state, message_id, target_id, reply)


def strip_own_mention_text(message: dict) -> str:
    """Remove ONLY the real own-mention span from the group text, using LINE's
    own mention metadata (index/range when present; the DSH-surface prefix
    fallback when LINE omits indices). Never consults the raw text to DETECT a
    mention (that stays `mention_gate_self`'s metadata-only job) — the span is
    stripped only after the gate has already passed."""
    text = message.get("text") or ""
    mentionees = ((message.get("mention") or {}).get("mentionees")) or []
    own = next((m for m in mentionees if isinstance(m, dict)
                and m.get("isSelf") is True), None)
    if own is None:
        return text
    idx, length = own.get("index"), own.get("length")
    if isinstance(idx, int) and isinstance(length, int) \
            and 0 <= idx and idx + length <= len(text):
        return (text[:idx] + text[idx + length:]).lstrip()
    # metadata present but no usable span: drop a leading @<surface> prefix
    return re.sub(r"^@\S+\s*", "", text).lstrip()


def handle_translate(state: dict, message_id: str, target_id: str,
                     user_id: str, source_type: str, cmd: dict,
                     event: dict) -> None:
    """Increment 2 translation dispatch. Runs strictly AFTER the Increment-1
    trust gate; same dedupe / eligibility-revalidation / person-binding
    protocol as normal dispatch. Empty source -> concise help, no specialist
    call. Specialist failure -> concise fixed failure line (NEVER a
    normal-model answer). F3 final-output filtering unchanged."""
    source = cmd.get("source") or ""
    if not source:
        with conversation_lock(f"{source_type}:{target_id}"):
            with STATE_LOCK:
                if message_id in state.get("delivered", []):
                    return
                if message_id in state.get("accepted", []):
                    pending = has_pending(state, message_id)
                    if pending and not pending.get("inFlight"):
                        log.info("translate redelivery: recovering help "
                                 "reply for %s (no re-dispatch)", message_id)
                        deliver_reply(state, message_id, target_id,
                                      pending["reply"])
                        return
                    log.info("translate dedupe skip (accepted): message %s",
                             message_id)
                    return
                if not mark_accepted(state, message_id):
                    return  # fail-closed (persistence failure already logged)
            ok = deliver_reply(state, message_id, target_id,
                               TRANSLATE_HELP_TEXT)
            if not ok:
                log.error("translate help push failed for msg %s (queued "
                          "for bounded retry)", message_id)
        return
    conv_key = f"{source_type}:{target_id}"
    with conversation_lock(conv_key):
        with STATE_LOCK:
            if message_id in state.get("delivered", []):
                return
            if message_id in state.get("accepted", []):
                # Hermes finding 1/2 fix: accepted-but-undelivered translate
                # reply (restart/redelivery window) recovers the DURABLE
                # pending reply — never a second specialist prompt.
                pending = has_pending(state, message_id)
                if pending and not pending.get("inFlight"):
                    log.info("translate redelivery: recovering undelivered "
                             "translation for %s (no re-prompt)", message_id)
                    deliver_reply(state, message_id, target_id,
                                  pending["reply"])
                    return
                log.info("translate dedupe skip (accepted): message %s",
                         message_id)
                return

        dispatch_id = uuid.uuid4().hex

        # admission revalidation + person binding in ONE transaction (same
        # protocol as normal dispatch): re-checked against FINAL admission
        # state under the exclusive lock immediately BEFORE any specialist
        # session/model work.
        def _translate_txn(adm):
            if source_type == "group" and group_state(adm, target_id) != "APPROVED":
                return adm, False
            if source_type == "user" and not line_user_dm_eligible(adm, user_id):
                log.critical("translate: DM eligibility withdrawn before "
                             "dispatch - message %s aborted (fail-closed)",
                             message_id)
                return adm, False
            p = person_for_line(adm, user_id)
            if p and p.get("__corrupt__"):
                log.critical("translate: person binding corrupt - message %s "
                             "NOT dispatched (fail-closed)", message_id)
                return adm, False
            if p is None:
                np_ = new_person(adm, None)
                np_["bindings"]["line"].append(user_id)
                np_["dmEligible"]["line"] = True
                np_["updatedAt"] = admission_now()
                p = np_
            return adm, p

        adm_final, person, persisted = transact_admission(_translate_txn)
        if adm_final is None or not persisted or not person:
            log.critical("translate: admission transaction failed/unhealthy - "
                         "message %s aborted (fail-closed)", message_id)
            return

        sid = get_or_create_session(state, conv_key)
        if not sid:
            log.error("translate: no DSH session for %s; message %s UNMARKED",
                      conv_key, message_id)
            return

        reply = translate_via_dsh(sid, dispatch_id, source)
        if reply is None:
            log.error("translation failed for line msg %s (session %s) - "
                      "concise failure via the SAME durable delivery path, "
                      "no model fallback", message_id, sid)
            reply = TRANSLATE_FAILURE_TEXT
        with STATE_LOCK:
            if not mark_accepted(state, message_id):
                return  # prompted but acceptance not durable -> fail-closed
        # Hermes round-1 findings 1-4 fix: NO bespoke marker. The translate
        # path resolves through the EXACT normal-delivery machinery —
        # durable pending record, claim/finish reservation, X-Line-Retry-Key
        # idempotent push, bounded background retry — so redelivery can never
        # re-prompt the specialist (accepted dedupe) and a failed push is
        # recovered durably exactly like any normal reply.
        deliver_reply(state, message_id, target_id, reply)


def _capture_txn(adm, source_type, target_id, user_id, summary, captured):
    """Candidate capture under the single-family-group invariant. No roster
    for PENDING/DECLINED (minimal metadata only)."""
    kind_state = group_state(adm, target_id) if target_id else "UNKNOWN"
    if kind_state in ("PENDING", "APPROVED"):
        captured["kind"] = "KNOWN"
        adm, _ = observe_group_message(adm, source_type, target_id, None,
                                       summary)  # summary refresh only
        return adm, True
    adm, ev = observe_group_message(adm, source_type, target_id, None, summary)
    captured["kind"] = ev
    return adm, True


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
    log.info("dsh-line-inbound v1.4.2 (identity+admission+translate+reply) "
             "listening on %s:%d%s",
             LISTEN_HOST, LISTEN_PORT, WEBHOOK_PATH)
    import threading as _t
    accept_thread = _t.Thread(target=server.serve_forever, daemon=True)
    accept_thread.start()
    retry = _t.Thread(target=pending_retry_loop,
                      args=(WebhookHandler.state, _stop_event), daemon=True)
    retry.start()
    maint = _t.Thread(target=admission_maintenance_loop,
                      args=(_stop_event,), daemon=True)
    maint.start()

    def _prune_loop():
        while not _stop_event.wait(3600.0):
            try:
                prune_outbound(WebhookHandler.state)
            except Exception as exc:
                log.error("outbound prune error: %s", exc.__class__.__name__)
    _t.Thread(target=_prune_loop, daemon=True).start()
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
    global STATE_DIR, STATE_PATH, ADMISSION_PATH, ADMISSION_LOCK_PATH
    STATE_DIR = Path(tempfile.mkdtemp(prefix="dsh-line-selftest"))
    STATE_PATH = STATE_DIR / "line-state.json"
    ADMISSION_PATH = STATE_DIR / "admission-state.json"
    ADMISSION_LOCK_PATH = STATE_DIR / "admission.lock"
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

    # 5. admission + mention trust gate (Increment 1)
    check("mention gate: isSelf dispatches", mention_gate_self([{"isSelf": True}]))
    check("mention gate: @All alone NEVER dispatches",
          not mention_gate_self([{"type": "all"}]))
    check("mention gate: other-user mention does not dispatch",
          not mention_gate_self([{"userId": "U9", "isSelf": False}]))
    check("mention gate: malformed mentionees fail closed",
          not mention_gate_self("x") and not mention_gate_self(None))

    def adm_with(gid, state, roster=None):
        a = json.loads(json.dumps(DEFAULT_ADMISSION))
        a["groups"][gid] = {"state": state, "kind": "group", "summary": None,
                            "firstSeen": admission_now(), "decidedAt": None,
                            "decidedBy": None, "roster": list(roster or []),
                            "leaveRequested": False, "left": False,
                            "leaveAttempts": 0}
        return a

    group_evt = {"mode": "active", "type": "message",
                 "source": {"type": "group", "groupId": "G1", "userId": "U1"},
                 "message": {"id": "m3", "type": "text", "text": "hi",
                             "mention": {"mentionees": [{"isSelf": True}]}}}
    d = classify_event(group_evt, adm_with("G1", "APPROVED"))
    check("approved group + isSelf -> dispatch (self_mentioned)",
          d["action"] == "APPROVED_GROUP_MSG" and d["self_mentioned"] is True)
    group_evt2 = json.loads(json.dumps(group_evt))
    group_evt2["message"]["mention"] = {"mentionees": [{"type": "all"}]}
    d2 = classify_event(group_evt2, adm_with("G1", "APPROVED"))
    check("approved group @All -> no dispatch (self_mentioned False)",
          d2["action"] == "APPROVED_GROUP_MSG" and d2["self_mentioned"] is False)
    group_evt3 = json.loads(json.dumps(group_evt))
    group_evt3["message"].pop("mention")
    group_evt3["message"]["text"] = "@DSH please"   # textual lookalike only
    d3 = classify_event(group_evt3, adm_with("G1", "APPROVED"))
    check("approved group literal @DSH text -> no dispatch",
          d3["action"] == "APPROVED_GROUP_MSG" and d3["self_mentioned"] is False)
    check("unapproved group + valid own-mention -> still no dispatch",
          classify_event(group_evt, adm_with("G1", "PENDING"))["action"]
          == "OBSERVE_PENDING")
    check("unknown group -> captured PENDING (never dispatched)",
          classify_event(group_evt, json.loads(json.dumps(DEFAULT_ADMISSION)))["action"]
          == "OBSERVE_PENDING")
    dm_evt = {"mode": "active", "type": "message",
              "source": {"type": "user", "userId": "U1"},
              "message": {"id": "m4", "type": "text", "text": "hello"}}
    a_dm = json.loads(json.dumps(DEFAULT_ADMISSION))
    a_dm["groups"]["Gok"] = {"state": "APPROVED", "kind": "group",
                             "summary": None, "firstSeen": admission_now(),
                             "decidedAt": None, "decidedBy": None,
                             "roster": ["U1"], "leaveRequested": False,
                             "left": False, "leaveAttempts": 0}
    check("family user (approved-group roster) DM dispatches without mention",
          classify_event({"mode": "active", "type": "message",
                          "source": {"type": "user", "userId": "U1"},
                          "message": {"id": "m4", "type": "text", "text": "x"}},
                         a_dm)["action"] == "DISPATCH_DM")
    check("unknown outsider DM -> denied",
          classify_event({"mode": "active", "type": "message",
                          "source": {"type": "user", "userId": "Ustranger"},
                          "message": {"id": "m5", "type": "text", "text": "hi"}},
                         json.loads(json.dumps(DEFAULT_ADMISSION)))["action"]
          == "DENIED_DM")
    standby_evt = {"mode": "standby", "type": "message", "source": {"type": "user",
                   "userId": "U1"}, "message": {"id": "m4", "type": "text", "text": "x"}}
    check("standby mode skipped", classify_event(standby_evt, a_dm)["action"] == "SKIP")

    # 5b. person identity semantics
    a_p = json.loads(json.dumps(DEFAULT_ADMISSION))
    p1 = new_person(a_p, "Test Person")
    p1["bindings"]["line"].append("Uline")
    check("person exists with LINE binding only", p1["bindings"]["discord"] == [])
    p2 = new_person(a_p, "Same Display Name")
    p2["bindings"]["line"].append("Uother")
    check("same-label persons stay separate (no name inference)",
          p1["personId"] != p2["personId"]
          and len(a_p["persons"]) == 2)
    p2["bindings"]["discord"].append("Ddisc")
    check("explicit second-channel binding lands on same person",
          len(p2["bindings"]["discord"]) == 1)
    check("person_for_line resolves bound user",
          person_for_line(a_p, "Uline")["personId"] == p1["personId"])
    a_p["persons"][p1["personId"]]["bindings"]["line"].append("Uline")
    check("duplicate binding corrupt -> fail-closed marker",
          person_for_line(a_p, "Uline").get("__corrupt__") is True)

    # 5c. malformed admission state -> deny-all
    bad = {"version": 1, "persons": "nope", "groups": {}, "dmGrants": {"line": {}}}
    check("malformed admission validation rejects", not _validate_admission(bad))
    good = json.loads(json.dumps(DEFAULT_ADMISSION))
    check("default admission validates", _validate_admission(good))

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

    # 12. admission persistence + fail-closed load
    a_save, ok1 = load_admission()
    check("empty admission store loads healthy deny-all", ok1)
    a_save = json.loads(json.dumps(DEFAULT_ADMISSION))
    a_save["groups"]["Gx"] = {"state": "PENDING", "kind": "group",
                              "summary": {"name": "Candidate"}, "firstSeen": admission_now(),
                              "decidedAt": None, "decidedBy": None, "roster": [],
                              "leaveRequested": False, "left": False,
                              "leaveAttempts": 0}
    check("save_admission persists", save_admission(a_save))
    a_back, ok2 = load_admission()
    check("admission round-trip",
          ok2 and group_state(a_back, "Gx") == "PENDING")
    ADMISSION_PATH.write_text("{ malformed ")
    _, ok3 = load_admission()
    check("malformed admission file -> unhealthy deny-all", not ok3)
    ADMISSION_PATH.unlink()
    a4, ok4 = load_admission()
    a4, k4 = observe_group_message(a4, "group", "Gnew", "U9",
                                   {"name": "Candidate Group"})
    check("unknown group observation -> NEW_PENDING",
          k4 == "NEW_PENDING" and group_state(a4, "Gnew") == "PENDING")

    # 13. Increment 2: translation command parser + mention-span strip
    check("translate: group-mode parser on stripped remainder",
          parse_translate_command("translate hello") == {"source": "hello"})
    check("translate: chat containing 'translate' NOT a command (group)",
          parse_translate_command("let's translate hello") is None)
    check("translate: bare 'translate' -> empty-source command",
          parse_translate_command("translate") == {"source": ""})
    check("translate: DM '@DSH translate hello' NOT silently stripped",
          parse_translate_command("@DSH translate hello") is None)
    check("translate: DM non-command containing the word -> None",
          parse_translate_command("how do I translate hello?") is None)
    check("translate: case-insensitive + margin whitespace",
          parse_translate_command("Translate  hello ") == {"source": "hello"})
    check("translate: multiline source preserved",
          parse_translate_command("translate line1\nline2") == {"source": "line1\nline2"})
    check("translate: whitespace-only source -> empty-source command",
          parse_translate_command("translate    ") == {"source": ""})
    check("translate: oversized source rejected",
          parse_translate_command("translate " + "x" * (TRANSLATE_CMD_MAX + 1)) is None)
    check("translate: max-boundary source accepted",
          parse_translate_command("translate " + "x" * TRANSLATE_CMD_MAX) is not None)
    check("translate: non-string input -> None",
          parse_translate_command(None) is None)
    check("translate: 'translates hello' NOT a command (word boundary)",
          parse_translate_command("translates hello") is None)
    check("translate: 'translator hello' NOT a command",
          parse_translate_command("translator hello") is None)
    check("translate: strip helper uses mention index/length",
          strip_own_mention_text({"text": "@DSH translate hi",
                                  "mention": {"mentionees": [
                                      {"isSelf": True, "index": 0, "length": 4}]}})
          == "translate hi")
    check("translate: strip helper prefix fallback (no indices)",
          strip_own_mention_text({"text": "@DSH translate hi",
                                  "mention": {"mentionees": [{"isSelf": True}]}})
          == "translate hi")
    check("translate: strip helper keeps OTHER-user mention span",
          strip_own_mention_text({"text": "@Alice translate hi",
                                  "mention": {"mentionees": [
                                      {"userId": "Ux", "isSelf": False,
                                       "index": 0, "length": 6}]}})
          == "@Alice translate hi")

    # 14. Increment 2b: outbound-authorship memory + reply-to-DSH gate
    st = load_state()
    check("outbound: record returns True on healthy persistence",
          record_outbound_ids(st, "group:Gx", ["M-dsh-1", "M-dsh-2"]) is True)
    check("outbound: sent ids recorded per conversation",
          dsh_sent_message_id(st, "M-dsh-1", "group:Gx")
          and dsh_sent_message_id(st, "M-dsh-2", "group:Gx"))
    check("outbound: unknown quoted id -> False",
          not dsh_sent_message_id(st, "M-unknown", "group:Gx"))
    check("outbound: other-conversation id -> False",
          not dsh_sent_message_id(st, "M-dsh-1", "group:Gother"))
    check("outbound: malformed entry ignored",
          not dsh_sent_message_id(st, "junk", "group:Gx"))
    # conv-key canonicalization (INC2 v1.4.2): raw Push targets and prefixed
    # internal keys must map to ONE canonical form, all LINE id kinds.
    check("convkey: raw U id -> user: form",
          outbound_conv_key("U123abc") == "user:U123abc")
    check("convkey: raw C id -> group: form",
          outbound_conv_key("C456def") == "group:C456def")
    check("convkey: raw R id -> room: form",
          outbound_conv_key("R789ghi") == "room:R789ghi")
    check("convkey: prefixed user: unchanged (idempotent)",
          outbound_conv_key("user:U123abc") == "user:U123abc")
    check("convkey: prefixed group: unchanged (idempotent)",
          outbound_conv_key("group:C456def") == "group:C456def")
    check("convkey: prefixed room: unchanged (idempotent)",
          outbound_conv_key("room:R789ghi") == "room:R789ghi")
    st_raw = {"dshOutbound": {"M-x": {"conv": "C456def", "at": 0.0}}}
    check("convkey: legacy raw stored entry matches prefixed lookup (rescue)",
          dsh_sent_message_id(st_raw, "M-x", "group:C456def"))
    st_amb = {"dshOutbound": {"M-y": {"conv": "group:U999zzz", "at": 0.0}}}
    check("convkey: prefixed group key never matches a user conversation",
          not dsh_sent_message_id(st_amb, "M-y", "user:U999zzz"))
    # deterministic empty-store baseline for retention tests
    st["dshOutbound"] = {}
    st["dshOutbound"]["M-keep"] = {"conv": "group:Gx", "at": time.time()}
    prune_outbound(st)
    check("outbound: prune keeps valid entries (in-memory)",
          "M-keep" in st["dshOutbound"])
    # ancient entry KEPT: retention is bounded count only (no time TTL)
    st["dshOutbound"]["M-ancient"] = {"conv": "group:Gx",
                                      "at": time.time() - 90 * 86400}
    prune_outbound(st)
    check("outbound: ancient entry KEPT (no time-based TTL)",
          "M-ancient" in st["dshOutbound"])
    # persistence: explicit save then reload (prune saves only on change)
    save_state(st)
    check("outbound: retained entries persist to disk",
          "M-keep" in load_state()["dshOutbound"]
          and "M-ancient" in load_state()["dshOutbound"])
    # bounded oldest-first eviction when full
    st["dshOutbound"] = {f"M{i}": {"conv": "group:Gx", "at": 1000 + i}
                         for i in range(DSH_OUTBOUND_MAX)}
    st["dshOutbound"]["M-oldest"] = {"conv": "group:Gx", "at": 1}
    st["dshOutbound"]["M-newest"] = {"conv": "group:Gx", "at": 9e9}
    record_outbound_ids(st, "group:Gx", ["M-trigger"])
    ob = load_state()["dshOutbound"]
    check("outbound: full store evicts OLDEST deterministically",
          len(ob) <= DSH_OUTBOUND_MAX and "M-oldest" not in ob
          and "M-newest" in ob and "M-trigger" in ob)
    # malformed entries pruned
    st["dshOutbound"]["M-bad"] = {"oops": 1}
    prune_outbound(st)
    save_state(st)
    check("outbound: malformed entries pruned",
          "M-bad" not in load_state()["dshOutbound"])
    # restart durability: entry persisted in line-state.json survives reload
    st2 = load_state()
    check("outbound: persisted across load_state (restart durability)",
          dsh_sent_message_id(st2, "M-trigger", "group:Gx"))
    # load_state normalization drops foreign malformed entries
    st3 = load_state()
    st3["dshOutbound"]["M-bad2"] = "not-a-dict"
    mod_normalize = load_state()
    check("outbound: load_state drops malformed foreign entries",
          isinstance(mod_normalize["dshOutbound"], dict)
          and "M-bad2" not in load_state()["dshOutbound"])

    print(f"  result: {'ALL PASS' if not failures else f'FAILURES: {failures}'}")
    return 0 if not failures else 1


if __name__ == "__main__":
    main()
