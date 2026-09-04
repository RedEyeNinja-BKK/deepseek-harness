#!/usr/bin/env python3
"""dsh_discord_inbound.py — minimal Discord Gateway listener for DSH-Edge.

One narrow job: receive Discord inbound messages for the existing Phase-4 bot
(your-bot-user-id / DSH-Edge), normalize them, and feed them into DSH via
the public session RPC (session.create / session.prompt). DSH itself replies
through the EXISTING Phase-4 Discord REST MCP (mcp__discord__send_message) —
this listener NEVER sends to Discord.

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
import json
import logging
import os
import sys
import time
from pathlib import Path

import discord

# --- constants ----------------------------------------------------------------

# Deployment-specific: set to your bot's Discord user ID.
EXPECTED_BOT_ID = 0
# Deployment-specific: DSH replies to EVERY message (no mention needed) in this
# Discord CATEGORY and all of its child channels (no mention needed). Set to a
# real category snowflake to enable, or 0 to disable the always-respond policy.
ALWAYS_RESPOND_CATEGORY_ID = 0
DSH_API = "http://127.0.0.1:3080/api/"
# Persistent state: systemd StateDirectory (STATE_DIRECTORY env is set by the
# unit; /var/lib/dsh-discord-inbound) — survives listener restart, service
# restart, and reboots. Local fallback for manual (non-systemd) test runs.
_state_dir_env = os.environ.get("STATE_DIRECTORY")
STATE_DIR = Path(_state_dir_env.split(":")[0]) if _state_dir_env else Path("/var/lib/dsh-discord-inbound")
STATE_PATH = STATE_DIR / "inbound-state.json"
LIBS = "/opt/dsh-inbound"

# make vendored libs importable BEFORE discord import
sys.path.insert(0, LIBS)

import discord  # noqa: E402
import urllib.request  # noqa: E402

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


def save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, STATE_PATH)


def remember_message(state: dict, message_id: int) -> None:
    state.setdefault("processed", [])
    state["processed"].append(str(message_id))
    state["processed"] = state["processed"][-500:]  # bounded replay window
    save_state(state)


def already_processed(state: dict, message_id: int) -> bool:
    return str(message_id) in state.get("processed", [])


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
    save_state(state)
    log.info("created session %s for %s", new_sid, map_key)
    return new_sid


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
        files = [a.url for a in message.attachments[:5]]
        lines.append(f"[attachments] {json.dumps(files)}")
    if not content and message.attachments:
        lines.append("(no text content; see attachments)")
    lines.append(content)
    return "\n".join(lines)


async def dispatch_to_dsh(state: dict, message: discord.Message) -> None:
    key = conversation_key(message)
    sid = get_or_create_session(state, key)
    if not sid:
        log.error("no DSH session for %s; message %s left UNMARKED (retryable "
                  "on gateway replay)", key, message.id)
        return
    envelope = build_envelope(message)
    try:
        # queue mode: if DSH is mid-turn, the prompt splices into next-turn inbox
        resp = dsh_rpc("session.prompt", {
            "sessionId": sid, "mode": "queue",
            "content": [{"type": "text", "text": envelope}],
            "clientTimeZone": "Asia/Bangkok"})
        if not resp.get("result", {}).get("ok"):
            raise RuntimeError(json.dumps(resp)[:300])
        log.info("dispatched msg %s to %s", message.id, sid)
    except Exception:
        log.exception("session.prompt failed for message %s (session %s); "
                      "left UNMARKED (retryable on gateway replay)",
                      message.id, sid)
        # dedupe advances only on confirmed acceptance (below): a failed RPC
        # leaves the message retryable if Discord replays the event.
        return
    remember_message(state, message.id)


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
        intents = discord.Intents.none()
        intents.guilds = True            # guild/channel context
        intents.guild_messages = True    # guild MESSAGE_CREATE
        intents.dm_messages = True       # DMs
        intents.message_content = True   # request; auto-degrades if not granted
        super().__init__(intents=intents)
        self.state = state

    async def setup_hook(self) -> None:
        # keep our own presence explicit; do not auto-sync anything
        log.info("gateway client initializing")

    async def on_ready(self):
        if self.user is None or self.user.id != EXPECTED_BOT_ID:
            log.critical("READY identity mismatch: %s — ABORTING (expected %s)",
                         getattr(self.user, "id", None), EXPECTED_BOT_ID)
            await self.close()
            os._exit(2)
        log.info("READY as %s (%s) — ONLINE; guilds=%d",
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
