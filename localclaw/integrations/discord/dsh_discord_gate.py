#!/usr/bin/python3
"""dsh-discord gate — root-side privileged Discord sender for the DSH integration.

Runs as uid dsh-discord via systemd socket activation (dsh-discord.socket).
Listens on AF_UNIX /run/dsh-discord/gate.sock (group dsh, 0660) so only uid dsh
(the DSH runtime) and root can connect.

Trust model:
- Bot token ONLY via systemd LoadCredential (discord-bot-token). Never logged,
  never echoed, never included in error payloads.
- Discord REST API v10 only (fixed base URL constant). No gateway/WSS.
- Well-defined op set (normal bot capability); Discord's own permissions are the
  channel authority boundary — no software channel pinning.
- One request per connection: newline-delimited JSON in, newline-delimited JSON out.
"""

import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

API_BASE = "https://discord.com/api/v10"
SOCK_PATH = "/run/dsh-discord/gate.sock"
CRED_NAME = "discord-bot-token"
MAX_MSG = 1900          # per-message content chunk bound (Discord limit 2000)
MAX_CHUNKS = 10
MAX_EMBEDS = 10
MAX_FILES = 5
MAX_FILE_BYTES = 24 * 1024 * 1024   # per-attachment read cap (rejected, not truncated)
MAX_REACTIONS_BODY = 256
HTTP_TIMEOUT = 30
RATE_RETRIES = 4
# Approved attachment source roots (finding M6 + operator correction §7):
# the gate may only read files under these FIXED roots — realpath-contained,
# symlink-proof, size-capped. Roots reflect the actual DSH artifact
# architecture: /opt/dsh/home (DSH runtime home; readable today) and
# /mnt/off-vm-nfs/comfyui-media (canonical multimedia artifact tree:
# image/video/music/composite). NOTE: reads under the NFS root additionally
# require a filesystem-level read grant for uid dsh-discord (currently
# vincent:vincent 0770) — that grant is an operator-gated host change
# outside this artifact set and is documented in the reviewer package.
ALLOWED_ATTACHMENT_ROOTS = (
    "/opt/dsh/home",
    "/mnt/off-vm-nfs/comfyui-media",
)
# Precompute contained realpaths once (root must exist at gate start).
_ATTACHMENT_ROOTS = tuple(os.path.realpath(r) for r in ALLOWED_ATTACHMENT_ROOTS)

CHANNEL_ID_RE = __import__("re").compile(r"^[0-9]{5,25}$")
MESSAGE_ID_RE = CHANNEL_ID_RE
THREAD_ID_RE = CHANNEL_ID_RE


class GateError(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg


def load_token():
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if not cred_dir:
        raise GateError("internal", "CREDENTIALS_DIRECTORY not set (must run under systemd LoadCredential)")
    path = os.path.join(cred_dir, CRED_NAME)
    try:
        with open(path, "rb") as f:
            token = f.read().decode("utf-8", "strict").strip()
    except OSError:
        raise GateError("internal", "credential unreadable")
    if not token:
        raise GateError("internal", "credential empty")
    return token


TOKEN = None  # lazy


def discord_request(method, path, *, json_body=None, data=None, headers=None,
                    content_type=None, raw_ok=False):
    """Single REST call with rate-limit handling. Returns parsed JSON (or None).
    Raises GateError("discord_denied"/"discord_not_found"/"discord_error") on
    failure — never includes request internals in the error text."""
    global TOKEN
    if TOKEN is None:
        TOKEN = load_token()
    url = API_BASE + path
    hdrs = {
        "Authorization": f"Bot {TOKEN}",
        "User-Agent": "DSH-Discord-Gate (localclaw-vm, 1.0)",
        "Accept": "application/json",
    }
    body = None
    if json_body is not None:
        body = json.dumps(json_body).encode()
        hdrs["Content-Type"] = "application/json"
    elif data is not None:
        body = data
        hdrs["Content-Type"] = content_type or "application/octet-stream"
    if headers:
        hdrs.update(headers)
    for attempt in range(RATE_RETRIES):
        req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                payload = resp.read()
                if raw_ok:
                    return resp.status, payload
                if not payload:
                    return resp.status, None
                return resp.status, json.loads(payload)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", "replace")[:300]
            if e.code == 429:
                retry_after = 1.0
                try:
                    retry_after = float(json.loads(err_body).get("retry_after", 1.0))
                except Exception:
                    pass
                if attempt < RATE_RETRIES - 1:
                    time.sleep(min(retry_after, 10.0) + 0.25)
                    continue
                raise GateError("rate_limited", "discord rate limit persisted")
            if e.code in (401, 403):
                raise GateError("discord_denied", f"discord refused the operation ({e.code})")
            if e.code == 404:
                raise GateError("discord_not_found", "discord target not found")
            raise GateError("discord_error", f"discord error {e.code}")
        except urllib.error.URLError:
            # Sanitized diagnostics only: the remote/transport error detail is
            # deliberately discarded (reviewer Mi7), never stored or returned.
            if attempt < RATE_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
    raise GateError("network", "discord unreachable")


def require(req, *fields):
    for f in fields:
        if f not in req or not isinstance(req[f], (str, int)):
            raise GateError("bad_request", f"missing/invalid field: {f}")


def chan_id(value, field="channel_id"):
    v = str(value)
    if not CHANNEL_ID_RE.match(v):
        raise GateError("bad_request", f"invalid {field}")
    return v


def _validate_attachment(p):
    """Validate one attachment path: absolute, realpath-contained in one of
    ALLOWED_ATTACHMENT_ROOTS, size-capped. Returns the resolved real path."""
    if not isinstance(p, str) or not p.startswith("/"):
        raise GateError("bad_request", "files must be absolute paths")
    real = os.path.realpath(p)
    for root in _ATTACHMENT_ROOTS:
        if real == root or real.startswith(root + os.sep):
            break
    else:
        raise GateError("bad_request", "file outside approved attachment roots")
    try:
        if os.path.getsize(real) > MAX_FILE_BYTES:
            raise GateError("bad_request", "file exceeds attachment size cap")
    except OSError:
        raise GateError("bad_request", "unreadable file")
    return real


def op_send_message(req):
    """Normal message send: content and/or embeds and/or files, optional reply,
    optional thread destination. Long content is chunked sequentially."""
    channel_id = chan_id(req.get("channel_id", ""))
    dest = chan_id(req["thread_id"], "thread_id") if req.get("thread_id") else channel_id
    content = req.get("content")
    embeds = req.get("embeds")
    files = req.get("files")
    reply_to = req.get("reply_to")
    if content is None and not embeds and not files:
        raise GateError("bad_request", "message requires content, embeds, or files")
    if content is not None and not isinstance(content, str):
        raise GateError("bad_request", "content must be a string")
    if embeds is not None:
        if not isinstance(embeds, list) or len(embeds) > MAX_EMBEDS or not embeds:
            raise GateError("bad_request", f"embeds must be a non-empty list of at most {MAX_EMBEDS}")
    if files is not None:
        if not isinstance(files, list) or not files or len(files) > MAX_FILES:
            raise GateError("bad_request", f"files must be a non-empty list of at most {MAX_FILES}")
        validated = [_validate_attachment(p) for p in files]
    chunks = [content] if content is not None else [None]
    if content and len(content) > MAX_MSG:
        chunks = []
        remaining = content
        while remaining and len(chunks) < MAX_CHUNKS:
            cut = min(MAX_MSG, len(remaining))
            if len(remaining) > MAX_MSG:
                cut = remaining.rfind("\n", 0, MAX_MSG)
                if cut < MAX_MSG // 2:
                    cut = MAX_MSG
            chunks.append(remaining[:cut])
            remaining = remaining[cut:]
        if remaining:
            raise GateError("bad_request", f"content exceeds {MAX_CHUNKS * MAX_MSG} chars")
    results = []
    total = len(chunks) * (1 if not files else 0) if not files else len(files)
    sent = 0
    for i, chunk in enumerate(chunks):
        payload = {}
        if chunk:
            payload["content"] = chunk if len(chunks) == 1 else f"{chunk} ({i + 1}/{len(chunks)})"
        if embeds and i == 0:
            payload["embeds"] = embeds
        if reply_to and i == 0:
            payload["message_reference"] = {"message_id": str(reply_to)}
        if files and i == 0:
            # multipart upload of all files with the first part
            boundary = "dshgate" + uuid.uuid4().hex
            parts = []
            if payload:
                parts.append(
                    f'--{boundary}\r\nContent-Disposition: form-data; name="payload_json"\r\n'
                    f"Content-Type: application/json\r\n\r\n{json.dumps(payload)}\r\n".encode()
                )
            for idx, fp in enumerate(validated):
                try:
                    with open(fp, "rb") as fh:
                        blob = fh.read(MAX_FILE_BYTES)
                except OSError:
                    raise GateError("bad_request", f"unreadable file: {os.path.basename(fp)}")
                fname = os.path.basename(fp) or "attachment.bin"
                parts.append(
                    f"--{boundary}\r\nContent-Disposition: form-data; name=\"files[{idx}]\"; "
                    f"filename=\"{fname}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode()
                    + blob + b"\r\n"
                )
            parts.append(f"--{boundary}--\r\n".encode())
            body = b"".join(parts)
            status, data = discord_request(
                "POST", f"/channels/{dest}/messages", data=body,
                content_type=f"multipart/form-data; boundary={boundary}")
        else:
            status, data = discord_request("POST", f"/channels/{dest}/messages", json_body=payload)
        sent += 1
        results.append({"status": status,
                        "message_id": (data or {}).get("id") if isinstance(data, dict) else None})
    return {"sent": sent, "messages": results}


def op_edit_message(req):
    require(req, "channel_id", "message_id")
    cid = chan_id(req["channel_id"])
    mid = chan_id(req["message_id"], "message_id")
    payload = {}
    if isinstance(req.get("content"), str):
        payload["content"] = req["content"]
    if isinstance(req.get("embeds"), list):
        payload["embeds"] = req["embeds"]
    if not payload:
        raise GateError("bad_request", "edit requires content and/or embeds")
    status, data = discord_request("PATCH", f"/channels/{cid}/messages/{mid}", json_body=payload)
    return {"status": status, "edited": isinstance(data, dict)}


def op_delete_message(req):
    require(req, "channel_id", "message_id")
    status, _ = discord_request("DELETE",
        f"/channels/{chan_id(req['channel_id'])}/messages/{chan_id(req['message_id'], 'message_id')}",
        raw_ok=True)
    return {"status": status, "deleted": True}


def op_read_messages(req):
    require(req, "channel_id")
    cid = chan_id(req["channel_id"])
    limit = req.get("limit", 50)
    if not isinstance(limit, int) or not (1 <= limit <= 100):
        raise GateError("bad_request", "limit must be 1..100")
    q = {"limit": str(limit)}
    for k in ("before", "after", "around"):
        if req.get(k):
            q[k] = chan_id(req[k], k)
    status, data = discord_request("GET",
        f"/channels/{cid}/messages?{urllib.parse.urlencode(q)}")
    if data is not None and not isinstance(data, list):
        raise GateError("discord_error", "unexpected discord response")
    out = []
    for m in data or []:
        out.append({
            "id": m.get("id"),
            "author": (m.get("author") or {}).get("username"),
            "author_id": (m.get("author") or {}).get("id"),
            "bot": (m.get("author") or {}).get("bot", False),
            "content": m.get("content"),
            "timestamp": m.get("timestamp"),
            "attachments": [a.get("filename") for a in m.get("attachments", [])],
        })
    return {"status": status, "messages": out}


def _reaction_op(req, method):
    require(req, "channel_id", "message_id", "emoji")
    cid = chan_id(req["channel_id"])
    mid = chan_id(req["message_id"], "message_id")
    emoji = urllib.parse.quote(str(req["emoji"])[:MAX_REACTIONS_BODY], safe="")
    who = "@me" if not req.get("user") else f"{chan_id(req['user'], 'user')}"
    status, _ = discord_request(method, f"/channels/{cid}/messages/{mid}/reactions/{emoji}/{who}", raw_ok=True)
    return {"status": status}


def op_add_reaction(req):
    return _reaction_op(req, "PUT")


def op_remove_reaction(req):
    return _reaction_op(req, "DELETE")


def op_pin_message(req):
    require(req, "channel_id", "message_id")
    cid = chan_id(req["channel_id"])
    mid = chan_id(req["message_id"], "message_id")
    method = "DELETE" if req.get("unpin") else "PUT"
    status, _ = discord_request(method, f"/channels/{cid}/messages/{mid}/pins", raw_ok=True)
    return {"status": status, "pinned": method == "PUT"}


def op_create_thread(req):
    require(req, "channel_id", "message_id", "name")
    cid = chan_id(req["channel_id"])
    mid = chan_id(req["message_id"], "message_id")
    name = str(req["name"])[:100]
    status, data = discord_request("POST", f"/channels/{cid}/messages/{mid}/threads",
                                   json_body={"name": name, "auto_archive_duration": 1440})
    return {"status": status, "thread_id": (data or {}).get("id") if isinstance(data, dict) else None}


def op_get_context(req):
    """Live identity/reach inventory. Read-only."""
    out = {}
    want_gid = chan_id(req["guild_id"], "guild_id") if req.get("guild_id") else None
    status, me = discord_request("GET", "/users/@me")
    out["bot"] = {"id": (me or {}).get("id"), "username": (me or {}).get("username")}
    status, guilds = discord_request("GET", "/users/@me/guilds")
    if guilds is not None and not isinstance(guilds, list):
        raise GateError("discord_error", "unexpected discord response")
    glist = []
    for g in guilds or []:
        entry = {"id": g.get("id"), "name": g.get("name"),
                 "permissions": g.get("permissions"), "owner": g.get("owner")}
        if want_gid and str(want_gid) == str(g.get("id")):
            st, channels = discord_request("GET", f"/guilds/{g['id']}/channels")
            entry["channels"] = [
                {"id": c.get("id"), "name": c.get("name"), "type": c.get("type"),
                 "permission_overwrites": len(c.get("permission_overwrites", []))}
                for c in channels or []
            ]
        glist.append(entry)
    out["guilds"] = glist
    return out


def op_set_typing(req):
    require(req, "channel_id")
    status, _ = discord_request("POST", f"/channels/{chan_id(req['channel_id'])}/typing", raw_ok=True)
    return {"status": status}


OPS = {
    "send_message": op_send_message,
    "edit_message": op_edit_message,
    "delete_message": op_delete_message,
    "read_messages": op_read_messages,
    "add_reaction": op_add_reaction,
    "remove_reaction": op_remove_reaction,
    "pin_message": op_pin_message,
    "create_thread": op_create_thread,
    "get_context": op_get_context,
    "set_typing": op_set_typing,
}


def handle_line(line: bytes) -> bytes:
    """Process one newline-delimited JSON request -> one JSON response line."""
    try:
        req = json.loads(line)
        if not isinstance(req, dict):
            raise GateError("bad_request", "request must be a JSON object")
        op = req.get("op")
        if op not in OPS:
            raise GateError("bad_request", f"unknown op: {op}")
        result = OPS[op](req)
        resp = {"ok": True, "op": op, "result": result}
    except GateError as e:
        resp = {"ok": False, "error": {"code": e.code, "message": e.msg}}
    except Exception:
        resp = {"ok": False, "error": {"code": "internal", "message": "internal gate error"}}
    return (json.dumps(resp) + "\n").encode()


def serve_connection(conn):
    try:
        rfile = conn.makefile("rb")
        wfile = conn.makefile("wb")
        for raw in rfile:
            raw = raw.strip()
            if not raw:
                continue
            wfile.write(handle_line(raw))
            wfile.flush()
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def get_listen_fd():
    """systemd socket activation: first passed fd."""
    pid = os.environ.get("LISTEN_PID")
    if pid and int(pid) == os.getpid():
        return 3
    return None


def main():
    fd = get_listen_fd()
    if fd is not None:
        srv = socket.socket(fileno=fd)
    else:
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(SOCK_PATH)
        os.chmod(SOCK_PATH, 0o660)
    srv.listen(16)
    while True:
        try:
            conn, _ = srv.accept()
        except OSError:
            continue
        serve_connection(conn)


if __name__ == "__main__":
    main()
