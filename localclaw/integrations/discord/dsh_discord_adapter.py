#!/usr/bin/python3
"""dsh-discord adapter — zero-secret stdio MCP bridge for DSH.

Runs as uid dsh (child of the DSH runtime). Speaks newline-delimited JSON-RPC 2.0
(MCP stdio transport) on stdin/stdout, and forwards tools/call requests to the
privileged dsh-discord gate over AF_UNIX /run/dsh-discord/gate.sock.

Holds NO credentials. The gate holds the token; this process only relays
well-formed operation requests. Model-visible surface = the 10 Discord tools.
"""

import json
import os
import socket
import sys

GATE_SOCK = "/run/dsh-discord/gate.sock"
PROTOCOL = "2024-11-05"

TOOLS = [
    ("send_message", "Send a message to a Discord channel/thread (text/markdown, embeds, attachments, replies). Long text is chunked automatically.",
     {"type": "object", "properties": {
         "channel_id": {"type": "string", "description": "Discord channel ID (snowflake digits)"},
         "content": {"type": "string", "description": "Message text (markdown allowed)"},
         "embeds": {"type": "array", "description": "Up to 10 Discord embed objects"},
         "files": {"type": "array", "items": {"type": "string"}, "description": "Absolute file paths to attach (max 5; must be under the approved attachment roots: /opt/dsh/home or /mnt/off-vm-nfs/comfyui-media)"},
         "reply_to": {"type": "string", "description": "Message ID to reply to"},
         "thread_id": {"type": "string", "description": "Thread channel ID to post into"},
     }, "required": ["channel_id"]}),
    ("edit_message", "Edit one of this bot's previously sent messages (content and/or embeds).",
     {"type": "object", "properties": {
         "channel_id": {"type": "string"}, "message_id": {"type": "string"},
         "content": {"type": "string"}, "embeds": {"type": "array"}},
      "required": ["channel_id", "message_id"]}),
    ("delete_message", "Delete a message (own messages, or others where Discord permissions allow).",
     {"type": "object", "properties": {"channel_id": {"type": "string"}, "message_id": {"type": "string"}},
      "required": ["channel_id", "message_id"]}),
    ("read_messages", "Read recent messages/history from a channel (channel context). Content of messages authored by others requires the bot's MESSAGE CONTENT intent.",
     {"type": "object", "properties": {
         "channel_id": {"type": "string"},
         "limit": {"type": "integer", "description": "1..100, default 50"},
         "before": {"type": "string"}, "after": {"type": "string"}, "around": {"type": "string"}},
      "required": ["channel_id"]}),
    ("add_reaction", "Add a reaction (emoji) to a message.",
     {"type": "object", "properties": {"channel_id": {"type": "string"}, "message_id": {"type": "string"}, "emoji": {"type": "string"}},
      "required": ["channel_id", "message_id", "emoji"]}),
    ("remove_reaction", "Remove a reaction (own by default; pass user where permitted).",
     {"type": "object", "properties": {"channel_id": {"type": "string"}, "message_id": {"type": "string"}, "emoji": {"type": "string"}, "user": {"type": "string"}},
      "required": ["channel_id", "message_id", "emoji"]}),
    ("pin_message", "Pin or unpin a message in a channel.",
     {"type": "object", "properties": {"channel_id": {"type": "string"}, "message_id": {"type": "string"}, "unpin": {"type": "boolean"}},
      "required": ["channel_id", "message_id"]}),
    ("create_thread", "Create a thread from a message.",
     {"type": "object", "properties": {"channel_id": {"type": "string"}, "message_id": {"type": "string"}, "name": {"type": "string"}},
      "required": ["channel_id", "message_id", "name"]}),
    ("get_context", "List guilds/channels the bot can access (live reach inventory); pass guild_id for its channel list.",
     {"type": "object", "properties": {"guild_id": {"type": "string"}}, "required": []}),
    ("set_typing", "Show a typing indicator in a channel (brief).",
     {"type": "object", "properties": {"channel_id": {"type": "string"}}, "required": ["channel_id"]}),
]


def gate_call(payload):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(180)
    try:
        s.connect(GATE_SOCK)
        s.sendall((json.dumps(payload) + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    finally:
        s.close()
    if not buf.strip():
        return {"ok": False, "error": {"code": "gate_unreachable", "message": "no reply from gate"}}
    try:
        return json.loads(buf.split(b"\n")[0])
    except json.JSONDecodeError:
        return {"ok": False, "error": {"code": "gate_protocol", "message": "malformed gate reply"}}


def tool_result(payload):
    reply = gate_call(payload)
    if reply.get("ok"):
        return {"content": [{"type": "text", "text": json.dumps(reply.get("result", {}))}],
                "isError": False}
    err = reply.get("error", {})
    return {"content": [{"type": "text",
                         "text": f"error: {err.get('code')}: {err.get('message')}"}],
            "isError": True}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = msg.get("method")
        rid = msg.get("id")
        if method == "initialize":
            result = {"protocolVersion": PROTOCOL,
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "dsh-discord", "version": "1.0.0"}}
        elif method == "tools/list":
            result = {"tools": [{"name": n, "description": d, "inputSchema": sc}
                                for n, d, sc in TOOLS]}
        elif method == "tools/call":
            params = msg.get("params", {})
            name = params.get("name", "")
            args = params.get("arguments", {}) or {}
            found = False
            for n, _, _ in TOOLS:
                if n == name:
                    found = True
                    break
            if not found:
                result = {"content": [{"type": "text", "text": f"error: bad_request: unknown tool {name}"}],
                          "isError": True}
            else:
                if "op" in args:
                    # Reviewer Mi8: reserved key must never reach the gate payload.
                    result = {"content": [{"type": "text",
                                           "text": "error: bad_request: reserved argument key: op"}],
                              "isError": True}
                else:
                    payload = {"op": name}
                    payload.update(args)
                    result = tool_result(payload)
        elif method == "ping":
            result = {}
        elif rid is None:
            continue  # notification (e.g. notifications/initialized)
        else:
            result = None
        if rid is not None:
            resp = {"jsonrpc": "2.0", "id": rid}
            if result is None and method != "ping":
                resp["error"] = {"code": -32601, "message": "method not found"}
            else:
                resp["result"] = result
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
