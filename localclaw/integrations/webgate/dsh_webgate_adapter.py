#!/usr/bin/env python3
"""dsh_webgate_adapter — zero-credential stdio MCP for DSH.

Forwards DSH's web_search / web_fetch tool calls to the dsh-webgate AF_UNIX
socket. Contains NO credentials, NO policy, NO routing logic: policy lives
entirely in the gate. Registered in DSH composition (cordis.patch.yml) as the
search/fetch provider for ctx.web; DSH source patches remain 0.
"""
from __future__ import annotations

import json
import os
import socket
import sys

SOCK_PATH = "/run/dsh-webgate/gate.sock"  # compiled-in transport (no env override; round-2 finding 8)

TOOLS = [
    {"name": "web_search", "description":
        "Search the public web. Returns a list of results (title, url, snippet).",
     "inputSchema": {"type": "object",
                     "properties": {"query": {"type": "string"}},
                     "required": ["query"]}},
    {"name": "web_fetch", "description":
        "Fetch a public web URL and return page text, headings and links.",
     "inputSchema": {"type": "object",
                     "properties": {"url": {"type": "string", "format": "uri"}},
                     "required": ["url"]}},
]


MAX_RESPONSE_BYTES = 1_048_576


def gate_call(payload: dict) -> dict:
    """One newline-JSON request to the gate. Fails closed with FIXED error
    categories — no exception text (finding 9); response size capped
    (finding 10); socket always closed."""
    s = None
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(120)
        s.connect(SOCK_PATH)
        s.sendall(json.dumps(payload).encode() + b"\n")
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
            if len(buf) > MAX_RESPONSE_BYTES:
                return {"ok": False, "error": "gate-oversized-response"}
    except socket.timeout:
        return {"ok": False, "error": "gate-timeout"}
    except OSError:
        return {"ok": False, "error": "gate-unreachable"}
    finally:
        if s is not None:
            try:
                s.close()
            except OSError:
                pass
    if not buf:
        return {"ok": False, "error": "empty-gate-response"}
    try:
        resp = json.loads(buf.decode())
        return resp if isinstance(resp, dict) else {"ok": False,
                                                    "error": "invalid-gate-response"}
    except (ValueError, UnicodeDecodeError):
        return {"ok": False, "error": "invalid-gate-response"}


def to_mcp_content(resp: dict) -> dict:
    if resp.get("ok"):
        return {"content": [{"type": "text",
                             "text": json.dumps(resp, ensure_ascii=False)}]}
    return {"content": [{"type": "text",
                         "text": json.dumps(resp, ensure_ascii=False)}],
            "isError": True}


def handle(req: dict) -> dict | None:
    if not isinstance(req, dict):
        return None
    m = req.get("method", "")
    rid = req.get("id")
    try:
        return _handle_inner(req, m, rid)
    except Exception:  # one malformed request must not kill the server (finding 15)
        if rid is not None:
            return {"jsonrpc": "2.0", "id": rid,
                    "error": {"code": -32603, "message": "internal error"}}
        return None


def _handle_inner(req: dict, m: str, rid) -> dict | None:
    if m == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": req.get("params", {}).get(
                "protocolVersion", "2024-11-05"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "dsh-webgate-adapter", "version": "1.0.0"}}}
    if m.startswith("notifications/"):
        return None
    if m == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": TOOLS}}
    if m == "tools/call":
        params = req.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("name"), str):
            return {"jsonrpc": "2.0", "id": rid,
                    "error": {"code": -32602, "message": "invalid params"}}
        name = params["name"]
        args = params.get("arguments")
        # Strict params (round-2 finding 9): exact keys, string values.
        if name == "web_search":
            if set(args) != {"query"} or not isinstance(args["query"], str):
                return {"jsonrpc": "2.0", "id": rid,
                        "error": {"code": -32602, "message": "invalid params"}}
            return {"jsonrpc": "2.0", "id": rid,
                    "result": to_mcp_content(gate_call(
                        {"op": "search", "query": args["query"]}))}
        if name == "web_fetch":
            if set(args) != {"url"} or not isinstance(args["url"], str):
                return {"jsonrpc": "2.0", "id": rid,
                        "error": {"code": -32602, "message": "invalid params"}}
            return {"jsonrpc": "2.0", "id": rid,
                    "result": to_mcp_content(gate_call(
                        {"op": "fetch", "url": args["url"]}))}
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": "unknown tool"}], "isError": True}}
    if rid is not None:
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601,
                "message": "method not found"}}
    return None


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except ValueError:
            continue
        resp = handle(req)
        if resp is not None:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
