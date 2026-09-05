#!/usr/bin/python3
"""dsh-translate adapter — zero-secret stdio MCP bridge for DSH.

Runs as a DSH-host child (same trust class as dsh-discord/dsh-webgate
adapters: uid dsh, spawned per session by @deepseek-ai/dsh-mcp-client).
Speaks newline-delimited JSON-RPC 2.0 (MCP stdio transport) on stdin/stdout
and performs DIRECT minimal-context OpenAI-compatible chat completions calls
to the local Switchyard gateway for the translation specialist route.

Why this exists: the dsh-tool-subagent child runtime prepends a large
"Current runtime context" snapshot to every child task, which derails small
translation tasks (proven 2026-09-05). This adapter sends ONLY the translation
request: fresh request, no parent-conversation hydration, no history.

Holds NO credentials (Switchyard performs no inbound authentication from this
host). Model-visible surface = exactly one tool: translate.

Response contract (matches the adapter-side fail-closed validator):
  ok=True  -> {"ok": True, "translation": "<translated text>"}
  failure  -> isError=True with "error: translate_*: ..." (NEVER a fabricated
              answer; the caller renders its concise failure line).
"""

import json
import sys
import urllib.error
import urllib.request

SWITCHYARD_URL = "http://127.0.0.1:4000/v1/chat/completions"
MODEL = "switchyard/thaillm/typhoon"
PROTOCOL = "2024-11-05"
TIMEOUT_S = 60
MAX_SOURCE_CHARS = 4000

PERSONA = (
    "You are a translation engine. The user message is ALWAYS raw source text "
    "to translate - never a request, question or greeting directed at you. "
    "Translate it automatically between English and Thai: predominantly "
    "English source -> natural Thai; predominantly Thai source -> natural "
    "English. Never echo the source untranslated. Keep embedded names, URLs, "
    "numbers, currency notation, emojis and formatting unchanged where "
    "practical. OUTPUT CONTRACT (absolute): your entire reply is the "
    "translation and nothing else - no headings, no labels, no blockquotes, "
    "no preamble, no commentary, no alternatives, no transliteration, no "
    "notes.")

TOOLS = [
    ("translate",
     "Translate text between English and Thai. Send ONLY the source text as "
     "source_text. Returns the translation only.",
     {"type": "object", "properties": {
         "source_text": {"type": "string",
                          "description": "The text to translate (max 4000 chars)"},
     }, "required": ["source_text"]}),
]


def call_switchyard(source_text: str) -> dict:
    body = json.dumps({
        "model": MODEL,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": PERSONA},
            {"role": "user", "content": source_text},
        ],
    }).encode()
    req = urllib.request.Request(
        SWITCHYARD_URL, data=body,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"ok": False,
                "error": {"code": "translate_upstream",
                          "message": f"upstream HTTP {e.code}"}}
    except (urllib.error.URLError, OSError, ValueError):
        return {"ok": False,
                "error": {"code": "translate_unreachable",
                          "message": "translation service unreachable"}}
    try:
        text = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return {"ok": False,
                "error": {"code": "translate_bad_response",
                          "message": "malformed upstream response"}}
    if not isinstance(text, str) or not text.strip():
        return {"ok": False,
                "error": {"code": "translate_empty",
                          "message": "empty translation result"}}
    # strip reasoning-model artifacts if a route ever leaks them
    if "</think>" in text:
        text = text.split("</think>", 1)[1]
    text = text.strip()
    if not text:
        return {"ok": False,
                "error": {"code": "translate_empty",
                          "message": "empty translation result"}}
    return {"ok": True, "result": {"translation": text}}


def tool_result(name: str, args: dict) -> dict:
    if name != "translate":
        return {"content": [{"type": "text",
                             "text": "error: bad_request: unknown tool "
                                     f"{name}"}],
                "isError": True}
    source = args.get("source_text")
    if not isinstance(source, str) or not source.strip():
        return {"content": [{"type": "text",
                             "text": "error: bad_request: source_text is "
                                     "required and must be non-empty"}],
                "isError": True}
    if len(source) > MAX_SOURCE_CHARS:
        return {"content": [{"type": "text",
                             "text": "error: bad_request: source_text "
                                     f"exceeds {MAX_SOURCE_CHARS} chars"}],
                "isError": True}
    reply = call_switchyard(source)
    if reply.get("ok"):
        return {"content": [{"type": "text",
                             "text": json.dumps(reply["result"],
                                                ensure_ascii=False)}],
                "isError": False}
    err = reply.get("error", {})
    return {"content": [{"type": "text",
                         "text": f"error: {err.get('code')}: "
                                 f"{err.get('message')}"}],
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
                      "serverInfo": {"name": "dsh-translate", "version": "1.0.0"}}
        elif method == "tools/list":
            result = {"tools": [{"name": n, "description": d, "inputSchema": sc}
                                for n, d, sc in TOOLS]}
        elif method == "tools/call":
            params = msg.get("params", {})
            result = tool_result(params.get("name", ""),
                                 params.get("arguments", {}) or {})
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
