#!/usr/bin/python3
"""dsh-image adapter — zero-secret stdio MCP bridge for DSH (image gen v1).

Same trust class as dsh-translate/dsh-webgate/dsh-discord adapters: spawned
per-session by @deepseek-ai/dsh-mcp-client, runs as uid dsh, holds NO
credentials. Model-visible surface = exactly one bounded tool:

    generate_image(prompt, aspect?, count?)

All lifecycle/GPU authority lives vincent-side in dsh-media-bridge (which
imports the established multimedia_mcp lifecycle). This adapter only
forwards the bounded request over loopback and relays the result.

Response contract:
  ok=True  -> {"ok": true, "completed": N, "requested": M, "images": [
                {"tx_id", "ref", "artifact", "width", "height", "bytes",
                 "mime", "original_url", "preview_url"}, ...]}
  failure  -> isError=True with "error: image_*: ..." (NEVER a fabricated
              success; callers render a concise failure).
"""

import json
import sys
import urllib.error
import urllib.request

BRIDGE = "http://127.0.0.1:8620/generate_image"
PROTOCOL = "2024-11-05"
TIMEOUT_S = 870  # bridge carries the bounded budget (840s) + margin

TOOLS = [
    ("generate_image",
     "Generate one or two images from a text description. Use ONLY when the "
     "user asks to create, generate, draw or make an image (clear generative "
     "intent) — never when merely discussing images. Pass the user's visual "
     "description as prompt (you may translate/expand it into a vivid "
     "English scene description). aspect: 'square' (default), 'landscape' "
     "or 'portrait'. count: 1 (default) or 2. Returns image metadata "
     "including original_url/preview_url and the artifact file path. When "
     "the user is on LINE, reply afterwards with ONLY the tool's JSON "
     "envelope unchanged so the delivery layer can send the image; on "
     "Discord, deliver via send_message with files=[artifact].",
     {"type": "object", "properties": {
         "prompt": {"type": "string", "description": "What to draw (max 4000 chars)"},
         "aspect": {"type": "string", "enum": ["square", "landscape", "portrait"],
                    "description": "Image shape, default square"},
         "count": {"type": "integer", "minimum": 1, "maximum": 2,
                   "description": "How many images, default 1"},
     }, "required": ["prompt"]}),
]


def call_bridge(args: dict) -> dict:
    body = json.dumps(args).encode()
    req = urllib.request.Request(BRIDGE, data=body,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        return {"ok": False, "classification": "bridge_http",
                "error": f"bridge HTTP {e.code}"}
    except (urllib.error.URLError, OSError, ValueError):
        return {"ok": False, "classification": "bridge_unreachable",
                "error": "image service unreachable"}
    # Fail-closed response contract: anything that is not a well-formed
    # success envelope with a consistent images list is a protocol error
    # (Hermes review H — never surface malformed data as success).
    try:
        out = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return {"ok": False, "classification": "bridge_protocol",
                "error": "malformed bridge response"}
    if not isinstance(out, dict):
        return {"ok": False, "classification": "bridge_protocol",
                "error": "malformed bridge response"}
    if out.get("ok") is not True:
        return out  # bridge failure envelope: pass through for classification
    completed = out.get("completed")
    requested = out.get("requested")
    images = out.get("images")
    if (isinstance(completed, int) and not isinstance(completed, bool)
            and 1 <= completed <= 2
            and requested in (1, 2)
            and completed <= requested
            and isinstance(images, list) and len(images) == completed):
        for img in images:
            if not (isinstance(img, dict)
                    and all(isinstance(img.get(k), str) and img.get(k)
                            for k in ("tx_id", "ref", "artifact",
                                      "original_url", "preview_url"))
                    and isinstance(img.get("width"), int)
                    and isinstance(img.get("height"), int)
                    and isinstance(img.get("bytes"), int)
                    and img.get("mime") == "image/png"):
                return {"ok": False, "classification": "bridge_protocol",
                        "error": "malformed image entry in bridge response"}
        return out
    return {"ok": False, "classification": "bridge_protocol",
            "error": "inconsistent success envelope from bridge"}


def tool_result(name: str, args: dict) -> dict:
    if name != "generate_image":
        return {"content": [{"type": "text",
                             "text": "error: image_bad_request: unknown tool "
                                     f"{name}"}],
                "isError": True}
    prompt = args.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        return {"content": [{"type": "text",
                             "text": "error: image_bad_request: prompt is "
                                     "required"}],
                "isError": True}
    out = call_bridge({"prompt": prompt,
                       "aspect": args.get("aspect", "square"),
                       "count": args.get("count", 1)})
    if out.get("ok"):
        return {"content": [{"type": "text",
                             "text": json.dumps(out, ensure_ascii=False)}],
                "isError": False}
    cls = out.get("classification", "image_failed")
    friendly = {
        "bridge_unreachable": "image service unreachable",
        "bridge_protocol": "image service returned an invalid response",
        "submit_rejected": "image service busy or unavailable",
        "preemption_admission": "GPU busy with a priority workload - try again later",
        "timeout": "generation timed out",
    }.get(cls, "image generation failed")
    return {"content": [{"type": "text",
                         "text": f"error: image_{cls}: {friendly}"}],
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
        method, rid = msg.get("method"), msg.get("id")
        if method == "initialize":
            result = {"protocolVersion": PROTOCOL,
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "dsh-image", "version": "1.0.0"}}
        elif method == "tools/list":
            result = {"tools": [{"name": n, "description": d, "inputSchema": sc}
                                for n, d, sc in TOOLS]}
        elif method == "tools/call":
            p = msg.get("params", {})
            result = tool_result(p.get("name", ""),
                                 p.get("arguments", {}) or {})
        elif method == "ping":
            result = {}
        elif rid is None:
            continue
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
