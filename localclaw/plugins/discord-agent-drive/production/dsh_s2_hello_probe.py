#!/usr/bin/env python3
"""dsh_s2_hello_probe.py — S2 live-gate hello/route probe (GATE TOOLING ONLY).

This file is NOT part of the S2 pilot candidate bytes. It is a small read-only
gate-side client used by dsh_s2_live_gate.sh to prove seam readiness against the
REAL discord-agent-drive plugin (which runs inside dsh.service):

  * connects to the plugin's AF_UNIX seam socket;
  * sends the SAME hello the real listener would send, carrying the current
    authoritative delivered-finalization list read from inbound-state.json;
  * asserts the hello-ack echoes the exact pilot conversation/session and the
    expected plugin route state;
  * optionally pushes one route frame (state) and asserts the route-ack;
  * collects any frames the plugin emits during a short quiet window (an
    unexpected finalization frame is surfaced to the gate, never delivered);
  * sends NO 'admitted' frame and never drives the session (no followup, no
    user event). Reconcile-on-hello only materializes the pinned session.

Exit codes: 0 = exact proof; 2 = protocol/identity/route mismatch;
3 = timeout/read failure; 4 = connect/permission failure; 5 = usage error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

PROTOCOL_VERSION = 1
VALID_ROUTES = ("OLD", "S2_ACTIVE", "QUIESCING_TO_OLD")
FRAME_MAX = 1024 * 1024


def delivered_fids(state_path: str, conv: str) -> list[str]:
    """Authoritative delivered finalizations for the conversation (mirror of
    s2_seam.s2_delivered_fids over the real inbound-state.json)."""
    try:
        state = json.loads(open(state_path, encoding="utf-8").read())
    except Exception as e:
        raise SystemExit(f"state file unreadable/invalid: {e}")
    convs = (state.get("s2") or {}).get("delivered_finalizations") or {}
    recs = convs.get(conv) or {}
    return [fid for fid, e in recs.items()
            if isinstance(e, dict) and e.get("state") == "delivered"]


async def run(args) -> int:
    sock = args.sock
    if args.state:
        delivered = delivered_fids(args.state, args.conv)
    else:
        delivered = []
    try:
        reader, writer = await asyncio.open_unix_connection(sock)
    except FileNotFoundError:
        print(json.dumps({"ok": False, "phase": "connect", "error": "socket-absent"}))
        return 4
    except PermissionError:
        print(json.dumps({"ok": False, "phase": "connect", "error": "permission-denied"}))
        return 4
    except Exception as e:
        print(json.dumps({"ok": False, "phase": "connect", "error": str(e)}))
        return 4

    def send(obj: dict) -> None:
        writer.write((json.dumps(obj) + "\n").encode("utf-8"))

    try:
        send({"type": "hello", "deliveredFinalizations": delivered})
        raw = await asyncio.wait_for(reader.readline(), args.timeout)
        ack = json.loads(raw.decode("utf-8", errors="replace").strip())
    except asyncio.TimeoutError:
        print(json.dumps({"ok": False, "phase": "hello", "error": "timeout-no-hello-ack"}))
        writer.close()
        return 3
    except Exception as e:
        print(json.dumps({"ok": False, "phase": "hello", "error": str(e)}))
        writer.close()
        return 2

    if ack.get("type") != "hello-ack":
        print(json.dumps({"ok": False, "phase": "hello", "helloAck": ack,
                          "error": "missing-hello-ack"}))
        writer.close()
        return 2
    if ack.get("v") != PROTOCOL_VERSION:
        print(json.dumps({"ok": False, "phase": "hello", "helloAck": ack,
                          "error": f"protocol-version {ack.get('v')}"}))
        writer.close()
        return 2
    if str(ack.get("pilotConversationKey") or "") != args.conv:
        print(json.dumps({"ok": False, "phase": "hello", "helloAck": ack,
                          "error": f"pilot-conv-echo {ack.get('pilotConversationKey')}"}))
        writer.close()
        return 2
    if str(ack.get("pilotSessionId") or "") != args.sid:
        print(json.dumps({"ok": False, "phase": "hello", "helloAck": ack,
                          "error": f"pilot-sid-echo {ack.get('pilotSessionId')}"}))
        writer.close()
        return 2
    hello_route = str(ack.get("route") or "")
    if args.expect_hello_route and hello_route != args.expect_hello_route:
        print(json.dumps({"ok": False, "phase": "hello", "helloAck": ack,
                          "error": f"route-echo {hello_route} != {args.expect_hello_route}"}))
        writer.close()
        return 2

    frames: list[dict] = []
    route_ack = None
    if args.route is not None:
        if args.route not in VALID_ROUTES:
            print(json.dumps({"ok": False, "error": "bad-route-arg"}))
            writer.close()
            return 5
        try:
            send({"type": "route", "state": args.route})
            raw = await asyncio.wait_for(reader.readline(), args.timeout)
            route_ack = json.loads(raw.decode("utf-8", errors="replace").strip())
        except asyncio.TimeoutError:
            print(json.dumps({"ok": False, "phase": "route", "error": "timeout-no-route-ack",
                              "frames": frames}))
            writer.close()
            return 3
        except Exception as e:
            print(json.dumps({"ok": False, "phase": "route", "error": str(e),
                              "frames": frames}))
            writer.close()
            return 2
        if route_ack.get("type") != "route-ack" or \
                str(route_ack.get("state") or "") != args.route:
            print(json.dumps({"ok": False, "phase": "route", "routeAck": route_ack,
                              "error": f"route-ack-state {route_ack.get('state')}"}))
            writer.close()
            return 2

    # Quiet window: collect anything the plugin emits (must be empty in stage;
    # surfaced as evidence in activate/rollback).
    deadline = asyncio.get_event_loop().time() + args.quiet
    try:
        while True:
            remain = deadline - asyncio.get_event_loop().time()
            if remain <= 0:
                break
            raw = await asyncio.wait_for(reader.readline(), remain)
            if not raw:
                break
            if len(raw) > FRAME_MAX:
                continue
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                frames.append(json.loads(line))
            except Exception:
                frames.append({"raw": line[:400]})
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass

    writer.close()
    result = {
        "ok": True,
        "phase": "done",
        "helloAck": ack,
        "routeAck": route_ack,
        "helloRoute": hello_route,
        "deliveredCount": len(delivered),
        "quietFrames": frames,
    }
    print(json.dumps(result))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sock", required=True)
    ap.add_argument("--conv", required=True)
    ap.add_argument("--sid", required=True)
    ap.add_argument("--state", default=os.environ.get("S2_STATE_FILE", ""))
    ap.add_argument("--expect-hello-route", default="", choices=list(VALID_ROUTES) + [""])
    ap.add_argument("--route", default=None, choices=list(VALID_ROUTES) + [None])
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--quiet", type=float, default=3.0)
    args = ap.parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 3


if __name__ == "__main__":
    sys.exit(main())
