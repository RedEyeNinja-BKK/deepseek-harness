#!/usr/bin/env python3
"""mock_plugin_server.py — hermetic AF_UNIX seam server (mock discord-agent-drive).

Emulates the plugin's socket side of the S2 protocol for gate regression tests
ONLY: hello-ack with route echo + pilot conv/sid, route-ack on route frames,
reconcile log lines. Emits NO finalization/admitted frames (a gate probe must
observe zero frames). Socket ownership/mode/group are made configurable so the
gate's perms checks can be exercised (badSock/badGroup).

Env: S2_MOCK_SOCK, S2_MOCK_EVID, S2_MOCK_CONV, S2_MOCK_SID,
     S2_MOCK_BAD_SOCK, S2_MOCK_BAD_GROUP.
"""

from __future__ import annotations

import grp
import json
import os
import socket
import sys
import time


def log_line(sock, text):
    try:
        if sock:
            with open(os.path.join(sock, "plugin.log"), "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {text}\n")
    except Exception:
        pass


def main():
    sock_path = os.environ["S2_MOCK_SOCK"]
    evid = os.environ.get("S2_MOCK_EVID", "")
    conv = os.environ.get("S2_MOCK_CONV", "")
    sid = os.environ.get("S2_MOCK_SID", "")
    bad_sock = bool(os.environ.get("S2_MOCK_BAD_SOCK"))
    bad_group = bool(os.environ.get("S2_MOCK_BAD_GROUP"))
    route = "OLD"
    try:
        os.makedirs(os.path.dirname(sock_path), exist_ok=True)
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(sock_path)
        # emulate plugin: socket 0660 normally; badSock => 0666
        os.chmod(sock_path, 0o666 if bad_sock else 0o660)
        if bad_group:
            # force the WRONG group (the process primary gid) so the gate's
            # exact group check fails
            try:
                os.chown(sock_path, -1, os.getegid())
            except Exception:
                pass
        elif not bad_sock:
            try:
                gid = grp.getgrnam("dsh-media").gr_gid
                os.chown(sock_path, -1, gid)
            except Exception:
                pass
        srv.listen(4)
    except Exception as e:
        print(f"mock server bind failed: {e}", file=sys.stderr)
        return 1
    # announce readiness to the gate's wait loop: the gate polls for evidence
    # after MainPID change, so an explicit marker write is not needed here.
    while True:
        try:
            conn, _ = srv.accept()
        except Exception:
            break
        with conn:
            buf = ""
            try:
                while True:
                    data = conn.recv(65536)
                    if not data:
                        break
                    buf += data.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        if not line.strip():
                            continue
                        try:
                            frame = json.loads(line)
                        except Exception:
                            continue
                        t = frame.get("type")
                        if t == "hello":
                            delivered = frame.get("deliveredFinalizations") or []
                            log_line(evid, f"reconcile deliveredCount={len(delivered)}")
                            conn.sendall((json.dumps({
                                "type": "hello-ack", "v": 1, "route": route,
                                "pilotConversationKey": conv,
                                "pilotSessionId": sid,
                            }) + "\n").encode())
                            log_line(evid, "reconcile done")
                        elif t == "route":
                            st = str(frame.get("state") or "").upper()
                            if st in ("OLD", "S2_ACTIVE", "QUIESCING_TO_OLD"):
                                route = st
                                conn.sendall((json.dumps({
                                    "type": "route-ack", "state": route,
                                    "fenceTurn": None,
                                }) + "\n").encode())
                                log_line(evid, f"route -> {route} fenceTurn=None")
                        elif t == "admitted":
                            # probe/gate never sends admitted; ack refused
                            conn.sendall((json.dumps({
                                "type": "ack", "for": "admitted",
                                "discordMessageId": frame.get("discordMessageId"),
                                "accepted": False, "code": "not-active",
                            }) + "\n").encode())
                        elif t == "ping":
                            conn.sendall((json.dumps({"type": "pong", "route": route}) + "\n").encode())
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
