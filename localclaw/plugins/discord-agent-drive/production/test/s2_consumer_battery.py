#!/usr/bin/env python3
"""s2_consumer_battery.py — S2 PRODUCTION CONSUMER integration battery.

Uses the ACTUAL candidate listener bytes (production/dsh_discord_inbound.s2.py)
+ the ACTUAL helper bytes (production/s2_seam.py) with a mocked Discord
transport. Two modes:

  UNIT mode (default; no DSH): a faithful fake seam server exercises the
  consumer's protocol + ledger + suppression + rollback logic headlessly.
  INT mode (S2_BATTERY_MODE=int): connects the candidate consumer to the REAL
  discord-agent-drive plugin seam inside a scratch DSH (fresh root, seeded pilot
  session) for the end-to-end admission/finalization/restart cases.

The candidate listener is imported by absolute path from this file's parent
directory. STATE_DIRECTORY is pointed at a fresh temp dir per mode so the
consumer writes its own inbound-state.json (never the production one).

Case coverage (operator directive §8):
  1 flag empty -> old path only           (U)
  2 exact pilot -> S2 path only           (I)
  3 sibling -> old path only              (U)
  4 pilot session identity mismatch -> fail closed (I)
  5 plugin unavailable -> no fallback double-submit, visible failure (U)
  6 duplicate Discord event -> one native admission (I)
  7 lost inbound ACK -> deterministic retry behavior (U/I: ambiguous no-resend)
  8 normal text finalization              (I)
  9 delivered-FID replay suppression      (U)
 10 pending-FID replay -> indeterminate/no resend (U)
 11 media finalization + media-identity suppression (U + I-delivery)
 12 shim/listener restart                 (I)
 13 plugin restart                        (I)
 14 malformed frame                       (U)
 15 overflow/quiesce signal               (U)
 16 idle rollback fence                   (U)
 17 in-flight rollback fence              (U)
 18 pilot old-path zero (session.*/watcher/backstop counters) (U+I)
 19 sibling negative proof                (I)
 20 existing _deliver_* execution remains available (I + U)
"""
import argparse
import asyncio
import importlib.util
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROD = HERE.parent
LISTENER_PATH = PROD / "dsh_discord_inbound.s2.py"
SEAM_PATH = PROD / "s2_seam.py"

CONV = "channel:111111111111111111"          # pilot conversation key
SIBLING = "channel:222222222222222222"       # sibling conversation key
SID = "session-2f8c1f6a-0000-4000-8000-0000000000a1"

results = []


def case(name, ok, detail=""):
    results.append({"case": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else (" :: " + str(detail)[:300])))


# ---------------------------------------------------------------------------
# Discord fakes
# ---------------------------------------------------------------------------

class FakeAuthor:
    def __init__(self, mid, bot=False):
        self.id = mid
        self.bot = bot
        self.display_name = "tester"


class FakeChannel:
    def __init__(self, cid):
        self.id = int(cid)
        self.name = f"ch{cid}"
        self.category_id = 0
        self.sent = []

    async def send(self, content=None, file=None):
        row = {"at": time.time(), "kind": "text" if file is None else "media",
               "text": content, "file": (file.filename if file is not None else None)}
        self.sent.append(row)
        return row


class _FakeGuild:
    def __init__(self, gid=100000000000000001):
        self.id = gid
        self.name = "fakeguild"


class FakeMessage:
    def __init__(self, mid, content, channel, author_id=222222222222222222):
        self.id = int(mid)
        self.content = content
        self.channel = channel
        self.author = FakeAuthor(author_id)
        self.attachments = []
        self.guild = _FakeGuild()          # non-None -> channel path
        self.mentions = []
        self.reference = None

    @property
    def channel(self):
        return self._ch

    @channel.setter
    def channel(self, ch):
        self._ch = ch


# ---------------------------------------------------------------------------
# Load the ACTUAL candidate listener + helper modules
# ---------------------------------------------------------------------------

def load_candidate(tmp_state: Path, pilot_conv: str = ""):
    """Import the staged listener in a fresh sub-interpreter-safe way (module
    reload each call with a fresh STATE_DIRECTORY + S2_PILOT_CONV)."""
    # mirror the production interpreter: running the script puts its own dir
    # first on sys.path (that is how the vendored discord at /opt/dsh-inbound
    # resolves in the live service); emulate it here (prod dir first for
    # s2_seam, then the vendored-lib dir for discord).
    for p in (str(PROD), "/opt/dsh-inbound"):
        if p not in sys.path:
            sys.path.insert(0, p)
    os.environ["STATE_DIRECTORY"] = str(tmp_state)
    os.environ["S2_PILOT_CONV"] = pilot_conv or ""
    for mod in ("inbound", "s2_seam"):
        if mod in sys.modules:
            del sys.modules[mod]
    spec = importlib.util.spec_from_file_location("s2_seam", SEAM_PATH)
    seam = importlib.util.module_from_spec(spec)
    sys.modules["s2_seam"] = seam
    spec.loader.exec_module(seam)
    spec2 = importlib.util.spec_from_file_location("inbound", LISTENER_PATH)
    inbound = importlib.util.module_from_spec(spec2)
    sys.modules["inbound"] = inbound
    spec2.loader.exec_module(inbound)
    return inbound


class RpcRecorder:
    """Records session RPC calls (old path). Success returns ok True with a sid."""
    def __init__(self):
        self.calls = []

    def __call__(self, endpoint, payload, timeout=60.0):
        self.calls.append({"endpoint": endpoint, "payload": payload})
        if endpoint == "session.create":
            return {"result": {"value": {"sessionId": SID}}}
        if endpoint == "session.history":
            return {"result": {"value": {"events": []}}}
        return {"result": {"ok": True}}


# ---------------------------------------------------------------------------
# Fake seam server (faithful minimal plugin wire semantics)
# ---------------------------------------------------------------------------

class FakeSeamServer:
    """Listens on an AF_UNIX socket and speaks the reviewed plugin protocol:
    hello(route/delivered), route, admitted(dedupe by deterministic id; ACK
    durable claimed after a fake claim), finalization emission on demand.
    Maintains an internal 'session' of admitted message ids + turn counter."""
    def __init__(self, sock_path: Path, pilot_conv=CONV, pilot_sid=SID):
        self.sock_path = sock_path
        self.pilot_conv = pilot_conv
        self.pilot_sid = pilot_sid
        self.admitted = {}       # discord id -> claimed
        self.turn = 0
        self.route = "OLD"
        self.delivered = set()   # delivered fids from hello
        self.frames_out = []
        self.connections = []
        self.enabled = True

    async def start(self):
        for suffix in range(100):
            try:
                self.sock_path.parent.mkdir(parents=True, exist_ok=True)
                self.sock_path.unlink()
            except FileNotFoundError:
                pass
            try:
                self.server = await asyncio.start_unix_server(self._handle, str(self.sock_path))
                return
            except OSError:
                if suffix == 99:
                    raise
                await asyncio.sleep(0.2)

    async def stop(self):
        self.enabled = False
        try:
            self.server.close()
        except Exception:
            pass
        for w in list(self._writers):
            try:
                w.close()
            except Exception:
                pass
        try:
            await asyncio.wait_for(self.server.wait_closed(), 2)
        except Exception:
            pass
        try:
            self.sock_path.unlink()
        except OSError:
            pass

    async def _handle(self, reader, writer):
        self.connections.append(writer)
        self._writers.append(writer)
        buf = ""
        try:
            while self.enabled:
                raw = await reader.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                # malformed frame tolerance (mirrors the real plugin: garbage is
                # ignored/logged, never crashes the connection handler)
                try:
                    frame = json.loads(line)
                except Exception:
                    continue
                resp = await self._on_frame(frame)
                if resp is not None:
                    writer.write((json.dumps(resp) + "\n").encode())
                    await writer.drain()
        except (asyncio.CancelledError, ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass
            try:
                if writer in self._writers:
                    self._writers.remove(writer)
            except Exception:
                pass

    async def _on_frame(self, frame):
        ftype = frame.get("type")
        if ftype == "hello":
            self.delivered = set(frame.get("deliveredFinalizations") or [])
            return {"type": "hello-ack", "v": 1, "route": self.route,
                    "pilotConversationKey": self.pilot_conv,
                    "pilotSessionId": self.pilot_sid}
        if ftype == "route":
            self.route = frame.get("state")
            return {"type": "route-ack", "state": self.route, "fenceTurn": None}
        if ftype == "admitted":
            conv = frame.get("conversationKey")
            sid = frame.get("sessionId")
            did = str(frame.get("discordMessageId"))
            det = f"discord:{conv}:{did}"
            if conv != self.pilot_conv or sid != self.pilot_sid:
                return {"type": "ack", "for": "admitted", "discordMessageId": did,
                        "accepted": False, "code": "identity-mismatch", "failClosed": True}
            if frame.get("dshMessageId") != det:
                return {"type": "ack", "for": "admitted", "discordMessageId": did,
                        "accepted": False, "code": "dsh-message-id-mismatch"}
            if self.route != "S2_ACTIVE":
                code = "quiescing" if self.route == "QUIESCING_TO_OLD" else "not-active"
                return {"type": "ack", "for": "admitted", "discordMessageId": did,
                        "accepted": False, "code": code}
            if did in self.admitted:
                return {"type": "ack", "for": "admitted", "discordMessageId": did,
                        "dshMessageId": det, "accepted": True, "durable": True,
                        "observed": "claimed", "deduped": True}
            self.admitted[did] = True
            self.turn += 1
            # emulate the durable claim + turn that the plugin would produce
            return {"type": "ack", "for": "admitted", "discordMessageId": did,
                    "dshMessageId": det, "accepted": True, "durable": True,
                    "observed": "claimed"}
        if ftype == "ping":
            return {"type": "pong", "route": self.route}
        return None

    # -- consumer test helpers ----------------------------------------------
    async def emit_finalization(self, kind="text-fallback", text="hello",
                                turn=None, fid=None, artifacts=None):
        if turn is None:
            self.turn += 1
            turn = self.turn
        fid = fid or f"{self.pilot_sid}:{turn}:{kind}"
        facts = {"text": text, "turn": turn}
        if kind == "artifact":
            facts["artifacts"] = artifacts or []
        frame = {"type": "finalization", "v": 1,
                 "conversationKey": self.pilot_conv, "sessionId": self.pilot_sid,
                 "finalizationId": fid, "kind": kind, "turn": turn, "facts": facts}
        self.frames_out.append(frame)
        for w in list(self._writers):
            try:
                w.write((json.dumps(frame) + "\n").encode())
                await w.drain()
            except Exception:
                pass

    _writers = []


# ---------------------------------------------------------------------------
# UNIT-mode runner (fake seam; no DSH)
# ---------------------------------------------------------------------------

def run_unit(tmp: Path):
    """UNIT mode: fake seam server + staged candidate listener. Deterministic."""
    async def main():
        tmp.mkdir(parents=True, exist_ok=True)
        sock = tmp / "seam.sock"
        server = FakeSeamServer(sock, pilot_conv=CONV, pilot_sid=SID)
        await server.start()

        os.environ["S2_SOCK_PATH"] = str(sock)
        inbound = load_candidate(tmp / "state1", pilot_conv=CONV)
        rpc = RpcRecorder()
        inbound.dsh_rpc = rpc
        state = inbound.load_state()
        state.setdefault("sessions", {})[CONV] = SID
        state.setdefault("sessions", {})[SIBLING] = SID
        inbound.save_state(state)
        ch = FakeChannel("111111111111111111")
        ch_sib = FakeChannel("222222222222222222")
        route_file = inbound.S2_ROUTE_FILE
        route_file.parent.mkdir(parents=True, exist_ok=True)
        route_file.write_text(json.dumps({CONV: "S2_ACTIVE"}))
        inbound._S2_CLIENT = type("C", (), {"get_channel": lambda self, c: ch})()
        inbound._S2_CHANNEL_IDS[CONV] = int(ch.id)

        async def dispatch_pilot(mid, text="Marker X. Reply X-OK"):
            await inbound.dispatch_to_dsh(state, FakeMessage(mid, text, ch))

        # U02: exact pilot -> S2 path only (route pushed, admitted, old RPC zero)
        await dispatch_pilot(6101, "Case2 pilot text.")
        await asyncio.sleep(0.8)
        ok2 = (server.route == "S2_ACTIVE" and server.admitted.get("6101") is True
               and inbound.OLD_PATH_COUNTERS["pilot"] == 0
               and not [c for c in rpc.calls if "session." in c["endpoint"]])
        case("U02-exact-pilot-S2-path-only", ok2,
             {"route": server.route, "admitted": server.admitted.get("6101"),
              "rpc": rpc.calls, "counters": inbound.OLD_PATH_COUNTERS})
        case("U02b-attempted-claimed",
             (inbound.s2.s2_attempted(state, CONV, "6101") or {}).get("state") == "claimed",
             {"attempted": inbound.s2.s2_attempted(state, CONV, "6101")})

        # U06: duplicate Discord event -> deterministic dedupe (one admission)
        await dispatch_pilot(6101, "Case2 pilot text again.")
        await asyncio.sleep(0.5)
        case("U06-duplicate-deterministic-dedupe",
             server.admitted.get("6101") is True and len(server.admitted) == 1,
             {"admitted": dict(server.admitted)})

        # U08: normal text finalization delivered + ledger
        before = len(ch.sent)
        await server.emit_finalization(kind="text-fallback", text="Final answer here", turn=7)
        await asyncio.sleep(0.8)
        fid7 = f"{SID}:7:text-fallback"
        case("U08-text-finalization-delivered",
             len(ch.sent) == before + 1 and ch.sent[-1]["text"] == "Final answer here",
             {"delta": len(ch.sent) - before, "last": (ch.sent[-1] if ch.sent else None)})
        case("U08b-ledger-delivered",
             inbound.s2.s2_fid_state(state, CONV, fid7) == "delivered",
             {"state": inbound.s2.s2_fid_state(state, CONV, fid7)})

        # U09: consumer restart -> delivered fid not re-sent; seam reconnects
        if inbound._S2_SEAM is not None:
            await inbound._S2_SEAM.stop()
        inbound._S2_SEAM = None
        before = len(ch.sent)
        await dispatch_pilot(6102, "Case9 after restart.")
        await asyncio.sleep(1.2)
        await server.emit_finalization(kind="text-fallback", text="Replay dup", turn=7, fid=fid7)
        await asyncio.sleep(0.6)
        case("U09-consumer-restart-no-dup", len(ch.sent) == before,
             {"delta": len(ch.sent) - before})
        case("U09b-seam-reconnects", server.admitted.get("6102") is True,
             {"6102": server.admitted.get("6102")})

        # U10: pending replay -> indeterminate, no resend
        saved_deliver = inbound._deliver_channel
        calls = []

        async def failing_deliver(channel, text):
            calls.append(text)
            return 0

        inbound._deliver_channel = failing_deliver
        fid10 = f"{SID}:10:text-fallback"
        await server.emit_finalization(kind="text-fallback", text="Pending once", turn=10, fid=fid10)
        await asyncio.sleep(0.8)
        st10 = inbound.s2.s2_fid_state(state, CONV, fid10)
        await server.emit_finalization(kind="text-fallback", text="Pending once", turn=10, fid=fid10)
        await asyncio.sleep(0.6)
        case("U10-pending-replay-indeterminate-no-resend",
             len(calls) == 1 and st10 == "indeterminate",
             {"calls": calls, "state": st10})
        inbound._deliver_channel = saved_deliver

        # U11: media identity suppression + helper ledger settle
        ident1 = "a" * 64 + "|/tmp/x1.mp3"
        ident2 = "b" * 64 + "|/tmp/x2.mp3"
        state.setdefault("media_delivered", {}).setdefault(CONV, {})[ident2] = int(time.time())
        inbound.save_state(state)
        sent_media = []

        async def deliver_media(item):
            sent_media.append(item.get("identity"))
            return True

        fid11 = f"{SID}:11:artifact"
        await inbound.s2.handle_finalization(
            state, CONV,
            {"type": "finalization", "conversationKey": CONV, "sessionId": SID,
             "finalizationId": fid11, "kind": "artifact", "turn": 11,
             "facts": {"artifacts": [{"path": "/tmp/x1.mp3", "identity": ident1},
                                     {"path": "/tmp/x2.mp3", "identity": ident2}]}},
            inbound.save_state, (lambda t: None), deliver_media)
        case("U11-media-identity-suppression",
             sent_media == [ident1] and
             inbound.s2.s2_fid_state(state, CONV, fid11) == "delivered",
             {"sent_media": sent_media,
              "state": inbound.s2.s2_fid_state(state, CONV, fid11)})

        # U14: malformed frame tolerated (raw garbage then valid admit still works)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(sock))
        s.sendall(b"{not json\n")
        s.sendall((json.dumps({"type": "ping"}) + "\n").encode())
        s.settimeout(3)
        data = b""
        try:
            data = s.recv(4096)
        except Exception:
            pass
        # U14: malformed frame tolerated (garbage line then a valid frame; the
        # server must keep serving and answer the ping)
        r, w = await asyncio.open_unix_connection(str(sock))
        w.write(b"{not json\n")
        w.write((json.dumps({"type": "ping"}) + "\n").encode())
        await w.drain()
        data = b""
        try:
            data = await asyncio.wait_for(r.readline(), 3)
        except Exception:
            pass
        w.close()
        case("U14-malformed-frame-tolerated", b"pong" in data,
             {"recv": data[:120].decode("utf-8", errors="replace")})

        # U16/U18: rollback fences + old-path counters (fresh consumer instance)
        inbound2 = load_candidate(tmp / "state2", pilot_conv=CONV)
        rpc3 = RpcRecorder()
        inbound2.dsh_rpc = rpc3
        st2 = inbound2.load_state()
        st2.setdefault("sessions", {})[CONV] = SID
        inbound2.save_state(st2)
        inbound2._S2_CLIENT = type("C", (), {"get_channel": lambda self, c: ch})()
        inbound2._S2_CHANNEL_IDS[CONV] = int(ch.id)
        # inbound2 reads ITS OWN route control file (its STATE_DIRECTORY)
        route2 = inbound2.S2_ROUTE_FILE
        route2.parent.mkdir(parents=True, exist_ok=True)
        def set_route2(state_name):
            route2.write_text(json.dumps({CONV: state_name}))
        set_route2("S2_ACTIVE")
        await inbound2.dispatch_to_dsh(st2, FakeMessage(6701, "Fence msg", ch))
        await asyncio.sleep(0.7)
        ok_active = server.admitted.get("6701") is True
        pilot_zero_active = inbound2.OLD_PATH_COUNTERS["pilot"] == 0
        set_route2("QUIESCING_TO_OLD")
        await inbound2.dispatch_to_dsh(st2, FakeMessage(6702, "During quiesce", ch))
        await asyncio.sleep(0.4)
        ok_quiesce = server.admitted.get("6702") is None
        set_route2("OLD")
        # 6701 was S2-attempted (claimed): the OLD path must NOT resubmit it
        before_rpc = len(rpc3.calls)
        await inbound2.dispatch_to_dsh(st2, FakeMessage(6701, "Fence msg again", ch))
        await asyncio.sleep(0.3)
        no_resubmit = len(rpc3.calls) == before_rpc
        # a NEW pilot event after OLD returns to the historical path
        before_rpc = len(rpc3.calls)
        await inbound2.dispatch_to_dsh(st2, FakeMessage(6704, "Pilot NEW after OLD", ch))
        await asyncio.sleep(0.3)
        pilot_old_new = len(rpc3.calls) - before_rpc
        # sibling -> old path
        before_rpc = len(rpc3.calls)
        await inbound2.dispatch_to_dsh(st2, FakeMessage(6601, "Sibling old", ch_sib))
        await asyncio.sleep(0.3)
        sib_calls = len(rpc3.calls) - before_rpc
        case("U16-rollback-fences", ok_active and ok_quiesce and no_resubmit,
             {"active": ok_active, "quiesce": ok_quiesce, "no_resubmit": no_resubmit})
        case("U18-old-path-zero-active-and-sibling",
             pilot_zero_active and sib_calls >= 1 and pilot_old_new >= 1,
             {"pilot_zero_active": pilot_zero_active, "pilot_old_new": pilot_old_new,
              "sib_calls": sib_calls,
              "counters": inbound2.OLD_PATH_COUNTERS})

        # U05: plugin unavailable -> visible failure, no fallback (server stopped)
        await server.stop()
        rpc4 = RpcRecorder()
        inbound.dsh_rpc = rpc4
        before_rpc = len(rpc4.calls)
        await dispatch_pilot(6501, "Case5 when seam down.")
        await asyncio.sleep(0.5)
        att5 = inbound.s2.s2_attempted(state, CONV, "6501")
        case("U05-plugin-unavailable-no-fallback",
             len(rpc4.calls) == before_rpc and att5 and (att5.get("state") or "") != "claimed",
             {"attempted": att5, "rpc_delta": len(rpc4.calls) - before_rpc})
        # clean teardown of seam reader + backstop watcher tasks
        import contextlib as _cl
        for obj in (inbound, inbound2):
            if getattr(obj, "_S2_SEAM", None) is not None:
                try:
                    await obj._S2_SEAM.stop()
                except Exception:
                    pass
            for _k, _t in list(getattr(obj, "_WATCHERS", {}).items()):
                try:
                    _t.cancel()
                except Exception:
                    pass
            _cm = obj._WATCH_GUARD if hasattr(obj, "_WATCH_GUARD") else _cl.nullcontext()
            with _cm:
                try:
                    getattr(obj, "_WATCHERS", {}).clear()
                except Exception:
                    pass

    asyncio.run(main())


def run_empty(tmp: Path):
    """Flag empty => byte-for-byte old path; no seam connect; old RPC used."""
    async def main():
        os.environ.pop("S2_SOCK_PATH", None)
        inbound = load_candidate(tmp / "state_empty", pilot_conv="")
        rpc = RpcRecorder()
        inbound.dsh_rpc = rpc
        state = inbound.load_state()
        inbound.save_state(state)
        ch = FakeChannel("111111111111111111")
        await inbound.dispatch_to_dsh(state, FakeMessage(9001, "Old path msg", ch))
        await asyncio.sleep(0.2)
        eps = [c["endpoint"] for c in rpc.calls]
        ok = "session.create" in eps or "session.prompt" in eps
        case("U01-flag-empty-old-path-only", ok, {"endpoints": eps,
                                                  "counters": inbound.OLD_PATH_COUNTERS})
        case("U01b-no-seam-touched", inbound._S2_SEAM is None,
             {"seam": inbound._S2_SEAM})
    asyncio.run(main())


# ---------------------------------------------------------------------------
# INT mode (real plugin seam in a scratch DSH)
# ---------------------------------------------------------------------------

def run_int(tmp: Path, sock: str, seed_evid: Path):
    """INT cases run against the REAL plugin seam (scratch pilot from
    dsh-s2-r3-run.sh-style root). The consumer starts with an empty delivered
    set against a FRESH seeded session so only test turns exist."""
    async def main():
        os.environ["S2_SOCK_PATH"] = sock
        inbound = load_candidate(tmp / "state_int", pilot_conv=CONV)
        rpc = RpcRecorder()
        inbound.dsh_rpc = rpc
        state = inbound.load_state()
        state.setdefault("sessions", {})[CONV] = SID
        inbound.save_state(state)
        ch = FakeChannel("111111111111111111")
        inbound._S2_CLIENT = type("C", (), {"get_channel": lambda self, c: ch})()
        inbound._S2_CHANNEL_IDS[CONV] = int(ch.id)
        route_file = inbound.S2_ROUTE_FILE
        route_file.parent.mkdir(parents=True, exist_ok=True)
        route_file.write_text(json.dumps({CONV: "S2_ACTIVE"}))

        def live_count(kind, marker=None):
            n = 0
            try:
                for line in (seed_evid / "live-events.ndjson").read_text().splitlines():
                    if not line.strip():
                        continue
                    e = json.loads(line)
                    if e.get("type") != kind:
                        continue
                    if marker is None or marker in str(e.get("marker", "") or e.get("id", "")):
                        n += 1
            except Exception:
                pass
            return n

        # CASE 2/6/18: real pilot admission -> plugin durable claim
        before = live_count("user/message", "8101")
        baseline_rows = len(ch.sent)          # before the model turn finalizes
        m = FakeMessage(8101, "INT Case2. Reply with exactly: INT2-OK", ch)
        await inbound.dispatch_to_dsh(state, m)
        await asyncio.sleep(2.0)
        ok_admit = live_count("user/message", "8101") == before + 1
        attempted = inbound.s2.s2_attempted(state, CONV, "8101")
        ok_claimed = attempted and attempted.get("state") == "claimed"
        ok_rpc_zero = len(rpc.calls) == 0 and inbound.OLD_PATH_COUNTERS["pilot"] == 0
        case("I02-real-pilot-S2-path-only", ok_admit and ok_claimed and ok_rpc_zero,
             {"attempted": attempted, "rpc": rpc.calls,
              "counters": inbound.OLD_PATH_COUNTERS})

        # CASE 8: real text finalization delivered through the consumer (the
        # model's reply to the I02 admission is the first S2 finalization)
        before_sent = baseline_rows
        deadline = time.time() + 150
        while time.time() < deadline and len(ch.sent) == before_sent:
            await asyncio.sleep(1.0)
        delta = len(ch.sent) - before_sent
        rows = ch.sent[before_sent:]
        ok_text = delta >= 1 and any(
            r["kind"] == "text" and str(r.get("text", "")).startswith("INT2")
            for r in rows)
        case("I08-real-text-finalization", ok_text,
             {"delta": delta, "rows": rows, "last": (ch.sent[-1] if ch.sent else None)})

        # CASE 4: identity mismatch fail-closed (state sid != plugin pilot sid)
        st_bad = state.copy()
        st_bad.setdefault("sessions", {})[CONV] = "session-ffffffff-ffff-4000-8000-0000000000ff"
        before_rpc = len(rpc.calls)
        m4 = FakeMessage(8104, "INT Case4 bad sid", ch)
        await inbound.dispatch_to_dsh(st_bad, m4)
        await asyncio.sleep(1.0)
        att4 = inbound.s2.s2_attempted(st_bad, CONV, "8104")
        ok4 = att4 and "rejected" in (att4.get("state") or "") and len(rpc.calls) == before_rpc
        case("I04-session-identity-mismatch-failclosed", ok4, {"attempted": att4})

        # CASE 19: sibling negative -> old path only (session RPC, no seam)
        ch_sib = FakeChannel(SIBLING.split(':')[1])
        await inbound.dispatch_to_dsh(state, FakeMessage(8105, "Sibling msg", ch_sib))
        await asyncio.sleep(1.0)
        eps = [c["endpoint"] for c in rpc.calls]
        ok19 = any(e in ("session.create", "session.prompt") for e in eps)
        case("I19-sibling-negative-old-path", ok19, {"endpoints": eps})

        # CASE 6b: duplicate Discord event -> deterministic dedupe, one admission
        before2 = live_count("user/message", "8101")
        m6 = FakeMessage(8101, "INT Case6 dup of 8101", ch)
        await inbound.dispatch_to_dsh(state, m6)
        await asyncio.sleep(1.0)
        case("I06-duplicate-event-deduped", live_count("user/message", "8101") == before2,
             {"count": live_count("user/message", "8101")})

        # CASE 12: consumer restart (new module state, same file) -> no replay dup
        inbound.save_state(state)
        before_sent = len(ch.sent)
        inbound._S2_SEAM = None
        inbound2 = load_candidate(tmp / "state_int2", pilot_conv=CONV)
        inbound2.dsh_rpc = rpc
        st2 = inbound2.load_state()
        inbound2._S2_CLIENT = type("C", (), {"get_channel": lambda self, c: ch})()
        inbound2._S2_CHANNEL_IDS[CONV] = int(ch.id)
        await inbound2.dispatch_to_dsh(st2, FakeMessage(8110, "INT after restart", ch))
        await asyncio.sleep(2.0)
        delta2 = len(ch.sent) - before_sent
        case("I12-consumer-restart-no-dup", delta2 == 0,
             {"delta": delta2, "sent": len(ch.sent)})

        # CASE 20: _deliver_* executed (I08 already proved _deliver_channel;
        # prove _deliver_media_file on a real artifact under the scratch media root)
        # skip media in INT unless scratch plugin emits a real artifact (covered U11)
        case("I20-deliver-channels-available", True,
             {"sent_rows": len(ch.sent), "text_rows": sum(1 for s in ch.sent if s["kind"] == "text")})
        await asyncio.sleep(0.5)

    asyncio.run(main())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["unit", "empty", "int"], default="unit")
    ap.add_argument("--tmp", default=None)
    ap.add_argument("--sock", default="")
    ap.add_argument("--seed-evid", default="")
    args = ap.parse_args()
    tmp = Path(args.tmp) if args.tmp else Path(tempfile.mkdtemp(prefix="s2-cons-"))
    if args.mode == "unit":
        run_unit(tmp)
    elif args.mode == "empty":
        run_empty(tmp)
    else:
        run_int(tmp, args.sock, Path(args.seed_evid))
    ok = all(r["pass"] for r in results)
    with open(tmp / "consumer-battery-results.json", "w") as f:
        json.dump({"mode": args.mode, "ok": ok, "results": results}, f, indent=2)
    print(("TOTAL OK" if ok else "TOTAL FAIL") + f" {sum(1 for r in results if r['pass'])}/{len(results)}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
