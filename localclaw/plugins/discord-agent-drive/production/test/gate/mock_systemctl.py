#!/usr/bin/env python3
"""mock_systemctl.py — hermetic systemctl double for S2 gate regression tests.

Simulates dsh.service + dsh-discord-inbound.service (+ platform units) inside an
overlay root. State is JSON under <ctl>/units/<unit>.json so every invocation
sees mutations made by earlier calls. On `restart dsh.service` the double
performs the actions the REAL plugin boot would produce (evidence dir, plugin
log lines, markers, S1 re-arm log tail) and starts a mock seam server; on
`restart dsh-discord-inbound.service` it writes a fresh <proc>/<pid>/environ
built from the drop-in EnvironmentFile (if present) — emulating systemd.

Config file (path in env S2_FAKE_CTL):
{
  "ctl": "<overlay>/ctl",
  "procDir": "<overlay>/proc",
  "plugin": {
    "evid": "...", "sock": "...", "conv": "...", "sid": "...",
    "noEvid": false, "bootFail": false, "badSock": false, "badSockGroup": false,
    "serverBin": "/path/mock_plugin_server.py", "log": "..."
  },
  "s1log": "...",
  "envFile": "...", "dropin": "...",
  "failInboundRestart": false, "failInboundOnce": false,
  "units": {"dsh.service": {"active": true, "enabled": true},
            "dsh-discord-inbound.service": {"active": true, "enabled": true}, ...}
}
Test-only tooling. Not used in production.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid

NOW_TS = "2026-09-09 00:00:00 UTC"


def cfg():
    p = os.environ.get("S2_FAKE_CTL")
    if not p:
        print("mock systemctl: S2_FAKE_CTL unset", file=sys.stderr)
        sys.exit(1)
    return json.load(open(p, encoding="utf-8"))


def unit_path(c, unit):
    return os.path.join(c["ctl"], "units", unit.replace("/", "_") + ".json")


def unit_state(c, unit, default=None):
    p = unit_path(c, unit)
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    d = {"active": False, "enabled": False, "pid": 0, "start": ""}
    d.update(default or {})
    return d


def write_unit(c, unit, st):
    os.makedirs(os.path.join(c["ctl"], "units"), exist_ok=True)
    with open(unit_path(c, unit), "w", encoding="utf-8") as f:
        json.dump(st, f)


def all_active_units(c):
    return [u for u in (c.get("units") or {}) if u.startswith(("dsh", "dsh-"))]


def gen_pid():
    return int(time.time() * 1000) % 400000 + 70000


def append(path, text):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception as e:
        print(f"mock: append failed {path}: {e}", file=sys.stderr)


def plugin_boot(c):
    pl = c.get("plugin") or {}
    evid = pl.get("evid")
    sock = pl.get("sock")
    conv = pl.get("conv") or "?"
    sid = pl.get("sid") or "?"
    no_evid = bool(pl.get("noEvid"))
    boot_fail = bool(pl.get("bootFail"))
    if boot_fail:
        return False
    # the real plugin only loads when its composition row/file is present; the
    # mock mirrors that by checking the plugin FILE exists at dsh boot time
    s2_present = bool(pl.get("file")) and os.path.exists(pl["file"])
    if s2_present and not no_evid and evid:
        os.makedirs(evid, exist_ok=True)
        log = os.path.join(evid, "plugin.log")
        if os.path.exists(log):
            try: os.remove(log)
            except Exception: pass
        append(log, f"{NOW_TS} apply: pilot={conv} sid={sid} sock={sock} configured=yes")
        append(log, f"{NOW_TS} socket listening {sock}")
        for name in ("plugin-loaded.json", "plugin-ready.json"):
            with open(os.path.join(evid, name), "w", encoding="utf-8") as f:
                json.dump({"at": NOW_TS, "pilotConv": conv, "pilotSid": sid}, f)
    # S1 re-arm evidence tail (exact strings the gate waits on)
    if c.get("s1log"):
        append(c["s1log"], f"{NOW_TS} INFO boot: start; configured schedule owner(s) = 2")
        append(c["s1log"], f"{NOW_TS} INFO schedule plugin entry active (agent/created listener installed)")
        append(c["s1log"], f"{NOW_TS} INFO resume ok: session-bb8442f9-3320-498e-9278-8d88db60d3f2 (durable schedule/change records = 13; live roots = 1)")
        append(c["s1log"], f"{NOW_TS} INFO resume ok: session-e5dc0463-536a-419a-8e27-9b5c2417358f (durable schedule/change records = 3; live roots = 2)")
        append(c["s1log"], f"{NOW_TS} INFO boot: done -> resumed=2 alreadyLive=0 missing=0 failed=0 invalid=0 (live roots=2)")
    # start the mock seam server (mirrors the plugin socket side)
    if s2_present and not no_evid and sock and pl.get("serverBin"):
        srv = pl["serverBin"]
        env = dict(os.environ)
        env["S2_MOCK_SOCK"] = sock
        env["S2_MOCK_EVID"] = evid or ""
        env["S2_MOCK_CONV"] = conv
        env["S2_MOCK_SID"] = sid
        env["S2_MOCK_BAD_SOCK"] = "1" if pl.get("badSock") else ""
        env["S2_MOCK_BAD_GROUP"] = "1" if pl.get("badSockGroup") else ""
        try:
            subprocess.Popen([sys.executable, srv], env=env,
                             start_new_session=True,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"mock: server start failed: {e}", file=sys.stderr)
    return True


def inbound_environ(c):
    """Emulate systemd EnvironmentFile expansion for the inbound unit."""
    out = ["PATH=/usr/bin:/bin", "LANG=C.UTF-8"]
    dropin = c.get("dropin") or ""
    envfile = c.get("envFile") or ""
    if dropin and os.path.exists(dropin):
        for line in open(dropin, encoding="utf-8"):
            line = line.strip()
            if line.startswith("EnvironmentFile="):
                ef = line.split("=", 1)[1]
                envfile = ef
    if envfile and os.path.exists(envfile):
        for line in open(envfile, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            out.append(line)
    return out


def main():
    c = cfg()
    args = sys.argv[1:]
    if not args:
        print("mock systemctl: no args", file=sys.stderr)
        return 1
    verb = args[0]
    rest = args[1:]
    if verb == "daemon-reload":
        return 0
    if verb in ("is-active", "is-enabled"):
        unit = rest[0] if rest else ""
        st = unit_state(c, unit, {"active": True, "enabled": True})
        ok = st.get("active", True) if verb == "is-active" else st.get("enabled", True)
        print("active" if ok else "inactive")
        return 0 if ok else 1
    if verb == "show":
        prop = " ".join(rest)
        unit = rest[-1] if rest else ""
        if "--value" in rest:
            unit = rest[rest.index("--value") + 1]
        st = unit_state(c, unit, {"pid": 0, "start": NOW_TS})
        if "MainPID" in prop:
            print(st.get("pid", 0))
        elif "ExecMainStartTimestamp" in prop:
            print(st.get("start", NOW_TS))
        elif "Result" in prop:
            print("success")
        elif "SubState" in prop:
            print("running" if st.get("active") else "dead")
        else:
            print("")
        return 0
    if verb == "restart":
        unit = rest[0] if rest else ""
        st = unit_state(c, unit, {"active": False, "enabled": True, "pid": 0})
        if unit == "dsh.service":
            if not plugin_boot(c):
                st["active"] = False
                write_unit(c, unit, st)
                return 1
            st["pid"] = gen_pid()
            st["start"] = NOW_TS
            st["active"] = True
            write_unit(c, unit, st)
            return 0
        if unit == "dsh-discord-inbound.service":
            once = bool(c.get("failInboundOnce")) and not c.get("_once_used")
            crash = bool(c.get("crashInboundOnce")) and not c.get("_crash_used")
            if c.get("failInboundRestart") or once:
                c["_once_used"] = True
                json.dump(c, open(os.environ["S2_FAKE_CTL"], "w"), indent=2)
                st["active"] = False
                write_unit(c, unit, st)
                return 1
            pid = gen_pid()
            st["pid"] = pid
            st["start"] = NOW_TS
            st["active"] = True
            write_unit(c, unit, st)
            if crash:
                c["_crash_used"] = True
                json.dump(c, open(os.environ["S2_FAKE_CTL"], "w"), indent=2)
                st["active"] = False
                write_unit(c, unit, st)
            proc = c.get("procDir") or ""
            if proc and not crash:
                os.makedirs(os.path.join(proc, str(pid)), exist_ok=True)
                with open(os.path.join(proc, str(pid), "environ"), "w", encoding="utf-8") as f:
                    f.write("\n".join(inbound_environ(c)) + "\n")
            return 0
        # generic platform unit restart: keep active
        st["pid"] = gen_pid(); st["start"] = NOW_TS; st["active"] = True
        write_unit(c, unit, st)
        return 0
    if verb == "start":
        unit = rest[0] if rest else ""
        st = unit_state(c, unit, {"active": False, "enabled": True, "pid": 0})
        st["active"] = True
        write_unit(c, unit, st)
        return 0
    if verb == "stop" or verb == "disable":
        unit = rest[0] if rest else ""
        st = unit_state(c, unit, {"active": False, "enabled": True, "pid": 0})
        if verb == "disable":
            st["enabled"] = False
        st["active"] = False
        write_unit(c, unit, st)
        return 0
    print(f"mock systemctl: unhandled {verb}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
