#!/usr/bin/env bash
# dsh_s2_gate_regression.sh — hermetic regression battery for the corrected S2
# live gate (production/dsh_s2_live_gate.sh). NO PRODUCTION MUTATION: every
# path is re-pointed into a fresh overlay under /tmp; systemctl/journalctl/
# systemd-tmpfiles are doubles; the mock "plugin" is a real AF_UNIX seam server.
#
# Usage: test/gate/dsh_s2_gate_regression.sh   (exit 0 = all PASS)
# Requires: real candidate + live-listener bytes reachable (see constants),
# python3, zstd, and membership of the test user in group dsh-media.

set -u

# The battery chgrps the overlay socket to group dsh-media; a recorded PASS
# therefore implies the runner really is a member (self-proving evidence).
id -nG | tr ' ' '\n' | grep -qx dsh-media || {
  echo "battery requires membership in group dsh-media (overlay socket chgrp)" >&2
  exit 1
}

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE_DIR="$(cd "$HARNESS_DIR/../.." && pwd)"          # .../production
GATE="$GATE_DIR/dsh_s2_live_gate.sh"
PROBE="$GATE_DIR/dsh_s2_hello_probe.py"
SRV="$HARNESS_DIR/mock_plugin_server.py"
SYSCTL="$HARNESS_DIR/mock_systemctl.py"
TMPF="$HARNESS_DIR/mock_tmpfiles.py"

EXPECT_LIVE_SHA="528cb84a1905097c3fd0a14f41ad05a7b5df4308a961341891f5723706c9c511"
EXPECT_LISTENER_SHA="b9907f40d4a34a618f22cbce6b3ef0a31b63c3bf868242c9f13a3fd980e32e08"
EXPECT_SEAM_SHA="fd40d6c4b770befd3a45bb1bf550015c02474ac19f137ae008ac04c2fc0ce8d2"
EXPECT_PLUGIN_SHA="01183e27b74044ef0f7b486e1123a9141b670221894a721f142e1f3ba6c49cd3"

CONV="channel:111122223333444455"
SID="session-aaaaaaaa-1111-2222-3333-444455556666"
SIB="channel:999988887777666655"
SIDSIB="session-bbbbbbbb-1111-2222-3333-444455556666"
PROV="scratch-local"
MODEL="switchyard/deepseek/deepseek-v4-flash-nt"

BASE="/tmp/dsh-s2-gate-regression"
rm -rf "$BASE"; mkdir -p "$BASE"
OV=""      # current overlay
PASS=0; FAIL=0
note() { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); note "  PASS  $*"; }
bad()  { FAIL=$((FAIL+1)); note "  FAIL  $*"; }

# ---------- overlay plumbing -------------------------------------------------
setup_overlay() { # setup_overlay <name>
  OV="$BASE/$1"
  R="$OV/root"
  unset S2_LISTENER_STAGED S2_HELPER_STAGED S2_PLUGIN_STAGED 2>/dev/null || true
  pkill -f 'mock_plugin_server.py' 2>/dev/null || true
  rm -rf "$OV"
  mkdir -p "$R/opt/dsh/home/plugins" "$R/opt/dsh-inbound" \
           "$R/var/lib/dsh-discord-inbound" "$R/etc/dsh" \
           "$R/etc/systemd/system/dsh-discord-inbound.service.d" \
           "$R/etc/tmpfiles.d" "$R/run" "$OV/bin" "$OV/proc" "$OV/evid" \
           "$OV/journal" "$OV/ctl/units"
  # live listener baseline = REAL current production bytes (approved hash)
  cp /opt/dsh-inbound/dsh_discord_inbound.py "$R/opt/dsh-inbound/dsh_discord_inbound.py"
  # composition = REAL current production cordis.patch.yml (no S2 row)
  cp /opt/dsh/home/cordis.patch.yml "$R/opt/dsh/home/cordis.patch.yml"
  # S1 plugin fixture (file presence is what the gate checks)
  echo '// s1 plugin fixture (hermetic)' > "$R/opt/dsh/home/plugins/schedule-boot-rearm.mjs"
  : > "$R/opt/dsh/home/plugins/schedule-boot-rearm.log"
  # PATH doubles
  cat > "$OV/bin/systemctl" <<EOF
#!/usr/bin/env bash
exec python3 "$SYSCTL" "\$@"
EOF
  cat > "$OV/bin/journalctl" <<'EOF'
#!/usr/bin/env bash
cat "${S2_FAKE_JOURNAL_FILE:-/dev/null}" 2>/dev/null || true
EOF
  cat > "$OV/bin/systemd-tmpfiles" <<EOF
#!/usr/bin/env bash
exec python3 "$TMPF" "\$@"
EOF
  chmod +x "$OV/bin/systemctl" "$OV/bin/journalctl" "$OV/bin/systemd-tmpfiles"
  # inbound-state.json fixture (pilot + sibling mapping; no s2 branch)
  cat > "$R/var/lib/dsh-discord-inbound/inbound-state.json" <<EOF
{"sessions":{"$CONV":"$SID","$SIB":"$SIDSIB"},"processed":[],"media_delivered":{},"s2":{}}
EOF
  # durable session logs (zstd) with request/header config pins
  mkdir -p "$R/opt/dsh/home/sessions/ns-1/$SID" "$R/opt/dsh/home/sessions/ns-1/$SIDSIB"
  write_durable "$R/opt/dsh/home/sessions/ns-1/$SID/session.jsonl.zstd" "$PROV" "$MODEL" ""
  write_durable "$R/opt/dsh/home/sessions/ns-1/$SIDSIB/session.jsonl.zstd" "$PROV" "$MODEL" ""
  # listener journal fixture (innocuous)
  cat > "$OV/journal/listener.log" <<'EOF'
2026-09-09T00:00:00Z INFO dsh-discord-inbound listener started (gate fixture)
2026-09-09T00:00:00Z INFO discord gateway connected
EOF
}

write_durable() { # write_durable <path> <provider> <model> [reasoning] [maxTokens]
  local path="$1" provider="$2" model="$3" reasoning="${4:-}" maxtok="${5:-}"
  local tmp="$path.raw" cfg="\"provider\":\"$provider\",\"model\":\"$model\""
  if [ -n "$reasoning" ]; then cfg="$cfg,\"reasoningEffort\":\"$reasoning\""; fi
  if [ -n "$maxtok" ]; then cfg="$cfg,\"maxTokens\":$maxtok"; fi
  mkdir -p "$(dirname "$path")"
  cat > "$tmp" <<EOF
{"type":"request/header","data":{"header":{"config":{$cfg}}}}
{"type":"turn/start","data":{"turn":1}}
{"type":"turn/end","data":{"turn":1,"reason":{"kind":"completed"}}}
EOF
  zstd -q -f "$tmp" -o "$path" 2>/dev/null
  rm -f "$tmp"
}

ctl_conf() { # ctl_conf  (default config JSON; then overrides applied by caller env via ctl_set)
  local sock="$R/run/dsh-discord-pilot/dsh.sock"
  python3 - "$OV" "$R" "$sock" <<'EOF'
import json,sys
ov,r,sock=sys.argv[1:4]
c={
 "ctl":f"{ov}/ctl",
 "procDir":f"{ov}/proc",
 "units":{},
 "plugin":{"evid":f"{r}/opt/dsh/home/plugins/discord-agent-drive-evidence",
           "file":f"{r}/opt/dsh/home/plugins/discord-agent-drive.mjs",
           "sock":sock,"conv":"channel:111122223333444455",
           "sid":"session-aaaaaaaa-1111-2222-3333-444455556666",
           "noEvid":False,"bootFail":False,"badSock":False,"badSockGroup":False,
           "serverBin":"PLACEHOLDER"},
 "s1log":f"{r}/opt/dsh/home/plugins/schedule-boot-rearm.log",
 "envFile":f"{r}/etc/dsh/dsh-s2.env",
 "dropin":f"{r}/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf",
 "failInboundRestart":False,"failInboundOnce":False,"crashInboundOnce":False,
}
for u in ("dsh.service","dsh-discord-inbound.service","dsh-line-inbound","dsh-discord","dsh-webgate"):
    c["units"][u]={"active":True,"enabled":True,"pid":50000+len(c["units"])}
json.dump(c,open(f"{ov}/ctl/conf.json","w"),indent=2)
EOF
  python3 - "$OV" "$SRV" <<'EOF'
import json,sys
ov,srv=sys.argv[1:3]
p=f"{ov}/ctl/conf.json"; c=json.load(open(p)); c["plugin"]["serverBin"]=srv
json.dump(c,open(p,"w"),indent=2)
EOF
}

ctl_set() { # ctl_set <dotted.key> <value>
  python3 - "$OV/ctl/conf.json" "$1" "$2" <<'EOF'
import json,sys
p,k,v=sys.argv[1:4]
c=json.load(open(p))
node=c
ks=k.split(".")
for kk in ks[:-1]: node=node.setdefault(kk,{})
node[ks[-1]]=v
json.dump(c,open(p,"w"),indent=2)
EOF
}

gate_env() {
  export S2_GATE_HERMETIC=1 S2_GATE_HERMETIC_ROOT="$R"
  export S2_DSH_HOME_DIR="$R/opt/dsh/home"
  export S2_LISTENER_LIVE="$R/opt/dsh-inbound/dsh_discord_inbound.py"
  export S2_HELPER_LIVE="$R/opt/dsh-inbound/s2_seam.py"
  export S2_STATE_DIR="$R/var/lib/dsh-discord-inbound"
  export S2_SOCK_DIR="$R/run/dsh-discord-pilot"
  export S2_ENV_FILE="$R/etc/dsh/dsh-s2.env"
  export S2_DROPIN_DIR="$R/etc/systemd/system/dsh-discord-inbound.service.d"
  export S2_TMPFILES_CONF="$R/etc/tmpfiles.d/dsh-s2-pilot.conf"
  export S2_EVID_BASE="$OV/evid"
  export S2_GATE_PROC_DIR="$OV/proc"
  export S2_EXPECT_SOCK_OWNER="vincent"
  export S2_EXPECT_SOCK_GROUP="dsh-media"
  export S2_SETTLE_S=1 S2_BOOT_WAIT_S=8 S2_S1_WAIT_S=8 S2_POST_RESTART_S=1
  export S2_PROBE_QUIET_S=1
  export S2_PILOT_CONV="$CONV" S2_PILOT_SID="$SID"
  export S2_PROVIDER="$PROV" S2_MODEL="$MODEL"
  export S2_REASONING_EFFORT="" S2_MAX_TOKENS=0
  export S2_FAKE_CTL="$OV/ctl/conf.json"
  export S2_FAKE_JOURNAL_FILE="$OV/journal/listener.log"
  export PATH="$OV/bin:$PATH"
}

run_gate() { # run_gate <mode> <logfile> ; sets RC
  local mode="$1" logf="$2"
  gate_env
  bash "$GATE" "$mode" > "$logf" 2>&1
  RC=$?
}

state_sha() { sha256sum "$R/var/lib/dsh-discord-inbound/inbound-state.json" | cut -d' ' -f1; }
file_sha()  { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }
count_occur() { grep -c -- "$1" "$2" 2>/dev/null || echo 0; }

# ---------- assertion helpers ------------------------------------------------
assert_rc() { # assert_rc <name> <want>
  if [ "$RC" = "$2" ]; then ok "$1 (rc=$RC)"; else bad "$1 (rc=$RC want $2)"; fi
}
assert_sha() { # assert_sha <name> <file> <want>
  local got; got="$(file_sha "$2")"
  if [ "$got" = "$3" ]; then ok "$1"; else bad "$1 (sha=$got want $3)"; fi
}
assert_absent() { # assert_absent <name> <path>
  if [ ! -e "$2" ]; then ok "$1"; else bad "$1 (exists: $2)"; fi
}
assert_present() { # assert_present <name> <path>
  if [ -e "$2" ]; then ok "$1"; else bad "$1 (missing: $2)"; fi
}
assert_grep() { # assert_grep <name> <file> <regex>
  if grep -q -- "$3" "$2" 2>/dev/null; then ok "$1"; else bad "$1 (no match $3 in $2)"; fi
}
assert_nogrep() { # assert_nogrep <name> <file> <regex>
  if grep -q -- "$3" "$2" 2>/dev/null; then bad "$1 (unexpected $3 in $2)"; else ok "$1"; fi
}
assert_json() { # assert_json <name> <file> <python-expr>
  if python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(0 if ('"$3"') else 1)' "$2"; then
    ok "$1"
  else
    bad "$1 (expr: $3)"
  fi
}

# =============================================================================
note "=== S2 gate regression battery ==="
note "overlay base: $BASE"

# T01 clean baseline preflight PASS (socket absent is OK pre-mount)
setup_overlay T01-clean-preflight; ctl_conf; gate_env
PRE_SHA="$(state_sha)"
run_gate preflight "$OV/gate.log"
assert_rc "T01 preflight exit 0" 0
assert_grep "T01 verdict PASS in log" "$OV/gate.log" "PREFLIGHT PASS"
assert_absent "T01 socket not required (absent)" "$R/run/dsh-discord-pilot/dsh.sock"
assert_sha "T01 listener untouched" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T01 route absent" "$R/var/lib/dsh-discord-inbound/s2-route.json"
[ "$(state_sha)" = "$PRE_SHA" ] && ok "T01 state file untouched" || bad "T01 state changed"
TX="$(ls -1dt "$OV"/evid/txn-* | head -1)"
assert_json "T01 txn verified" "$TX/manifest.json" 'd.get("state")=="verified"'

# T02 bad listener source hash prevents stage (zero mutation)
setup_overlay T02-bad-listener-sha; ctl_conf; gate_env
mkdir -p "$OV/bad"
cp "$GATE_DIR/dsh_discord_inbound.s2.py" "$OV/bad/dsh_discord_inbound.s2.py"
printf '\x41' | dd of="$OV/bad/dsh_discord_inbound.s2.py" bs=1 seek=700 count=1 conv=notrunc 2>/dev/null
export S2_LISTENER_STAGED="$OV/bad/dsh_discord_inbound.s2.py"
PRE_SHA="$(state_sha)"; PRE_PID="$(python3 -c 'import json;print(json.load(open("'$OV'/ctl/conf.json"))["units"]["dsh.service"]["pid"])')"
run_gate stage "$OV/gate.log"
assert_rc "T02 stage refused (bad listener hash)" 1
assert_grep "T02 zero-mutation message" "$OV/gate.log" "ZERO MUTATION"
assert_sha "T02 listener untouched" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T02 plugin absent" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_nogrep "T02 no discord-agent-drive row" "$R/opt/dsh/home/cordis.patch.yml" "discord-agent-drive"
assert_absent "T02 socket absent" "$R/run/dsh-discord-pilot/dsh.sock"
[ "$(state_sha)" = "$PRE_SHA" ] && ok "T02 state untouched" || bad "T02 state changed"

# T03 bad binding prevents stage (zero mutation)
setup_overlay T03-bad-binding; ctl_conf; gate_env
python3 - "$R" <<EOF
import json
p="$R/var/lib/dsh-discord-inbound/inbound-state.json"
d=json.load(open(p)); d["sessions"]["$CONV"]="session-ffffffff-1111-2222-3333-444455556666"
json.dump(d,open(p,"w"))
EOF
run_gate stage "$OV/gate.log"
assert_rc "T03 stage refused (bad binding)" 1
assert_grep "T03 zero-mutation message" "$OV/gate.log" "ZERO MUTATION"
assert_sha "T03 listener untouched" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T03 plugin absent" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"

# T04 bad session/pin prevents stage (duplicate logs + pin mismatch)
setup_overlay T04a-dup-session; ctl_conf; gate_env
mkdir -p "$R/opt/dsh/home/sessions/ns-2/$SID"
write_durable "$R/opt/dsh/home/sessions/ns-2/$SID/session.jsonl.zstd" "$PROV" "$MODEL" ""
run_gate stage "$OV/gate.log"
assert_rc "T04a stage refused (duplicate session logs)" 1
assert_grep "T04a zero-mutation message" "$OV/gate.log" "ZERO MUTATION"
setup_overlay T04b-pin-mismatch; ctl_conf; gate_env
python3 - "$R/opt/dsh/home/sessions/ns-1/$SID/session.jsonl.zstd" <<EOF
import json,subprocess,os,sys
p=sys.argv[1]
raw=p+".raw"
subprocess.run(["zstd","-q","-d","-c",p],stdout=open(raw,"w"))
s=open(raw).read().replace("$MODEL","switchyard/other/model-nt")
open(raw,"w").write(s)
subprocess.run(["zstd","-q","-f",raw,"-o",p])
os.remove(raw)
EOF
run_gate stage "$OV/gate.log"
assert_rc "T04b stage refused (pin mismatch)" 1
assert_grep "T04b zero-mutation message" "$OV/gate.log" "ZERO MUTATION"

# T16 recorded non-empty reasoningEffort/maxTokens pin parity (not inferred)
setup_overlay T16-pin-parity; ctl_conf; gate_env
write_durable "$R/opt/dsh/home/sessions/ns-1/$SID/session.jsonl.zstd" "$PROV" "$MODEL" "high" "4096"
gate_env
S2_REASONING_EFFORT=high S2_MAX_TOKENS=4096 bash "$GATE" stage > "$OV/gate.log" 2>&1
RC=$?
assert_rc "T16 stage exit 0 (recorded reasoning/max parity)" 0
assert_grep "T16 row carries reasoningEffort high" "$R/opt/dsh/home/cordis.patch.yml" "reasoningEffort: 'high'"
assert_grep "T16 row carries maxTokens 4096" "$R/opt/dsh/home/cordis.patch.yml" "maxTokens: 4096"
assert_absent "T16 route stays absent" "$R/var/lib/dsh-discord-inbound/s2-route.json"
assert_present "T16 socket ready" "$R/run/dsh-discord-pilot/dsh.sock"

# T05 invalid operator inputs -> stage zero mutation
setup_overlay T05-bad-inputs; ctl_conf; gate_env
S2_PILOT_CONV='channel:has space' bash "$GATE" stage > "$OV/gate.log" 2>&1
RC=$?
assert_rc "T05 stage refused (invalid inputs)" 1
assert_sha "T05 listener untouched" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T05 plugin absent" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"

# T06 listener install/restart failure rolls the listener back
setup_overlay T06-listener-fail; ctl_conf; ctl_set failInboundOnce true; gate_env
PRE_SHA="$(state_sha)"
run_gate stage "$OV/gate.log"
assert_rc "T06 stage failed -> nonzero" 1
assert_grep "T06 full rollback performed" "$OV/gate.log" "FULL baseline rollback complete"
assert_sha "T06 listener rolled back to baseline" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T06 helper rolled back (absent)" "$R/opt/dsh-inbound/s2_seam.py"
assert_absent "T06 plugin rolled back" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_nogrep "T06 no row left" "$R/opt/dsh/home/cordis.patch.yml" "discord-agent-drive"
assert_absent "T06 route absent" "$R/var/lib/dsh-discord-inbound/s2-route.json"
[ "$(state_sha)" = "$PRE_SHA" ] && ok "T06 state untouched" || bad "T06 state changed"

# T07 listener health failure after restart -> rollback
setup_overlay T07-health-fail; ctl_conf; ctl_set crashInboundOnce true; gate_env
run_gate stage "$OV/gate.log"
assert_rc "T07 stage failed -> nonzero" 1
assert_sha "T07 listener rolled back to baseline" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T07 plugin rolled back" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"

# T08 plugin hash mismatch stops before ANY mutation (incl. before dsh restart)
setup_overlay T08-plugin-sha; ctl_conf; gate_env
mkdir -p "$OV/bad"
cp "$GATE_DIR/../discord-agent-drive.mjs" "$OV/bad/discord-agent-drive.mjs"
printf '\x41' | dd of="$OV/bad/discord-agent-drive.mjs" bs=1 seek=300 count=1 conv=notrunc 2>/dev/null
export S2_PLUGIN_STAGED="$OV/bad/discord-agent-drive.mjs"
DPID0="$(python3 -c 'import json;print(json.load(open("'$OV'/ctl/conf.json"))["units"]["dsh.service"]["pid"])')"
run_gate stage "$OV/gate.log"
assert_rc "T08 stage refused (plugin hash)" 1
assert_grep "T08 zero-mutation message" "$OV/gate.log" "ZERO MUTATION"
assert_absent "T08 plugin file absent" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_absent "T08 plugin evid absent" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin-ready.json"
assert_sha "T08 listener untouched" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
DPID1="$(python3 -c 'import json;print(json.load(open("'$OV'/ctl/conf.json"))["units"]["dsh.service"]["pid"])')"
[ "$DPID0" = "$DPID1" ] && ok "T08 dsh never restarted" || bad "T08 dsh restarted (pid $DPID0->$DPID1)"
unset S2_PLUGIN_STAGED

# T09 plugin mount/readiness failure restores composition (full baseline)
setup_overlay T09-plugin-readiness; ctl_conf; ctl_set plugin.noEvid true; gate_env
run_gate stage "$OV/gate.log"
assert_rc "T09 stage failed -> nonzero" 1
assert_sha "T09 listener restored to baseline" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T09 plugin removed" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_nogrep "T09 row removed" "$R/opt/dsh/home/cordis.patch.yml" "discord-agent-drive"
assert_absent "T09 socket dir removed" "$R/run/dsh-discord-pilot"
assert_absent "T09 tmpfiles rule removed" "$R/etc/tmpfiles.d/dsh-s2-pilot.conf"

# T10 socket wrong group causes rollback
setup_overlay T10-socket-group; ctl_conf; ctl_set plugin.badSockGroup true; gate_env
run_gate stage "$OV/gate.log"
assert_rc "T10 stage failed -> nonzero" 1
assert_grep "T10 rollback evidence" "$OV/gate.log" "FULL baseline rollback complete"
assert_sha "T10 listener restored to baseline" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T10 plugin removed" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_absent "T10 socket dir removed" "$R/run/dsh-discord-pilot"

# T11 staged success leaves route OLD + full happy-stage assertions
setup_overlay T11-stage-ok; ctl_conf; gate_env
PRE_SHA="$(state_sha)"
run_gate stage "$OV/gate.log"
assert_rc "T11 stage exit 0" 0
assert_grep "T11 stage PASS" "$OV/gate.log" "STAGE PASS"
assert_sha "T11 listener now candidate" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LISTENER_SHA"
assert_sha "T11 helper installed" "$R/opt/dsh-inbound/s2_seam.py" "$EXPECT_SEAM_SHA"
assert_sha "T11 plugin file installed" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs" "$EXPECT_PLUGIN_SHA"
[ "$(count_occur 'discord-agent-drive' "$R/opt/dsh/home/cordis.patch.yml")" = "3" ] \
  && ok "T11 composition row added" || bad "T11 row count wrong"
assert_absent "T11 route stays absent" "$R/var/lib/dsh-discord-inbound/s2-route.json"
assert_absent "T11 env file absent (default off)" "$R/etc/dsh/dsh-s2.env"
assert_absent "T11 drop-in absent (default off)" "$R/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf"
assert_present "T11 socket present" "$R/run/dsh-discord-pilot/dsh.sock"
python3 - "$R" <<'EOF' && ok "T11 socket perms exact" || bad "T11 socket perms"
import os,stat,grp,sys
r=sys.argv[1]
sd=f"{r}/run/dsh-discord-pilot"; ss=f"{sd}/dsh.sock"
gid=grp.getgrnam("dsh-media").gr_gid
asserts=[
 (stat.S_ISDIR(os.lstat(sd).st_mode) and stat.S_IMODE(os.lstat(sd).st_mode)==0o2770 and os.lstat(sd).st_gid==gid, "dir 2770 gid dsh-media"),
 (stat.S_ISSOCK(os.lstat(ss).st_mode) and stat.S_IMODE(os.lstat(ss).st_mode)==0o660 and os.lstat(ss).st_gid==gid, "socket 0660 gid dsh-media"),
]
for c,n in asserts:
    print(("  PASS  " if c else "  FAIL  ")+n)
    if not c: sys.exit(1)
EOF
assert_grep "T11 plugin boot evidence" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin.log" "socket listening"
assert_grep "T11 probe reconcile evidence" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin.log" "reconcile done"
TX="$(ls -1dt "$OV"/evid/txn-* | head -1)"
assert_json "T11 txn verified" "$TX/manifest.json" 'd.get("state")=="verified"'
assert_present "T11 probe.json present" "$TX/probe.json"
assert_json "T11 probe ok zero frames" "$TX/probe.json" 'd.get("ok") and not d.get("quietFrames")'
assert_nogrep "T11 no activation frames" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin.log" "CLAIM observed"
[ "$(state_sha)" = "$PRE_SHA" ] && ok "T11 state untouched" || bad "T11 state changed"
STAGED_TX="$TX"

# T12 activate without a staged-success receipt refuses (zero mutation)
setup_overlay T12-no-receipt; ctl_conf; gate_env
run_gate activate "$OV/gate.log"
assert_rc "T12 activate refused (no receipt)" 1
assert_grep "T12 reason" "$OV/gate.log" "requires a staged-success receipt"
assert_absent "T12 env file absent" "$R/etc/dsh/dsh-s2.env"
assert_absent "T12 drop-in absent" "$R/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf"
assert_absent "T12 route absent" "$R/var/lib/dsh-discord-inbound/s2-route.json"

# T13 activate only affects the exact pilot + T15 sibling proof
setup_overlay T13-activate; ctl_conf; gate_env
PRE_SHA="$(state_sha)"
run_gate stage "$OV/gate.log"
assert_rc "T13 stage exit 0" 0
STAGE_PATCH_SHA="$(file_sha "$R/opt/dsh/home/cordis.patch.yml")"
run_gate activate "$OV/gate.log"
assert_rc "T13 activate exit 0" 0
assert_grep "T13 activate PASS" "$OV/gate.log" "ACTIVATE PASS"
TXA="$(ls -1dt "$OV"/evid/txn-* | head -1)"
assert_json "T13 activate txn verified" "$TXA/manifest.json" 'd.get("state")=="verified"'
# route file: single key == pilot conv, S2_ACTIVE
python3 - "$R" "$CONV" <<'EOF' && ok "T13 route single-key exact pilot S2_ACTIVE" || bad "T13 route file"
import json,sys
r,conv=sys.argv[1:3]
d=json.load(open(f"{r}/var/lib/dsh-discord-inbound/s2-route.json"))
asserts=[(list(d.keys())==[conv], f"keys={list(d.keys())}"),(d[conv]=="S2_ACTIVE",f"value={d[conv]}")]
for c,n in asserts:
    print(("  PASS  " if c else "  FAIL  ")+n)
    if not c: sys.exit(1)
EOF
# env file only carries pilot conv; drop-in points at it
assert_grep "T13 env file pilot" "$R/etc/dsh/dsh-s2.env" "^S2_PILOT_CONV=$CONV$"
[ "$(grep -c 'S2_PILOT' "$R/etc/dsh/dsh-s2.env")" = "1" ] && ok "T13 env single var" || bad "T13 env extra vars"
assert_grep "T13 drop-in installed" "$R/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf" "EnvironmentFile="
# listener process env read-back
assert_grep "T13 listener env exact" "$OV/gate.log" "listener process env carries exact S2_PILOT_CONV"
# sibling unchanged: state file untouched; sibling mapping intact; patch unchanged vs end-of-stage
[ "$(state_sha)" = "$PRE_SHA" ] && ok "T13/T15 state untouched (sibling ledger)" || bad "T13 state changed"
python3 - "$R" "$SIB" "$SIDSIB" <<'EOF' && ok "T15 sibling binding intact" || bad "T15 sibling binding"
import json,sys
r,sib,sidsib=sys.argv[1:4]
d=json.load(open(f"{r}/var/lib/dsh-discord-inbound/inbound-state.json"))
sys.exit(0 if d["sessions"].get(sib)==sidsib else 1)
EOF
[ "$(file_sha "$R/opt/dsh/home/cordis.patch.yml")" = "$STAGE_PATCH_SHA" ] \
  && ok "T15 patch unchanged by activate (only pilot row from stage)" || bad "T15 patch changed by activate"
assert_nogrep "T15 no sibling in route/env" "$R/etc/dsh/dsh-s2.env" "$SIB"
assert_grep "T15 plugin route -> S2_ACTIVE" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin.log" "route -> S2_ACTIVE"
ACT_TX="$TXA"

# T14 rollback returns exact pilot to OLD (authority; plugin stays mounted; env fence armed)
run_gate rollback "$OV/gate.log"
assert_rc "T14 rollback exit 0" 0
python3 - "$R" "$CONV" <<'EOF' && ok "T14 route OLD exact" || bad "T14 route file"
import json,sys
r,conv=sys.argv[1:3]
d=json.load(open(f"{r}/var/lib/dsh-discord-inbound/s2-route.json"))
sys.exit(0 if list(d.keys())==[conv] and d[conv]=="OLD" else 1)
EOF
assert_present "T14 plugin stays mounted" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_present "T14 env fence armed (file kept)" "$R/etc/dsh/dsh-s2.env"
assert_grep "T14 plugin route -> OLD" "$R/opt/dsh/home/plugins/discord-agent-drive-evidence/plugin.log" "route -> OLD"
assert_grep "T14 rollback PASS" "$OV/gate.log" "ROLLBACK PASS"
assert_sha "T14 listener still candidate" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LISTENER_SHA"

# T15b rerun stage while the plugin row is STILL mounted is refused (no double stage)
run_gate stage "$OV/gate.log"
assert_rc "T15b re-stage refused" 1
assert_grep "T15b zero mutation on re-stage" "$OV/gate.log" "ZERO MUTATION"

# T14b restore-baseline = full byte rollback to pre-S2
run_gate restore-baseline "$OV/gate.log"
assert_rc "T14b restore-baseline exit 0" 0
assert_sha "T14b listener restored to baseline" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LIVE_SHA"
assert_absent "T14b helper removed" "$R/opt/dsh-inbound/s2_seam.py"
assert_absent "T14b plugin removed" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
assert_nogrep "T14b row removed" "$R/opt/dsh/home/cordis.patch.yml" "discord-agent-drive"
assert_absent "T14b route removed" "$R/var/lib/dsh-discord-inbound/s2-route.json"
assert_absent "T14b env removed" "$R/etc/dsh/dsh-s2.env"
assert_absent "T14b drop-in removed" "$R/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf"
assert_absent "T14b socket dir removed" "$R/run/dsh-discord-pilot"

# T13b activate failure (listener restart crash) auto-rolls back to staged OLD
setup_overlay T13b-activate-fail; ctl_conf; gate_env
run_gate stage "$OV/gate.log"
assert_rc "T13b stage exit 0" 0
# arm the crash for the inbound restart that ACTIVATE will perform (stage's own
# inbound restart already passed above)
ctl_set crashInboundOnce true
run_gate activate "$OV/gate.log"
assert_rc "T13b activate failed -> nonzero" 1
assert_grep "T13b auto rollback message" "$OV/gate.log" "activate failure -> auto rollback to staged OLD"
assert_absent "T13b env removed" "$R/etc/dsh/dsh-s2.env"
assert_absent "T13b drop-in removed" "$R/etc/systemd/system/dsh-discord-inbound.service.d/s2-pilot.conf"
assert_sha "T13b listener stays candidate (staged)" "$R/opt/dsh-inbound/dsh_discord_inbound.py" "$EXPECT_LISTENER_SHA"
assert_present "T13b plugin stays mounted" "$R/opt/dsh/home/plugins/discord-agent-drive.mjs"
if [ -e "$R/var/lib/dsh-discord-inbound/s2-route.json" ]; then
  python3 - "$R" "$CONV" <<'EOF' && ok "T13b route OLD (not ACTIVE)" || bad "T13b route file"
import json,sys
r,conv=sys.argv[1:3]
d=json.load(open(f"{r}/var/lib/dsh-discord-inbound/s2-route.json"))
sys.exit(0 if list(d.keys())==[conv] and d[conv]=="OLD" else 1)
EOF
else
  ok "T13b route absent (never activated)"
fi

note
note "=== gate regression RESULT: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ]
