#!/usr/bin/env bash
# dsh_s2_live_gate.sh — S2 LIVE PILOT GATE (operator-run, transactional,
# fail-closed, proportional to this small cutover; mirrors the S1 gate shape).
# NON-PRODUCTION until executed by the operator at GO. It performs NO mutation
# unless run with --apply; the default is a read-only preflight.
set -u
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-preflight}"               # preflight | apply
LISTENER_LIVE=/opt/dsh-inbound/dsh_discord_inbound.py
LISTENER_STAGED="$ROOT_DIR/dsh_discord_inbound.s2.py"
SEAM_HELPER="$ROOT_DIR/s2_seam.py"
PLUGIN_FILE="$ROOT_DIR/../discord-agent-drive.mjs"
STATE_DIR="/var/lib/dsh-discord-inbound"
S2_ROUTE_FILE="$STATE_DIR/s2-route.json"

EXPECT_LIVE_SHA="528cb84a1905097c3fd0a14f41ad05a7b5df4308a961341891f5723706c9c511"
EXPECT_LISTENER_SHA="b9907f40d4a34a618f22cbce6b3ef0a31b63c3bf868242c9f13a3fd980e32e08"
EXPECT_SEAM_SHA="fd40d6c4b770befd3a45bb1bf550015c02474ac19f137ae008ac04c2fc0ce8d2"
EXPECT_PLUGIN_SHA="01183e27b74044ef0f7b486e1123a9141b670221894a721f142e1f3ba6c49cd3"

pass=0; fail=0
chk() { # chk <name> <cmd...>
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then pass=$((pass+1)); echo "PASS $name";
  else fail=$((fail+1)); echo "FAIL $name"; fi
}

echo "=== S2 live pilot gate [$MODE] ==="
# --- preflight (read-only; fail closed on any ambiguity) ---
chk "listener live sha baseline" bash -c "[ \"\$(sha256sum $LISTENER_LIVE | cut -d' ' -f1)\" = \"$EXPECT_LIVE_SHA\" ]"
chk "staged listener sha == reviewed" bash -c "[ \"\$(sha256sum $LISTENER_STAGED | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]"
chk "seam helper sha == reviewed" bash -c "[ \"\$(sha256sum $SEAM_HELPER | cut -d' ' -f1)\" = \"$EXPECT_SEAM_SHA\" ]"
chk "plugin sha == reviewed" bash -c "[ \"\$(sha256sum $PLUGIN_FILE | cut -d' ' -f1)\" = \"$EXPECT_PLUGIN_SHA\" ]"
chk "dsh.service healthy" systemctl is-active --quiet dsh.service
chk "dsh-discord-inbound healthy" systemctl is-active --quiet dsh-discord-inbound.service
chk "s1 plugin present (unaffected)" bash -c "ls $ROOT_DIR/../../schedule-boot-rearm.mjs >/dev/null 2>&1 || ls /opt/dsh/home/plugins/*boot-rearm* >/dev/null 2>&1"
# pilot binding (operator-supplied): S2_PILOT_CONV + S2_PILOT_SID env must be set
chk "S2_PILOT_CONV provided" bash -c "[ -n \"\${S2_PILOT_CONV:-}\" ]"
chk "S2_PILOT_SID provided" bash -c "[ -n \"\${S2_PILOT_SID:-}\" ]"
if [ -n "${S2_PILOT_CONV:-}" ]; then
  chk "pilot session mapped in inbound-state" bash -c "python3 -c \"import json,os;d=json.load(open('$STATE_DIR/inbound-state.json'));assert d['sessions'].get(os.environ['S2_PILOT_CONV'])==os.environ.get('S2_PILOT_SID')\""
fi
chk "S2 route currently OLD/absent" bash -c "[ ! -f $S2_ROUTE_FILE ] || grep -q 'OLD' $S2_ROUTE_FILE"
chk "plugin not unexpectedly active" bash -c "! systemctl show dsh.service -p ExecMainStartTimestamp >/dev/null 2>&1 || journalctl -u dsh.service --since '10 minutes ago' 2>/dev/null | grep -q discord-agent-drive || true"
# AF_UNIX read-back (after plugin mount + socket creation by dsh)
chk "s2 socket present" bash -c "ls -l /run/dsh-discord-pilot/dsh.sock 2>/dev/null | grep -q 'srw-rw----'"
chk "socket group dsh-media" bash -c "stat -c %G /run/dsh-discord-pilot/dsh.sock 2>/dev/null | grep -q dsh-media"

if [ "$MODE" != "apply" ]; then
  echo "=== preflight result: $pass pass / $fail fail (no mutation) ==="
  [ "$fail" -eq 0 ] || echo "STOP: correct failures before GO"
  exit $([ "$fail" -eq 0 ] && echo 0 || echo 1)
fi

echo "=== APPLY (operator GO assumed) ==="
# Cutover ordering guarantees no dual authority.
# 1) install reviewed listener + helper bytes (atomic; restart ONLY the listener)
install -m 0640 -o dsh-discord -g dsh-discord "$LISTENER_STAGED" "$LISTENER_LIVE"
install -m 0640 -o dsh-discord -g dsh-discord "$SEAM_HELPER" /opt/dsh-inbound/s2_seam.py
systemctl restart dsh-discord-inbound.service
sleep 3
chk "listener restarted healthy" systemctl is-active --quiet dsh-discord-inbound.service
chk "listener now reviewed sha" bash -c "[ \"\$(sha256sum $LISTENER_LIVE | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]"
# 2) mount plugin + restart dsh (operator may prefer a separate step; S2 flag
#    stays empty so nothing changes until route flip)
# 3) route activation is a SEPARATE explicit step (not automatic here)
echo "=== apply stage 1 complete. Route activation + supervised live battery are the NEXT operator step (explicit GO). ==="
