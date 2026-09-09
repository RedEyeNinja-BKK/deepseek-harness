#!/usr/bin/env bash
# schedule-boot-rearm overlay battery — GO §8 pre-production proof.
#
# Proves, on the ACTUAL installed DSH 0.1.1-rc.2 packages and an isolated
# $DSH_HOME under /tmp:
#   1. plugin mounts through the normal home-level cordis.patch.yml layer
#   2. plugin contains no browser /api / session.* RPC usage (static check)
#   3. ctx.agents.resume is used (static + live)
#   4. native schedule tools reappear on a resumed schedule owner (schedule_list)
#   5. a persisted synthetic schedule survives a simulated restart
#   6. the persisted model pin survives restart even when the deployment
#      default model is changed (pin-parity; GO gate #4 analogue)
#   7. repeated resume is idempotent across a second restart (no duplicate
#      schedule/change records, same owner ids, control session untouched)
#   8. malformed/missing target ids fail safely (log + skip, no state change)
#   9. no unrelated-session disturbance (control session never gets schedules)
#
# Requires (all present on localclaw-vm):
#   - installed rc.2 tree: ${DSH_BIN:-/opt/dsh/node_modules/.bin/dsh}
#   - anonymous OpenAI-compatible gateway on 127.0.0.1:4000 (E-05 Switchyard;
#     any bearer accepted, model routes validated downstream)
#
# Usage: run-battery.sh          (writes /tmp/dsh-sbr-battery; exit 0 = PASS)

set -u
DSH_BIN="${DSH_BIN:-/opt/dsh/node_modules/.bin/dsh}"
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"          # plugin dir (canonical)
PLUGIN="$SRC_DIR/schedule-boot-rearm.mjs"
VERIFY="$SRC_DIR/test/sbr-seed-verify.mjs"
OVL="${OVL:-/tmp/dsh-sbr-battery}"
HOME_DIR="$OVL/home"
EVID="$OVL/evidence"
WS="$HOME_DIR/workspace"
ANON_KEY=anonymous-local
PROVIDER=scratch-local
MODEL_A='switchyard/deepseek/deepseek-v4-flash-nt'   # persisted pin
MODEL_B='switchyard/htpc/qwen3.5-9b-mtp'             # wrong deployment default (phase 2+)

SID_A="session-$(cat /proc/sys/kernel/random/uuid)"
SID_C="session-$(cat /proc/sys/kernel/random/uuid)"
SID_BOGUS="session-$(cat /proc/sys/kernel/random/uuid)" # valid shape, never created

PASS=0; FAIL=0
note() { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); note "  PASS  $*"; }
bad()  { FAIL=$((FAIL+1)); note "  FAIL  $*"; }

rm -rf "$OVL"
mkdir -p "$HOME_DIR/plugins/node_modules" "$EVID" "$WS" "$HOME_DIR/profiles/web"

# node_modules reachability for the OUT-OF-TREE plugin file under
# $DSH_HOME/plugins (its imports resolve by walking up from the file). The
# shared $DSH_HOME/profiles/node_modules closure is created/managed by dsh
# itself (healProfilesModuleFallback) — never pre-populate it.
ln -sfn /opt/dsh/node_modules/@deepseek-ai "$HOME_DIR/plugins/node_modules/@deepseek-ai"

cp "$PLUGIN" "$HOME_DIR/plugins/schedule-boot-rearm.mjs"
cp "$VERIFY" "$HOME_DIR/plugins/sbr-seed-verify.mjs"

cat > "$HOME_DIR/profiles/web/package.json" <<EOF
{
  "name": "dsh-profile-web",
  "private": true,
  "dependencies": {},
  "dsh": { "profile": { "bundles": [ "@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app" ] } }
}
EOF
printf '[]\n' > "$HOME_DIR/profiles/web/cordis.yml"
printf '[]\n' > "$HOME_DIR/profiles/web/cordis.patch.yml"

write_settings() {
  local default_model="$1"
  cat > "$HOME_DIR/settings.yaml" <<EOF
llm-pi-ai:
  providers:
    $PROVIDER:
      displayName: SBR overlay
      apiKeyEnv: DSH_SPIKE_ANON_KEY
      api: openai-completions
      baseURL: http://127.0.0.1:4000/v1
      models:
        - id: $MODEL_A
          contextWindow: 131072
          maxTokens: 4096
        - id: $MODEL_B
          contextWindow: 131072
          maxTokens: 4096
agent-default-model:
  provider: $PROVIDER
  model: $default_model
EOF
}

boot() { # boot <phase-label> <cordis-patch-file> <out-log>
  local label="$1" patch="$2" out="$3"
  cp "$patch" "$HOME_DIR/cordis.patch.yml"
  ( cd "$WS" && DSH_HOME="$HOME_DIR" \
      DSH_PERMISSION_MODE=danger-full-access \
      DSH_TELEMETRY_DISABLED=1 \
      DSH_SPIKE_ANON_KEY="$ANON_KEY" \
      "$DSH_BIN" web --no-open --host 127.0.0.1 --port 0 >"$out" 2>&1 & echo $! > "$EVID/.pid-$label" )
}

wait_evidence() { # wait_evidence <label> <file> <timeout-s>
  local label="$1" file="$2" timeout_s="$3" n=0
  while [ ! -s "$EVID/$file" ]; do
    n=$((n+1))
    if [ "$n" -ge "$timeout_s" ]; then
      note "  TIMEOUT waiting for $file (boot log tail):"
      tail -40 "$EVID/boot-$label.log" 2>/dev/null | sed 's/^/    /'
      return 1
    fi
    local pid
    pid="$(cat "$EVID/.pid-$label" 2>/dev/null || true)"
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
      note "  PROCESS DIED before $file (log tail):"
      tail -60 "$EVID/boot-$label.log" 2>/dev/null | sed 's/^/    /'
      return 1
    fi
    sleep 1
  done
  return 0
}

stop_boot() { # stop_boot <label>
  local label="$1" n=0 pid
  pid="$(cat "$EVID/.pid-$label" 2>/dev/null || true)"
  [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null
  while [ "$n" -lt 30 ] && [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; do n=$((n+1)); sleep 1; done
  [ -n "$pid" ] && kill -KILL "$pid" 2>/dev/null || true
}

note "=== schedule-boot-rearm overlay battery ==="
note "overlay: $OVL"
note "seed sessions: A=$SID_A  C=$SID_C  bogus=$SID_BOGUS"

# ---- phase 1: seed schedule-bearing session A (pin A) + control C ----
note "[phase 1] seed"
write_settings "$MODEL_A"
cat > "$HOME_DIR/cordis.patch.seed.yml" <<EOF
- insert:
  - id: schedule
    name: '@deepseek-ai/dsh-schedule'
  - id: time-context
    name: '@deepseek-ai/dsh-time-context'
  - id: sbr-seed
    name: '$HOME_DIR/plugins/sbr-seed-verify.mjs'
EOF
export SBR_MODE=seed SBR_EVID="$EVID" SBR_SESSION_A="$SID_A" SBR_SESSION_C="$SID_C" \
  SBR_PROVIDER="$PROVIDER" SBR_MODEL="$MODEL_A" SBR_WS="$WS"
boot p1 "$HOME_DIR/cordis.patch.seed.yml" "$EVID/boot-p1.log" || { bad "phase1 boot launch"; exit 1; }
if ! wait_evidence p1 COMPLETE-SEED.json 240; then bad "phase1 seed evidence"; stop_boot p1; exit 1; fi
sleep 2
stop_boot p1
note "phase1 evidence: $(python3 -c 'import json;d=json.load(open("'$EVID'/COMPLETE-SEED.json"));print("a_schedule=%s a_model=%s c_schedule=%s"%(d["a"]["scheduleChange"],d["a"]["route"]["model"],d["c"]["scheduleChange"]))')"

# ---- static source checks ----
note "[static] plugin source checks"
if grep -qE "session\.(models|prompt|list|create)|/api/|https?://|fetch\(|XMLHttpRequest|WebSocket" "$PLUGIN"; then
  bad "plugin references browser/session RPC or http"
else
  ok "plugin has no browser /api or session.* RPC usage"
fi
if python3 - "$PLUGIN" <<'PY'
import re,sys
src=open(sys.argv[1],encoding='utf-8').read()
allowed={'node:fs','node:path','@deepseek-ai/dsh-agent','@deepseek-ai/schemastery'}
imports=re.findall(r"^import\s+.*?\s+from\s+'([^']+)'",src,re.M)
unknown=[i for i in imports if i not in allowed]
print(('  PASS  ' if not unknown else '  FAIL  ')+f"import allowlist respected (imports={sorted(set(imports))})")
sys.exit(1 if unknown else 0)
PY
then ok "import allowlist check"; else bad "import allowlist check"; fi
if grep -q "ctx.agents.resume" "$PLUGIN"; then ok "plugin uses ctx.agents.resume"; else bad "plugin missing ctx.agents.resume"; fi
if cmp -s "$PLUGIN" "$HOME_DIR/plugins/schedule-boot-rearm.mjs"; then ok "overlay copy byte-identical to canonical"; else bad "overlay copy drift"; fi

# ---- phase 2: restart with boot-rearm mounted; deployment default = MODEL_B ----
note "[phase 2] restart #1 with boot-rearm (default model changed to B)"
write_settings "$MODEL_B"
cat > "$HOME_DIR/cordis.patch.boot.yml" <<EOF
- insert:
  - id: schedule
    name: '@deepseek-ai/dsh-schedule'
  - id: time-context
    name: '@deepseek-ai/dsh-time-context'
  - id: schedule-boot-rearm
    name: '$HOME_DIR/plugins/schedule-boot-rearm.mjs'
    config:
      scheduleSessionIds:
        - $SID_A
        - $SID_C
        - $SID_BOGUS
  - id: sbr-verify
    name: '$HOME_DIR/plugins/sbr-seed-verify.mjs'
EOF
export SBR_MODE=verify SBR_TAG=p2 SBR_EVID="$EVID" SBR_SESSION_A="$SID_A" SBR_SESSION_C="$SID_C" \
  SBR_PROVIDER="$PROVIDER" SBR_MODEL="$MODEL_A" SBR_WS="$WS"
boot p2 "$HOME_DIR/cordis.patch.boot.yml" "$EVID/boot-p2.log" || { bad "phase2 boot launch"; exit 1; }
if ! wait_evidence p2 COMPLETE-VERIFY-p2.json 300; then bad "phase2 verify evidence"; stop_boot p2; fi
sleep 2
stop_boot p2

P2="$EVID/COMPLETE-VERIFY-p2.json"
PLOG2="$HOME_DIR/plugins/schedule-boot-rearm.log"
if [ -s "$P2" ]; then
  if python3 - "$P2" "$PLOG2" "$SID_A" "$SID_C" "$MODEL_A" "$MODEL_B" <<'PY'
import json,sys,re
p,log,a,c,ma,mb=sys.argv[1:]
d=json.load(open(p))
if "fatal" in d: print("  fatal:",d["fatal"])
ok=True
def chk(cond,name):
    global ok
    print(("  PASS  " if cond else "  FAIL  ")+name)
    if not cond: ok=False
chk(d.get("a",{}).get("live") and d.get("c",{}).get("live"), "boot-rearm resumed A and C (live)")
chk(d.get("a",{}).get("sawScheduleList"), "schedule_list tool invoked on resumed owner")
chk("LIST-OK" in (d.get("a",{}).get("assistantTail") or ""), "owner replied LIST-OK (schedule tool works)")
chk(d.get("a",{}).get("scheduleChangeBefore",-1)>=1, "persisted schedule exists after restart")
chk(d.get("a",{}).get("scheduleChangeBefore")==d.get("a",{}).get("scheduleChangeAfter"), "schedule_list added no schedule/change records")
route=d.get("a",{}).get("route") or {}
chk(route.get("provider")=="scratch-local" and route.get("model")==ma, f"persisted pin model survives (got {route.get('model')}, expected {ma}; default was {mb})")
chk(d.get("c",{}).get("scheduleChange",-1)==0, "control session has no schedules (unrelated session undisturbed)")
txt=open(log,encoding="utf-8",errors="replace").read()
chk(f"resume ok: {a}" in txt, f"boot log: resumed {a}")
chk(f"resume ok: {c}" in txt, f"boot log: resumed {c}")
chk("not present in the native persistence index" in txt, "boot log: bogus id missing handled fail-narrow")
chk(f"resume default model selection = scratch-local/{mb}" in txt, f"boot log: default really was {mb} (pin test not vacuous)")
m=re.search(r"boot: done -> resumed=(\d+) alreadyLive=(\d+) missing=(\d+) failed=(\d+)",txt)
chk(bool(m) and m.group(1)=="2" and m.group(3)=="1" and m.group(4)=="0", "boot summary resumed=2 missing=1 failed=0")
sys.exit(0 if ok else 1)
PY
  then ok "phase 2 assertions"; else bad "phase 2 assertions"; fi
else
  bad "phase 2 evidence missing"
fi

# ---- phase 3: restart #2 (idempotence gate) ----
# Snapshot the phase-2 plugin-evidence line count BEFORE phase 3 appends.
SNAP2="$(wc -l < "$PLOG2" 2>/dev/null || echo 0)"
echo "$SNAP2" > "$EVID/.snap2"
note "[phase 3] restart #2 (idempotence)"
export SBR_MODE=verify SBR_TAG=p3 SBR_EVID="$EVID" SBR_SESSION_A="$SID_A" SBR_SESSION_C="$SID_C" \
  SBR_PROVIDER="$PROVIDER" SBR_MODEL="$MODEL_A" SBR_WS="$WS"
boot p3 "$HOME_DIR/cordis.patch.boot.yml" "$EVID/boot-p3.log" || { bad "phase3 boot launch"; exit 1; }
if ! wait_evidence p3 COMPLETE-VERIFY-p3.json 300; then bad "phase3 verify evidence"; stop_boot p3; fi
sleep 2
stop_boot p3

P3="$EVID/COMPLETE-VERIFY-p3.json"
if [ -s "$P3" ] && [ -s "$P2" ]; then
  # plugin evidence for phase 3 only = lines after the phase-2 snapshot
  SNAP2="$(cat "$EVID/.snap2" 2>/dev/null || echo 0)"
  tail -n +$((SNAP2 + 1)) "$PLOG2" > "$EVID/plugin-phase3.log" 2>/dev/null || true
  if python3 - "$P3" "$P2" "$EVID/plugin-phase3.log" "$SID_A" "$SID_C" "$MODEL_A" <<'PY'
import json,sys,re
p3,p2,log,a,c,ma=sys.argv[1:]
d=json.load(open(p3)); prev=json.load(open(p2))
ok=True
def chk(cond,name):
    global ok
    print(("  PASS  " if cond else "  FAIL  ")+name)
    if not cond: ok=False
chk(d.get("a",{}).get("live") and d.get("c",{}).get("live"), "A and C live after restart #2")
route=d.get("a",{}).get("route") or {}
chk(route.get("model")==ma, f"pin model still {ma} after restart #2")
chk(d.get("a",{}).get("scheduleChangeBefore")==prev.get("a",{}).get("scheduleChangeBefore"), "schedule record count identical across restarts (no duplication)")
chk(d.get("c",{}).get("scheduleChange",-1)==0, "control session still schedule-free after restart #2")
txt=open(log,encoding="utf-8",errors="replace").read()
chk(txt.count(f"resume ok: {a}")==1, "boot log: exactly one resume of A this boot")
chk("failed=0" in txt, "boot log: failed=0")
sys.exit(0 if ok else 1)
PY
  then ok "phase 3 assertions"; else bad "phase 3 assertions"; fi
else
  bad "phase 3 evidence missing"
fi

note
note "=== RESULT: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ]
