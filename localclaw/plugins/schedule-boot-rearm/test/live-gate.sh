#!/usr/bin/env bash
# S1 live gate — controlled DSH restart under the native schedule-boot-rearm
# plugin (replaces the external dsh-scheduler-materialize oneshot for the
# schedule-owner re-arm role).
#
# Canonical source: localclaw/plugins/schedule-boot-rearm/test/live-gate.sh
# in RedEyeNinja-BKK/deepseek-harness (Hermes-reviewed). Deploy copy:
#   /opt/dsh-stage/s1-live-gate.sh   (run as root)
#
# Modes:
#   gate1     cutover: preflight -> baseline -> stop+disable external oneshot
#             -> install plugin (atomic patch replace) -> ONE controlled dsh
#             restart -> wait for exact fresh boot evidence -> 10-point verify.
#             Auto-rollback from the per-run transaction manifest on failure.
#   gate2     idempotence: same flow with the external oneshot still disabled,
#             after re-asserting plugin hash / exact row / unit state.
#   rollback  restore the old path from the LATEST transaction manifest:
#             stop external unit first, remove plugin row+file atomically,
#             restart DSH (no materializer transient), then enable+start the
#             external oneshot once and verify Result/schedule continuity.
#
# Invariants: exactly one materializer authoritative at every acceptance
# restart; no schedule creation/deletion; no gold report regeneration; all
# evidence under the report evidence dir. `set -euo pipefail` with explicit
# step tracking so a mid-transaction failure always leaves a recoverable
# manifest.

set -euo pipefail

MODE="${1:-}"
DSH_HOME_DIR=/opt/dsh/home
PLUGIN_DIR="$DSH_HOME_DIR/plugins"
PLUGIN_FILE="$PLUGIN_DIR/schedule-boot-rearm.mjs"
PLUGIN_LOG="$PLUGIN_DIR/schedule-boot-rearm.log"
PATCH_FILE="$DSH_HOME_DIR/cordis.patch.yml"
PLUGIN_ROW_ID="schedule-boot-rearm"
UNIT=dsh-scheduler-materialize.service
DSH_SERVICE=dsh.service
CANONICAL_SRC=/home/vincent/shared-workspace/operations/dsh-native-pivot-2026-09-09/report/s1-review-pack/schedule-boot-rearm.mjs
CANONICAL_SHA256=f41b44d913890bb763bd9f17a2bc5d24edf8ebd9df4b99ebea9b23cbd9252eaf
EVID=/home/vincent/shared-workspace/operations/dsh-native-pivot-2026-09-09/report/evidence/s1-live-gate-2026-09-09
PY=/usr/bin/python3
SID_A=session-bb8442f9-3320-498e-9278-8d88db60d3f2
SID_B=session-e5dc0463-536a-419a-8e27-9b5c2417358f
TS="$(date +%Y%m%d%H%M%S)"
TX="$EVID/txn-$TS"
STEP=0

log() { echo "[s1-live-gate] $*"; }
step() { STEP=$((STEP+1)); log "step $STEP: $*"; }
die() { log "FATAL: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing: $1"; }

mkdir -p "$EVID"
[ "$(id -u)" -eq 0 ] || die "run as root"
need zstd; need systemctl
case "$MODE" in gate1|gate2|rollback) ;; *) die "usage: $0 gate1|gate2|rollback" ;; esac

ROW_BLOCK=$(cat <<'YAML'
- insert:
  - id: schedule-boot-rearm
    name: '/opt/dsh/home/plugins/schedule-boot-rearm.mjs'
    config:
      scheduleSessionIds:
        - session-bb8442f9-3320-498e-9278-8d88db60d3f2
        - session-e5dc0463-536a-419a-8e27-9b5c2417358f
YAML
)

# ---------------- read-only helpers ----------------
yaml_ok() { "$PY" -c 'import sys,yaml; yaml.safe_load(open(sys.argv[1]))' "$1" >/dev/null 2>&1; }

row_present() { grep -q -- "- id: $PLUGIN_ROW_ID" "$PATCH_FILE"; }
row_exact() { cmp -s <(printf '%s' "$ROW_BLOCK") <(tail -n 7 "$PATCH_FILE"); }
unit_enabled() { systemctl is-enabled "$UNIT" >/dev/null 2>&1; }
unit_active()  { systemctl is-active  "$UNIT" >/dev/null 2>&1; }
dsh_mainpid()  { systemctl show -p MainPID --value "$DSH_SERVICE" 2>/dev/null || echo 0; }

session_state() { # <out-json>  (fails loudly on missing/malformed/multiple logs)
  "$PY" - "$1" "$DSH_HOME_DIR/sessions" "$SID_A" "$SID_B" <<'PY'
import json,sys,subprocess,glob,os
out,root,a,b=sys.argv[1:5]
def decode(sid):
    hits=sorted(glob.glob(os.path.join(root,"*",sid,"session.jsonl.zstd")))
    if not hits: return None
    if len(hits)>1: raise SystemExit(f"duplicate session logs for {sid}: {hits}")
    r=subprocess.run(["zstd","-q","-d","-c",hits[0]],capture_output=True)
    if r.returncode!=0: raise SystemExit(f"zstd failed for {sid}: {r.stderr.decode(errors='replace')}")
    evs=[]
    for ln in r.stdout.splitlines():
        if not ln.strip(): continue
        try: evs.append(json.loads(ln))
        except Exception as e: raise SystemExit(f"bad json for {sid}: {e}")
    return evs
def sched_rows(evs):
    out=[]
    for e in evs:
        if e.get("type")!="schedule/change": continue
        d=e.get("data",{})
        out.append({"seq":e.get("seq"),"id":d.get("id"),
                    "acceptedAt":d.get("acceptedAt"),"scheduledAt":d.get("scheduledAt"),
                    "afterSeconds":d.get("afterSeconds"),"everySeconds":d.get("everySeconds"),
                    "prompt":d.get("prompt"),"kind":d.get("kind")})
    return out
def pins(evs):
    for e in reversed(evs):
        if e.get("type")=="request/header":
            c=e.get("data",{}).get("header",{}).get("config")
            if isinstance(c,dict) and c.get("model"):
                return {"provider":c.get("provider"),"model":c.get("model"),
                        "reasoningEffort":c.get("reasoningEffort")}
    return None
res={}
for sid in (a,b):
    evs=decode(sid)
    res[sid]={"present":evs is not None}
    if evs is not None:
        res[sid].update({"eventCount":len(evs),
                         "scheduleRows":sched_rows(evs),
                         "scheduleChange":sum(1 for e in evs if e.get("type")=="schedule/change"),
                         "dispatches":sum(1 for r in sched_rows(evs) if r["acceptedAt"] is not None),
                         "pins":pins(evs),
                         "turnStarts":sum(1 for e in evs if e.get("type")=="turn/start")})
json.dump(res,open(out,"w"),indent=2)
PY
}

unit_health() { # prints name=status lines
  for u in "$DSH_SERVICE" dsh-line-inbound dsh-discord-inbound dsh-discord dsh-webgate; do
    echo "$u=$(systemctl is-active "$u" 2>/dev/null || echo unknown)"
  done
}

# ---------------- transaction manifest ----------------
make_manifest() { # <label>
  cat > "$TX/manifest.json" <<EOF
{"label":"$1","ts":"$TS","state":"created","stateAt":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","pluginSha256":"$CANONICAL_SHA256","sidA":"$SID_A","sidB":"$SID_B"}
EOF
}
manifest_step() { # <manifest.json> <state>  (writes state + appends append-only history)
  "$PY" - "$1" "$2" <<'PY'
import json,sys,datetime,os
path,state=sys.argv[1:3]
try: d=json.load(open(path))
except Exception: sys.exit(0)
d["state"]=state
d["stateAt"]=datetime.datetime.utcnow().isoformat()+"Z"
json.dump(d,open(path,"w"),indent=2)
hist=os.path.join(os.path.dirname(path),"steps.log")
with open(hist,"a") as f: f.write(f"{state} {d['stateAt']}\n")
PY
}
latest_txn() { ls -1dt "$EVID"/txn-* 2>/dev/null | head -1 || true; }

# ---------------- mutations (root) ----------------
patch_atomic_install() { # <new-content-file>
  local tmp="$PATCH_FILE.s1-tmp-$TS"
  cp "$PATCH_FILE" "$TX/patch-original.yml"
  cp "$1" "$tmp"
  yaml_ok "$tmp" || die "staged patch invalid"
  chown dsh:dsh "$tmp"; chmod 0644 "$tmp"
  mv "$tmp" "$PATCH_FILE"   # same-dir atomic replace
  row_present || die "row missing after atomic replace"
}

install_plugin() {
  mkdir -p "$PLUGIN_DIR"
  local src sha
  sha="$(sha256sum "$CANONICAL_SRC" | cut -d' ' -f1)"
  [ "$sha" = "$CANONICAL_SHA256" ] || die "canonical source hash mismatch: $sha"
  cp "$CANONICAL_SRC" "$PLUGIN_FILE.s1-new-$TS"
  chown dsh:dsh "$PLUGIN_FILE.s1-new-$TS"; chmod 0640 "$PLUGIN_FILE.s1-new-$TS"
  mv "$PLUGIN_FILE.s1-new-$TS" "$PLUGIN_FILE"
  chown dsh:dsh "$PLUGIN_DIR"; chmod 0750 "$PLUGIN_DIR"
  sha="$(sha256sum "$PLUGIN_FILE" | cut -d' ' -f1)"
  [ "$sha" = "$CANONICAL_SHA256" ] || die "installed plugin hash mismatch: $sha"
  log "plugin installed (hash ok, dsh:dsh 0640)"
}

# ---------------- readiness ----------------
wait_boot_complete() { # <timeout-s> <old-pid>  waits for a FRESH plugin log with exact success summary
  local n=0 t="$1" old_pid="$2" post_pid
  while [ "$n" -lt "$t" ]; do
    post_pid="$(dsh_mainpid)"
    if [ -n "$post_pid" ] && [ "$post_pid" != "0" ] && [ "$post_pid" != "$old_pid" ] \
       && [ -s "$PLUGIN_LOG" ] \
       && tail -6 "$PLUGIN_LOG" 2>/dev/null | grep -q "boot: done -> resumed=2 alreadyLive=0 missing=0 failed=0 invalid=0" \
       && tail -6 "$PLUGIN_LOG" 2>/dev/null | grep -q "live roots=2"; then
      return 0
    fi
    n=$((n+1)); sleep 1
  done
  log "wait_boot_complete timeout; plugin log tail:"; tail -8 "$PLUGIN_LOG" 2>/dev/null | sed 's/^/    /' || true
  return 1
}

# ---------------- 10-point verification ----------------
verify_gate() { # <label>
  local label="$1"
  "$PY" - "$label" "$TX" "$SID_A" "$SID_B" <<'PY'
import json,sys,os,re
label,tx,a,b=sys.argv[1:5]
ok=True
def chk(c,n):
    global ok
    print(("  PASS  " if c else "  FAIL  ")+n)
    if not c: ok=False
# evidence integrity: every required artifact present and non-empty
required=[f"{label}-baseline.json",f"{label}-post.json","pluginlog-after.log","health-after.txt","patch-before.yml","materializer-before.txt","materializer-after.txt"]
missing=[f for f in required if not (os.path.exists(f"{tx}/{f}") and os.path.getsize(f"{tx}/{f}")>0)]
chk(not missing, "evidence integrity (all artifacts present+non-empty)")
pre=json.load(open(f"{tx}/{label}-baseline.json"))
post=json.load(open(f"{tx}/{label}-post.json"))
txt=open(f"{tx}/pluginlog-after.log",encoding='utf-8',errors='replace').read()
health=dict(l.split("=",1) for l in open(f"{tx}/health-after.txt").read().splitlines() if "=" in l)
pa=pre.get(a,{}).get("pins"); qa=post.get(a,{}).get("pins")
pb=pre.get(b,{}).get("pins"); qb=post.get(b,{}).get("pins")
chk("schedule plugin entry active" in txt and "boot: done" in txt, "1 plugin loaded through DSH lifecycle")
chk(all(f"resume ok: {s}" in txt for s in (a,b)), "2 persisted schedule owners resumed (same session ids)")
chk("live roots = 2" in txt, "3 schedule runtime re-attached (live roots = 2)")
chk(pa==qa and isinstance(pa,dict) and bool(pa.get("model")), f"4 model pin unchanged for A ({pa.get('model') if pa else None})")
chk(pb==qb and isinstance(pb,dict) and bool(pb.get("model")), f"4b model pin unchanged for B ({pb.get('model') if pb else None})")
chk(bool(pre.get(b,{}).get("present")) and bool(post.get(b,{}).get("present")), "5 successor session present exactly once (single durable log)")
chk(pre.get(a,{}).get("scheduleRows")==post.get(a,{}).get("scheduleRows") and pre.get(b,{}).get("scheduleRows")==post.get(b,{}).get("scheduleRows"), "6 next-fire schedule state unchanged (full normalized rows, both sessions)")
chk(pre.get(a,{}).get("dispatches")==post.get(a,{}).get("dispatches") and pre.get(b,{}).get("dispatches")==post.get(b,{}).get("dispatches"), "7 no restart-induced schedule dispatch (both sessions)")
resumes=re.findall(r"resume ok: (session-[0-9a-f-]+)",txt)
chk(sorted(set(resumes))==sorted([a,b]) and len(resumes)==2, "8 no duplicate owner/session (exactly one resume per owner)")
chk(pre.get(a,{}).get("scheduleRows")==post.get(a,{}).get("scheduleRows") and pre.get(b,{}).get("scheduleRows")==post.get(b,{}).get("scheduleRows"), "9 no duplicate/new schedule records (full normalized equality, both sessions)")
units=sorted(health.keys())
states=[health[u] for u in units]
chk(units==sorted(["dsh.service","dsh-line-inbound","dsh-discord-inbound","dsh-discord","dsh-webgate"]) and all(s=="active" for s in states), "10 platform units active: "+", ".join(f"{u}={health[u]}" for u in units))
json.dump({"ok":ok,"resumeLines":resumes,"health":health,"evidenceMissing":missing},open(f"{tx}/{label}-VERDICT.json","w"))
sys.exit(0 if ok else 1)
PY
}

# ---------------- gate flow ----------------
gate() { # <label>
  local label="$1"
  mkdir -p "$TX"
  make_manifest "$label"

  step "preflight (read-only)"
  manifest_step "$TX/manifest.json" preflight
  [ -f "$CANONICAL_SRC" ] || die "canonical source missing"
  [ "$(sha256sum "$CANONICAL_SRC" | cut -d' ' -f1)" = "$CANONICAL_SHA256" ] || die "canonical source hash mismatch"
  [ -f "$PATCH_FILE" ] || die "patch file missing"
  yaml_ok "$PATCH_FILE" || die "live patch invalid"
  systemctl is-active "$DSH_SERVICE" >/dev/null 2>&1 || die "dsh.service not active"
  if [ "$label" = gate1 ]; then
    row_present && die "gate1: plugin row already present (refusing double cutover)"
    unit_enabled || die "gate1: external materializer not enabled (unexpected pre-state — old path must be authoritative before gate1)"
    # The old materializer is intentionally the CURRENT authoritative re-arm
    # mechanism before gate1: it already ran for the present boot and, being
    # Type=oneshot RemainAfterExit=yes, legitimately sits in 'active (exited)'
    # (Result=success) until the next dsh restart. That completed state does NOT
    # mean it will execute during the upcoming gate restart — disable --now
    # below makes it non-participating BEFORE the restart. Accept and log it;
    # reject only a genuinely failed old path (Result != success).
    if unit_active; then
      RB_SUB="$(systemctl show "$UNIT" -p SubState --value 2>/dev/null || echo unknown)"
      RB_RES="$(systemctl show "$UNIT" -p Result --value 2>/dev/null || echo unknown)"
      log "gate1: external materializer active ($RB_SUB, Result=$RB_RES) — expected completed-oneshot state from the present boot; will be disabled before the gate restart"
      [ "$RB_RES" = success ] || die "gate1: external materializer Result=$RB_RES (not success); refusing cutover on an unhealthy old path"
    else
      log "gate1: external materializer inactive — acceptable pre-state (still enabled/authoritative)"
    fi
  else
    [ -f "$PLUGIN_FILE" ] || die "gate2: plugin file missing"
    [ "$(sha256sum "$PLUGIN_FILE" | cut -d' ' -f1)" = "$CANONICAL_SHA256" ] || die "gate2: plugin hash mismatch"
    row_present || die "gate2: plugin row missing"
    row_exact  || die "gate2: plugin row not exact"
    unit_enabled && die "gate2: external materializer still enabled"
    unit_active  && die "gate2: external materializer still active"
    g1v="$(ls -1 "$EVID"/txn-*/gate1-VERDICT.json 2>/dev/null | head -1 || true)"
    if [ -z "$g1v" ]; then
      die "gate2: no prior gate1 manifest found — gate2 is the idempotence gate AFTER a PASSED gate1"
    fi
    grep -q '"ok": true' "$g1v" 2>/dev/null || die "previous gate1 did not PASS; refusing gate2"
  fi

  step "baseline capture"
  session_state "$TX/$label-baseline.json"
  unit_health > "$TX/health-before.txt"
  cp "$PATCH_FILE" "$TX/patch-before.yml"
  echo "enabled=$(unit_enabled && echo yes || echo no) active=$(unit_active && echo yes || echo no)" > "$TX/materializer-before.txt"
  if [ -f "$PLUGIN_LOG" ]; then mv "$PLUGIN_LOG" "$TX/pluginlog-pre.log"; fi
  manifest_step "$TX/manifest.json" baseline

  step "cutover: stop external authority, install native plugin, restart dsh"
  if [ "$label" = gate1 ]; then
    # Disable the old materializer FIRST so it can never participate in the
    # upcoming restart (no window where both S1 and S-01 are eligible).
    systemctl disable --now "$UNIT" >/dev/null 2>&1
    unit_enabled && die "failed to disable $UNIT"
    unit_active  && die "failed to stop $UNIT"
    log "external materializer disabled + inactive (cannot participate in the gate restart)"
    # staged patch = original + row (temp file, then atomic replace)
    cp "$PATCH_FILE" "$TX/staged-patch.yml"
    printf '\n%s\n' "$ROW_BLOCK" >> "$TX/staged-patch.yml"
    yaml_ok "$TX/staged-patch.yml" || die "staged patch invalid"
    patch_atomic_install "$TX/staged-patch.yml"
    install_plugin
    manifest_step "$TX/manifest.json" cutover-installed
  else
    log "gate2: plugin+row already authoritative; unit already disabled/inactive (asserted in preflight)"
  fi

  # STRONG no-dual-authority invariant immediately before the restart: old
  # materializer disabled+inactive AND native plugin file+row present.
  if unit_enabled || unit_active; then
    die "pre-restart invariant violated: external materializer still enabled/active — aborting before restart (rollback path applies)"
  fi
  [ -f "$PLUGIN_FILE" ] || die "pre-restart invariant violated: native plugin file missing before restart"
  row_present || die "pre-restart invariant violated: native plugin row missing before restart"
  log "pre-restart invariant OK: external materializer disabled+inactive; S1 sole re-arm authority"

  PRE_PID="$(dsh_mainpid)"
  systemctl restart "$DSH_SERVICE"
  wait_boot_complete 300 "$PRE_PID" || die "plugin boot evidence not observed (fresh log + exact summary)"
  sleep 3
  manifest_step "$TX/manifest.json" restarted

  step "post capture + verify"
  session_state "$TX/$label-post.json"
  unit_health > "$TX/health-after.txt"
  echo "enabled=$(unit_enabled && echo yes || echo no) active=$(unit_active && echo yes || echo no)" > "$TX/materializer-after.txt"
  cp "$PLUGIN_LOG" "$TX/pluginlog-after.log"
  manifest_step "$TX/manifest.json" post-captured

  if verify_gate "$label"; then
    manifest_step "$TX/manifest.json" verified
    log "$label VERDICT: PASS"
  else
    log "$label VERDICT: FAIL"
    return 1
  fi
}

# ---------------- flows ----------------
case "$MODE" in
  gate1)
    gate gate1 && { log "gate1 PASS (evidence: $TX)"; exit 0; }
    log "gate1 FAIL -> auto rollback"; bash "$0" rollback || log "rollback errors"; exit 1
    ;;
  gate2)
    gate gate2 && { log "gate2 PASS (evidence: $TX)"; exit 0; }
    log "gate2 FAIL -> auto rollback"; bash "$0" rollback || log "rollback errors"; exit 1
    ;;
  rollback)
    RB_TX="$(latest_txn)"
    [ -n "$RB_TX" ] && [ -f "$RB_TX/manifest.json" ] || die "no transaction manifest to roll back"
    step "rollback from $RB_TX"
    manifest_step "$RB_TX/manifest.json" rollback-start
    [ -f "$RB_TX/patch-original.yml" ] || die "rollback: patch-original.yml missing from txn"
    # 1) make sure the external unit cannot run while the plugin still exists
    systemctl disable "$UNIT" >/dev/null 2>&1 || true
    systemctl stop "$UNIT" >/dev/null 2>&1 || true
    # 2) restore the pre-install patch (atomic) and remove the plugin file
    cp "$RB_TX/patch-original.yml" "$PATCH_FILE.s1-rb-tmp-$TS"
    yaml_ok "$PATCH_FILE.s1-rb-tmp-$TS" || die "rollback patch invalid"
    row_present && grep -q -- "- id: $PLUGIN_ROW_ID" "$PATCH_FILE.s1-rb-tmp-$TS" && die "rollback patch still contains row"
    cmp -s "$PATCH_FILE.s1-rb-tmp-$TS" "$RB_TX/patch-original.yml" || die "rollback patch byte mismatch vs manifest"
    chown dsh:dsh "$PATCH_FILE.s1-rb-tmp-$TS"; chmod 0644 "$PATCH_FILE.s1-rb-tmp-$TS"
    mv "$PATCH_FILE.s1-rb-tmp-$TS" "$PATCH_FILE"
    rm -f "$PLUGIN_FILE"
    manifest_step "$RB_TX/manifest.json" rollback-native-removed
    # 3) restart DSH with NO materializer (plugin removed, unit stopped) -> clean state
    systemctl restart "$DSH_SERVICE"
    sleep 5
    systemctl is-active "$DSH_SERVICE" >/dev/null 2>&1 || die "dsh not active after rollback restart"
    # 4) re-enable+run the external oneshot once (old path authoritative again)
    systemctl enable "$UNIT" >/dev/null 2>&1 || die "re-enable $UNIT failed"
    systemctl start "$UNIT" || die "start $UNIT failed"
    RN=0
    while [ "$RN" -lt 30 ]; do
      RES="$(systemctl show -p Result --value "$UNIT" 2>/dev/null || echo failed)"
      [ "$RES" = success ] && break
      RN=$((RN+1)); sleep 1
    done
    [ "$(systemctl show -p Result --value "$UNIT" 2>/dev/null)" = success ] || die "external oneshot did not complete successfully"
    log "external oneshot re-enabled and completed"
    manifest_step "$RB_TX/manifest.json" rollback-oneshot-ok
    session_state "$RB_TX/rollback-state.json"
    unit_health > "$RB_TX/rollback-health.txt"
    # 5) continuity: schedule rows + model pins unchanged vs gate1 baseline (old-path state)
    if [ -f "$RB_TX/gate1-baseline.json" ]; then
      if "$PY" - "$RB_TX" "$SID_A" "$SID_B" <<'PY'
import json,sys
rb,a,b=sys.argv[1:4]
base=json.load(open(f"{rb}/gate1-baseline.json")); now=json.load(open(f"{rb}/rollback-state.json"))
ok=True
for s in (a,b):
    if base[s].get("scheduleRows")!=now[s].get("scheduleRows") or base[s].get("pins")!=now[s].get("pins"):
        print(f"  FAIL  rollback continuity {s}: schedule rows or pins changed vs gate1 baseline"); ok=False
    else:
        print(f"  PASS  rollback continuity {s}: schedule rows + pins unchanged vs gate1 baseline")
sys.exit(0 if ok else 1)
PY
      then log "rollback continuity verified"
      else log "rollback continuity MISMATCH (see above)"; die "rollback continuity check failed"
      fi
    else
      log "rollback: no gate1 baseline in this txn (pre-gate rollback); continuity compare skipped"
    fi
    manifest_step "$RB_TX/manifest.json" rollback-complete
    log "rollback complete (evidence: $RB_TX)"
    ;;
esac
