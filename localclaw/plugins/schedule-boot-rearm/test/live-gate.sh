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
CANONICAL_SHA256=ba3ef15381eeec4d4d0ffd6741c2e9705329207db3eeecd531ea2bee903064c9
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
{"label":"$1","ts":"$TS","pluginSha256":"$CANONICAL_SHA256","sidA":"$SID_A","sidB":"$SID_B"}
EOF
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
wait_boot_complete() { # <timeout-s>  waits for a FRESH plugin log with exact success summary
  local n=0 t="$1" pre_pid post_pid
  pre_pid="$(dsh_mainpid)"
  while [ "$n" -lt "$t" ]; do
    post_pid="$(dsh_mainpid)"
    if [ "$post_pid" != "$pre_pid" ] && [ "$post_pid" != "0" ] \
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
import json,sys
label,tx,a,b=sys.argv[1:5]
ok=True
def chk(c,n):
    global ok
    print(("  PASS  " if c else "  FAIL  ")+n)
    if not c: ok=False
pre=json.load(open(f"{tx}/{label}-baseline.json"))
post=json.load(open(f"{tx}/{label}-post.json"))
txt=open(f"{tx}/pluginlog-after.log",encoding='utf-8',errors='replace').read()
health=dict(l.split("=",1) for l in open(f"{tx}/health-after.txt").read().splitlines() if "=" in l)
pa=pre.get(a,{}).get("pins"); qa=post.get(a,{}).get("pins")
pb=pre.get(b,{}).get("pins"); qb=post.get(b,{}).get("pins")
chk("schedule plugin entry active" in txt and "boot: done" in txt, "1 plugin loaded through DSH lifecycle")
ok_ab=all(f"resume ok: {s}" in txt for s in (a,b))
chk(ok_ab, "2 persisted schedule owners resumed (same session ids)")
chk("live roots = 2" in txt, "3 schedule runtime re-attached (live roots = 2)")
chk(pa==qa and isinstance(pa,dict) and bool(pa.get("model")), f"4 model pin unchanged for A ({pa.get('model') if pa else None})")
chk(bool(pre.get(b,{}).get("present")) and bool(post.get(b,{}).get("present")), "5 successor session present exactly once (single durable log)")
chk(pre.get(a,{}).get("scheduleRows")==post.get(a,{}).get("scheduleRows") and pre.get(b,{}).get("scheduleRows")==post.get(b,{}).get("scheduleRows"), "6 next-fire schedule state unchanged (full normalized rows, both sessions)")
chk(pre.get(a,{}).get("dispatches")==post.get(a,{}).get("dispatches") and pre.get(b,{}).get("dispatches")==post.get(b,{}).get("dispatches"), "7 no restart-induced schedule dispatch (both sessions)")
import re
resumes=re.findall(r"resume ok: (session-[0-9a-f-]+)",txt)
chk(sorted(set(resumes))==sorted([a,b]) and len(resumes)==2, "8 no duplicate owner/session (exactly one resume per owner)")
chk(pre.get(a,{}).get("scheduleRows")==post.get(a,{}).get("scheduleRows") and pre.get(b,{}).get("scheduleRows")==post.get(b,{}).get("scheduleRows"), "9 no duplicate schedule records (both sessions)")
units=[u for u,s in health.items()]
states=[s for u,s in health.items()]
chk(sorted(units)==sorted(["dsh.service","dsh-line-inbound","dsh-discord-inbound","dsh-discord","dsh-webgate"]) and all(s=="active" for s in states), "10 platform units active: "+", ".join(f"{u}={s}" for u,s in health.items()))
json.dump({"ok":ok,"resumeLines":resumes,"health":health},open(f"{tx}/{label}-VERDICT.json","w"))
sys.exit(0 if ok else 1)
PY
}

# ---------------- gate flow ----------------
gate() { # <label>
  local label="$1"
  mkdir -p "$TX"
  make_manifest "$label"

  step "preflight (read-only)"
  [ -f "$CANONICAL_SRC" ] || die "canonical source missing"
  [ "$(sha256sum "$CANONICAL_SRC" | cut -d' ' -f1)" = "$CANONICAL_SHA256" ] || die "canonical source hash mismatch"
  [ -f "$PATCH_FILE" ] || die "patch file missing"
  yaml_ok "$PATCH_FILE" || die "live patch invalid"
  systemctl is-active "$DSH_SERVICE" >/dev/null 2>&1 || die "dsh.service not active"
  if [ "$label" = gate1 ]; then
    row_present && die "gate1: plugin row already present (refusing double cutover)"
    unit_enabled || die "gate1: external materializer not enabled (unexpected pre-state)"
    unit_active  && die "gate1: external materializer active (unexpected pre-state)"
  else
    [ -f "$PLUGIN_FILE" ] || die "gate2: plugin file missing"
    [ "$(sha256sum "$PLUGIN_FILE" | cut -d' ' -f1)" = "$CANONICAL_SHA256" ] || die "gate2: plugin hash mismatch"
    row_present || die "gate2: plugin row missing"
    row_exact  || die "gate2: plugin row not exact"
    unit_enabled && die "gate2: external materializer still enabled"
    unit_active  && die "gate2: external materializer still active"
    g1v="$(ls -1 "$EVID"/txn-*/gate1-VERDICT.json 2>/dev/null | head -1 || true)"
    if [ -n "$g1v" ]; then
      grep -q '"ok": true' "$g1v" 2>/dev/null || die "previous gate1 did not PASS; refusing gate2"
    else
      log "gate2: no prior gate1 manifest found; proceeding per operator intent"
    fi
  fi

  step "baseline capture"
  session_state "$TX/$label-baseline.json"
  unit_health > "$TX/health-before.txt"
  cp "$PATCH_FILE" "$TX/patch-before.yml"
  echo "enabled=$(unit_enabled && echo yes || echo no) active=$(unit_active && echo yes || echo no)" > "$TX/materializer-before.txt"
  if [ -f "$PLUGIN_LOG" ]; then mv "$PLUGIN_LOG" "$TX/pluginlog-pre.log"; fi

  step "cutover: stop external authority, install native plugin, restart dsh"
  if [ "$label" = gate1 ]; then
    systemctl disable --now "$UNIT" >/dev/null 2>&1
    unit_enabled && die "failed to disable $UNIT"
    unit_active  && die "failed to stop $UNIT"
    # staged patch = original + row (temp file, then atomic replace)
    cp "$PATCH_FILE" "$TX/staged-patch.yml"
    printf '\n%s\n' "$ROW_BLOCK" >> "$TX/staged-patch.yml"
    yaml_ok "$TX/staged-patch.yml" || die "staged patch invalid"
    patch_atomic_install "$TX/staged-patch.yml"
    install_plugin
  else
    log "gate2: plugin+row already authoritative; unit already disabled/inactive (asserted in preflight)"
  fi

  local pre_pid
  pre_pid="$(dsh_mainpid)"
  systemctl restart "$DSH_SERVICE"
  wait_boot_complete 300 || die "plugin boot evidence not observed (fresh log + exact summary)"
  sleep 3

  step "post capture + verify"
  session_state "$TX/$label-post.json"
  unit_health > "$TX/health-after.txt"
  echo "enabled=$(unit_enabled && echo yes || echo no) active=$(unit_active && echo yes || echo no)" > "$TX/materializer-after.txt"
  cp "$PLUGIN_LOG" "$TX/pluginlog-after.log"

  if verify_gate "$label"; then
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
    # 1) make sure the external unit cannot run while the plugin still exists
    systemctl disable "$UNIT" >/dev/null 2>&1 || true
    systemctl stop "$UNIT" >/dev/null 2>&1 || true
    # 2) restore the pre-install patch (atomic) and remove the plugin file
    cp "$RB_TX/patch-original.yml" "$PATCH_FILE.s1-rb-tmp-$TS"
    yaml_ok "$PATCH_FILE.s1-rb-tmp-$TS" || die "rollback patch invalid"
    row_present && grep -q -- "- id: $PLUGIN_ROW_ID" "$PATCH_FILE.s1-rb-tmp-$TS" && die "rollback patch still contains row"
    chown dsh:dsh "$PATCH_FILE.s1-rb-tmp-$TS"; chmod 0644 "$PATCH_FILE.s1-rb-tmp-$TS"
    mv "$PATCH_FILE.s1-rb-tmp-$TS" "$PATCH_FILE"
    rm -f "$PLUGIN_FILE"
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
    session_state "$RB_TX/rollback-state.json"
    unit_health > "$RB_TX/rollback-health.txt"
    log "rollback complete (evidence: $RB_TX)"
    ;;
esac
