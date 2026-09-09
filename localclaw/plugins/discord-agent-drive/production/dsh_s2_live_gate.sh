#!/usr/bin/env bash
# =============================================================================
# dsh_s2_live_gate.sh — S2 LIVE PILOT GATE (operator-run, transactional,
# fail-closed). Gate-ONLY repair revision (2026-09-09, Vincent gate review).
#
# The S2 pilot candidate APPLICATION bytes are unchanged (plugin, listener
# delta, s2_seam helper — hashes in the header below). This revision fixes ONLY
# the deployment transaction mechanics of the gate itself.
#
# Modes (mutating modes require root):
#   preflight         read-only baseline of the CURRENT OLD production.
#                     PROVES: candidate hashes exact, live listener is the
#                     approved baseline, services healthy, pilot binding exact,
#                     durable pilot session exists exactly once + sane, current
#                     provider/model/reasoning captured and matching the
#                     operator-supplied inputs, route absent/exact OLD, plugin
#                     row/file/socket absent, S1 healthy. Writes evidence; does
#                     NOT require a live S2 socket.
#   stage             transactional deploy: listener+helper candidate bytes
#                     (S2 default OFF) -> restart listener + prove historical
#                     default; plugin+composition install -> tmpfiles socket dir
#                     -> ONE dsh restart -> prove plugin boot/socket/session/pin
#                     readiness via a hello-only probe (no admission, no user
#                     event); route stays OLD. Auto full-baseline rollback on
#                     any FAIL/INDETERMINATE.
#   activate          requires a PASSED stage transaction. Persistent listener
#                     config surface (systemd EnvironmentFile drop-in) set to
#                     the exact pilot; route file flipped S2_ACTIVE; read-backs;
#                     seam hello+route proof via the probe. No synthetic user
#                     event. Supervised real battery remains a separate operator
#                     step. Auto rollback to staged OLD on failure.
#   rollback          authority rollback to OLD from the newest PASSED activate
#                     txn: S2_ACTIVE -> QUIESCING_TO_OLD -> OLD (settle window),
#                     plugin-side route OLD. Plugin may stay mounted (healthy);
#                     S2_PILOT_CONV env is KEPT so the never-resubmit fence for
#                     S2-attempted messages stays armed on the OLD path.
#   restore-baseline  FULL byte rollback to the complete pre-S2 baseline from
#                     the newest PASSED stage txn: restore prior listener/helper
#                     bytes, remove plugin file + composition row + tmpfiles
#                     rule + socket dir + env/drop-in + route file; restart
#                     inbound + dsh; verify old hashes/health/S1.
#
# Invariants:
#   * Mutating modes run the full preflight battery FIRST and abort with ZERO
#     mutation on any failure. There is no override/bypass flag.
#   * Every required post-mutation check is mandatory: FAIL/INDETERMINATE ->
#     ROLLBACK + STOP (no "stage complete" on a failed assertion).
#   * inbound-state.json is NEVER written by this gate (hash-compare proof).
#   * Route control file is gate-owned; the listener only reads it.
#   * The socket dir is created by the gate (root) via a tmpfiles rule with
#     setgid 2770 owner=dsh group=dsh-media BEFORE the dsh restart, so the
#     plugin-created AF_UNIX socket inherits group dsh-media (no world access).
#
# Test surface (hermetic overlay): every path is overridable via S2_* env vars
# (documented inline). S2_GATE_HERMETIC=1 relaxes ONLY the uid check and is
# rejected unless every live-file/service path is re-pointed under
# S2_GATE_HERMETIC_ROOT (accidental production use refuses to run).
# =============================================================================

set -u

MODE="${1:-}"
[ -n "$MODE" ] || { echo "usage: $0 preflight|stage|activate|rollback|restore-baseline" >&2; exit 64; }

# --- config (all paths overridable for hermetic overlay tests) ---------------
GATE_SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DSH_HOME_DIR="${S2_DSH_HOME_DIR:-/opt/dsh/home}"
PLUGIN_DIR="$DSH_HOME_DIR/plugins"
PLUGIN_FILE="${S2_PLUGIN_FILE:-$PLUGIN_DIR/discord-agent-drive.mjs}"
PLUGIN_EVID_DIR="${S2_PLUGIN_EVID_DIR:-$PLUGIN_DIR/discord-agent-drive-evidence}"
PATCH_FILE="${S2_PATCH_FILE:-$DSH_HOME_DIR/cordis.patch.yml}"
S1_PLUGIN_FILE="$PLUGIN_DIR/schedule-boot-rearm.mjs"
S1_PLUGIN_LOG="$PLUGIN_DIR/schedule-boot-rearm.log"
S1_ROW_ID="schedule-boot-rearm"
LISTENER_LIVE="${S2_LISTENER_LIVE:-/opt/dsh-inbound/dsh_discord_inbound.py}"
HELPER_LIVE="${S2_HELPER_LIVE:-/opt/dsh-inbound/s2_seam.py}"
LISTENER_STAGED="${S2_LISTENER_STAGED:-$GATE_SRC_DIR/dsh_discord_inbound.s2.py}"
HELPER_STAGED="${S2_HELPER_STAGED:-$GATE_SRC_DIR/s2_seam.py}"
PLUGIN_STAGED="${S2_PLUGIN_STAGED:-$GATE_SRC_DIR/../discord-agent-drive.mjs}"
PROBE="$GATE_SRC_DIR/dsh_s2_hello_probe.py"
STATE_DIR="${S2_STATE_DIR:-/var/lib/dsh-discord-inbound}"
STATE_FILE="$STATE_DIR/inbound-state.json"
ROUTE_FILE="$STATE_DIR/s2-route.json"
SESSIONS_DIR="${S2_SESSIONS_DIR:-$DSH_HOME_DIR/sessions}"
SOCK_DIR="${S2_SOCK_DIR:-/run/dsh-discord-pilot}"
SOCK_PATH="$SOCK_DIR/dsh.sock"
ENV_FILE="${S2_ENV_FILE:-/etc/dsh/dsh-s2.env}"
DROPIN_DIR="${S2_DROPIN_DIR:-/etc/systemd/system/dsh-discord-inbound.service.d}"
DROPIN="$DROPIN_DIR/s2-pilot.conf"
TMPFILES_CONF="${S2_TMPFILES_CONF:-/etc/tmpfiles.d/dsh-s2-pilot.conf}"
TMPFILES_CMD="${S2_TMPFILES_CMD:-systemd-tmpfiles}"
EVID_BASE="${S2_EVID_BASE:-/home/vincent/shared-workspace/operations/dsh-native-pivot-2026-09-09/report/evidence/s2-live-gate-2026-09-09}"
INBOUND_SERVICE="${S2_INBOUND_SERVICE:-dsh-discord-inbound.service}"
DSH_SERVICE="${S2_DSH_SERVICE:-dsh.service}"
PLATFORM_UNITS="${S2_PLATFORM_UNITS:-dsh.service dsh-line-inbound dsh-discord-inbound dsh-discord dsh-webgate}"
SOCK_OWNER="${S2_EXPECT_SOCK_OWNER:-dsh}"
SOCK_GROUP="${S2_EXPECT_SOCK_GROUP:-dsh-media}"
SOCK_DIR_MODE="${S2_EXPECT_DIR_MODE:-2770}"
SOCK_MODE="${S2_EXPECT_SOCK_MODE:-660}"
S2_CONSUMER_USER="${S2_CONSUMER_USER:-dsh-discord}"
SETTLE_S="${S2_SETTLE_S:-30}"
BOOT_WAIT_S="${S2_BOOT_WAIT_S:-300}"
S1_WAIT_S="${S2_S1_WAIT_S:-60}"
POST_RESTART_S="${S2_POST_RESTART_S:-3}"
PROBE_QUIET_S="${S2_PROBE_QUIET_S:-3}"
HERM_ROOT="${S2_GATE_HERMETIC_ROOT:-}"
PY=/usr/bin/python3

EXPECT_LIVE_SHA="528cb84a1905097c3fd0a14f41ad05a7b5df4308a961341891f5723706c9c511"
EXPECT_LISTENER_SHA="b9907f40d4a34a618f22cbce6b3ef0a31b63c3bf868242c9f13a3fd980e32e08"
EXPECT_SEAM_SHA="fd40d6c4b770befd3a45bb1bf550015c02474ac19f137ae008ac04c2fc0ce8d2"
EXPECT_PLUGIN_SHA="01183e27b74044ef0f7b486e1123a9141b670221894a721f142e1f3ba6c49cd3"

# --- operator inputs (exact pilot binding + pins) ----------------------------
S2_PILOT_CONV="${S2_PILOT_CONV:-}"
S2_PILOT_SID="${S2_PILOT_SID:-}"
S2_PROVIDER="${S2_PROVIDER:-}"
S2_MODEL="${S2_MODEL:-}"
S2_REASONING_EFFORT="${S2_REASONING_EFFORT:-}"
S2_MAX_TOKENS="${S2_MAX_TOKENS:-0}"
INPUTS_JSON=""

TS="$(date +%Y%m%d%H%M%S)"
TX=""
STEP=0
PASS=0; FAIL=0

log()  { echo "[s2-live-gate] $*"; }
step() { STEP=$((STEP+1)); log "step $STEP: $*"; }
die()  { log "FATAL: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing: $1"; }
sha()  { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }

# ---------- hermetic guard ----------------------------------------------------
if [ "${S2_GATE_HERMETIC:-0}" = "1" ]; then
  [ -n "$HERM_ROOT" ] || die "S2_GATE_HERMETIC=1 requires S2_GATE_HERMETIC_ROOT"
  for p in "$LISTENER_LIVE" "$HELPER_LIVE" "$PLUGIN_FILE" "$PLUGIN_EVID_DIR" \
           "$PATCH_FILE" "$STATE_DIR" "$SOCK_DIR" "$ENV_FILE" "$DROPIN" \
           "$TMPFILES_CONF" "$S1_PLUGIN_FILE" "$S1_PLUGIN_LOG"; do
    case "$p" in
      "$HERM_ROOT"*) ;;
      *) die "hermetic mode refused: path outside overlay root: $p" ;;
    esac
  done
  log "hermetic overlay mode (uid check relaxed; paths confined to $HERM_ROOT)"
fi
if [ "$MODE" != "preflight" ] && [ "${S2_GATE_HERMETIC:-0}" != "1" ]; then
  [ "$(id -u)" -eq 0 ] || die "run as root (mutating mode $MODE)"
fi

# ---------- evidence + manifest ----------------------------------------------
mkdir -p "$EVID_BASE"
TX="$EVID_BASE/txn-$TS"
mkdir -p "$TX"

inputs_valid() { # returns 0/1; sets INPUTS_JSON
  local bad="" all
  [ -n "$S2_PILOT_CONV" ]   || bad="$bad conv"
  [ -n "$S2_PILOT_SID" ]    || bad="$bad sid"
  [ -n "$S2_PROVIDER" ]     || bad="$bad provider"
  [ -n "$S2_MODEL" ]        || bad="$bad model"
  all="$S2_PILOT_CONV$S2_PILOT_SID$S2_PROVIDER$S2_MODEL$S2_REASONING_EFFORT"
  case "$all" in *$'\n'*) bad="$bad newline";; esac
  printf '%s' "$S2_PILOT_CONV" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9:_/@.-]*$' || bad="$bad conv-charset"
  printf '%s' "$S2_PILOT_SID" | grep -Eq '^session-[0-9a-fA-F-]{8,}$'       || bad="$bad sid-charset"
  printf '%s' "$S2_PROVIDER"  | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._/:@-]*$' || bad="$bad provider-charset"
  printf '%s' "$S2_MODEL"     | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._/:@-]*$' || bad="$bad model-charset"
  if [ -n "$S2_REASONING_EFFORT" ]; then
    printf '%s' "$S2_REASONING_EFFORT" | grep -Eq '^[A-Za-z0-9._/-]+$' || bad="$bad reasoning-charset"
  fi
  printf '%s' "$S2_MAX_TOKENS" | grep -Eq '^[0-9]+$'                        || bad="$bad maxTokens"
  if [ -n "$bad" ]; then log "invalid operator inputs:$bad"; return 1; fi
  INPUTS_JSON="$("$PY" -c 'import json,sys;print(json.dumps({"conv":sys.argv[1],"sid":sys.argv[2],"provider":sys.argv[3],"model":sys.argv[4],"reasoningEffort":sys.argv[5],"maxTokens":int(sys.argv[6])}))' \
      "$S2_PILOT_CONV" "$S2_PILOT_SID" "$S2_PROVIDER" "$S2_MODEL" \
      "$S2_REASONING_EFFORT" "$S2_MAX_TOKENS")"
  return 0
}

make_manifest() { # <label>
  "$PY" - "$TX/manifest.json" "$MODE" "$1" "$TS" "$INPUTS_JSON" \
      "$EXPECT_LIVE_SHA" "$EXPECT_LISTENER_SHA" "$EXPECT_SEAM_SHA" "$EXPECT_PLUGIN_SHA" <<'PY'
import json,sys,datetime
try:
    utc=datetime.datetime.now(datetime.timezone.utc).isoformat()
except Exception:
    utc=datetime.datetime.utcnow().isoformat()+"Z"
out,mode,label,ts,inputs,live,cl,cs,cp=sys.argv[1:10]
d={"mode":mode,"label":label,"ts":ts,"state":"created","stateAt":utc,
   "inputs":json.loads(inputs or "{}"),
   "baselineLiveSha":live,
   "candidate":{"listener":cl,"seam":cs,"plugin":cp}}
json.dump(d,open(out,"w"),indent=2)
PY
}
manifest_step() { # <state>
  "$PY" - "$TX/manifest.json" "$1" <<'PY'
import json,sys,os,datetime
try:
    utc=datetime.datetime.now(datetime.timezone.utc).isoformat()
except Exception:
    utc=datetime.datetime.utcnow().isoformat()+"Z"
path,state=sys.argv[1:3]
d=json.load(open(path)); d["state"]=state
d["stateAt"]=utc
json.dump(d,open(path,"w"),indent=2)
with open(os.path.join(os.path.dirname(path),"steps.log"),"a") as f:
    f.write(f"{state} {utc}\n")
PY
}

chk() { # chk <name> <cmd...>  (battery; appends to PASS/FAIL)
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then PASS=$((PASS+1)); echo "  PASS  $name";
  else FAIL=$((FAIL+1)); echo "  FAIL  $name"; fi
}
req() { # req <name> <cmd...>  (mandatory mutating-stage check; nonzero => fail)
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "  OK    $name";
  else echo "  FAIL  $name"; return 1; fi
}
battery_reset() { PASS=0; FAIL=0; }

# ---------- python validators (heredoc helpers) ------------------------------
route_state() { # route_state <route-file> <conv> ; prints JSON, exit 0/1
  "$PY" - "$1" "$2" <<'PY'
import json,sys,os
path,conv=sys.argv[1:3]
if not os.path.exists(path):
    print(json.dumps({"status":"absent"})); sys.exit(0)
try:
    data=json.load(open(path,encoding="utf-8"))
except Exception as e:
    print(json.dumps({"status":"fail","error":f"malformed:{e}"})); sys.exit(1)
if not isinstance(data,dict):
    print(json.dumps({"status":"fail","error":"not-object"})); sys.exit(1)
valid={"OLD","S2_ACTIVE","QUIESCING_TO_OLD"}
keys=list(data.keys())
if len(keys)!=1:
    print(json.dumps({"status":"fail","error":f"keys={keys}"})); sys.exit(1)
k=keys[0]
if k not in (conv,"route"):
    print(json.dumps({"status":"fail","error":f"unexpected-key:{k}"})); sys.exit(1)
v=data[k]
if not isinstance(v,str) or v not in valid:
    print(json.dumps({"status":"fail","error":f"value:{v!r}"})); sys.exit(1)
print(json.dumps({"status":"ok","key":k,"route":v}))
PY
}

route_matches() { # route_matches <route-file> <conv> <expected|absent> ; exit0=ok
  local out rc rv exp="$3"
  out="$(route_state "$1" "$2")" || { printf '%s\n' "$out" | sed 's/^/  /'; return 1; }
  rc="$(printf '%s\n' "$out" | "$PY" -c 'import json,sys;d=json.load(sys.stdin);print(d.get("status","fail"))')"
  if [ "$rc" = "absent" ]; then
    if [ "$exp" = "absent" ] || [ "$exp" = "OLD" ]; then return 0; fi
    echo "  route absent (expected $exp)"; return 1
  fi
  if [ "$rc" != "ok" ]; then printf '%s\n' "$out"; return 1; fi
  rv="$(printf '%s\n' "$out" | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["route"])')"
  if [ "$rv" = "$exp" ]; then return 0; fi
  echo "  route=$rv (expected $exp)"; return 1
}

bind_capture() { # bind_capture <out.json>  (hard failures exit nonzero)
  "$PY" - "$1" "$STATE_FILE" "$SESSIONS_DIR" "$S2_PILOT_CONV" "$S2_PILOT_SID" \
      "$S2_PROVIDER" "$S2_MODEL" "$S2_REASONING_EFFORT" "$S2_MAX_TOKENS" <<'PY'
import json,sys,os,glob,subprocess
out,state_path,root,conv,sid,exp_provider,exp_model,exp_reason,exp_max=sys.argv[1:10]
res={"conv":conv,"sid":sid}
def fail(msg):
    res["ok"]=False; res["error"]=msg
    json.dump(res,open(out,"w")); sys.exit(1)
if not os.path.exists(state_path): fail("inbound-state.json missing")
try: state=json.load(open(state_path,encoding="utf-8"))
except Exception as e: fail(f"inbound-state.json malformed: {e}")
if not isinstance(state,dict): fail("state not object")
sessions=state.get("sessions")
if not isinstance(sessions,dict): fail("state.sessions missing")
res["mappedSid"]=sessions.get(conv)
if res["mappedSid"]!=sid: fail(f"binding mismatch: sessions[{conv}]={res['mappedSid']!r} != {sid!r}")
s2b=state.get("s2") or {}
att=(s2b.get("attempted") or {}).get(conv) or {}
df=(s2b.get("delivered_finalizations") or {}).get(conv) or {}
res["s2"]={"attempted":len(att),"delivered":{k:v.get("state") for k,v in list(df.items())[:20]}}
pending=[k for k,v in df.items() if isinstance(v,dict) and v.get("state")=="pending"]
indet=[k for k,v in df.items() if isinstance(v,dict) and v.get("state")=="indeterminate"]
amb=[k for k,v in att.items() if isinstance(v,dict) and v.get("state") in ("ambiguous","claimed")]
if att or pending or indet:
    res["handoffClean"]=False
    res["handoffDetail"]={"attempted":len(att),"pending":pending[:5],"indeterminate":indet[:5]}
    fail(f"stale S2 handoff state for pilot (attempted={len(att)} pending={len(pending)} indeterminate={len(indet)})")
res["handoffClean"]=True
hits=sorted(glob.glob(os.path.join(root,"*",sid,"session.jsonl.zstd")))
res["durableHits"]=len(hits)
if len(hits)!=1: fail(f"durable session logs != 1 ({len(hits)})")
r=subprocess.run(["zstd","-q","-d","-c",hits[0]],capture_output=True)
if r.returncode!=0: fail(f"zstd decode failed: {r.stderr.decode(errors='replace')[:200]}")
evs=[]
for ln in r.stdout.splitlines():
    if not ln.strip(): continue
    try: evs.append(json.loads(ln))
    except Exception: continue
res["eventCount"]=len(evs)
if not evs: fail("durable session log empty")
types={}
cfg=None
for e in reversed(evs):
    if not isinstance(e,dict): continue
    types[e.get("type")]=types.get(e.get("type"),0)+1
    if e.get("type")=="request/header" and cfg is None:
        c=(e.get("data") or {}).get("header") or {}
        c=c.get("config") or {}
        if isinstance(c,dict) and c.get("provider") and c.get("model"):
            cfg={"provider":c["provider"],"model":c["model"]}
            for k in ("reasoningEffort","maxTokens"):
                if k in c and c[k] not in (None,""): cfg[k]=c[k]
res["types"]=types
if cfg is None: fail("no request/header config pins found in durable log (cannot infer model)")
res["pins"]=cfg
res["pinMatchProvider"]=cfg.get("provider")==exp_provider
res["pinMatchModel"]=cfg.get("model")==exp_model
# reasoning/max tokens: operator input must equal the durable pin whenever the
# durable log records one (fail closed; never inferred from defaults).
def norm_int(v):
    try: return int(v)
    except Exception: return None
cap_reason=cfg.get("reasoningEffort")
cap_max=norm_int(cfg.get("maxTokens"))
exp_reason_v=exp_reason or ""
exp_max_v=norm_int(exp_max)
res["pinReasonMatch"]=True
res["pinMaxMatch"]=True
if cap_reason not in (None,""):
    res["pinReasonMatch"]=(str(cap_reason)==str(exp_reason_v))
    if not res["pinReasonMatch"]: fail(f"durable reasoningEffort {cap_reason!r} != operator input {exp_reason_v!r}")
if cap_max is not None and cap_max>0:
    res["pinMaxMatch"]=(cap_max==exp_max_v and exp_max_v is not None)
    if not res["pinMaxMatch"]: fail(f"durable maxTokens {cap_max} != operator input {exp_max_v}")
if not (res["pinMatchProvider"] and res["pinMatchModel"]):
    fail(f"durable pins {cfg} != operator inputs {exp_provider}/{exp_model}")
res["ok"]=True
json.dump(res,open(out,"w"),indent=2)
PY
}

socket_dir_ok() { # dir-only perms check (used before the plugin binds the socket)
  "$PY" - "$SOCK_DIR" "$SOCK_OWNER" "$SOCK_GROUP" "$SOCK_DIR_MODE" <<'PY'
import json,sys,os,stat,pwd,grp
d,owner,group,dmode=sys.argv[1:5]
exp=int(dmode,8)
def getuid(n):
    try: return pwd.getpwnam(n).pw_uid
    except KeyError: return None
def getgid(n):
    try: return grp.getgrnam(n).gr_gid
    except KeyError: return None
euid=getuid(owner); egid=getgid(group)
if euid is None or egid is None:
    print(json.dumps({"ok":False,"error":f"unknown {owner}/{group}"})); sys.exit(1)
try: s=os.stat(d)
except OSError:
    print(json.dumps({"ok":False,"error":"socket dir missing"})); sys.exit(1)
if not stat.S_ISDIR(s.st_mode):
    print(json.dumps({"ok":False,"error":"not a directory"})); sys.exit(1)
if stat.S_IMODE(s.st_mode)!=exp or s.st_uid!=euid or s.st_gid!=egid:
    print(json.dumps({"ok":False,"error":"dir perms/owner/group mismatch",
                      "dir":{"mode":oct(stat.S_IMODE(s.st_mode)),"uid":s.st_uid,"gid":s.st_gid,
                             "want":{"mode":oct(exp),"uid":euid,"gid":egid}}})); sys.exit(1)
print(json.dumps({"ok":True,"dir":d,"dirMode":oct(stat.S_IMODE(s.st_mode)),
                  "dirUid":s.st_uid,"dirGid":s.st_gid}))
PY
}

socket_perms() { # socket_perms ; prints JSON, exit nonzero on mismatch
  "$PY" - "$SOCK_DIR" "$SOCK_PATH" "$SOCK_OWNER" "$SOCK_GROUP" \
      "$SOCK_DIR_MODE" "$SOCK_MODE" <<'PY'
import json,sys,os,stat,pwd,grp
d,p,owner,group,dmode,smode=sys.argv[1:7]
exp_dir=int(dmode,8); exp_sock=int(smode,8)
def getuid(n):
    try: return pwd.getpwnam(n).pw_uid
    except KeyError: return None
def getgid(n):
    try: return grp.getgrnam(n).gr_gid
    except KeyError: return None
euid=getuid(owner); egid=getgid(group)
if euid is None or egid is None:
    print(json.dumps({"ok":False,"error":f"unknown expected owner/group {owner}/{group}"})); sys.exit(1)
try: sd=os.stat(d)
except OSError:
    print(json.dumps({"ok":False,"error":"socket dir missing"})); sys.exit(1)
if not stat.S_ISDIR(sd.st_mode):
    print(json.dumps({"ok":False,"error":"socket dir not a directory"})); sys.exit(1)
if stat.S_IMODE(sd.st_mode)!=exp_dir or sd.st_uid!=euid or sd.st_gid!=egid:
    print(json.dumps({"ok":False,"error":"dir perms/owner/group mismatch",
                      "dir":{"mode":oct(stat.S_IMODE(sd.st_mode)),"uid":sd.st_uid,"gid":sd.st_gid,
                             "want":{"mode":oct(exp_dir),"uid":euid,"gid":egid}}})); sys.exit(1)
try: ss=os.lstat(p)
except OSError:
    print(json.dumps({"ok":False,"error":"socket file missing"})); sys.exit(1)
if stat.S_ISLNK(ss.st_mode):
    print(json.dumps({"ok":False,"error":"socket path is a symlink"})); sys.exit(1)
if not stat.S_ISSOCK(ss.st_mode):
    print(json.dumps({"ok":False,"error":"socket path not AF_UNIX socket"})); sys.exit(1)
if stat.S_IMODE(ss.st_mode)!=exp_sock or ss.st_uid!=euid or ss.st_gid!=egid:
    print(json.dumps({"ok":False,"error":"socket perms/owner/group mismatch",
                      "sock":{"mode":oct(stat.S_IMODE(ss.st_mode)),"uid":ss.st_uid,"gid":ss.st_gid,
                              "want":{"mode":oct(exp_sock),"uid":euid,"gid":egid}}})); sys.exit(1)
print(json.dumps({"ok":True,"dir":d,"sock":p,
                  "dirMode":oct(stat.S_IMODE(sd.st_mode)),"sockMode":oct(stat.S_IMODE(ss.st_mode)),
                  "dirUid":sd.st_uid,"dirGid":sd.st_gid,"sockUid":ss.st_uid,"sockGid":ss.st_gid}))
PY
}

row_validate() { # row_validate ; uses patch + inputs + plugin file
  "$PY" - "$PATCH_FILE" "$PLUGIN_FILE" "$S2_PILOT_CONV" "$S2_PILOT_SID" \
      "$SOCK_PATH" "$S2_PROVIDER" "$S2_MODEL" "$S2_REASONING_EFFORT" \
      "$S2_MAX_TOKENS" "$PLUGIN_EVID_DIR" <<'PY'
import json,sys,yaml
patch,plugin,conv,sid,sock,provider,model,reason,max_tokens,evid=sys.argv[1:11]
try: docs=yaml.safe_load(open(patch,encoding="utf-8"))
except Exception as e:
    print(json.dumps({"ok":False,"error":f"patch yaml invalid: {e}"})); sys.exit(1)
if docs is None: docs=[]
rows=[]
for doc in (docs if isinstance(docs,list) else [docs]):
    if not isinstance(doc,dict) or "insert" not in doc: continue
    ins=doc["insert"]
    if not isinstance(ins,list): continue
    for ent in ins:
        if isinstance(ent,dict) and ent.get("id")=="discord-agent-drive": rows.append(ent)
if len(rows)!=1:
    print(json.dumps({"ok":False,"error":f"discord-agent-drive rows={len(rows)}"})); sys.exit(1)
ent=rows[0]; cfg=ent.get("config") or {}
want={"pilotConversationKey":conv,"pilotSessionId":sid,"socketPath":sock,
      "provider":provider,"model":model,
      "mediaRoot":"/mnt/off-vm-nfs/comfyui-media","evidenceDir":evid,
      "stubDiscordTool":False}
probs=[]
if str(ent.get("name"))!=plugin: probs.append(f"name={ent.get('name')}")
for k,v in want.items():
    got=cfg.get(k)
    if got!=v: probs.append(f"{k}={got!r} != {v!r}")
if (cfg.get("reasoningEffort") or "")!=(reason or ""): probs.append(f"reasoningEffort={cfg.get('reasoningEffort')!r}")
try:
    if int(cfg.get("maxTokens",0))!=int(max_tokens): probs.append(f"maxTokens={cfg.get('maxTokens')!r}")
except Exception: probs.append(f"maxTokens unparsable {cfg.get('maxTokens')!r}")
if probs:
    print(json.dumps({"ok":False,"error":"row mismatch","problems":probs})); sys.exit(1)
print(json.dumps({"ok":True,"row":ent}))
PY
}

# ---------- service/env helpers ----------------------------------------------
dsh_mainpid()     { systemctl show -p MainPID --value "$DSH_SERVICE" 2>/dev/null || echo 0; }
inbound_mainpid() { systemctl show -p MainPID --value "$INBOUND_SERVICE" 2>/dev/null || echo 0; }
unit_active()     { systemctl is-active "$1" >/dev/null 2>&1; }
unit_health() {
  for u in $PLATFORM_UNITS; do echo "$u=$(systemctl is-active "$u" 2>/dev/null || echo unknown)"; done
}
journal_s2_window() { # journal_s2_window <since-ts>
  journalctl -u "$INBOUND_SERVICE" --since "$1" --no-pager 2>/dev/null | grep -E 's2[ :]|seam|S2 ' || true
}
environ_of() { # environ_of <pid> ; prints env lines
  if [ -n "${S2_GATE_PROC_DIR:-}" ]; then
    cat "${S2_GATE_PROC_DIR}/$1/environ" 2>/dev/null || true
  else
    tr '\0' '\n' < "/proc/$1/environ" 2>/dev/null || true
  fi
}
listener_env_has() { # listener_env_has <expected-conv> ; exact S2_PILOT_CONV presence
  local pid env
  pid="$(inbound_mainpid)"
  env="$(environ_of "$pid")"
  if [ -n "$env" ]; then
    printf '%s\n' "$env" | grep -q "^S2_PILOT_CONV=$1$"
  else
    return 1
  fi
}
listener_env_clean() { # true when S2_PILOT_CONV absent from listener env
  local pid env
  pid="$(inbound_mainpid)"
  env="$(environ_of "$pid")"
  [ -z "$env" ] || ! printf '%s\n' "$env" | grep -q '^S2_PILOT_CONV='
}

file_snapshot() { # file_snapshot <txn-file> <path>
  if [ -e "$2" ]; then cp -a "$2" "$1"; else printf 'ABSENT\n' > "$1"; fi
}
restore_snapshot() { # restore_snapshot <txn-file> <path> <mode> <owner> <group>
  local src="$1" dst="$2" mode="$3" owner="$4" grp="$5" tmp
  if grep -q '^ABSENT$' "$src" 2>/dev/null; then
    rm -f "$dst"
    log "restore: $dst removed (was absent)"
    return 0
  fi
  [ -f "$src" ] || { log "restore: no snapshot for $dst"; return 1; }
  tmp="$dst.tmp-restore.$TS.$$"
  cp "$src" "$tmp"
  chmod "$mode" "$tmp"
  chown "$owner:$grp" "$tmp" 2>/dev/null || true
  mv "$tmp" "$dst"
  log "restore: $dst replaced"
}

state_hash() { sha256sum "$STATE_FILE" 2>/dev/null | cut -d' ' -f1; }

# =============================================================================
# PREFLIGHT BATTERY
# =============================================================================
preflight_battery() { # preflight_battery <expected-live-sha> <phase:pre|stage|activate>
  local exp_live="$1" phase="$2"
  battery_reset
  chk "listener candidate sha == reviewed"    bash -c "[ \"\$(sha256sum \"$LISTENER_STAGED\" | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]"
  chk "seam helper candidate sha == reviewed" bash -c "[ \"\$(sha256sum \"$HELPER_STAGED\"  | cut -d' ' -f1)\" = \"$EXPECT_SEAM_SHA\" ]"
  chk "plugin candidate sha == reviewed"      bash -c "[ \"\$(sha256sum \"$PLUGIN_STAGED\"  | cut -d' ' -f1)\" = \"$EXPECT_PLUGIN_SHA\" ]"
  chk "live listener sha == $([ "$phase" = activate ] && echo candidate || echo baseline)" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" | cut -d' ' -f1)\" = \"$exp_live\" ]"
  if [ "$phase" = "activate" ]; then
    chk "helper live sha == reviewed"         bash -c "[ \"\$(sha256sum \"$HELPER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_SEAM_SHA\" ]"
  else
    chk "helper absent (clean baseline)"      test ! -e "$HELPER_LIVE"
  fi
  chk "dsh.service healthy"           unit_active "$DSH_SERVICE"
  chk "dsh-discord-inbound healthy"   unit_active "$INBOUND_SERVICE"
  chk "S1 plugin file present"        test -f "$S1_PLUGIN_FILE"
  chk "S1 composition row present"    grep -q -- "- id: $S1_ROW_ID" "$PATCH_FILE"
  if [ "$phase" != "activate" ]; then
    chk "S2 plugin row absent"        bash -c "! grep -q -- '- id: discord-agent-drive' \"$PATCH_FILE\""
    chk "S2 plugin file absent"       test ! -e "$PLUGIN_FILE"
    chk "S2 plugin evidence absent"   test ! -e "$PLUGIN_EVID_DIR/plugin-ready.json"
    chk "S2 socket absent"            test ! -e "$SOCK_PATH"
    chk "S2 socket dir absent"        test ! -e "$SOCK_DIR"
    chk "S2 env file absent"          test ! -e "$ENV_FILE"
    chk "S2 drop-in absent"           test ! -e "$DROPIN"
    chk "S2 tmpfiles rule absent"     test ! -e "$TMPFILES_CONF"
  else
    chk "S2 plugin row present exact" row_validate
    chk "S2 plugin file hash"         bash -c "[ \"\$(sha256sum \"$PLUGIN_FILE\" | cut -d' ' -f1)\" = \"$EXPECT_PLUGIN_SHA\" ]"
    chk "S2 socket ready"             socket_perms
    chk "S2 env file absent (pre-activate)" test ! -e "$ENV_FILE"
    chk "S2 drop-in absent (pre-activate)"  test ! -e "$DROPIN"
  fi
  chk "route absent or exact OLD"     route_matches "$ROUTE_FILE" "$S2_PILOT_CONV" OLD
  chk "operator inputs valid"         inputs_valid
  local cap="$TX/binding-baseline.json"
  if bind_capture "$cap"; then
    chk "binding + durable session + pin parity" true
  else
    chk "binding + durable session + pin parity" false
  fi
}

# =============================================================================
# capture helpers used by mutating transactions
# =============================================================================
capture_rollback_baseline() { # <phase>
  step "capture rollback baseline ($1)"
  file_snapshot "$TX/listener-prior.bin"   "$LISTENER_LIVE"
  stat -c '%a %U %G' "$LISTENER_LIVE" > "$TX/listener-prior.meta" 2>/dev/null || echo "0 ? ?" > "$TX/listener-prior.meta"
  file_snapshot "$TX/helper-prior.bin"     "$HELPER_LIVE"
  file_snapshot "$TX/patch-prior.bin"      "$PATCH_FILE"
  file_snapshot "$TX/plugin-prior.bin"     "$PLUGIN_FILE"
  file_snapshot "$TX/env-prior.bin"        "$ENV_FILE"
  file_snapshot "$TX/dropin-prior.bin"     "$DROPIN"
  file_snapshot "$TX/tmpfiles-prior.bin"   "$TMPFILES_CONF"
  file_snapshot "$TX/route-prior.bin"      "$ROUTE_FILE"
  stat -c '%a %U %G' "$PATCH_FILE" > "$TX/patch-prior.meta" 2>/dev/null || echo "644 dsh dsh" > "$TX/patch-prior.meta"
  echo "inbound_pid=$(inbound_mainpid) start_ts=$(systemctl show -p ExecMainStartTimestamp --value "$INBOUND_SERVICE" 2>/dev/null || true)" > "$TX/inbound-before.txt"
  echo "dsh_pid=$(dsh_mainpid) start_ts=$(systemctl show -p ExecMainStartTimestamp --value "$DSH_SERVICE" 2>/dev/null || true)" > "$TX/dsh-before.txt"
  unit_health > "$TX/health-before.txt"
  sha256sum "$LISTENER_LIVE" > "$TX/listener-before.sha"
  cp -a "$PATCH_FILE" "$TX/patch-before.yml"
  manifest_step baseline
}

# ---- mutating sub-steps (each returns nonzero -> caller rollback) -----------
install_listener_candidate() {
  step "install listener + helper (S2 default OFF)"
  local tmp live_owner
  live_owner="$(stat -c %U "$LISTENER_LIVE" 2>/dev/null || echo dsh-discord)"
  tmp="$LISTENER_LIVE.s2-new-$TS"
  cp "$LISTENER_STAGED" "$tmp"
  chmod 0640 "$tmp"
  if ! chown dsh-discord:dsh-discord "$tmp" 2>/dev/null; then chown "$live_owner" "$tmp"; fi
  mv "$tmp" "$LISTENER_LIVE"
  req "listener sha advanced to reviewed candidate" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]" || return 1
  local htmp
  htmp="$HELPER_LIVE.s2-new-$TS"
  cp "$HELPER_STAGED" "$htmp"
  chmod 0640 "$htmp"
  if ! chown dsh-discord:dsh-discord "$htmp" 2>/dev/null; then chown "$live_owner" "$htmp"; fi
  mv "$htmp" "$HELPER_LIVE"
  req "helper sha == reviewed" bash -c "[ \"\$(sha256sum \"$HELPER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_SEAM_SHA\" ]" || return 1
  local pre_pid
  pre_pid="$(inbound_mainpid)"
  systemctl restart "$INBOUND_SERVICE"
  sleep "$POST_RESTART_S"
  req "listener restarted healthy"  unit_active "$INBOUND_SERVICE" || return 1
  req "listener pid advanced"       bash -c "[ \"$(inbound_mainpid)\" != \"$pre_pid\" ]" || return 1
  req "live listener sha == reviewed" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]" || return 1
  manifest_step listener-installed
}

verify_listener_default_off() {
  step "verify listener default-off (historical path; no seam)"
  local lines
  lines="$(journal_s2_window '5 minutes ago')"
  req "no s2/seam lines in listener journal (fresh window)" test -z "$lines" || return 1
  req "S2_PILOT_CONV unset in listener env" listener_env_clean || return 1
  manifest_step listener-default-off
}

install_plugin_composition() {
  step "install plugin file + composition row"
  local psha staged row tmp
  psha="$(sha "$PLUGIN_STAGED")"
  [ "$psha" = "$EXPECT_PLUGIN_SHA" ] || { log "plugin staged hash mismatch: $psha"; return 1; }
  mkdir -p "$PLUGIN_DIR"
  local pfile="$PLUGIN_FILE.new-$TS"
  cp "$PLUGIN_STAGED" "$pfile"
  chmod 0640 "$pfile"; chown dsh:dsh "$pfile" 2>/dev/null || true
  mv "$pfile" "$PLUGIN_FILE"
  req "plugin file sha == reviewed" \
      bash -c "[ \"\$(sha256sum \"$PLUGIN_FILE\" | cut -d' ' -f1)\" = \"$EXPECT_PLUGIN_SHA\" ]" || return 1
  row=$(cat <<YAML
- insert:
  - id: discord-agent-drive
    name: '$PLUGIN_FILE'
    config:
      pilotConversationKey: '$S2_PILOT_CONV'
      pilotSessionId: '$S2_PILOT_SID'
      socketPath: '$SOCK_PATH'
      provider: '$S2_PROVIDER'
      model: '$S2_MODEL'
      reasoningEffort: '$S2_REASONING_EFFORT'
      maxTokens: $S2_MAX_TOKENS
      mediaRoot: '/mnt/off-vm-nfs/comfyui-media'
      evidenceDir: '$PLUGIN_EVID_DIR'
      stubDiscordTool: false
      clientTimeZone: 'Asia/Bangkok'
YAML
)
  staged="$TX/staged-patch.yml"
  cp "$PATCH_FILE" "$staged"
  printf '\n%s\n' "$row" >> "$staged"
  "$PY" -c 'import sys,yaml; yaml.safe_load(open(sys.argv[1]))' "$staged" \
      || { log "staged patch invalid"; return 1; }
  tmp="$PATCH_FILE.s2-new-$TS"
  cp "$staged" "$tmp"; chmod 0644 "$tmp"; chown dsh:dsh "$tmp" 2>/dev/null || true
  mv "$tmp" "$PATCH_FILE"
  req "composition row present + exact" row_validate || return 1
  manifest_step plugin-installed
}

install_socket_dir_tmpfiles() {
  step "install tmpfiles rule + create setgid socket dir"
  local tmp
  tmp="$TMPFILES_CONF.s2-new-$TS"
  printf 'd %s %s %s %s -\n' "$SOCK_DIR" "$SOCK_DIR_MODE" "$SOCK_OWNER" "$SOCK_GROUP" > "$tmp"
  chmod 0644 "$tmp"; chown root:root "$tmp" 2>/dev/null || true
  mv "$tmp" "$TMPFILES_CONF"
  req "tmpfiles rule installed" grep -q "^d $SOCK_DIR " "$TMPFILES_CONF" || return 1
  "$TMPFILES_CMD" --create "$TMPFILES_CONF" || { log "tmpfiles create failed"; return 1; }
  req "socket dir exists with exact perms/owner/group" socket_dir_ok >/dev/null || return 1
  manifest_step socket-dir-ready
}

restart_dsh_once() {
  step "restart dsh.service once (plugin mount)"
  local pre_pid n ok pid
  pre_pid="$(dsh_mainpid)"
  systemctl restart "$DSH_SERVICE"
  n=0; ok=0
  while [ "$n" -lt "$BOOT_WAIT_S" ]; do
    pid="$(dsh_mainpid)"
    if [ -n "$pid" ] && [ "$pid" != "0" ] && [ "$pid" != "$pre_pid" ] \
       && unit_active "$DSH_SERVICE" \
       && [ -s "$PLUGIN_EVID_DIR/plugin.log" ] \
       && tail -30 "$PLUGIN_EVID_DIR/plugin.log" 2>/dev/null | grep -q "socket listening $SOCK_PATH" \
       && [ -f "$PLUGIN_EVID_DIR/plugin-loaded.json" ] \
       && [ -f "$PLUGIN_EVID_DIR/plugin-ready.json" ]; then
      ok=1; break
    fi
    n=$((n+1)); sleep 1
  done
  if [ "$ok" != 1 ]; then
    log "wait plugin boot timeout; plugin.log tail:"
    tail -30 "$PLUGIN_EVID_DIR/plugin.log" 2>/dev/null | sed 's/^/    /' || true
    return 1
  fi
  req "dsh.service active after restart" unit_active "$DSH_SERVICE" || return 1
  manifest_step dsh-restarted
}

verify_s1_rearmed() {
  step "verify S1 still healthy after dsh restart"
  local n ok
  n=0; ok=0
  while [ "$n" -lt "$S1_WAIT_S" ]; do
    if [ -s "$S1_PLUGIN_LOG" ] && tail -12 "$S1_PLUGIN_LOG" 2>/dev/null \
         | grep -q "boot: done -> resumed=2 .*live roots=2"; then ok=1; break; fi
    n=$((n+1)); sleep 1
  done
  req "S1 schedule-boot-rearm re-armed (resumed=2 live roots=2)" [ "$ok" = 1 ] || return 1
  req "S1 plugin file present" test -f "$S1_PLUGIN_FILE" || return 1
  local h
  h="$(unit_health)"
  req "platform units active" bash -c "! printf '%s' \"\$1\" | grep -Eq '=(inactive|unknown|failed)'" _ "$h" || return 1
  manifest_step s1-healthy
}

run_hello_probe() { # run_hello_probe <expect-hello-route> [--route R] [out-json]
  local exp_route="$1"; shift
  local out="$TX/probe.json"
  local ra=()
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --route) ra=(--route "$2"); shift 2 ;;
      *) out="$1"; shift ;;
    esac
  done
  "$PY" "$PROBE" --sock "$SOCK_PATH" --conv "$S2_PILOT_CONV" --sid "$S2_PILOT_SID" \
      --state "$STATE_FILE" --expect-hello-route "$exp_route" \
      ${ra[@]+"${ra[@]}"} --timeout 20 --quiet "$PROBE_QUIET_S" \
      > "$out" 2>"$TX/probe.stderr"
}

verify_post_mount_readiness() {
  step "post-mount readiness (socket/session/pin; route OLD; no activation)"
  req "socket dir+socket perms/type exact" socket_perms >/dev/null || return 1
  req "plugin apply configured=yes" \
      bash -c "grep -q 'apply: pilot=.*configured=yes' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  req "plugin socket listening evidence" \
      bash -c "grep -q 'socket listening $SOCK_PATH' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  req "plugin markers present" \
      bash -c "[ -s \"$PLUGIN_EVID_DIR/plugin-loaded.json\" ] && [ -s \"$PLUGIN_EVID_DIR/plugin-ready.json\" ]" || return 1
  if ! run_hello_probe OLD; then
    log "hello probe FAILED:"; sed 's/^/    /' "$TX/probe.json" 2>/dev/null || true
    return 1
  fi
  req "hello-ack exact pilot + route OLD + zero frames" \
      "$PY" -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(0 if d.get("ok") and not d.get("quietFrames") else 1)' \
      "$TX/probe.json" || return 1
  req "plugin reconcile evidence" \
      bash -c "grep -q 'reconcile done' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  req "no plugin resolve/reconcile failure markers" \
      bash -c "! grep -Eq 'resolveAgent resume FAIL|pilot session not materialized|FINALIZATION OVERFLOW' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  local cap2="$TX/binding-post.json"
  req "durable session pins unchanged after materialize" bind_capture "$cap2" || return 1
  req "listener env still S2_PILOT_CONV-free" listener_env_clean || return 1
  req "no activation frames in plugin log" \
      bash -c "! grep -Eq 'CLAIM observed|route -> S2_ACTIVE|admission acked|FINALIZATION ' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  manifest_step post-mount-ready
}

# =============================================================================
# ROLLBACK FUNCTIONS
# =============================================================================
rollback_full_baseline() { # full pre-S2 byte restore from this txn's snapshots (fail-closed)
  step "FULL baseline rollback from $TX"
  manifest_step rollback-start
  local rb_ok=1
  rm -f "$ROUTE_FILE" "$ENV_FILE" "$DROPIN" "$TMPFILES_CONF"
  if ! systemctl daemon-reload; then log "rollback: daemon-reload FAILED"; return 1; fi
  rm -rf "$SOCK_DIR"
  # every restoration step below is MANDATORY: any failure stops the rollback
  # (never claim completion on partial restoration)
  if ! restore_snapshot "$TX/patch-prior.bin" "$PATCH_FILE" \
      "$(awk '{print $1}' "$TX/patch-prior.meta" 2>/dev/null || echo 0644)" \
      "$(awk '{print $2}' "$TX/patch-prior.meta" 2>/dev/null || echo dsh)" \
      "$(awk '{print $3}' "$TX/patch-prior.meta" 2>/dev/null || echo dsh)"; then
    log "rollback: patch restore FAILED"; rb_ok=0
  fi
  rm -f "$PLUGIN_FILE"
  if ! restore_snapshot "$TX/listener-prior.bin" "$LISTENER_LIVE" \
      "$(awk '{print $1}' "$TX/listener-prior.meta" 2>/dev/null || echo 0640)" \
      "$(awk '{print $2}' "$TX/listener-prior.meta" 2>/dev/null || echo dsh-discord)" \
      "$(awk '{print $3}' "$TX/listener-prior.meta" 2>/dev/null || echo dsh-discord)"; then
    log "rollback: listener restore FAILED"; rb_ok=0
  fi
  if ! restore_snapshot "$TX/helper-prior.bin" "$HELPER_LIVE" 0640 dsh-discord dsh-discord; then
    log "rollback: helper restore FAILED"; rb_ok=0
  fi
  if [ "$rb_ok" != 1 ]; then
    manifest_step rollback-failed
    return 1
  fi
  manifest_step rollback-bytes-restored
  if ! systemctl restart "$INBOUND_SERVICE"; then log "rollback: inbound restart FAILED"; return 1; fi
  if ! systemctl restart "$DSH_SERVICE"; then log "rollback: dsh restart FAILED"; return 1; fi
  sleep 5
  # plugin evidence dir removed too so a later baseline is byte-clean
  rm -rf "$PLUGIN_EVID_DIR"
  manifest_step rollback-restarted
  req "listener restored to baseline sha" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" 2>/dev/null | cut -d' ' -f1)\" = \"$EXPECT_LIVE_SHA\" ]" || return 1
  req "helper absent (baseline)" test ! -e "$HELPER_LIVE" || return 1
  req "plugin file absent (baseline)" test ! -e "$PLUGIN_FILE" || return 1
  req "composition row absent (baseline)" \
      bash -c "! grep -q -- '- id: discord-agent-drive' \"$PATCH_FILE\"" || return 1
  req "route file absent (baseline)" test ! -e "$ROUTE_FILE" || return 1
  req "socket dir removed (baseline)" test ! -e "$SOCK_DIR" || return 1
  req "dsh.service healthy after rollback" unit_active "$DSH_SERVICE" || return 1
  req "inbound healthy after rollback" unit_active "$INBOUND_SERVICE" || return 1
  unit_health > "$TX/health-after-rollback.txt"
  manifest_step rollback-complete
  log "FULL baseline rollback complete (evidence: $TX)"
}

rollback_authority_old() { # ACTIVE -> QUIESCING -> OLD; plugin stays mounted
  step "AUTHORITY rollback to OLD (plugin may stay mounted)"
  local conv="$1"
  # refuse to touch a route file that names a DIFFERENT conversation
  if [ -e "$ROUTE_FILE" ]; then
    "$PY" - "$ROUTE_FILE" "$conv" <<'PY' || die "route file names another conversation — operator inspect before rollback"
import json,sys,os
path,conv=sys.argv[1:3]
try: d=json.load(open(path))
except Exception: sys.exit(1)
if not isinstance(d,dict) or list(d.keys())!= [conv]: sys.exit(1)
PY
  fi
  manifest_step rollback-start
  "$PY" -c 'import json,sys;json.dump({sys.argv[1]:"QUIESCING_TO_OLD"},open(sys.argv[2],"w"))' \
      "$conv" "$ROUTE_FILE"
  chown dsh-discord:dsh-discord "$ROUTE_FILE" 2>/dev/null || true
  chmod 0640 "$ROUTE_FILE"
  req "route file QUIESCING_TO_OLD written" route_matches "$ROUTE_FILE" "$conv" QUIESCING_TO_OLD || return 1
  log "quiesce window ${SETTLE_S}s (in-flight pilot turns settle)"
  sleep "$SETTLE_S"
  "$PY" -c 'import json,sys;json.dump({sys.argv[1]:"OLD"},open(sys.argv[2],"w"))' \
      "$conv" "$ROUTE_FILE"
  chown dsh-discord:dsh-discord "$ROUTE_FILE" 2>/dev/null || true
  chmod 0640 "$ROUTE_FILE"
  req "route file OLD written" route_matches "$ROUTE_FILE" "$conv" OLD || return 1
  run_hello_probe "" --route OLD >/dev/null 2>&1 || log "plugin route push failed (listener pushes on next event)"
  local h
  h="$(unit_health)"
  req "platform healthy after authority rollback" \
      bash -c "! printf '%s' \"\$1\" | grep -Eq '=(inactive|unknown|failed)'" _ "$h" || return 1
  manifest_step rollback-complete
  log "AUTHORITY rollback complete (evidence: $TX)"
}

# =============================================================================
# MODE: PREFLIGHT
# =============================================================================
run_preflight() {
  log "=== S2 live gate PREFLIGHT (read-only) ==="
  inputs_valid || die "operator inputs required (S2_PILOT_CONV/S2_PILOT_SID/S2_PROVIDER/S2_MODEL)"
  make_manifest preflight
  if grep -q -- '- id: discord-agent-drive' "$PATCH_FILE" 2>/dev/null; then
    log "detected S2 plugin row present -> running ACTIVATE precondition battery"
    preflight_battery "$EXPECT_LISTENER_SHA" activate
  else
    log "no S2 mount -> running clean-OLD baseline battery"
    preflight_battery "$EXPECT_LIVE_SHA" pre
  fi
  unit_health > "$TX/health-preflight.txt"
  echo "--- preflight result: $PASS pass / $FAIL fail (no mutation) ---"
  if [ "$FAIL" -eq 0 ]; then
    manifest_step verified
    log "PREFLIGHT PASS (evidence: $TX)"
    exit 0
  fi
  manifest_step failed
  log "PREFLIGHT FAIL — correct failures before GO (evidence: $TX)"
  exit 1
}

# =============================================================================
# MODE: STAGE
# =============================================================================
run_stage() {
  log "=== S2 live gate STAGE ==="
  inputs_valid || die "operator inputs required"
  make_manifest stage
  preflight_battery "$EXPECT_LIVE_SHA" stage
  if [ "$FAIL" -gt 0 ]; then
    manifest_step failed
    die "STAGE preflight FAILED ($FAIL failures) — ZERO MUTATION performed"
  fi
  manifest_step preflight-pass
  capture_rollback_baseline stage
  local state_before
  state_before="$(state_hash)"
  if ! install_listener_candidate; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage listener install failed -> full rollback (evidence: $TX)"
  fi
  if ! verify_listener_default_off; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage default-off verify failed -> full rollback (evidence: $TX)"
  fi
  if ! install_plugin_composition; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage plugin/composition install failed -> full rollback (evidence: $TX)"
  fi
  if ! install_socket_dir_tmpfiles; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage socket dir failed -> full rollback (evidence: $TX)"
  fi
  if ! restart_dsh_once; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage dsh restart/plugin boot failed -> full rollback (evidence: $TX)"
  fi
  if ! verify_s1_rearmed; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage S1 rearm failed -> full rollback (evidence: $TX)"
  fi
  if ! verify_post_mount_readiness; then
    rollback_full_baseline || log "rollback errors"; manifest_step rollback-failed
    die "stage post-mount readiness failed -> full rollback (evidence: $TX)"
  fi
  req "route still absent/OLD (gate never activated)" route_matches "$ROUTE_FILE" "$S2_PILOT_CONV" OLD \
      || { rollback_full_baseline || true; die "route changed during stage -> full rollback"; }
  req "inbound-state.json untouched by gate" [ "$(state_hash)" = "$state_before" ] \
      || { rollback_full_baseline || true; die "state hash changed -> full rollback"; }
  unit_health > "$TX/health-after.txt"
  manifest_step verified
  log "STAGE PASS — route OLD, S2 default OFF, plugin mounted healthy (evidence: $TX)"
  log "NEXT: operator runs: $0 activate  (explicit GO), then the supervised live battery."
}

# =============================================================================
# MODE: ACTIVATE
# =============================================================================
latest_stage_txn() {
  local d
  for d in $(ls -1dt "$EVID_BASE"/txn-* 2>/dev/null); do
    [ -f "$d/manifest.json" ] || continue
    if "$PY" - "$d/manifest.json" <<'PY' >/dev/null
import json,sys
m=json.load(open(sys.argv[1]))
sys.exit(0 if m.get("mode")=="stage" and m.get("state")=="verified" else 1)
PY
    then echo "$d"; return 0; fi
  done
  return 1
}
latest_activate_txn() {
  local d
  for d in $(ls -1dt "$EVID_BASE"/txn-* 2>/dev/null); do
    [ -f "$d/manifest.json" ] || continue
    if "$PY" - "$d/manifest.json" <<'PY' >/dev/null
import json,sys
m=json.load(open(sys.argv[1]))
sys.exit(0 if m.get("mode")=="activate" else 1)
PY
    then echo "$d"; return 0; fi
  done
  return 1
}
json_input() { "$PY" -c 'import json,sys;print(json.load(open(sys.argv[1]))["inputs"].get(sys.argv[2],""))' "$1" "$2"; }

run_activate() {
  log "=== S2 live gate ACTIVATE ==="
  local stx
  stx="$(latest_stage_txn)" || die "no PASSED stage transaction found — activate requires a staged-success receipt"
  log "using staged txn: $stx"
  local t_conv t_sid t_provider t_model t_reason t_max
  t_conv="$(json_input "$stx/manifest.json" conv)"
  t_sid="$(json_input "$stx/manifest.json" sid)"
  t_provider="$(json_input "$stx/manifest.json" provider)"
  t_model="$(json_input "$stx/manifest.json" model)"
  t_reason="$(json_input "$stx/manifest.json" reasoningEffort)"
  t_max="$(json_input "$stx/manifest.json" maxTokens)"
  if [ -n "${S2_PILOT_CONV:-}${S2_PILOT_SID:-}${S2_PROVIDER:-}${S2_MODEL:-}" ]; then
    { [ "$S2_PILOT_CONV" = "$t_conv" ] && [ "$S2_PILOT_SID" = "$t_sid" ] \
      && [ "$S2_PROVIDER" = "$t_provider" ] && [ "$S2_MODEL" = "$t_model" ]; } \
      || die "operator env inputs do not match the staged txn inputs"
  fi
  S2_PILOT_CONV="$t_conv"; S2_PILOT_SID="$t_sid"; S2_PROVIDER="$t_provider"
  S2_MODEL="$t_model"; S2_REASONING_EFFORT="$t_reason"; S2_MAX_TOKENS="$t_max"
  inputs_valid || die "staged txn inputs invalid"
  make_manifest activate
  log "activate targets pilot conv=$S2_PILOT_CONV sid=$S2_PILOT_SID"
  preflight_battery "$EXPECT_LISTENER_SHA" activate
  if [ "$FAIL" -gt 0 ]; then manifest_step failed; die "ACTIVATE precondition FAILED — ZERO MUTATION"; fi
  # no double-activate: refuse if a verified activate txn is newer than stx
  local ax
  for ax in $(ls -1dt "$EVID_BASE"/txn-* 2>/dev/null); do
    [ "$ax" = "$TX" ] && break
    if "$PY" - "$ax/manifest.json" <<'PY' >/dev/null 2>&1
import json,sys
m=json.load(open(sys.argv[1]))
sys.exit(0 if m.get("mode")=="activate" and m.get("state")=="verified" else 1)
PY
    then die "a verified activate txn already exists ($ax) — run rollback first"; fi
  done
  manifest_step preflight-pass
  step "capture activate rollback baseline"
  file_snapshot "$TX/env-prior.bin" "$ENV_FILE"
  file_snapshot "$TX/dropin-prior.bin" "$DROPIN"
  file_snapshot "$TX/route-prior.json" "$ROUTE_FILE"
  echo "inbound_pid=$(inbound_mainpid)" > "$TX/inbound-before.txt"
  unit_health > "$TX/health-before.txt"
  local state_before
  state_before="$(state_hash)"
  printf '%s' "$state_before" > "$TX/state-before.hash"
  manifest_step baseline

  # --- 1) persistent listener config (EnvironmentFile drop-in) ---
  step "install listener S2_PILOT_CONV EnvironmentFile drop-in"
  local tmp
  tmp="$ENV_FILE.s2-new-$TS"
  printf '# S2 pilot listener config (gate-owned; rollback bytes in txn)\nS2_PILOT_CONV=%s\n' "$S2_PILOT_CONV" > "$tmp"
  chmod 0600 "$tmp"; chown root:root "$tmp" 2>/dev/null || true
  mv "$tmp" "$ENV_FILE"
  mkdir -p "$DROPIN_DIR"
  tmp="$DROPIN.s2-new-$TS"
  printf '[Service]\nEnvironmentFile=%s\n' "$ENV_FILE" > "$tmp"
  chmod 0644 "$tmp"; chown root:root "$tmp" 2>/dev/null || true
  mv "$tmp" "$DROPIN"
  req "drop-in installed with exact env file" grep -q "EnvironmentFile=$ENV_FILE" "$DROPIN" \
      || { rm -f "$ENV_FILE" "$DROPIN"; die "activate env install failed -> rollback done (no restart)"; }
  req "env file contains exact pilot conv" grep -q "^S2_PILOT_CONV=$S2_PILOT_CONV$" "$ENV_FILE" \
      || { rm -f "$ENV_FILE" "$DROPIN"; die "activate env value mismatch -> rollback done (no restart)"; }
  systemctl daemon-reload
  manifest_step env-installed

  # --- 2) restart listener; read back actual process env ---
  step "restart listener with pilot config; read back env"
  local pre_pid
  pre_pid="$(inbound_mainpid)"
  systemctl restart "$INBOUND_SERVICE"
  sleep 3
  req "listener restarted healthy" unit_active "$INBOUND_SERVICE" || return 1
  req "listener pid advanced" bash -c "[ \"$(inbound_mainpid)\" != \"$pre_pid\" ]" || return 1
  req "listener process env carries exact S2_PILOT_CONV" listener_env_has "$S2_PILOT_CONV" || return 1
  req "live listener sha still reviewed candidate" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]" || return 1
  manifest_step listener-pilot

  # --- 3) route file flip S2_ACTIVE (atomic; gate-owned; read back) ---
  step "write route file S2_ACTIVE (exact pilot only)"
  local rtmp
  rtmp="$ROUTE_FILE.s2-new-$TS"
  "$PY" -c 'import json,sys;json.dump({sys.argv[1]:"S2_ACTIVE"},open(sys.argv[2],"w"))' \
      "$S2_PILOT_CONV" "$rtmp"
  chown dsh-discord:dsh-discord "$rtmp" 2>/dev/null || true
  chmod 0640 "$rtmp"
  mv "$rtmp" "$ROUTE_FILE"
  req "route file read-back S2_ACTIVE (single key)" route_matches "$ROUTE_FILE" "$S2_PILOT_CONV" S2_ACTIVE || return 1
  manifest_step route-active

  # --- 4) seam hello + plugin route proof (no synthetic user event) ---
  step "prove plugin seam hello + push plugin-side S2_ACTIVE"
  if ! run_hello_probe OLD --route S2_ACTIVE "$TX/activate-probe.json"; then
    log "activate hello/route probe failed; probe:"; sed 's/^/    /' "$TX/activate-probe.json" 2>/dev/null || true
    return 1
  fi
  req "hello-ack exact + route-ack S2_ACTIVE" \
      "$PY" -c 'import json,sys;d=json.load(open(sys.argv[1]));a=d.get("helloAck") or {};r=d.get("routeAck") or {};sys.exit(0 if d.get("ok") and a.get("pilotConversationKey")==sys.argv[2] and a.get("pilotSessionId")==sys.argv[3] and r.get("state")=="S2_ACTIVE" else 1)' \
      "$TX/activate-probe.json" "$S2_PILOT_CONV" "$S2_PILOT_SID" || return 1
  req "no admitted/user frames during activate probe" \
      "$PY" -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(0 if not d.get("quietFrames") else 1)' \
      "$TX/activate-probe.json" || return 1
  manifest_step seam-proven

  # --- 5) final read-backs + sibling/no-state-change proofs ---
  step "activate read-backs + sibling proof"
  req "route file still exact S2_ACTIVE" route_matches "$ROUTE_FILE" "$S2_PILOT_CONV" S2_ACTIVE || return 1
  req "listener env still exact pilot" listener_env_has "$S2_PILOT_CONV" || return 1
  req "inbound-state.json untouched by activate" [ "$(state_hash)" = "$state_before" ] || return 1
  req "plugin log shows route -> S2_ACTIVE" \
      bash -c "grep -q 'route -> S2_ACTIVE' \"$PLUGIN_EVID_DIR/plugin.log\"" || return 1
  unit_health > "$TX/health-after.txt"
  manifest_step verified
  log "ACTIVATE PASS — pilot S2_ACTIVE; listener env exact; plugin seam proven; NO synthetic user event."
  log "NEXT (operator): supervised live battery on the exact pilot. Rollback: $0 rollback (authority) / $0 restore-baseline (full bytes)."
}

activate_failure_rollback() { # auto rollback on activate failure -> staged OLD (fail-closed)
  log "activate failure -> auto rollback to staged OLD"
  manifest_step rollback-start
  local conv
  conv="$(json_input "$TX/manifest.json" conv)"
  # every rollback operation below is mandatory and verified
  if ! rm -f "$ENV_FILE" "$DROPIN"; then log "rollback: could not remove env/drop-in"; return 1; fi
  if ! systemctl daemon-reload; then log "rollback: daemon-reload FAILED"; return 1; fi
  if [ -f "$ROUTE_FILE" ]; then
    "$PY" -c 'import json,sys;json.dump({sys.argv[1]:"OLD"},open(sys.argv[2],"w"))' "$conv" "$ROUTE_FILE"
    chown dsh-discord:dsh-discord "$ROUTE_FILE" 2>/dev/null || true
    chmod 0640 "$ROUTE_FILE"
  fi
  local pre_pid
  pre_pid="$(inbound_mainpid)"
  if ! systemctl restart "$INBOUND_SERVICE"; then log "rollback: inbound restart FAILED"; return 1; fi
  sleep "$POST_RESTART_S"
  # mandatory post-rollback verification
  req "rollback: inbound healthy" unit_active "$INBOUND_SERVICE" || return 1
  req "rollback: listener pid advanced" bash -c "[ \"$(inbound_mainpid)\" != \"$pre_pid\" ]" || return 1
  req "rollback: listener env cleared (no S2_PILOT_CONV)" listener_env_clean || return 1
  req "rollback: route absent or exact OLD" route_matches "$ROUTE_FILE" "$conv" OLD || return 1
  if [ -f "$TX/state-before.hash" ]; then
    req "rollback: inbound-state.json unchanged" [ "$(state_hash)" = "$(cat "$TX/state-before.hash")" ] || return 1
  fi
  # push + VERIFY plugin-side OLD (fail closed if the seam cannot be reached)
  if ! run_hello_probe "" --route OLD "$TX/rollback-probe.json"; then
    log "rollback: plugin route push/probe FAILED (indeterminate)"; return 1
  fi
  req "rollback: plugin route-ack OLD" \
      "$PY" -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(0 if d.get("ok") and (d.get("routeAck") or {}).get("state")=="OLD" else 1)' \
      "$TX/rollback-probe.json" || return 1
  unit_health > "$TX/health-after-rollback.txt"
  manifest_step rollback-complete
  log "activate auto-rollback complete (verified) — staged OLD state restored (plugin mounted; route OLD)"
}

# =============================================================================
# MODE: ROLLBACK / RESTORE-BASELINE
# =============================================================================
run_rollback() {
  log "=== S2 live gate ROLLBACK (authority to OLD; plugin may stay mounted) ==="
  local atx
  atx="$(latest_activate_txn)" || {
    log "no activate txn found — authority is already OLD (nothing to quiesce)."
    exit 0
  }
  local conv
  conv="$(json_input "$atx/manifest.json" conv)"
  S2_PILOT_CONV="$conv"
  TX="$atx"
  log "rollback from activate txn $atx (pilot conv=$conv)"
  # nothing to quiesce when authority is already OLD/absent
  if route_matches "$ROUTE_FILE" "$conv" OLD >/dev/null 2>&1; then
    log "route already OLD/absent — nothing to quiesce (env fence may stay armed)"
    exit 0
  fi
  if ! rollback_authority_old "$conv"; then
    manifest_step rollback-failed
    die "authority rollback FAILED (manual recovery: write route OLD + restart inbound)"
  fi
  log "ROLLBACK PASS — pilot authority OLD; env fence armed; plugin mounted healthy."
  log "Full byte rollback: $0 restore-baseline (operator GO)."
}

run_restore_baseline() {
  log "=== S2 live gate RESTORE-BASELINE (full byte rollback to pre-S2) ==="
  local stx conv
  stx="$(latest_stage_txn)" || die "no PASSED stage txn found to restore from"
  TX="$stx"
  log "restoring from stage txn $stx"
  # authority constraint: refuse to restore baseline while a newer verified
  # activate txn is still the ACTIVE authority (operator must rollback first)
  local ax
  for ax in $(ls -1dt "$EVID_BASE"/txn-* 2>/dev/null); do
    [ "$ax" = "$stx" ] && break
    if "$PY" - "$ax/manifest.json" <<'PY' >/dev/null 2>&1
import json,sys
m=json.load(open(sys.argv[1]))
sys.exit(0 if m.get("mode")=="activate" and m.get("state")=="verified" else 1)
PY
    then die "a verified activate txn is newer than the stage receipt ($ax) — run: $0 rollback  first"; fi
  done
  # staged-identity constraint: current live state must match the receipt
  conv="$(json_input "$stx/manifest.json" conv)"
  S2_PILOT_CONV="$conv"
  req "current listener == staged candidate (receipt match)" \
      bash -c "[ \"\$(sha256sum \"$LISTENER_LIVE\" | cut -d' ' -f1)\" = \"$EXPECT_LISTENER_SHA\" ]" || die "live listener does not match stage receipt — inspect before restore-baseline"
  req "current plugin file == reviewed (receipt match)" \
      bash -c "[ \"\$(sha256sum \"$PLUGIN_FILE\" 2>/dev/null | cut -d' ' -f1)\" = \"$EXPECT_PLUGIN_SHA\" ]" || die "plugin state does not match stage receipt — inspect before restore-baseline"
  req "composition row present (receipt match)" row_validate || die "composition does not match stage receipt — inspect before restore-baseline"
  if ! rollback_full_baseline; then
    manifest_step restore-failed
    die "restore-baseline FAILED"
  fi
  log "RESTORE-BASELINE PASS — pre-S2 state restored (evidence: $TX)"
}

case "$MODE" in
  preflight)        run_preflight ;;
  stage)            run_stage ;;
  activate)         if run_activate; then :; else
                      activate_failure_rollback || die "activate auto-rollback INDETERMINATE (manual recovery: route OLD + remove env/drop-in + restart inbound)"
                      exit 1
                    fi ;;
  rollback)         run_rollback ;;
  restore-baseline) run_restore_baseline ;;
  *) echo "usage: $0 preflight|stage|activate|rollback|restore-baseline" >&2; exit 64 ;;
esac
