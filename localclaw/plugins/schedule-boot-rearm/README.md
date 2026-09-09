# dsh-schedule-boot-rearm

In-process DSH-native boot re-arm for persisted schedule owners. Replaces the
external `dsh-scheduler-materialize.{sh,service}` oneshot (ledger S-01) without
changing schedule semantics.

| | |
|---|---|
| DSH baseline | `@deepseek-ai/dsh@0.1.1-rc.2` (published npm) |
| Kind | Cordis function plugin, plain ESM (no build step; mounted out-of-tree by file path) |
| Canonical source | `localclaw/plugins/schedule-boot-rearm/schedule-boot-rearm.mjs` in this fork |
| Live mount | byte-identical copy + one `cordis.patch.yml` row in the deployment `$DSH_HOME` layer |
| Scope | S-01 only — no scheduler redesign, no cron replacement, no schedule DB, no gold logic |

## Why it exists

`@deepseek-ai/dsh-schedule` installs its three session-scoped tools
(`schedule_create` / `schedule_list` / `schedule_delete`) only on **root Agents
published after the schedule plugin loads** (upstream load-order boundary; see
its README "Load-order boundary" and `lib/index.js` `agent/created` listener).
After a DSH restart every previously live schedule owner is cold, so it must be
re-materialized through the AgentRegistry factory before schedule tools/runtime
re-attach and the durable `schedule/change` records in that session's log
re-arm.

The historical fix was an external systemd oneshot that poked the browser RPC
endpoint (`session.models`) after every `dsh.service` start. This plugin does
the same re-arm **in-process through the native lifecycle primitive**
(`ctx.agents.resume`) and then gets out of the way.

## What it does (exactly)

1. Reads its config list of schedule-owner session ids from the patch row
   (`scheduleSessionIds`).
2. STRICTLY fail-closed readiness: only once the `@deepseek-ai/dsh-schedule`
   loader entry is positively observed **active** (its `agent/created` listener
   installed) does it proceed — see “Readiness gate”.
3. Then, for each id:
   - skips if the session is already live in this process (no duplicate);
   - validates the id exists in the native persistence index
     (`ctx.sessionPersistence.list()`), otherwise logs and skips (fail-narrow);
   - `ctx.agents.resume({ resumeSessionId, agentOptions, setup })`.
4. The resume's `setup` installs the same agent-scoped model selection the host
   materialization path installs (`installModelSelection` from
   `@deepseek-ai/dsh-agent`, reading the session's own last logged
   request/header config first) — so a resumed owner keeps its recorded model
   pin/effort (e.g. Terra) instead of silently falling back to the default.
5. Logs one journal line per owner and a summary; owns nothing else.

It never creates schedules, never writes schedule records, never deletes
anything, never calls browser-facing `session.*` RPC, and never scans or
disturbs unrelated sessions.

## Config

Home-level `cordis.patch.yml` row (place it after the `schedule` row; the
specifier MUST be absolute — Cordis resolves relative entry names against the
profile directory, so a home-level `./…` name would resolve per-profile):

```yaml
- insert:
  - id: schedule-boot-rearm
    name: '/opt/dsh/home/plugins/schedule-boot-rearm.mjs'
    config:
      scheduleSessionIds:
        - session-00000000-0000-0000-0000-000000000000   # replace: schedule owner ids
```

The id list is the deployment declaration of which persisted sessions own
schedules — the direct successor of the retired `DSH_SCHEDULE_SESSIONS`
contract. rc.2 keeps schedule state only inside each session's own durable log;
there is no native central "sessions that own schedules" index, so an explicit
list is the smallest honest discovery source. Ids are validated against the
native persistence index before any resume.

## Idempotence and failure behavior

- Repeated plugin starts / DSH restarts are harmless: schedule records live in
  the durable session log and are never re-created by a resume; at most one
  live agent exists per session id in a process; already-live ids are skipped.
- Per-id, fail-narrow: a missing/invalid/failed id is logged and skipped; its
  persisted state is never deleted or replaced; one failure never blocks DSH
  startup (the summary line reports `failed=N` for health evidence).
- On unload, agent-loop **owner-context teardown disposes the agents this
  plugin instance resumed** (rc.2 native ownership — no parallel manual
  ownership mechanism). Durable session state is untouched, so a later instance
  resumes the same owners exactly once: no duplicate agent, no duplicate
  schedule, no duplicate wake, no schedule occurrence emitted by the
  unload/remount cycle.

## Readiness gate (strictly fail-closed)

rc.2 `@deepseek-ai/dsh-schedule` installs its runtime/tools only from its
`agent/created` listener. If a cold persisted owner were resumed before that
listener exists, the owner would become live **without** being re-armed, and
later schedule activation never recreates the missed `agent/created` event.
Resumes therefore happen ONLY after the schedule loader entry is positively
observed active (`ctx.loader.entries()` entry with id/name
`@deepseek-ai/dsh-schedule` and an installed fiber):

- entry active → resume configured owners;
- entry present but inactive until the 60s deadline → **no resume**;
- entry absent / loader unobservable → **no resume**;
- loader/readiness exception → **no resume**.

Elapsed settling time is never treated as proof of readiness (a bounded loader
appearance wait only waits for the loader *service*, never for the schedule
listener). The external oneshot remains the rollback mechanism for a failed
cutover, so fail-closed is the correct posture.

## Journal evidence

All journal lines are prefixed `[schedule-boot-rearm]`; the same lines are also
appended (best-effort, rotated at 1 MiB) to
`$DSH_HOME/plugins/schedule-boot-rearm.log` so acceptance/health tooling can
read them without depending on the logger transport. The directory is created
by the plugin at first boot. Journald remains the authoritative record.

```
boot: start; configured schedule owner(s) = 2
resume ok: session-… (durable schedule/change records = N; live roots = M)
resume skip: session-… not present in the native persistence index (missing/malformed target; state untouched)
boot: done -> resumed=2 alreadyLive=0 missing=1 failed=0 invalid=0 (live roots=2)
```

## Test battery

`test/run-battery.sh` builds an isolated overlay (`$DSH_HOME` under `/tmp`),
seeds synthetic schedule sessions against an anonymous loopback Switchyard
gateway, and proves the GO §8 pre-production claims:

- **phase 0** — readiness matrix (unit-level, no network): schedule active →
  resume; present-but-inactive → no resume; absent → no resume; loader
  unobservable → no resume; loader `entries()` throws → no resume; already-live
  owner skipped (no duplicate).
- **phase 1** — seed schedule-bearing session A (with a persisted model pin) +
  control session C.
- **phases 2–3** — two simulated DSH restarts with the plugin mounted: mount via
  cordis patch, no browser `/api`/`session.*` RPC, `ctx.agents.resume`, native
  `schedule_list` re-arms, persisted synthetic schedule survives, pin parity
  (persisted model A retained while the deployment default switches to B),
  idempotence across restarts, malformed/missing target fails narrow, control
  session undisturbed, import allowlist respected.
- **phase 4** — lifecycle (real boot): mount S1 → owners resume once → unload S1
  → agent-loop owner-context teardown disposes the S1-owned agents → remount S1
  → same owners resume exactly once; session identity, schedule rows, and
  model/reasoning pin identical; no schedule occurrence emitted by
  unload/remount.

Requires the installed rc.2 tree (`/opt/dsh/node_modules/.bin/dsh`) and a
reachable anonymous OpenAI-compatible gateway at `127.0.0.1:4000`.

## Known limitations and deferred work

- **Config-list discovery** — schedule ownership is declared by id list rather
  than discovered by log scan; rc.2 has no native schedule-owner index and full
  log scanning was deliberately out of scope for S1. If upstream adds a
  schedule registry/index, this plugin should adopt it and the list becomes a
  hard override.
- **Upstream candidate** — the underlying lifecycle gap (schedule tools only on
  agents created after plugin load) is an upstream `@deepseek-ai/dsh-schedule`
  concern; filed as upstream-candidate in the migration ledger (G).
