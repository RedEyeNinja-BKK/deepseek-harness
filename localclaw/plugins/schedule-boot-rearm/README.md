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
2. Waits until the loader tree is idle (schedule plugin is listening), then for
   each id:
   - skips if the session is already live in this process (no duplicate);
   - validates the id exists in the native persistence index
     (`ctx.sessionPersistence.list()`), otherwise logs and skips (fail-narrow);
   - `ctx.agents.resume({ resumeSessionId, agentOptions, setup })`.
3. The resume's `setup` installs the same agent-scoped model selection the host
   materialization path installs (`installModelSelection` from
   `@deepseek-ai/dsh-agent`, reading the session's own last logged
   request/header config first) — so a resumed owner keeps its recorded model
   pin/effort (e.g. Terra) instead of silently falling back to the default.
4. Logs one journal line per owner and a summary; owns nothing else.

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
- Plugin unload leaves resumed agents live (a later instance only re-resumes
  cold ids), so a hot reload cannot double-materialize or strand schedules.

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
gateway, simulates two DSH restarts with this plugin mounted, and asserts the
GO §8 pre-production proofs (mount, resume, schedule re-arm, pin parity,
idempotence, missing-target fail-narrow, no `session.*` RPC in source).
Requires the installed rc.2 tree (`/opt/dsh/node_modules/.bin/dsh`) and a
reachable anonymous OpenAI-compatible gateway at `127.0.0.1:4000`.

## Readiness gate

Before any resume the plugin waits for the `@deepseek-ai/dsh-schedule` loader
entry to become **active** (`ctx.loader.entries()` entry with an installed
fiber), because schedule tools attach only to root agents created after the
schedule plugin's `agent/created` listener is installed. If the entry is present
but never activates within 60s the plugin fails closed (skips re-arm this
start; the external oneshot remains the rollback path). If the loader
entry-state API is unavailable or the entry is absent, it falls back to a short
settle and proceeds with a warning.

## Known limitations and deferred work

- **Config-list discovery** — schedule ownership is declared by id list rather
  than discovered by log scan; rc.2 has no native schedule-owner index and full
  log scanning was deliberately out of scope for S1. If upstream adds a
  schedule registry/index, this plugin should adopt it and the list becomes a
  hard override.
- **Upstream candidate** — the underlying lifecycle gap (schedule tools only on
  agents created after plugin load) is an upstream `@deepseek-ai/dsh-schedule`
  concern; filed as upstream-candidate in the migration ledger (G).
