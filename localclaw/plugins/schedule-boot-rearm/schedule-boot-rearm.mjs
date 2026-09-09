/**
 * dsh-schedule-boot-rearm — DSH-native boot re-arm for persisted schedule owners.
 *
 * Purpose (S-01 replacement, DSH native-plugin convergence slice 1)
 * -------------------------------------------------------------------
 * rc.2's `@deepseek-ai/dsh-schedule` installs its session-scoped reminder tools
 * only on root Agents created AFTER the schedule plugin loads (upstream
 * load-order boundary, README + source). After a DSH restart every previously
 * live schedule owner is cold, so it must be re-materialized through the
 * native AgentRegistry factory in order for the schedule plugin's
 * `agent/created` listener to re-attach tools/runtime and re-arm the durable
 * `schedule/change` records already owned by that persisted session.
 *
 * This plugin performs exactly that lifecycle invocation at boot:
 *
 *   configured schedule-owner ids
 *     -> validated against the native persistence index
 *     -> ctx.agents.resume({ resumeSessionId, agentOptions, setup })
 *     -> agent/created fires after @deepseek-ai/dsh-schedule is loaded
 *     -> schedule tools/runtime re-install; persisted schedules re-arm in-DSH
 *
 * It deliberately is NOT a second scheduler, NOT a cron replacement, NOT a
 * schedule database, NOT gold-specific, and NOT a browser-RPC client: it
 * invokes the native lifecycle primitive and gets out of the way.
 *
 * Model-pin parity
 * ----------------
 * The host's own materialization path (dsh-host-apiproxy `ensureSession`)
 * resumes a persisted session with the deployment-default model plus a setup
 * that installs the session-local model selection (which reads the session's
 * own last logged request/header config — e.g. a per-session model pin such as
 * Terra). This plugin mirrors that exact shape so a resumed owner keeps its
 * recorded model/effort instead of silently falling back to the default.
 * Registration is done through `installModelSelection` exported by
 * `@deepseek-ai/dsh-agent` (the same public helper the api-proxy uses).
 *
 * Idempotence
 * -----------
 * Repeated plugin starts / DSH restarts are harmless: schedule records live in
 * the durable session log (never re-created by a resume), at most one live
 * agent exists per session id in a process, and already-live ids are skipped.
 *
 * Failure behavior
 * ----------------
 * Per-id, fail-narrow: a missing/invalid/rollback-failed id is logged and
 * skipped; its persisted state is never deleted or replaced; unrelated
 * sessions are never touched; a single failure never blocks DSH startup.
 *
 * Config (patch row, supported home cordis.patch.yml layer):
 *   scheduleSessionIds: [ "session-…", "session-…" ]
 * The list is the deployment declaration of which persisted sessions own
 * schedules (mirrors the retired DSH_SCHEDULE_SESSIONS contract). rc.2 keeps
 * schedule state only inside each session's own durable log — there is no
 * native central "which sessions own schedules" index — so the id list is the
 * smallest honest discovery source; ids are validated against the native
 * persistence index before any resume.
 */
import { installModelSelection } from '@deepseek-ai/dsh-agent'
import Schema from '@deepseek-ai/schemastery'
import { appendFileSync } from 'node:fs'
import { join } from 'node:path'

export const name = 'schedule-boot-rearm'

/** Services that must exist before this plugin activates (same class as @deepseek-ai/dsh-schedule). */
export const inject = ['agents', 'sessionPersistence']

/** Validated entry config: the deployment-declared schedule-owner session ids. */
export const Config = Schema.object({
  scheduleSessionIds: Schema.array(String).default([]),
})

const SID_RE = /^session-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
/** Small settle after the loader tree is idle so the schedule plugin is provably listening. */
const SETTLE_MS = 1500
/** How long we wait for the loader tree to reach idle before proceeding anyway. */
const LOADER_WAIT_MS = 60000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** Durable boot evidence file, next to the plugin ($DSH_HOME/plugins). */
function evidenceLogPath() {
  try {
    const home = process.env.DSH_HOME
    if (!home) return undefined
    const dir = join(home, 'plugins')
    const path = join(dir, 'schedule-boot-rearm.log')
    return { dir, path }
  } catch {
    return undefined
  }
}

/** Read the deployment default agent model ({provider, model}) when available. */
function defaultAgentOptions(ctx) {
  try {
    const svc = ctx.get('agentDefaultModel')
    if (svc && typeof svc.currentSelection === 'function') {
      const current = svc.currentSelection()
      if (current && typeof current.provider === 'string' && current.provider && typeof current.model === 'string' && current.model) {
        return { provider: current.provider, model: current.model }
      }
    }
  } catch {
    /* fall through */
  }
  return undefined
}

/**
 * Mirror dsh-host-apiproxy `selectionFor` + `installSelection`: a mutable
 * selection whose `current` reads the resumed session's own last logged
 * request/header config first, else the deployment default. Never mutates.
 */
function installSessionSelection(agentCtx, ctx) {
  const fallback = () => {
    try {
      const svc = ctx.get('agentDefaultModel')
      if (svc && typeof svc.currentSelection === 'function') return svc.currentSelection()
    } catch {
      /* fall through */
    }
    return undefined
  }
  const loggedConfig = () => {
    try {
      const header = agentCtx.agent?.session?.requestHeader?.()
      const config = header?.config
      if (config && typeof config.provider === 'string' && config.provider && typeof config.model === 'string' && config.model) {
        return {
          provider: config.provider,
          model: config.model,
          ...(config.reasoningEffort === undefined ? {} : { reasoningEffort: config.reasoningEffort }),
        }
      }
    } catch {
      /* fall through */
    }
    return undefined
  }
  const selection = {
    get current() {
      return loggedConfig() ?? fallback()
    },
    set current(_next) {
      /* read-only boot selection: never switch a resumed owner's model */
    },
    assembled: undefined,
  }
  return installModelSelection(agentCtx, selection)
}

/** Count durable schedule/change records in a live session's event log (read-only). */
function scheduleRecordCount(agent) {
  try {
    const events = agent?.session?.events
    if (!Array.isArray(events)) return -1
    return events.filter((event) => event && event.type === 'schedule/change').length
  } catch {
    return -1
  }
}

export function apply(ctx, entryConfig) {
  const config = entryConfig ?? {}
  const rawIds = Array.isArray(config.scheduleSessionIds) ? config.scheduleSessionIds : []
  const logger = ctx.logger
  const evidence = evidenceLogPath()
  const log = (message, level = 'info') => {
    try {
      const fn = logger?.[level]
      if (typeof fn === 'function') fn(`[schedule-boot-rearm] ${message}`)
    } catch {
      /* logging must never break boot */
    }
    if (evidence) {
      try {
        appendFileSync(evidence.path, `${new Date().toISOString()} ${level.toUpperCase()} ${message}\n`)
      } catch {
        /* best-effort evidence file */
      }
    }
  }

  let stopping = false
  const started = new Set() // ids this plugin instance already handled this process

  async function waitForLoaderIdle() {
    const loader = ctx.get('loader')
    if (!loader || typeof loader.await !== 'function') return false
    try {
      await Promise.race([loader.await(), sleep(LOADER_WAIT_MS)])
      return true
    } catch (error) {
      log(`loader.await() reported a settled failure: ${String(error)}`, 'warn')
      return true
    }
  }

  async function persistedIndex() {
    const persistence = ctx.get('sessionPersistence')
    if (!persistence || typeof persistence.list !== 'function') return undefined
    try {
      const rows = await persistence.list()
      return new Map((Array.isArray(rows) ? rows : []).filter((row) => row && row.id).map((row) => [String(row.id), row]))
    } catch (error) {
      log(`persistence.list() failed; skipping persisted-index validation: ${String(error)}`, 'warn')
      return undefined
    }
  }

  async function boot() {
    try {
      const valid = rawIds.filter((id) => SID_RE.test(String(id).trim()))
      const invalid = rawIds.length - valid.length
      if (invalid > 0) log(`config: ${invalid} invalid id(s) rejected (must be session-UUID)`)
      log(`boot: start; configured schedule owner(s) = ${valid.length}${valid.length ? '' : ' (none)'}`, 'info')

      // Let the full entry tree (schedule plugin included) reach idle before
      // any resume so agent/created is guaranteed to be observed by schedule.
      await waitForLoaderIdle()
      await sleep(SETTLE_MS)

      const defaults = defaultAgentOptions(ctx)
      if (defaults) {
        log(`boot: resume default model selection = ${defaults.provider}/${defaults.model}`)
      } else {
        log('boot: ctx.agentDefaultModel unavailable; resumes omit agentOptions (installSessionSelection still restores logged pins)', 'warn')
      }

      const index = await persistedIndex()
      const stats = { resumed: 0, alreadyLive: 0, missing: 0, failed: 0, invalid }

      for (const raw of valid) {
        if (stopping) return
        const sid = raw.trim()
        if (started.has(sid)) continue
        started.add(sid)

        if (ctx.agents.get(sid)) {
          stats.alreadyLive += 1
          log(`resume skip: ${sid} already live (no duplicate created)`)
          continue
        }

        if (index && !index.has(sid)) {
          stats.missing += 1
          log(`resume skip: ${sid} not present in the native persistence index (missing/malformed target; state untouched)`, 'error')
          continue
        }

        try {
          const handle = await ctx.agents.resume({
            resumeSessionId: sid,
            agentOptions: defaultAgentOptions(ctx),
            setup: (agentCtx) => {
              // Mirror the host path: install the session-local model selection
              // and return nothing (a setup may only return a commit object or
              // void — never a disposer).
              installSessionSelection(agentCtx, ctx)
            },
          })
          if (stopping) {
            log(`resume note: ${sid} resumed while stopping; leaving live agent intact`, 'warn')
          }
          const records = scheduleRecordCount(handle.agent)
          stats.resumed += 1
          log(`resume ok: ${sid} (durable schedule/change records = ${records >= 0 ? records : 'n/a'}; live roots = ${ctx.agents.roots().length})`)
        } catch (error) {
          stats.failed += 1
          log(`resume FAILED: ${sid} -> ${String(error)} (persisted state untouched; no replacement schedule created; continuing with other owners)`, 'error')
        }
      }

      log(`boot: done -> resumed=${stats.resumed} alreadyLive=${stats.alreadyLive} missing=${stats.missing} failed=${stats.failed} invalid=${stats.invalid} (live roots=${ctx.agents.roots().length})`, stats.failed > 0 ? 'warn' : 'info')
    } catch (error) {
      log(`boot: FATAL ${String(error)} (no schedule owner resumed; external oneshot remains the rollback path)`, 'error')
    }
  }

  void boot()

  return () => {
    stopping = true
    log('unloaded (owned agents stay live; a later instance resumes only cold ids)')
  }
}
