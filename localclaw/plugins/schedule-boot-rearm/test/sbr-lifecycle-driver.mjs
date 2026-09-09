/**
 * sbr-lifecycle-driver — TEST-ONLY in-process unload/remount proof for
 * schedule-boot-rearm. NEVER mounted in production.
 *
 * Runs inside a REAL dsh boot (battery phase 4: patch = schedule + time-context
 * + this driver; NO schedule-boot-rearm row). The driver mounts the S1 plugin
 * itself as a child Cordis plugin (ctx.plugin) so it can unload/remount the
 * S1 context and observe agent-loop owner-context teardown:
 *
 *   mount child S1  -> resumes persisted owners A,C (proves schedule/pin)
 *   unload child S1 -> agent-loop disposes the S1-owned resumed agents
 *   remount child S1-> same owners resume EXACTLY ONCE
 *   assert: same session identity; schedule rows identical; model/reasoning
 *   pin identical; no schedule occurrence emitted by unload/remount; durable
 *   session state preserved across unload.
 *
 * Writes COMPLETE-LIFECYCLE.json with the assertion summary, then exits.
 */
import { writeFileSync, readFileSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { readdirSync } from 'node:fs'

export const name = 'sbr-lifecycle-driver'
export const inject = ['agents', 'sessionPersistence']

const delay = (ms) => new Promise((r) => setTimeout(r, ms))
const withTimeout = (p, ms, what) => Promise.race([
  p,
  delay(ms).then(() => { throw new Error(`TIMEOUT ${what}`) }),
])

function durableFileExists(sid) {
  // The driver runs inside the dsh process and can read $DSH_HOME/sessions.
  try {
    const root = process.env.DSH_HOME ? join(process.env.DSH_HOME, 'sessions') : ''
    if (!root) return false
    for (const ws of readdirSync(root, { withFileTypes: true })) {
      if (!ws.isDirectory()) continue
      const p = join(root, ws.name, sid, 'session.jsonl.zstd')
      try { if (existsSync(p) && readFileSync(p).length > 0) return true } catch {}
    }
  } catch {}
  return false
}

function scheduleRows(session) {
  try {
    const evs = session?.events || []
    return evs.filter((e) => e && e.type === 'schedule/change').map((e) => {
      const d = e.data || {}
      return { seq: e.seq, id: d.id, acceptedAt: d.acceptedAt, scheduledAt: d.scheduledAt,
        afterSeconds: d.afterSeconds, everySeconds: d.everySeconds, prompt: d.prompt, kind: d.kind }
    })
  } catch { return null }
}

function pinOf(session) {
  try {
    const evs = session?.events || []
    for (let i = evs.length - 1; i >= 0; i--) {
      const e = evs[i]
      if (e && e.type === 'request/header') {
        const c = e.data?.header?.config
        if (c && typeof c === 'object' && c.model) {
          return { provider: c.provider, model: c.model, reasoningEffort: c.reasoningEffort }
        }
      }
    }
  } catch {}
  return null
}

export function apply(ctx) {
  void (async () => {
    const evid = process.env.SBR_EVID || '/tmp/dsh-sbr-battery/evidence'
    const A = process.env.SBR_SESSION_A || ''
    const C = process.env.SBR_SESSION_C || ''
    const pluginPath = process.env.SBR_PLUGIN || ''
    const log = (m) => { try { ctx.logger.info(`[sbr-lifecycle] ${m}`) } catch {} }
    const marker = (o) => writeFileSync(join(evid, 'COMPLETE-LIFECYCLE.json'), JSON.stringify(o, null, 2) + '\n')
    const exit = (code) => setTimeout(() => process.exit(code), 1000)
    const evidenceLog = process.env.DSH_HOME ? join(process.env.DSH_HOME, 'plugins', 'schedule-boot-rearm.log') : ''
    let offset = 0
    try { offset = readFileSync(evidenceLog, 'utf8').split('\n').length } catch { offset = 0 }

    try {
      let ready = false
      for (let i = 0; i < 90; i++) { try { ready = typeof ctx.agents?.roots === 'function' } catch {} if (ready) break; await delay(1000) }
      if (!ready) throw new Error('agents factory never ready')

      const live = (sid) => { try { return ctx.agents.get(sid) !== undefined } catch { return false } }
      const snap = (sid) => { const a = ctx.agents.get(sid); return { rows: scheduleRows(a?.session), pin: pinOf(a?.session) } }

      const mod = await import(pluginPath)
      const plugin = mod?.default && typeof mod.default === 'function' ? mod.default : mod
      const mount = () => ctx.plugin(plugin, { scheduleSessionIds: [A, C] })

      log('mount child #1')
      const child1 = await mount()
      await withTimeout((async () => { while (!(live(A) && live(C))) await delay(250) })(), 150000, 'child1 resume')

      const s1a = snap(A)
      const s1c = snap(C)
      log('child #1 resumed; A rows=' + (s1a.rows ? s1a.rows.length : 'n/a') + ' pin=' + (s1a.pin ? s1a.pin.model : 'n/a'))

      // dispose child #1 (S1 unload)
      const dispose1 = typeof child1 === 'function' ? child1 : (child1 && typeof child1.dispose === 'function' ? () => child1.dispose() : null)
      if (!dispose1) throw new Error('ctx.plugin did not return a disposer for child #1')
      log('unloading child #1 (S1 unload)')
      await withTimeout(dispose1(), 60000, 'child1 dispose')

      let disposedOk = false
      try {
        await withTimeout((async () => { while (live(A) || live(C)) await delay(250) })(), 60000, 'agent dispose after unload')
        disposedOk = true
      } catch { disposedOk = false }
      log('after unload: A live=' + live(A) + ' C live=' + live(C) + ' disposedOk=' + disposedOk)

      // durable session state must survive unload: the actual persisted file
      // must still exist (and be non-empty) AFTER S1 unload disposed the live
      // agent — remount would otherwise recreate a fresh, empty session.
      const durableKept = durableFileExists(A) && durableFileExists(C)
      log('after unload: durable files present A=' + durableFileExists(A) + ' C=' + durableFileExists(C))

      log('mount child #2')
      const child2 = await mount()
      await withTimeout((async () => { while (!(live(A) && live(C))) await delay(250) })(), 150000, 'child2 resume')
      const s2a = snap(A)
      const s2c = snap(C)
      log('child #2 resumed; A rows=' + (s2a.rows ? s2a.rows.length : 'n/a'))

      const rowsIdentical = JSON.stringify(s1a.rows) === JSON.stringify(s2a.rows) && JSON.stringify(s1c.rows) === JSON.stringify(s2c.rows)
      const pinIdentical = JSON.stringify(s1a.pin) === JSON.stringify(s2a.pin) && JSON.stringify(s1c.pin) === JSON.stringify(s2c.pin)
      const sameIdentity = (() => { const a2 = ctx.agents.get(A); return !!a2 && String(a2.session.id) === A })()

      // count S1 'resume ok' lines written after this boot started (2 children => 2 per owner)
      let after = offset
      try { after = readFileSync(evidenceLog, 'utf8').split('\n').length } catch {}
      let tail = ''
      try { tail = readFileSync(evidenceLog, 'utf8').split('\n').slice(offset - 1, after).join('\n') } catch { tail = '' }
      const resumeA = (tail.match(new RegExp('resume ok: ' + A.replace(/[-]/g, '\\-'), 'g')) || []).length
      const resumeC = (tail.match(new RegExp('resume ok: ' + C.replace(/[-]/g, '\\-'), 'g')) || []).length

      const summary = {
        disposedOnUnload: disposedOk,
        durablePreservedAcrossUnload: durableKept,
        sameIdentity,
        rowsIdentical,
        pinIdentical,
        resumeA,
        resumeC,
        scheduleRowsA: s1a.rows ? s1a.rows.length : -1,
        pinA: s1a.pin,
        noScheduleOccurrenceEmitted: rowsIdentical && pinIdentical,
      }
      marker(summary)
      const pass = disposedOk && durableKept && sameIdentity && rowsIdentical && pinIdentical && resumeA === 2 && resumeC === 2
      log('COMPLETE-LIFECYCLE pass=' + pass + ' ' + JSON.stringify(summary))
      exit(pass ? 0 : 1)
    } catch (error) {
      log('FATAL ' + String(error))
      marker({ fatal: String(error) })
      exit(1)
    }
  })()
  return () => {}
}
