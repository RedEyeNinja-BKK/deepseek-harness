/**
 * readiness-matrix — TEST-ONLY unit matrix for schedule-boot-rearm's STRICTLY
 * FAIL-CLOSED schedule-readiness gate. NEVER mounted in production.
 *
 * Drives the REAL plugin module (apply(ctx, config) with a minimal fake ctx)
 * and proves, against the plugin's own waitForScheduleActive()/boot() code:
 *   1. schedule entry ACTIVE       -> resumes configured owners
 *   2. entry present but inactive  -> NO resume (after the readiness deadline)
 *   3. entry ABSENT                -> NO resume
 *   4. loader UNOBSERVABLE         -> NO resume
 *   5. loader entries() THROWS     -> NO resume
 * (and already-live owners are skipped — no duplicate resume)
 *
 * Every case uses its own throwaway $DSH_HOME so plugin evidence files never
 * collide. Run: node <dir>/readiness-matrix.mjs <base-home>
 */
import { mkdirSync, readFileSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import * as sbr from './schedule-boot-rearm.mjs'

const BASE = process.argv[2] || '/tmp/dsh-sbr-readiness'
const A = 'session-11111111-1111-4111-8111-111111111111'
const C = 'session-22222222-2222-4222-8222-222222222222'
const SID_RE = /^session-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

let PASS = 0
let FAIL = 0
const ok = (c, n) => { console.log(`  ${c ? 'PASS' : 'FAIL'}  ${n}`); c ? PASS++ : FAIL++ }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/** Minimal fake agents service: records resumes; returns live roots for those. */
function fakeAgents() {
  const records = []
  return {
    records,
    get() { return undefined },
    roots() { return records.map((sid) => ({ session: { id: sid } })) },
    async resume(opts) {
      records.push(opts.resumeSessionId)
      return { agent: { session: { id: opts.resumeSessionId, events: [] } } }
    },
  }
}

function loaderWithEntries(entriesFn) {
  return {
    entries: entriesFn,
    await: async () => {},
  }
}

function makeCtx(loader, agents) {
  const svcs = {}
  if (loader !== undefined) svcs.loader = loader
  if (agents) svcs.agents = agents
  return {
    get(name) { return svcs[name] },
    logger: { info() {}, warn() {}, error() {}, debug() {} },
  }
}

function makeHome(name) {
  const dir = join(BASE, name)
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(join(dir, 'plugins'), { recursive: true })
  return dir
}

/** Run one case and wait for the terminal marker (or timeout). Returns {logText, records}. */
async function runCase(name, loader, cfgIds, { expectedResumes, marker, timeoutMs }) {
  const home = makeHome(name)
  const oldHome = process.env.DSH_HOME
  process.env.DSH_HOME = home
  const agents = fakeAgents()
  const ctx = makeCtx(loader, agents)
  let disposer
  try {
    disposer = sbr.apply(ctx, { scheduleSessionIds: cfgIds })
  } catch (error) {
    console.log(`  FAIL  ${name}: apply() threw ${String(error)}`)
    FAIL++
    process.env.DSH_HOME = oldHome
    return
  }
  const logPath = join(home, 'plugins', 'schedule-boot-rearm.log')
  const deadline = Date.now() + (timeoutMs || 15000)
  let text = ''
  for (;;) {
    try { text = readFileSync(logPath, 'utf8') } catch { text = '' }
    if (marker.some((m) => text.includes(m))) break
    if (Date.now() >= deadline) {
      console.log(`  FAIL  ${name}: TIMEOUT waiting for marker (${marker.join('|')}); log:\n${text.split('\n').slice(-12).join('\n')}`)
      FAIL++
      try { disposer?.() } catch {}
      process.env.DSH_HOME = oldHome
      return
    }
    await sleep(250)
  }
  // For the ACTIVE case the terminal marker is the exact success summary; for
  // fail-closed cases it is the fail-closed log line (boot aborts before done).
  const actual = agents.records.length
  ok(actual === expectedResumes, `${name}: resumed=${actual} (expected ${expectedResumes})`)
  const resumeCount = (text.match(/resume ok:/g) || []).length
  ok(resumeCount === expectedResumes, `${name}: evidence 'resume ok' count = ${resumeCount} (expected ${expectedResumes})`)
  if (expectedResumes === 0) {
    ok(!text.includes('boot: done -> resumed='), `${name}: no 'boot: done' summary (aborted fail-closed)`)
  } else {
    ok(text.includes('boot: done -> resumed=2 alreadyLive=0 missing=0 failed=0 invalid=0'), `${name}: exact success summary present`)
  }
  try { disposer?.() } catch {}
  process.env.DSH_HOME = oldHome
}

async function main() {
  rmSync(BASE, { recursive: true, force: true })
  mkdirSync(BASE, { recursive: true })
  console.log('=== schedule-boot-rearm readiness matrix (fail-closed) ===')

  // 1. ACTIVE -> resume occurs
  await runCase('case1-active', loaderWithEntries(() => [
    { options: { id: 'schedule', name: '@deepseek-ai/dsh-schedule' }, fiber: {} },
  ]), [A, C], { expectedResumes: 2, marker: ['boot: done -> resumed=2'], timeoutMs: 20000 })

  // 2. PRESENT-BUT-INACTIVE -> NO resume (waits readiness deadline)
  await runCase('case2-inactive', loaderWithEntries(() => [
    { options: { id: 'schedule', name: '@deepseek-ai/dsh-schedule' }, fiber: null },
  ]), [A, C], { expectedResumes: 0, marker: ['present but not active before deadline'], timeoutMs: 75000 })

  // 3. ABSENT -> NO resume (immediate fail-closed)
  await runCase('case3-absent', loaderWithEntries(() => [
    { options: { id: 'time-context' }, fiber: {} },
  ]), [A, C], { expectedResumes: 0, marker: ['absent from loader entries'], timeoutMs: 15000 })

  // 4. UNOBSERVABLE (no loader service) -> NO resume (immediate fail-closed)
  await runCase('case4-unobservable', undefined, [A, C], { expectedResumes: 0, marker: ['loader/entries unavailable'], timeoutMs: 15000 })

  // 5. loader entries() THROWS -> NO resume
  await runCase('case5-throw', loaderWithEntries(() => { throw new Error('readiness boom') }), [A, C], { expectedResumes: 0, marker: ['entries() threw'], timeoutMs: 15000 })

  // 6. already-live owner skipped (active loader; owner reported live) — no duplicate
  const home = makeHome('case6-already-live')
  const oldHome = process.env.DSH_HOME
  process.env.DSH_HOME = home
  const agentsLive = { records: [], get() { return { session: { id: A } } }, roots() { return [{ session: { id: A } }] }, async resume(o) { this.records.push(o.resumeSessionId); return { agent: { session: { id: o.resumeSessionId, events: [] } } } } }
  const loaderLive = loaderWithEntries(() => [{ options: { id: 'schedule' }, fiber: {} }])
  const ctxLive = makeCtx(loaderLive, agentsLive)
  const logLive = join(home, 'plugins', 'schedule-boot-rearm.log')
  let dispLive
  try { dispLive = sbr.apply(ctxLive, { scheduleSessionIds: [A, C] }) } catch (e) { console.log(`  FAIL  case6: apply threw ${String(e)}`); FAIL++ }
  const dL = Date.now() + 20000
  let tL = ''
  for (;;) {
    try { tL = readFileSync(logLive, 'utf8') } catch { tL = '' }
    if (tL.includes('boot: done -> resumed=1 alreadyLive=1')) break
    if (Date.now() >= dL) { console.log('  FAIL  case6: timeout; log tail:\n' + tL.split('\n').slice(-10).join('\n')); FAIL++; break }
    await sleep(250)
  }
  ok(agentsLive.records.length === 1 && agentsLive.records[0] === C, 'case6-already-live: only cold C resumed; live A skipped (no duplicate)')
  ok(tL.includes('resume skip: ' + A + ' already live'), "case6-already-live: evidence 'already live' skip")
  try { dispLive?.() } catch {}
  process.env.DSH_HOME = oldHome

  console.log(`=== readiness matrix RESULT: PASS=${PASS} FAIL=${FAIL} ===`)
  process.exit(FAIL === 0 ? 0 : 1)
}

main().catch((error) => { console.error('matrix fatal:', error); process.exit(1) })
