/**
 * old-path-driver — S2 OLD-authority guard probe (DISPOSABLE, NON-PRODUCTION).
 *
 * Mounted in the SAME scratch pilot profile as discord-agent-drive (guarded
 * build). Proves the one-DSH-driving-authority property for OLD rollback while
 * the plugin is still mounted:
 *   1) after seed + plugin-ready, resolve the pilot agent and follow up a
 *      NON-SEAM user message (random createUserMessage id, source.kind user) —
 *      the historical session.prompt equivalent;
 *   2) the plugin observer must NOT emit any finalization for that turn (left
 *      to the old-path backstop authority);
 *   3) then follow up a SEAM-STYLE user message (deterministic discord: id via
 *      freezeMessage, exactly like admittedUserMessage) — the plugin MUST emit
 *      exactly one finalization for that turn.
 * Evidence written to guard-evidence.json.
 */
import { randomUUID } from 'node:crypto'
import { appendFileSync, existsSync, writeFileSync } from 'node:fs'
import { createUserMessage, MessageId, freezeMessage } from '@deepseek-ai/dsh-llm'
import net from 'node:net'

export const name = 'old-path-driver'
export const inject = ['agents']

const SID = process.env.SEED_SID || 'session-2f8c1f6a-0000-4000-8000-0000000000a1'
const SOCK = process.env.S2_SOCK || '/tmp/dsh-s2-r4-guard/sock/dsh.sock'
const CONV = process.env.S2_PILOT_CONV || 'channel:111111111111111111'
const EVID = process.env.S2_EVID || '/tmp/dsh-s2-r4-guard/evidence'
const delay = (ms) => new Promise((r) => setTimeout(r, ms))

export function apply(ctx) {
  void (async () => {
    const frames = []
    const evid = (o) => { try { writeFileSync(EVID + '/guard-evidence.json', JSON.stringify(o, null, 2) + '\n') } catch {} }
    const log = (s) => { try { appendFileSync(EVID + '/guard.log', `${new Date().toISOString()} ${s}\n`) } catch {} }
    try {
      // wait for plugin-ready + factory
      let ready = false
      for (let i = 0; i < 60 && !ready; i++) {
        ready = existsSync(EVID + '/plugin-ready.json')
        if (!ready) await delay(1000)
      }
      log('plugin-ready=' + ready)
      // connect a seam observer client (hello only; finalization reader) with
      // connect retries (the socket may lag the plugin-ready marker file)
      const client = net.createConnection(SOCK)
      let buf = ''
      const onData = (chunk) => {
        buf += chunk.toString('utf8')
        let idx
        while ((idx = buf.indexOf('\n')) >= 0) {
          const raw = buf.slice(0, idx); buf = buf.slice(idx + 1)
          if (!raw.trim()) continue
          try {
            const f = JSON.parse(raw)
            if (f.type === 'finalization') frames.push(f)
          } catch {}
        }
      }
      client.on('data', onData)
      let connected = false
      for (let i = 0; i < 30 && !connected; i++) {
        try {
          await new Promise((res, rej) => { client.once('connect', res); client.once('error', rej) })
          connected = true
        } catch (e) {
          log('connect attempt ' + i + ' ' + String(e).slice(0, 80))
          await delay(1000)
        }
      }
      if (!connected) throw new Error('seam connect never succeeded')
      client.write(JSON.stringify({ type: 'hello', deliveredFinalizations: [] }) + '\n')
      await delay(800)
      log('seam observer connected')
      // Resolve the pilot agent: borrow if the plugin's hello-reconcile already
      // materialized it (ctx.agents.get), else resume (probe-only). NO create.
      let handle = null
      let borrowed = false
      for (let i = 0; i < 15 && !handle; i++) {
        const live = ctx.agents.get(SID)
        if (live) { handle = { agent: live }; borrowed = true; break }
        await delay(500)
      }
      if (handle) log('borrowed live agent')
      for (let i = 0; i < 30 && !handle; i++) {
        try {
          handle = await ctx.agents.resume({
            resumeSessionId: SID,
            agentOptions: { provider: process.env.SEED_PROVIDER || 'scratch-local', model: process.env.SEED_MODEL || 'switchyard/deepseek/deepseek-v4-flash-nt' },
          })
        } catch (e) { log('resume attempt ' + i + ' ' + String(e).slice(0, 120)); await delay(2000) }
      }
      if (!handle) throw new Error('no agent handle')
      log('agent resolved ' + SID + (borrowed ? ' (borrowed)' : ' (owned)'))
      const { agent } = handle
      // --- 1. NON-SEAM user turn (old-path equivalent) ---
      const before = frames.length
      const nonSeam = createUserMessage({ content: [{ type: 'text', text: 'Guard probe A. Reply with exactly: OLD-OK' }], source: { kind: 'user' } })
      log('non-seam id prefix: ' + String(nonSeam.id).slice(0, 24))
      agent.followup(nonSeam)
      await Promise.race([agent.whenIdle(), delay(120000)])
      await delay(6000)  // enough time for a (forbidden) finalization to appear
      const afterNonSeam = frames.length
      log(`frames after non-seam: ${afterNonSeam - before} (expected 0)`)
      // --- 2. SEAM-STYLE user turn (deterministic id) ---
      const b2 = frames.length
      const seamMsg = freezeMessage({
        id: MessageId(`discord:${CONV}:9001`),
        role: 'user',
        content: [{ type: 'text', text: 'Guard probe B. Reply with exactly: SEAM-OK' }],
        source: { kind: 'user', clientTimeZone: 'Asia/Bangkok' },
      })
      agent.followup(seamMsg)
      await Promise.race([agent.whenIdle(), delay(120000)])
      await delay(6000)
      const afterSeam = frames.length
      log(`frames after seam-style: ${afterSeam - b2} (expected >=1)`)
      const seamKinds = frames.slice(b2).map((f) => f.kind)
      evid({
        ok: (afterNonSeam - before) === 0 && (afterSeam - b2) >= 1,
        nonSeamFrames: afterNonSeam - before,
        seamFrames: afterSeam - b2,
        seamKinds,
        frames: frames.map((f) => f.finalizationId),
      })
      log('guard evidence written')
      client.end()
      setTimeout(() => process.exit(0), 500)
    } catch (e) {
      log('guard FAIL ' + String(e))
      evid({ ok: false, error: String(e) })
      setTimeout(() => process.exit(1), 500)
    }
  })()
  return () => {}
}
