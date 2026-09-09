/**
 * seed-pilot-session — DISPOSABLE isolated seed. Creates the exact pinned pilot
 * session in a scratch DSH_HOME (simulating the pre-existing persistent session
 * that in production already exists from RPC/UI history) with a pinned model,
 * runs one plain turn, and exits so a later boot can ctx.agents.resume it.
 * NON-PRODUCTION; never part of the shipped plugin tree.
 */
import { randomUUID } from 'node:crypto'
import { appendFileSync, writeFileSync } from 'node:fs'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'

export const name = 'seed-pilot-session'
export const inject = ['agents']

const SID = process.env.SEED_SID || 'session-2f8c1f6a-0000-4000-8000-0000000000a1'
const PROVIDER = process.env.SEED_PROVIDER || 'scratch-local'
const MODEL = process.env.SEED_MODEL || 'switchyard/deepseek/deepseek-v4-flash-nt'
const CWD = process.env.SEED_WORKSPACE || '/tmp/dsh-s2-seed/home/workspace'
const EVID = process.env.SEED_EVID || '/tmp/dsh-s2-seed/evidence'
const MARKER = process.env.SEED_MARKER || '/tmp/dsh-s2-seed/sid.txt'
const delay = (ms) => new Promise((r) => setTimeout(r, ms))

export function apply(ctx) {
  void (async () => {
    try {
      const delayMs = (ms) => new Promise((r) => setTimeout(r, ms))
      // Agent-loop's factory registers during plugin activation; retry create
      // (bounded) rather than assuming readiness from a vacuous probe.
      let handle = null
      writeFileSync(MARKER, SessionId(SID) + '\n')
      for (let attempt = 1; attempt <= 25; attempt++) {
        try {
          handle = await ctx.agents.create({
            sessionId: SessionId(SID),
            meta: { cwd: CWD },
            agentOptions: { provider: PROVIDER, model: MODEL, maxTokens: 1024 },
          })
          break
        } catch (e) {
          appendFileSync(EVID + '/seed.log', `${new Date().toISOString()} create attempt ${attempt} failed: ${String(e)}\n`)
          await delayMs(2000)
        }
      }
      if (!handle) throw new Error('seed create never succeeded (factory unavailable)')
      appendFileSync(EVID + '/seed.log', `${new Date().toISOString()} created ${handle.agent.session.id}\n`)
      const msg = createUserMessage({ content: [{ type: 'text', text: 'Reply with exactly: SEED-OK' }], source: { kind: 'user' } })
      handle.agent.followup(msg)
      await Promise.race([handle.agent.whenIdle(), delay(120000)])
      await delay(1500)
      appendFileSync(EVID + '/seed.log', `${new Date().toISOString()} seed COMPLETE\n`)
      writeFileSync(EVID + '/seed-complete.json', JSON.stringify({ ok: true, sid: String(handle.agent.session.id) }))
      setTimeout(() => process.exit(0), 800)
    } catch (e) {
      appendFileSync(EVID + '/seed.log', `${new Date().toISOString()} seed FAIL ${String(e)}\n`)
      setTimeout(() => process.exit(1), 500)
    }
  })()
  return () => {}
}
