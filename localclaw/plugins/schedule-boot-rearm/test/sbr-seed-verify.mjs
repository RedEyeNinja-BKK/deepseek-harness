/**
 * sbr-seed-verify — TEST-ONLY out-of-tree plugin for the schedule-boot-rearm
 * overlay battery. NEVER mounted in production.
 *
 * Modes (SBR_MODE):
 *   seed   — create SBR_SESSION_A (drive one schedule_create turn) and
 *            SBR_SESSION_C (drive one plain turn, no schedule), leave both
 *            live, write COMPLETE-SEED.json, then exit the process (simulated
 *            DSH stop; persistence is durable per event).
 *   verify — wait until the mounted schedule-boot-rearm plugin has resumed A
 *            and C (ctx.agents.get), run one schedule_list turn on A, then
 *            write COMPLETE-VERIFY-<SBR_TAG>.json with route/count evidence
 *            and exit the process.
 *
 * Model route: every seeded session is created with SBR_PROVIDER/SBR_MODEL
 * (the "persisted pin"). The battery changes the deployment default to a
 * different model between seed and verify so a lost pin is observable.
 */
import { randomUUID } from 'node:crypto'
import { writeFileSync, mkdirSync } from 'node:fs'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'

export const name = 'sbr-seed-verify'
export const inject = ['agents']

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const withTimeout = (promise, ms, what) => Promise.race([
  promise,
  delay(ms).then(() => { throw new Error(`TIMEOUT ${what}`) }),
])

function scheduleChangeCount(session) {
  try {
    const events = session?.events
    if (!Array.isArray(events)) return -1
    return events.filter((e) => e && e.type === 'schedule/change').length
  } catch { return -1 }
}

function lastRequestHeaderConfig(session) {
  try {
    const events = session?.events
    if (!Array.isArray(events)) return null
    for (let i = events.length - 1; i >= 0; i--) {
      const event = events[i]
      if (event && event.type === 'request/header' && event.data?.header?.config) return event.data.header.config
    }
    return null
  } catch { return null }
}

function lastAssistantText(session) {
  try {
    const events = session?.events
    if (!Array.isArray(events)) return null
    for (let i = events.length - 1; i >= 0; i--) {
      const event = events[i]
      if (event && event.type === 'assistant/message' && event.data?.message?.content) {
        const text = event.data.message.content.filter((b) => b && b.type === 'text').map((b) => b.text).join('\n')
        if (text) return text
      }
    }
    return null
  } catch { return null }
}

function sawToolCall(session, toolName) {
  try {
    const events = session?.events
    if (!Array.isArray(events)) return false
    const serialized = JSON.stringify(events)
    return serialized.includes(`"${toolName}"`)
  } catch { return false }
}

function delayExit(code) {
  setTimeout(() => process.exit(code), 1500)
}

/** Retry agent creation until the agent-loop factory registers (boot race). */
async function createAgentWithFactoryRetry(ctx, options, what) {
  let lastError = null
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      return await withTimeout(ctx.agents.create(options), 120000, what)
    } catch (error) {
      lastError = error
      if (String(error).includes('no agent factory registered')) {
        await delay(1000)
        continue
      }
      throw error
    }
  }
  throw lastError || new Error(`factory never registered for ${what}`)
}

export function apply(ctx) {
  const evid = process.env.SBR_EVID || '/tmp/dsh-sbr-battery/evidence'
  const mode = process.env.SBR_MODE || 'seed'
  const tag = process.env.SBR_TAG || 'p'
  const sidA = process.env.SBR_SESSION_A || ''
  const sidC = process.env.SBR_SESSION_C || ''
  const provider = process.env.SBR_PROVIDER || 'scratch-local'
  const model = process.env.SBR_MODEL || 'switchyard/deepseek/deepseek-v4-flash-nt'
  mkdirSync(evid, { recursive: true })
  const log = (msg) => { try { ctx.logger.info(`[sbr-${mode}] ${msg}`) } catch {} }
  const marker = (name, payload) => writeFileSync(`${evid}/${name}`, JSON.stringify(payload, null, 2) + '\n')

  void (async () => {
    try {
      // Wait until the agent factory is available.
      let ready = false
      for (let i = 0; i < 90; i++) {
        try { ready = typeof ctx.agents?.roots === 'function' } catch { ready = false }
        if (ready) break
        await delay(1000)
      }
      if (!ready) throw new Error('agents factory never became ready')
      log('factory ready; roots=' + ctx.agents.roots().length)

      if (mode === 'seed') {
        // A: schedule-bearing owner (the persisted pin is provider/model).
        const a = SessionId(sidA)
        const handleA = await createAgentWithFactoryRetry(ctx, {
          sessionId: a,
          meta: { cwd: process.env.SBR_WS || '/tmp/dsh-sbr-battery/home/workspace', origin: undefined },
          agentOptions: { provider, model, maxTokens: 512 },
        }, 'create A')
        log('created A ' + sidA)
        handleA.agent.followup(createUserMessage({
          content: [{ type: 'text', text: 'Call schedule_create with after_seconds 7200 and prompt "SBR battery schedule proof". Then reply with exactly the word: SCHEDULED' }],
          source: { kind: 'user' },
        }))
        await withTimeout(handleA.agent.whenIdle(), 150000, 'seed A idle')
        await delay(2500)
        const aBefore = scheduleChangeCount(handleA.agent.session)
        const aRoute = lastRequestHeaderConfig(handleA.agent.session)
        const aTail = lastAssistantText(handleA.agent.session)

        // C: control owner (persisted, resumed later, but never schedule-bearing).
        const c = SessionId(sidC)
        const handleC = await createAgentWithFactoryRetry(ctx, {
          sessionId: c,
          meta: { cwd: process.env.SBR_WS || '/tmp/dsh-sbr-battery/home/workspace', origin: undefined },
          agentOptions: { provider, model, maxTokens: 512 },
        }, 'create C')
        log('created C ' + sidC)
        handleC.agent.followup(createUserMessage({
          content: [{ type: 'text', text: 'Reply with exactly the word: CONTROL-OK' }],
          source: { kind: 'user' },
        }))
        await withTimeout(handleC.agent.whenIdle(), 120000, 'seed C idle')
        await delay(2000)

        marker('COMPLETE-SEED.json', {
          a: { sid: sidA, scheduleChange: aBefore, route: aRoute, assistantTail: aTail },
          c: { sid: sidC, scheduleChange: scheduleChangeCount(handleC.agent.session) },
          roots: ctx.agents.roots().map((agent) => String(agent.session.id)),
        })
        log('SEED COMPLETE; exiting process')
        delayExit(0)
      } else {
        // verify: wait for boot-rearm to resume A and C, then prove state.
        let liveA = ctx.agents.get(sidA)
        let liveC = ctx.agents.get(sidC)
        for (let i = 0; i < 120 && (!liveA || !liveC); i++) {
          await delay(1000)
          liveA = ctx.agents.get(sidA)
          liveC = ctx.agents.get(sidC)
        }
        if (!liveA || !liveC) {
          marker('COMPLETE-VERIFY-' + tag + '.json', {
            fatal: 'boot-rearm did not make A and C live',
            liveA: !!liveA, liveC: !!liveC,
            roots: ctx.agents.roots().map((agent) => String(agent.session.id)),
          })
          log('VERIFY FATAL: not live A=' + !!liveA + ' C=' + !!liveC)
          delayExit(2)
          return
        }
        log('A and C live after boot-rearm')

        const before = scheduleChangeCount(liveA.session)
        liveA.followup(createUserMessage({
          content: [{ type: 'text', text: 'Call schedule_list and report how many active schedules exist. Then reply with exactly the word: LIST-OK' }],
          source: { kind: 'user' },
        }))
        await withTimeout(liveA.whenIdle(), 150000, 'verify A idle')
        await delay(2500)

        const after = scheduleChangeCount(liveA.session)
        const route = lastRequestHeaderConfig(liveA.session)
        marker('COMPLETE-VERIFY-' + tag + '.json', {
          a: {
            sid: sidA,
            live: true,
            scheduleChangeBefore: before,
            scheduleChangeAfter: after,
            route,
            sawScheduleList: sawToolCall(liveA.session, 'schedule_list'),
            assistantTail: lastAssistantText(liveA.session),
          },
          c: { sid: sidC, live: true, scheduleChange: scheduleChangeCount(liveC.session) },
          roots: ctx.agents.roots().map((agent) => String(agent.session.id)),
        })
        log('VERIFY ' + tag + ' COMPLETE; exiting process')
        delayExit(0)
      }
    } catch (error) {
      try { ctx.logger.error(`[sbr-${mode}] FATAL: ${String(error)}`) } catch {}
      marker('COMPLETE-' + (mode === 'seed' ? 'SEED' : 'VERIFY-' + tag) + '.json', { fatal: String(error) })
      delayExit(1)
    }
  })()

  return () => { log('unloaded') }
}
