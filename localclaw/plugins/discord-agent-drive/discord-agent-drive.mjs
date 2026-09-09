/**
 * discord-agent-drive — S2 pilot plugin (Discord native Agent-drive +
 * durable-finalization). NON-PRODUCTION staged source under the LocalClaw fork;
 * byte-identical copy is isolated-tested in the S2 pre-live battery and would be
 * the eventual mounted plugin at cutover (separate GO).
 *
 * Owns ONLY the pilot conversation's DSH-side Agent driving and durable
 * finalization/reconciliation seam. All external Discord authority (transport,
 * token, admission/dedupe, conversation→session mapping, attachment ingest,
 * D-01 gate/adapter, send execution, delivered state) stays in the shim.
 *
 * Surfaces used (installed DSH 0.1.1-rc.2 only):
 *   ctx.agents.get / ctx.agents.resume          (NO create on the live pilot path)
 *   ctx.on('session/event')                     (durable event observation)
 *   agent.followup / agent.whenIdle
 *   agent.session.events                        (authoritative in-process log)
 *   installModelSelection                       (session's own model pin restore)
 *   MessageId / freezeMessage / UserMessage     (deterministic stable identity)
 *
 * No browser-facing session.* RPC anywhere (the plugin performs none).
 */
import Schema from '@deepseek-ai/schemastery'
import { installModelSelection } from '@deepseek-ai/dsh-agent'
import { MessageId, freezeMessage } from '@deepseek-ai/dsh-llm'
import { createServer as createNetServer } from 'node:net'
import { appendFileSync, chmodSync, existsSync, mkdirSync, realpathSync, unlinkSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'

export const name = 'discord-agent-drive'
export const inject = ['agents', 'tools']

export const PROTOCOL_VERSION = 1
export const MAX_FRAME_BYTES = 1024 * 1024
export const MAX_PENDING_OUTBOUND = 500
export const DSH_MSG_PREFIX = 'discord:'
export const DEFAULT_TZ = 'Asia/Bangkok'

export const Config = Schema.object({
  pilotConversationKey: Schema.string().default(''),
  pilotSessionId: Schema.string().default(''),
  socketPath: Schema.string().default(''),
  provider: Schema.string().default(''),
  model: Schema.string().default(''),
  reasoningEffort: Schema.string().default(''),
  maxTokens: Schema.number().default(0),
  mediaRoot: Schema.string().default('/mnt/off-vm-nfs/comfyui-media'),
  evidenceDir: Schema.string().default('/tmp/dsh-s2-evidence'),
  stubDiscordTool: Schema.boolean().default(false),
  clientTimeZone: Schema.string().default(DEFAULT_TZ),
})

const delay = (ms) => new Promise((r) => setTimeout(r, ms))
const now = () => new Date().toISOString()

// Deterministic DSH user-message identity per spec §4.
export function dshMessageIdFor(conversationKey, discordMessageId) {
  return MessageId(`${DSH_MSG_PREFIX}${conversationKey}:${discordMessageId}`)
}
// createUserMessage() re-randomizes the id every call; freezeMessage() preserves
// the caller-owned stable identity (proven against installed dsh-llm).
export function admittedUserMessage(conversationKey, discordMessageId, envelopeText, clientTimeZone) {
  return freezeMessage({
    id: dshMessageIdFor(conversationKey, discordMessageId),
    role: 'user',
    content: [{ type: 'text', text: envelopeText }],
    source: { kind: 'user', ...(clientTimeZone ? { clientTimeZone } : {}) },
  })
}

// Discord gate send-contract classifier (mirror of shim _send_outcome_from_result).
export function classifySendOutcome(text, isError) {
  if (isError) return 'failed'
  if (!text) return null
  let obj
  try { obj = JSON.parse(text) } catch { return null }
  if (!obj || typeof obj !== 'object') return null
  const sent = obj.sent
  const msgs = obj.messages
  if (typeof sent === 'number' && Array.isArray(msgs)) {
    if (sent >= 1 && msgs.length === sent && msgs.every((m) => m && typeof m === 'object' && m.status === 200 && typeof m.message_id === 'string' && m.message_id)) return 'ok'
    return 'failed'
  }
  return null
}

// Pure decision matrix over durable turn facts (mirror of shim _flush_turn).
// sendCalls = [{cid, outcome}] outcome 'ok'|'failed'|null; mediaList = artifact
// records. Returns {kind, facts} (kind: noop|text-fallback|artifact|failure-notice).
export function decideFinalization({ text = '', sendCalls = [], sendFilesByCall = {}, sendSeen = false, media = [], end_error = false, turn = null }) {
  const outcomes = sendCalls.map((c) => c.outcome).filter((v) => v !== null)
  const outcome = !outcomes.length ? null : outcomes.includes('failed') ? 'failed' : outcomes.every((v) => v === 'ok') ? 'ok' : null
  const deliveredPaths = new Set()
  for (const c of sendCalls) if (c.outcome === 'ok') for (const p of sendFilesByCall[c.cid] || []) deliveredPaths.add(p)
  let pending = media.filter((m) => !deliveredPaths.has(m.path))
  const base = { turn, abnormal: !!end_error, sendSeen: !!sendSeen, outcome }
  // media auto-delivery only on abnormal/empty endings (production v1.3 rule);
  // a normal conversational reply with media falls through to the text path.
  if (pending.length && !end_error && text) pending = []
  if (pending.length) {
    const textPending = text && (outcome === 'failed' || (outcome === null && sendSeen))
    return { kind: 'artifact', facts: { ...base, artifacts: pending.map((m) => ({ path: m.path, sha256: m.sha256, mime: m.mime, bytes: m.bytes, identity: `${m.sha256}|${m.path}` })), ...(textPending ? { text } : {}) } }
  }
  if (text) {
    if (outcome === 'ok') return { kind: 'noop', facts: { ...base, suppress: 'confirmed-send' } }
    if (outcome === 'failed') return { kind: 'text-fallback', facts: { ...base, text, reason: 'failed-send' } }
    if (sendSeen) return { kind: 'text-fallback', facts: { ...base, text, reason: 'unknown-send' } }
    return { kind: 'text-fallback', facts: { ...base, text, reason: 'no-send' } }
  }
  if (outcome === 'failed') return { kind: 'failure-notice', facts: base }
  return { kind: 'noop', facts: { ...base, note: outcome === 'ok' ? 'terminal-empty' : 'nothing-honest' } }
}

export function apply(ctx, entryConfig) {
  const cfg = entryConfig || {}
  const pilotConv = cfg.pilotConversationKey
  const pilotSid = cfg.pilotSessionId
  const sockPath = cfg.socketPath
  const EVID = cfg.evidenceDir || '/tmp/dsh-s2-evidence'
  const mediaRoot = cfg.mediaRoot || ''
  try { mkdirSync(EVID, { recursive: true, mode: 0o750 }) } catch {}
  const evlog = (s) => { try { appendFileSync(join(EVID, 'plugin.log'), `${now()} ${s}\n`) } catch {} }
  const marker = (nm, payload) => { try { writeFileSync(join(EVID, nm), JSON.stringify(payload, null, 2) + '\n') } catch {} }
  const ok = pilotConv && pilotSid && sockPath && cfg.provider && cfg.model
  evlog(`apply: pilot=${pilotConv} sid=${pilotSid} sock=${sockPath} configured=${ok ? 'yes' : 'no'}`)

  // ---- routing state machine (spec §16): OLD | S2_ACTIVE | QUIESCING_TO_OLD ----
  let route = ok ? 'OLD' : 'OLD'
  let fenceTurn = null

  // ---- ownership (spec §13): borrowed (get) vs owned (resume) ----
  let ownedHandle = null

  // ---- admission dedupe + outbound state ----
  const admitted = new Map()       // dshMessageId -> {observed, at}
  const pendingOutbound = []       // bounded queue of finalization frames
  const outboundAcked = new Map()  // finalizationId -> ack at
  let shim = null
  let server = null

  // ---- per-session capture + turn reducer ----
  const sessions = new Map()
  const sb = (sid) => {
    let s = sessions.get(sid)
    if (!s) { s = { events: [], cur: null }; sessions.set(sid, s) }
    return s
  }
  function sessionEventLog(sid) {
    try {
      const live = ctx.agents.get(sid)
      if (live?.session?.events && Array.isArray(live.session.events)) return live.session.events
    } catch {}
    return []
  }
  function latestTurn(events) {
    let max = 0
    for (const e of events) {
      if (e.type === 'turn/end' && Number.isFinite(Number(e.data?.turn))) max = Math.max(max, Number(e.data.turn))
    }
    return max
  }
  function emitFinalization(kind, facts) {
    const turn = latestTurn(sessionEventLog(pilotSid))
    const fid = `${pilotSid}:${turn}:${kind}`
    const frame = { type: 'finalization', v: PROTOCOL_VERSION, conversationKey: pilotConv, sessionId: pilotSid, finalizationId: fid, kind, turn, facts }
    if (outboundAcked.has(fid)) return // already acked (replay guard)
    if (pendingOutbound.length >= MAX_PENDING_OUTBOUND) { evlog(`FINALIZATION DROPPED (bounded): ${fid}`); marker('outbound-overflow.json', frame); return }
    pendingOutbound.push(frame)
    flushOutbound()
  }
  function flushOutbound() {
    if (!shim) return
    while (pendingOutbound.length) {
      const f = pendingOutbound[0]
      try { shim.write(JSON.stringify(f) + '\n') } catch { return }
      pendingOutbound.shift()
    }
  }
  function realpathSafe(p) { try { return realpathSync(p) } catch { return p } }
  const freshTurnState = () => ({ buf: [], send_calls: new Map(), send_files: new Map(), send_seen: false, media_calls: new Set(), media: new Map(), end_error: false, active: false })
  const isSendName = (nm) => { const s = (nm || '').toLowerCase(); return s.includes('send_message') || s.includes('discord') }
  const isMediaName = (nm) => (nm || '').toLowerCase().includes('generate_music')
  function reduceSendOutcomes(calls) {
    const outs = [...calls.values()].filter((v) => v !== null)
    if (!outs.length) return null
    if (outs.includes('failed')) return 'failed'
    return outs.every((v) => v === 'ok') ? 'ok' : null
  }
  function sendFilesFromArgs(a) {
    if (typeof a === 'string') { try { a = JSON.parse(a) } catch { return [] } }
    if (!a || typeof a !== 'object') return []
    return Array.isArray(a.files) ? a.files.filter((f) => typeof f === 'string' && f) : []
  }
  function mediaFromResult(text, isError) {
    if (isError || !text) return null
    let obj; try { obj = JSON.parse(text) } catch { return null }
    if (!obj || obj.ok !== true) return null
    const { artifact: path, sha256: sha, mime, bytes } = obj
    const shaOk = typeof sha === 'string' && /^[0-9a-fA-F]{64}$/.test(sha)
    if (!(typeof path === 'string' && path && shaOk && typeof mime === 'string' && mime.toLowerCase().startsWith('audio/') && typeof bytes === 'number' && bytes > 0)) return null
    if (mediaRoot) {
      const real = realpathSafe(path)
      const root = realpathSafe(mediaRoot)
      if (!real.startsWith(root + '/')) return null
      return { path: real, sha256: sha.toLowerCase(), mime: mime.toLowerCase(), bytes }
    }
    return { path, sha256: sha.toLowerCase(), mime: mime.toLowerCase(), bytes }
  }

  // Decision matrix mirror of the shim backstop _flush_turn, but over native
  // durable facts only. Returns the normalized finalization decision.
  function finalizeTurn(sid, cur, turnNo) {
    const text = cur.buf.join('').trim()
    const sendCalls = [...cur.send_calls.entries()].map(([cid, outcome]) => ({ cid, outcome }))
    const media = [...cur.media.values()]
    const decision = decideFinalization({
      text,
      sendCalls,
      sendFilesByCall: Object.fromEntries([...cur.send_files.entries()]),
      sendSeen: cur.send_seen,
      media,
      end_error: cur.end_error,
      turn: turnNo,
    })
    emitFinalization(decision.kind, decision.facts)
  }

  // ---- per-turn event reducer (streaming over durable session events) ----
  function handleSessionEvent(sid, event, isReplay = false) {
    if (sid !== pilotSid) return
    const s = sb(sid)
    s.events.push(event)
    const t = event.type
    const d = event.data || {}
    if (!isReplay) {
      try { appendFileSync(join(EVID, 'session-events-raw.jsonl'), JSON.stringify({ sid, type: t, data: d }) + '\n') } catch {}
    }
    if (!isReplay && t === 'user/message' && String(d?.id || '').startsWith(DSH_MSG_PREFIX)) {
      try { appendFileSync(join(EVID, 'live-events.ndjson'), JSON.stringify({ type: 'user/message', id: d.id, marker: JSON.stringify(d).slice(0, 400) }) + '\n') } catch {}
    }
    if (!isReplay && t === 'turn/end') { try { appendFileSync(join(EVID, 'live-events.ndjson'), JSON.stringify({ type: 'turn/end', turn: d.turn, reason: (d.reason || {}).kind }) + '\n') } catch {} }
    if (!isReplay && t === 'assistant/message') { try { appendFileSync(join(EVID, 'live-events.ndjson'), JSON.stringify({ type: 'assistant/message', text: JSON.stringify((d.message || {}).content).slice(0, 400) }) + '\n') } catch {} }

    if (t === 'agent/inbox/spliced') {
      for (const m of d.inserted || []) {
        const mid = m?.id
        if (mid && String(mid).startsWith(DSH_MSG_PREFIX) && String(mid).includes(pilotConv)) {
          admitted.set(mid, { observed: 'spliced', at: now() })
          if (!isReplay) for (const w of [...admissionWaiters]) if (w.dshMessageId === mid) { w.resolve(); admissionWaiters.delete(w) }
        }
      }
      return
    }
    if (t === 'user/message') {
      const mid = d?.id
      const realUser = (d?.source || {}).kind === 'user'
      if (mid && admitted.has(mid) && realUser) { admitted.set(mid, { ...admitted.get(mid), observed: 'claimed', at: now() }); if (!isReplay) for (const w of [...admissionWaiters]) if (w.dshMessageId === mid) { w.resolve(); admissionWaiters.delete(w) } }
      if (realUser) { if (!s.cur) s.cur = freshTurnState(); s.cur.active = true }
      return
    }
    if (t === 'turn/start') {
      if (s.cur?.active) { s.cur.end_error = true; finalizeTurn(sid, s.cur, d.turn ?? latestTurn(sessionEventLog(sid))); s.cur = null }
      s.cur = freshTurnState()
      s.cur._turn = d.turn ?? null
      return
    }
    const cur = s.cur
    if (!cur) return
    if (t === 'assistant/chunk') {
      const c = d.chunk || {}
      cur.active = true
      if (c.type === 'tool-call-delta' && c.name && isSendName(c.name)) cur.send_seen = true
      if (c.type === 'block-end' && c.block?.type === 'tool-call') {
        if (isSendName(c.block.name)) { cur.send_seen = true; if (c.block.id) cur.send_calls.set(c.block.id, null) }
        else if (isMediaName(c.block.name) && c.block.id) cur.media_calls.add(c.block.id)
      }
    } else if (t === 'tool/call') {
      cur.active = true
      const cid = d.callId
      if (isSendName(d.name)) { cur.send_seen = true; if (cid) { cur.send_calls.set(cid, null); cur.send_files.set(cid, sendFilesFromArgs(d.arguments)) } }
      else if (isMediaName(d.name) && cid) cur.media_calls.add(cid)
    } else if (t === 'tool/result') {
      const callId = d.message?.source?.callId ?? d.message?.callId ?? d.callId
      let rtext = ''
      let rerr = false
      for (const item of (d.message?.content) || []) {
        if (item?.type === 'tool-result') {
          rerr = !!item.isError
          if (typeof item.content === 'string') rtext += item.content
          else for (const piece of item.content || []) if (piece?.type === 'text') rtext += piece.text || ''
        }
      }
      if (callId) {
        if (cur.send_calls.has(callId) && cur.send_calls.get(callId) === null) cur.send_calls.set(callId, classifySendOutcome(rtext, rerr))
        if (cur.media_calls.has(callId) && !cur.media.has(callId)) { const rec = mediaFromResult(rtext, rerr); if (rec) cur.media.set(callId, rec) }
        if (!isReplay) { try { appendFileSync(join(EVID, 'live-events.ndjson'), JSON.stringify({ type: 'tool/result', callId, text: rtext.slice(0, 400), isError: rerr }) + '\n') } catch {} }
      }
    } else if (t === 'assistant/message') {
      cur.active = true
      for (const c of (d.message?.content) || []) if (c?.type === 'text') cur.buf.push(c.text || '')
    } else if (t === 'turn/end') {
      const reason = (d.reason || {}).kind || ''
      if (reason && reason !== 'completed') cur.end_error = true
      if (cur.active) finalizeTurn(sid, cur, d.turn ?? cur._turn)
      s.cur = null
    }
  }

  // Reconciliation: replay committed turns after the shim's authoritative
  // external delivered boundary (a durable turn number). Replay does NOT write
  // live evidence; duplicate finalization fids are suppressed by the shim ledger
  // (and by outboundAcked here).
  function reconcile(boundaryTurn) {
    evlog(`reconcile boundaryTurn=${boundaryTurn}`)
    const evs = sessionEventLog(pilotSid)
    sb(pilotSid).cur = null
    sessions.get(pilotSid).events = []
    let last = 0
    for (const e of evs) {
      const t = e?.type
      if (t === 'turn/end' && Number.isFinite(Number(e.data?.turn))) last = Number(e.data.turn)
      if (t === 'turn/end' && last <= (boundaryTurn || 0)) continue
      if (['agent/inbox/spliced', 'user/message', 'assistant/chunk', 'tool/call', 'tool/result', 'assistant/message', 'turn/start', 'turn/end'].includes(t)) {
        handleSessionEvent(pilotSid, e, true)
      }
    }
    if (sessions.get(pilotSid)?.cur?.active) { sessions.get(pilotSid).cur.end_error = true; finalizeTurn(pilotSid, sessions.get(pilotSid).cur, null) }
    evlog('reconcile done')
  }

  const admissionWaiters = new Set()

  // ---- agent resolution (get | resume; NO create) ----
  async function resolveAgent() {
    const existing = ctx.agents.get(pilotSid)
    if (existing) return { agent: existing, ownedByUs: false }
    try {
      const handle = await ctx.agents.resume({
        resumeSessionId: pilotSid,
        agentOptions: { provider: cfg.provider, model: cfg.model },
        setup: (agentCtx) => {
          installSessionSelection(agentCtx, ctx)
          if (cfg.stubDiscordTool) registerStubTool(agentCtx)
        },
      })
      ownedHandle = handle
      return { agent: handle.agent, ownedByUs: true }
    } catch (e) {
      evlog(`resolveAgent resume FAIL (fail-closed): ${String(e)}`)
      return { error: String(e) }
    }
  }
  function installSessionSelection(agentCtx, rootCtx) {
    const fallback = () => {
      try {
        const svc = rootCtx.get('agentDefaultModel')
        if (svc && typeof svc.currentSelection === 'function') return svc.currentSelection()
      } catch {}
      return undefined
    }
    const loggedConfig = () => {
      try {
        const header = agentCtx.agent?.session?.requestHeader?.()
        const config = header?.config
        if (config && config.provider && config.model) return { provider: config.provider, model: config.model, ...(config.reasoningEffort === undefined ? {} : { reasoningEffort: config.reasoningEffort }) }
      } catch {}
      return undefined
    }
    const selection = {
      get current() { return loggedConfig() ?? fallback() },
      set current(_next) {},
      assembled: undefined,
    }
    installModelSelection(agentCtx, selection)
  }
  function registerStubTool(scopeCtx) {
    // rc.2 native tool contract: parameters (root JSON Schema), output {schema,render},
    // async execute(args, exec).
    if (!scopeCtx?.tools?.register) { evlog('no tools.register on scope; stub skipped'); return }
    try {
      scopeCtx.tools.register({
        type: 'function',
        name: 'mcp__discord__send_message',
        description: 'Send a message to Discord (isolation stub; gate contract shape).',
        parameters: {
          type: 'object',
          properties: {
            channel_id: { type: 'string', description: 'Discord channel ID' },
            content: { type: 'string', description: 'Message text' },
            files: { type: 'array', items: { type: 'string' }, description: 'Absolute file paths to attach' },
          },
          required: ['channel_id'],
          additionalProperties: false,
        },
        output: {
          schema: { type: 'object', properties: { sent: { type: 'number' }, messages: { type: 'array' } }, required: ['sent', 'messages'], additionalProperties: true },
          render: (_args, value) => [{ type: 'text', text: JSON.stringify(value) }],
        },
        async execute(args) {
          const sent = Array.isArray(args.files) && args.files.length ? args.files.length : 1
          // EXACT gate contract result: {"sent":N,"messages":[{status,message_id}]}
          return { sent, messages: Array.from({ length: sent }, (_, i) => ({ status: 200, message_id: `synthetic-${Date.now()}-${i}` })) }
        },
      })
      evlog('stub discord send tool registered on ' + (scopeCtx === ctx ? 'ctx root' : 'agent setup'))
    } catch (e) { evlog(`stub send register failed: ${String(e)}`) }
    if (cfg.stubDiscordTool && cfg.mediaRoot) {
      try {
        scopeCtx.tools.register({
          type: 'function',
          name: 'mcp__image__generate_music',
          description: 'Generate music (isolation stub returning an artifact under the approved root).',
          parameters: { type: 'object', properties: { prompt: { type: 'string', description: 'Prompt' }, delayMs: { type: 'number', description: 'Delay ms' } }, required: ['prompt'], additionalProperties: false },
          output: {
            schema: { type: 'object', properties: { ok: { type: 'boolean' }, artifact: { type: 'string' }, sha256: { type: 'string' }, mime: { type: 'string' }, bytes: { type: 'number' } }, required: ['ok', 'artifact', 'sha256', 'mime', 'bytes'], additionalProperties: true },
            render: (_args, value) => [{ type: 'text', text: JSON.stringify(value) }],
          },
          async execute(args) {
            const ms = Math.max(0, Math.min(20000, Number(args.delayMs) || 0))
            if (ms) await delay(ms)
            const file = join(cfg.mediaRoot, 's2-media-' + Date.now() + '.mp3')
            try { mkdirSync(cfg.mediaRoot, { recursive: true, mode: 0o755 }) } catch {}
            const content = 'id3-stub-media-content'
            try { appendFileSync(file, content) } catch {}
            return { ok: true, artifact: file, sha256: 'a'.repeat(64), mime: 'audio/mpeg', bytes: Buffer.byteLength(content) }
          },
        })
        evlog('stub generate_music registered')
      } catch (e) { evlog(`stub music register failed: ${String(e)}`) }
      try {
        scopeCtx.tools.register({
          type: 'function',
          name: 's2_delay',
          description: 'Wait the requested ms (isolation delay tool).',
          parameters: { type: 'object', properties: { ms: { type: 'number', description: 'Milliseconds' } }, required: ['ms'], additionalProperties: false },
          output: {
            schema: { type: 'object', properties: { ok: { type: 'boolean' }, waitedMs: { type: 'number' } }, required: ['ok', 'waitedMs'], additionalProperties: true },
            render: (_args, value) => [{ type: 'text', text: String(value.waitedMs) }],
          },
          async execute(args) { const ms = Math.max(1, Math.min(20000, Number(args.ms) || 1)); await delay(ms); return { ok: true, waitedMs: ms } },
        })
      } catch (e) { evlog(`stub delay register failed: ${String(e)}`) }
    }
  }

  // ---- wire protocol ----
  async function handleFrame(conn, raw) {
    let frame
    try { frame = JSON.parse(raw) } catch { writeFrame(conn, { type: 'error', code: 'malformed' }); return }
    if (!frame || typeof frame !== 'object') { writeFrame(conn, { type: 'error', code: 'bad-frame' }); return }
    switch (frame.type) {
      case 'hello': {
        const boundary = Number.isFinite(Number(frame.deliveredBoundaryTurn)) ? Math.max(0, Number(frame.deliveredBoundaryTurn)) : 0
        shim = conn
        if (route === 'S2_ACTIVE' && boundary > 0) reconcile(boundary)
        writeFrame(conn, { type: 'hello-ack', v: PROTOCOL_VERSION, route, pilotConversationKey: pilotConv, pilotSessionId: pilotSid })
        flushOutbound()
        break
      }
      case 'admitted': {
        if (route !== 'S2_ACTIVE') { writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: frame.discordMessageId, accepted: false, code: route === 'QUIESCING_TO_OLD' ? 'quiescing' : 'not-active' }); return }
        if (String(frame.conversationKey) !== pilotConv || String(frame.sessionId) !== pilotSid) {
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: frame.discordMessageId, accepted: false, code: 'identity-mismatch', failClosed: true })
          return
        }
        const discordId = String(frame.discordMessageId || '')
        const envelope = typeof frame.content === 'string' ? frame.content : ''
        if (!discordId || !envelope.trim()) { writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, accepted: false, code: 'missing-fields' }); return }
        const deterministic = dshMessageIdFor(pilotConv, discordId)
        if (frame.dshMessageId && String(frame.dshMessageId) !== deterministic) { writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, accepted: false, code: 'dsh-message-id-mismatch' }); return }
        if (admitted.has(deterministic)) {
          const rec = admitted.get(deterministic)
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, dshMessageId: deterministic, accepted: true, durable: true, observed: rec.observed, deduped: true })
          return
        }
        const waiter = { dshMessageId: deterministic, resolve: null, discordId }
        waiter.resolve = () => {}
        const done = new Promise((res) => { waiter.resolve = res })
        admissionWaiters.add(waiter)
        const { agent, ownedByUs, error } = await resolveAgent()
        if (error || !agent) {
          admissionWaiters.delete(waiter)
          evlog(`admission fail-closed (no agent): ${discordId} ${error || ''}`)
          marker('admission-fail-closed.json', { discordId, deterministic, error: error || 'no-agent' })
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, accepted: false, code: 'agent-unavailable', failClosed: true })
          return
        }
        const msg = admittedUserMessage(pilotConv, discordId, envelope, cfg.clientTimeZone)
        try { agent.followup(msg) } catch (e) {
          admissionWaiters.delete(waiter)
          evlog(`followup FAIL (ambiguous, no resend): ${deterministic} ${String(e)}`)
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, accepted: false, code: 'followup-error', ambiguous: true })
          return
        }
        // durable splice commits before followup() returns; the observer above
        // resolves the waiter synchronously. Bounded wait guards a rare miss.
        let observed = false
        try { await Promise.race([done.then(() => { observed = true }), delay(1500).then(() => {})]) } catch {}
        admissionWaiters.delete(waiter)
        if (observed) {
          admitted.set(deterministic, { observed: 'spliced', at: now() })
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, dshMessageId: deterministic, accepted: true, durable: true, observed: 'spliced' })
          evlog(`admission acked durable: ${deterministic}`)
        } else {
          evlog(`admission AMBIGUOUS (no durable observation): ${deterministic}`)
          marker('admission-ambiguous.json', { discordId, deterministic, at: now() })
          writeFrame(conn, { type: 'ack', for: 'admitted', discordMessageId: discordId, accepted: false, code: 'ambiguous-no-durable-observation', ambiguous: true })
        }
        break
      }
      case 'ack': {
        if (frame.for === 'finalization' && frame.finalizationId) {
          outboundAcked.set(frame.finalizationId, now())
          evlog(`shim acked finalization ${frame.finalizationId}`)
        }
        break
      }
      case 'route': {
        const next = String(frame.state || '').toUpperCase()
        if (!['OLD', 'S2_ACTIVE', 'QUIESCING_TO_OLD'].includes(next)) { writeFrame(conn, { type: 'error', code: 'bad-state' }); return }
        if (next === 'QUIESCING_TO_OLD' && route === 'S2_ACTIVE') fenceTurn = latestTurn(sessionEventLog(pilotSid))
        route = next
        writeFrame(conn, { type: 'route-ack', state: route, fenceTurn })
        evlog(`route -> ${route} fenceTurn=${fenceTurn}`)
        break
      }
      case 'ping': writeFrame(conn, { type: 'pong', route }); break
      default: writeFrame(conn, { type: 'error', code: 'unknown-type' })
    }
  }
  function writeFrame(conn, obj) {
    try {
      const line = JSON.stringify(obj) + '\n'
      if (Buffer.byteLength(line, 'utf8') > MAX_FRAME_BYTES) return false
      return conn.write(line)
    } catch { return false }
  }

  // ---- socket lifecycle ----
  function removeStale() { try { if (existsSync(sockPath)) unlinkSync(sockPath) } catch {} }
  function startServer() {
    try { mkdirSync(dirname(sockPath), { recursive: true, mode: 0o770 }) } catch {}
    removeStale()
    server = createNetServer({ allowHalfOpen: false }, (conn) => {
      let buf = ''
      conn.on('data', (chunk) => {
        buf += chunk.toString('utf8')
        if (Buffer.byteLength(buf, 'utf8') > MAX_FRAME_BYTES * 2) { try { writeFrame(conn, { type: 'error', code: 'oversize' }) } catch {}; conn.destroy(); return }
        let idx
        while ((idx = buf.indexOf('\n')) >= 0) {
          const raw = buf.slice(0, idx)
          buf = buf.slice(idx + 1)
          if (!raw.trim()) continue
          handleFrame(conn, raw).catch((e) => evlog(`handleFrame error: ${String(e)}`))
        }
      })
      conn.on('error', (e) => { evlog(`conn error: ${String(e)}`); if (shim === conn) shim = null })
      conn.on('close', () => { if (shim === conn) { shim = null; evlog('shim disconnected (outbound held for reconnect)') } })
    })
    server.on('error', (e) => evlog(`server error: ${String(e)}`))
    server.listen(sockPath, () => {
      try { chmodSync(sockPath, 0o660) } catch {}
      evlog(`socket listening ${sockPath}`)
      marker('plugin-ready.json', { sockPath, pilotConv, pilotSid, at: now() })
    })
  }

  // observe durable session events from boot (armed before any admission)
  ctx.on('session/event', (session, event) => {
    try {
      const sid = session?.header?.id ? String(session.header.id) : String(session?.id ?? session)
      try { appendFileSync(join(EVID, 'observer-debug.log'), `${now()} ${sid} ${event?.type}\n`) } catch {}
      handleSessionEvent(sid, event)
    } catch (e) { evlog(`session/event handler error: ${String(e)}`) }
  })

  if (cfg.stubDiscordTool) {
    registerStubTool(ctx)
    evlog('stub discord send tool requested (isolation only)')
  }

  startServer()
  marker('plugin-loaded.json', { at: now(), pilotConv, pilotSid })

  return async () => {
    evlog(`teardown route=${route} pending=${pendingOutbound.length} owned=${ownedHandle ? 'yes' : 'no'}`)
    const deadline = Date.now() + 2500
    while (pendingOutbound.length && Date.now() < deadline) await delay(50)
    for (const f of pendingOutbound) marker('indeterminate-at-teardown.json', f)
    pendingOutbound.length = 0
    if (ownedHandle) {
      try { await ownedHandle.dispose() } catch (e) { evlog(`owned dispose error: ${String(e)}`) }
      ownedHandle = null
      evlog('owned agent disposed (borrowed agent untouched)')
    }
    try { server?.close?.() } catch {}
    removeStale()
    evlog('teardown complete')
  }
}
