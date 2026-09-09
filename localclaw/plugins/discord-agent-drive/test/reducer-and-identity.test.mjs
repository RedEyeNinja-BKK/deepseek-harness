// S2 reducer + deterministic-identity fixture test (Hermes Minor 5 closure).
// Imports the REAL plugin bytes and proves the pure decision matrix against:
//  - the exact durable tool/result string captured from a real rc.2 turn
//    (mcp__discord__send_message returning the D-01 gate contract);
//  - gate-contract ok / failed / malformed variants;
//  - the media / abnormal / conversational matrix;
//  - deterministic caller-owned DSH message identity (spec §4).
// Run from inside a scratch profile dir so '@deepseek-ai/*' resolves.
import { decideFinalization, classifySendOutcome, dshMessageIdFor, admittedUserMessage } from './discord-agent-drive.mjs'

let pass = 0, fail = 0
function check(name, cond, detail = '') {
  if (cond) { pass++; console.log('PASS ' + name) } else { fail++; console.log('FAIL ' + name + ' :: ' + detail) }
}

// ---- A. identity (spec §4) ----
const conv = 'channel:111111111111111111'
const sid = 'session-2f8c1f6a-0000-4000-8000-0000000000a1'
const id1 = dshMessageIdFor(conv, '1001')
const id2 = dshMessageIdFor(conv, '1002')
check('A1 id deterministic', id1 === dshMessageIdFor(conv, '1001') && id1.startsWith('discord:'))
check('A2 distinct per discord id', id1 !== id2)
const m1 = admittedUserMessage(conv, '1001', 'hello', 'Asia/Bangkok')
check('A3 freeze preserves caller id', m1.id === id1)
check('A4 source.kind user', m1.source?.kind === 'user')
check('A5 role user + tz', m1.role === 'user' && m1.source?.clientTimeZone === 'Asia/Bangkok')
const m1b = admittedUserMessage(conv, '1001', 'hello', 'Asia/Bangkok')
check('A6 same input same id (retry-safe)', m1b.id === m1.id)
check('A7 namespace unambiguous', /^discord:channel:[0-9]+:[0-9]+$/.test(id1))

// ---- B. real gate-contract classifier ----
// This string is the verbatim durable tool/result text captured from a real
// rc.2 turn (isolated run turn 16, stub mcp__discord__send_message).
const realOkText = '{"sent":1,"messages":[{"status":200,"message_id":"synthetic-1788932844980-0"}]}'
check('B1 real ok text -> ok', classifySendOutcome(realOkText, false) === 'ok')
check('B2 failed status -> failed', classifySendOutcome('{"sent":1,"messages":[{"status":500,"message_id":"x"}]}', false) === 'failed')
check('B3 malformed -> null', classifySendOutcome('not-json-or-contract', false) === null)
check('B4 isError -> failed', classifySendOutcome(realOkText, true) === 'failed')
check('B5 partial -> failed', classifySendOutcome('{"sent":2,"messages":[{"status":200,"message_id":"x"}]}', false) === 'failed')

// ---- C. decision matrix ----
const mediaRec = { path: '/mnt/off-vm-nfs/comfyui-media/s2-media-x.mp3', sha256: 'a'.repeat(64), mime: 'audio/mpeg', bytes: 20 }
function d1(text, sendCalls, sendFilesByCall, sendSeen, media, end_error, turn) {
  return decideFinalization({ text, sendCalls, sendFilesByCall, sendSeen, media, end_error, turn })
}
check('C1 no-send text -> text-fallback', d1('hi', [], {}, false, [], false, 1).kind === 'text-fallback')
check('C2 send ok + text -> noop', d1('hi', [{ cid: 'c1', outcome: 'ok' }], { c1: [] }, true, [], false, 2).kind === 'noop')
check('C3 send failed + text -> text-fallback', d1('hi', [{ cid: 'c1', outcome: 'failed' }], { c1: [] }, true, [], false, 3).facts?.reason === 'failed-send')
check('C4 send unknown + text -> text-fallback', d1('hi', [{ cid: 'c1', outcome: null }], { c1: [] }, true, [], false, 4).facts?.reason === 'unknown-send')
check('C5 empty + failed -> failure-notice', d1('', [{ cid: 'c1', outcome: 'failed' }], { c1: [] }, true, [], false, 5).kind === 'failure-notice')
check('C6 empty + ok -> noop terminal', d1('', [{ cid: 'c1', outcome: 'ok' }], { c1: [] }, true, [], false, 6).facts?.note === 'terminal-empty')
const art = d1('', [{ cid: 'c1', outcome: 'failed' }], { c1: [] }, true, [mediaRec], true, 7)
check('C7 abnormal + media + empty -> artifact', art.kind === 'artifact' && art.facts?.artifacts?.length === 1)
const convMedia = d1('ok text', [{ cid: 'c1', outcome: null }], { c1: [] }, true, [mediaRec], false, 8)
check('C8 normal + text + media (no send ok) -> text, no artifact', convMedia.kind === 'text-fallback' && !convMedia.facts?.artifacts)
const abnormalMedia = d1('fallback text', [{ cid: 'c1', outcome: 'failed' }], { c1: [] }, true, [mediaRec], true, 9)
check('C9 abnormal + media + text -> artifact with text', abnormalMedia.kind === 'artifact' && abnormalMedia.facts?.text === 'fallback text')
const dupDelivered = d1('hi', [{ cid: 'c1', outcome: 'ok' }], { c1: ['/x.mp3'] }, true, [mediaRec], false, 10)
check('C10 send ok delivered file -> noop (file suppressed)', dupDelivered.kind === 'noop')

// ---- D. real-event reducer path (fixture assembly like the plugin) ----
// feed the reducer with facts derived from the REAL captured turn 16 sequence:
// tool/call send + tool/result (string contract) + turn text absent -> noop.
const realOutcome = classifySendOutcome(realOkText, false)
const realTurn = d1('', [{ cid: 'call_00_5YCPH1F8RFf4i4Suawde6687', outcome: realOutcome }], { call_00_5YCPH1F8RFf4i4Suawde6687: [] }, true, [], false, 16)
check('D1 real send-ok turn -> noop (no duplicate fallback)', realTurn.kind === 'noop')

console.log(`\nTOTAL ${pass}/${pass + fail}`)
process.exit(fail ? 1 : 0)
