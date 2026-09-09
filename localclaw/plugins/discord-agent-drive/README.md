# discord-agent-drive (S2 pilot) — Discord native Agent-drive + durable-finalization

NON-PRODUCTION staged plugin for the S2 pre-live slice. Byte-identical copy is
isolated-tested in the S2 seam battery (`report/evidence/s2-pre-live-seam-2026-09-09/`)
and would be the eventual mounted plugin at cutover (separate GO).

## Purpose
For ONE exact existing persistent Discord conversation, replace the historical DSH
integration seam with native Agent semantics:
- input admitted over a purpose-specific AF_UNIX JSONL seam from the external shim
  (which keeps Discord transport/token/admission/dedupe/session-map/attachment/D-01
  send + delivered state authoritative);
- the plugin resolves the pinned session (`ctx.agents.get`, else `ctx.agents.resume`;
  NO `create` on the live path), admits with a deterministic caller-owned DSH
  `MessageId` (`discord:<convKey>:<discordMessageId>` via `MessageId` +
  `freezeMessage`, preserving `source.kind == 'user'`), `followup()`s, and ACKs only
  once the exact id is durably represented (`agent/inbox/spliced` / `user/message`);
- durable `session/event` observation + a decision matrix mirror of the shim backstop
  emit normalized finalization facts over the seam; the external delivered ledger
  suppresses duplicates (media by artifact identity, everything by
  `sessionId:turn:kind`);
- reconciliation replays committed turns after the shim's authoritative delivered
  identity list (`deliveredFinalizations` on `hello`; replay-skip for every fid the
  authoritative external ledger reports delivered) with replay-only handling (no live
  evidence duplication, no second DSH delivered ledger).

## Finalization identity (exact durable turn)
`finalizationId = sessionId:turn:kind` where `turn` is the EXACT durable turn the
finalization represents (its own `turn/end`, falling back to the turn number captured
at its own `turn/start`). The identity is never derived from a whole-log latest-turn
scan: during reconciliation multiple historical turns of the same kind would
otherwise collapse onto one FID and the external delivered set would suppress owed
older finalizations as duplicates. During an ordinary reconnect an in-flight (open)
turn at the log tail is RETAINED, never finalized as abnormal — its real durable
`turn/end` finalizes it exactly once.

## Admission-index rebuild (restart-safe)
The in-memory admission dedupe index is rebuilt from authoritative session history on
EVERY `hello`, including an empty `deliveredFinalizations` list. Inbound duplicate
recovery therefore never depends on whether any outbound finalization has already
been delivered: a lost inbound ACK followed by a plugin/process restart still
dedupes a retried event and never issues a second `followup()`.

## One-DSH-driving-authority guard (r4, consumer integration)
The plugin owns finalization ONLY for turns it admitted over the seam
(deterministic `discord:` user-message ids). A non-seam real-user turn on the
pilot session (the old-path driver, e.g. during OLD rollback authority while the
plugin is still mounted) is observed but NEVER activated/finalized by the plugin
(`turn/start` creates the accumulator but content cannot bring it alive unless
the turn's user message was seam-owned). This makes OLD authority single-owner
and prevents duplicate delivery when the plugin remains mounted. Guard probe:
non-seam turn -> 0 finalization frames; seam turn -> exactly 1.

Routing state machine OLD / S2_ACTIVE / QUIESCING_TO_OLD (route control frames) plus
ownership rules: a live agent found via `ctx.agents.get` is borrowed and never
disposed by S2; an agent this plugin resumed is owned and disposed at teardown only
after fence/drain/quiesce. Reconciliation materializes the pinned session
(get | resume, no create) when needed so durable history is readable even right
after a DSH process restart.

## Supported rc.2 surfaces used
`ctx.agents.get/resume`, `ctx.on('session/event')`, `agent.followup`,
`agent.session.events` (authoritative log), `installModelSelection` (session's own
model pin), `MessageId`/`freezeMessage`. No browser-facing `session.*` RPC anywhere.

## Config (home `cordis.patch.yml` row; absolute path)
```yaml
- insert:
  - id: discord-agent-drive
    name: '/opt/dsh/home/plugins/discord-agent-drive.mjs'
    config:
      pilotConversationKey: 'channel:<id>'     # exact pilot selector (equality only)
      pilotSessionId: 'session-…'              # exact pinned DSH session
      socketPath: '/run/dsh-discord-pilot/dsh.sock'
      provider: '…'                            # pilot session's durable selection (captured)
      model: '…'                               # captured at cutover; fail-closed if unknown
      reasoningEffort: ''                      # optional, captured at cutover
      maxTokens: 0
      mediaRoot: '/mnt/off-vm-nfs/comfyui-media'
      evidenceDir: '/var/lib/dsh/home/plugins'  # diagnostics only (dsh-owned)
      stubDiscordTool: false                   # MUST be false in production
```

## Seam (inbound / outbound), JSONL over AF_UNIX
- inbound: `hello{deliveredFinalizations:[fid,…]}` / `route{state}` / `admitted{conversationKey,
  sessionId, discordMessageId, dshMessageId, authorId, content, attachmentRefs, ts}`
- outbound: `hello-ack`, `route-ack`, `ack{for:'admitted'|'finalization', …}`,
  `finalization{conversationKey, sessionId, finalizationId: session:turn:kind, kind,
  turn, facts}`; errors `{type:'error', code}`.
Frame cap 1 MiB; bounded outbound queue (500); stale-socket cleanup + 0660 socket in a
dedicated dir. Socket dir/socket group is a service-to-service shared group (see
`test/README` + live-gate): the DSH plugin (user `dsh`) creates the socket; the
Discord inbound consumer is a different service user, and both already share an
existing group so the client connects without world access (`0770` dir + `0660`
socket).

## Delivery guarantee (honest wording)
The shim ledger is two-phase: a finalization is durably begun (pending) BEFORE the
external send and marked delivered after it. A replay of a `pending` fid is
INDETERMINATE and never auto-resent (prevents an automatic duplicate after an
ambiguous crash). Across the unavoidable send/commit crash window (`pending`
committed -> crash -> external send never happened) the semantics are **at-most-once
automatic external send with durable fail-visible INDETERMINATE state**, not
unconditional exactly-once visible delivery. Pending state stays surfaced in the
ledger and is persisted as indeterminate evidence for the operator. No transaction
coordinator or broker is introduced to close that window.

## Isolation-only stub
`stubDiscordTool: true` registers `mcp__discord__send_message`,
`mcp__image__generate_music`, `s2_delay` on the ctx root and on agent setup using the
exact rc.2 native tool contract (`parameters` root JSON Schema, `output.render` ->
text-block array, `execute`). Production MUST keep this false (real D-01 MCP tools
already compose on the session).

## Tests
- `test/reducer-and-identity.test.mjs` — 30/30 (deterministic id incl. E-series
  finalization-identity: distinct FIDs per durable turn, no latest-turn
  collapse; gate-contract classifier incl. a real captured rc.2 tool/result text;
  decision matrix).
- `test/dsh_s2_shim.py` — seam battery driver (C01-C21 pre-restart incl. the three
  review-blocker regressions: C19 multi-turn same-kind exact-FID reconcile,
  C20 reconnect-during-in-flight no-premature-finalization, C21 lost-inbound-ACK
  pre-restart staging; P01-P05 post-restart incl. P05 restart + EMPTY delivered
  set + retry dedupe; isolated scratch DSH).
- `test/old-path-driver.mjs` — OLD-authority guard probe (non-seam turn => 0
  frames; seam turn => 1).
- `production/test/s2_consumer_battery.py` — real production-consumer-candidate
  battery (flag-empty 2/2; fake-seam UNIT 13/13; real-plugin INT 7/7).
- Full results + evidence under the report tree (S2 r4 consumer dir).

## Production consumer (cutover-gated)
See `production/` (listener candidate + `s2_seam.py` helper + live gate +
integration battery). The consumer is default-off (`S2_PILOT_CONV=""`); the
plugin is mounted via the home profile patch row with `stubDiscordTool:false`;
the live gate + supervised battery execute only at operator GO.

## Production integration direction (native-first; cutover-gated)
No new daemon and no standalone client subsystem. The S2 plugin is the DSH-native
side (mounted via the home profile patch row, exactly like the S1 boot-rearm plugin;
`stubDiscordTool:false`). The only boundary crossing is the AF_UNIX seam itself; the
production consumer is the SMALLEST surgical reuse inside the existing
`/opt/dsh-inbound` listener — its own native ledger + Discord send authority + D-01
gate — routed by the pilot-conversation equality flag. That listener delta is
authored, Hermes-reviewed and byte-captured as part of the cutover GO; no production
mutation happens before that GO.

## Rollback
No identity translation, no ledger merge: flip `S2_PILOT_CONV` off (quiesce/fence
then old path only for new events), continue against the same durable DSH session and
the same external delivered ledger; the old listener code is retained throughout.
