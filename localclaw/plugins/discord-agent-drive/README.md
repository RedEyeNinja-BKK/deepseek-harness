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

Routing state machine OLD / S2_ACTIVE / QUIESCING_TO_OLD (route control frames) plus
ownership rules: a live agent found via `ctx.agents.get` is borrowed and never
disposed by S2; an agent this plugin resumed is owned and disposed at teardown only
after fence/drain/quiesce.

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
dedicated dir (owner/group per service model, see live-gate).

## Isolation-only stub
`stubDiscordTool: true` registers `mcp__discord__send_message`,
`mcp__image__generate_music`, `s2_delay` on the ctx root and on agent setup using the
exact rc.2 native tool contract (`parameters` root JSON Schema, `output.render` ->
text-block array, `execute`). Production MUST keep this false (real D-01 MCP tools
already compose on the session).

## Tests
- `test/reducer-and-identity.test.mjs` — 23/23 (deterministic id, gate-contract
  classifier incl. a real captured rc.2 tool/result text, decision matrix).
- `test/dsh_s2_shim.py` — seam battery driver (15 cases pre-restart, 4 post-restart;
  isolated scratch DSH).
- `test/seed-pilot-session.mjs` — disposable seed that creates the pinned pilot
  session (never in production).
- Full results + evidence under the report tree (S2 seam battery dir).

## Production integration (cutover, SEPARATE GO — not done)
1. Capture read-only exact pilot `channel:<id>`/`thread:<id>`, mapped `sessionId`,
   durable provider/model/reasoning from the pilot session's request/header.
2. Add a small `s2_client.py` in the inbound listener (`/opt/dsh-inbound`) that owns
   the AF_UNIX client + route flag `S2_PILOT_CONV`; when the flag equals the pilot
   conversation the listener skips ALL historical DSH-driving/backstop paths for that
   conversation and uses the seam instead (one DSH-driving authority + one fallback
   authority + the shared Discord send authority).
3. Mount the plugin row; boot; supervised live battery; rollback = one route flag
   disable through QUIESCING_TO_OLD then OLD (plugin unmount optional cleanup).

## Rollback
No identity translation, no ledger merge: flip `S2_PILOT_CONV` off (quiesce/fence
then old path only for new events), continue against the same durable DSH session and
the same external delivered ledger; the old listener code is retained throughout.
