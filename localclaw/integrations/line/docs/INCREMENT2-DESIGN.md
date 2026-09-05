# DSH LINE Family Increment 2 — Explicit EN↔TH Translation — DESIGN RECORD

**Status:** BUILT + SELF-TESTED — Hermes review in flight — deployment gated.
**Date:** 2026-09-04 late evening.
**Parent:** checkpoint-dsh-fork-line-increment1-2026-09-04.md (§4.5: translation is
the next execution line). This increment does NOT redesign identity/admission.

## 1. Scope and standing behavior (per operator directive)

- Approved family group only; group translation requires a REAL LINE own-mention
  of DSH (`isSelf==true` metadata — never `@All`, never textual lookalikes).
- DMs require no mention (Increment-1 roster/DM-eligibility unchanged).
- Command forms: `@DSH translate <text>` in the approved group; `translate <text>`
  in an admitted DM. Source text supplied in the SAME message — nothing inferred
  from prior messages.
- Direction: automatic EN↔TH by the specialist (no adapter-side detection).
- Output: translation only — no commentary/alternates/transliteration.
- Names, URLs, numbers, emojis, formatting preserved where practical.
- Normal DSH conversation unchanged when the command is not invoked.

## 2. Architecture (smallest viable; no new subsystems)

- **Adapter (v5, candidate `dsh_line_inbound.py` sha256 `dba6c979…`)**: adds
  (a) `parse_translate_command()` — smallest unambiguous parser; (b)
  `strip_own_mention_text()` — strips only the REAL own-mention span using
  LINE's mention metadata (index/length; `@<surface>` prefix fallback), used
  only after the Increment-1 gate has passed; (c) `translate_via_dsh()` — sends
  ONLY the source text into the SAME session.prompt → bounded poll → F3
  final-output extraction path as normal dispatches (marker
  `[line translate <dispatch_id>]`); (d) `handle_translate()` — runs strictly
  AFTER the Increment-1 trust gate and reuses its exact revalidation protocol
  (admission re-check + person binding in one flock-guarded transaction);
  (e) `TRANSLATE_FAILURE_TEXT` — concise fixed failure line; specialist failure
  NEVER falls back to a normal-model answer.
- **Specialist (DSH-side, staged `cordis.patch.translate.yml.new`)**: additive
  `- insert` row `tool-translate` re-introducing the Phase-2 translation
  subagent (`toolName: translate`, provider spawn, model
  `switchyard/thaillm/typhoon`, maxTokens 4096, toolFilter allow:[]) with a
  translation-engine persona. Sits alongside the untouched `thai_analyst` row
  (gold lane). NOTE: the old `translate` row was repurposed to `thai_analyst`
  in the 2026-09-03 gold revision, so this row is a re-introduction, additive
  by design.
- **Direction/preservation enforcement**: entirely in the specialist persona
  ("predominantly English → Thai; predominantly Thai → English; keep names,
  URLs, numbers, emojis, formatting; output ONLY the translation"). No adapter
  heuristics; no second language-detection service.
- **State**: new durable `translatePending` map in line-state.json (marker
  `{to, dispatchId}`) for observability/fail-closed persistence symmetry;
  normalized in `load_state()` (malformed entries dropped); resolved in
  `_translate_finish()`; failure path is NO-RETRY by design (a stale
  "unavailable" push after recovery would mislead the family).

## 3. Parser contract (smallest unambiguous)

- Group mode: caller strips the real mention span first; parser matches
  `translate(?:\s+(.*))?$` (DOTALL, IGNORECASE) on the remainder.
- DM mode: same regex anchored at the START of the raw text. `"@DSH translate
  hello"` in a DM is NOT a command (leading `@DSH` breaks the anchor — correct:
  DSH's DM nickname is not a summon and must not be silently stripped).
- Bare `translate` / whitespace-only source → valid command with empty source →
  concise help text, NO specialist call.
- Word-boundary safe: `translates hello`, `translator hello`, mid-sentence
  "how do I translate X" → None (normal conversation path).
- Oversized source (>4000 chars) → None (bounded payload; no silent truncate).

## 4. Gating chain (Increment 1 preserved)

Group: signature/shape → admission(APPROVED) → roster bootstrap → mention gate
(metadata only) → mention-span strip → parser → handle_translate (revalidates
eligibility AGAIN under the lock) → specialist. Literal `@DSH` text and `@All`
never pass the metadata gate, so they can never reach translation — proven by
tests T2/T3 and by the retained Increment-1 suite.
DM: admission/eligibility → parser → handle_translate (revalidates).

## 5. Failure behavior

- Specialist failure (RPC error, timeout, empty/None reply): concise fixed
  line, no fabricated content, no retry, marker resolved durably, F3 filter
  unchanged (failure text goes through the same bounded push path).
- Empty command: concise help line (no specialist call).

## 6. Tests (all synthetic; no real family traffic needed pre-deployment)

- Selftest (in-module): 81 checks ALL PASS (13 new parser/strip checks).
- `test_translate_command.py`: 24/24 PASS (T1–T10 covering the directive §9
  matrix: group+mention path, literal-text silence, @All silence, DM path,
  normal-conversation path, empty-command help, verbatim EN→TH and TH→EN
  fixtures, names/URLs/numbers/emoji intact, failure-no-fallback, dedupe,
  revoked-eligibility abort).
- F3 battery: 11/11 PASS on v5. Family admission suite: 56/56 PASS on v5.

## 7. Deployment plan (boundaries)

1. Operator A/B/C live checks on Increment 1 (LIVE bytes 51746f42) complete.
2. Hermes verdict on Increment 2 (this review).
3. Root pack (adapted phase-2f pattern): staged cordis patch install
   (hash-anchored, backup, dsh:dsh ownership) → HMR hot-apply expected
   (`watchUserPatches`); verify `tool-translate` visible to DSH; fallback =
   dsh.service restart line (operator-run).
4. Adapter activation: `sudo install -o vincent -g vincent -m 664
   /opt/dsh-line/dsh_line_inbound.py.new /opt/dsh-line/dsh_line_inbound.py &&
   sudo systemctl restart dsh-line-inbound` (operator-run; same boundary as
   Increment 1).
5. Post-restart verification battery (same shape as Increment 1).
6. Fork commit on `localclaw/dsh-v0.1.1-rc.2` (Increment-1 commit push
   precedes, after Increment-1 acceptance).

## 8. Rollback

- Adapter: cp rollback-anchor fee0eadf bytes or the Increment-1 51746f42 bytes
  back + restart (same as Increment-1 rollback).
- Cordis patch: restore backup of b0dae9f7 bytes (HMR reapplies; no restart).

## 9. Candidate hashes

- `dsh_line_inbound_v5.py` / staged `.py.new`: dba6c9799446c1ac43cb7ae71fbdb8ef9966cf28d5e6e756d88943d0ced7dfba
- `cordis.patch.translate.yml.new`: (record at pack time)
- Baseline (deployed Increment 1): 51746f421f5655ea3dd2d95c463099eab0807a1a49c3f06558793ad47417f22e
- Baseline cordis: b0dae9f7fb281541e69a8f1cc8e7ea982eff852353149a3d399bbfb8d642a751

## 10. Hermes round 1 (run_35fd2597) - REJECT - resolution record

| # | Finding | Resolution in 5e448e5e candidate |
|---|---|---|
| 1 | Critical: translatePending not redelivery-resilient (double specialist prompt possible) | Bespoke marker machinery DELETED entirely. Translate path now uses the exact normal-delivery flow (mark_accepted + deliver_reply); new pending-aware dedupe/recovery branch in handle_translate recovers durable pending replies on redelivery (T11) - never a second specialist prompt |
| 2 | Major: marker-resolution save failure loses the reply | Same fix - no marker state machine; deliver_reply's record_pending/claim/finish semantics apply unchanged |
| 3 | Major: failure path bypassed reservation protocol + push_messages(retry_key_seed) TypeError | Failure text now rides deliver_reply (durable pending + X-Line-Retry-Key + bounded retry); raw push_messages call removed; T8 asserts single-path delivery |
| 4 | Major: failure not retryable -> loss window | Same deliver_reply path - failure text is durable + retryable like any reply |
| 5 | Minor: group_mode param misleading | Parser signature reduced to parse_translate_command(text); caller contracts documented (DM = raw text, group = pre-stripped remainder) |
| 6 | Minor: marker schema normalization incomplete | Moot - marker removed; load_state back to baseline shape |

Post-fix suites: translate 25/25 (new T11 redelivery-recovery test), family 56/56,
F3 ALL PASS, selftest 81 ALL PASS. Candidate sha256 5e448e5ec15b6a80e86463be4581fb202bec3de1c9c9d817e72b9cfa004c3bc1.

## 11. Revision 2b additions + live §6 proof record (2026-09-05 ~02:40)

Reply-to-DSH (v6, adapter a824fb52): implemented per directive - outbound
authorship via Push `sentMessages[].id` (LINE OpenAPI: required id field;
verified live body shapes vary → fail-open to mention-only), bounded
dshOutbound store (500 ids / 7-day TTL, ids + conv key only, no bodies),
strict composition (reply-proof counts only when message has NO mentionees
→ @All never invokes), invocation = self-mention OR verified reply-quote.
Tests R1-R7 all pass.

Translation-output contract: persona hardened (no labels/headers, no echo,
currency+emoji preservation); adapter adds translate_output_valid fail-closed
gate (error-shaped / exact-echo outputs → concise failure, never delivered).
Test T10b added. All suites green: translate 40/40, family 56/56, F3 ALL,
selftest ALL.

### §7 Cordis activation verdict: CASE B - restart required
Physical evidence: watchUserPatches IS wired in the installed bundle
(profile-boot calls it on home + profile patch paths) and inotify watches on
the exact patch files DO exist in a running probe instance - but file edits
(append, invalid-YAML append, valid tool-translate insert) produced NO reload,
and a NEW session created after the edit still lacked the inserted tool.
Composition inserts are effectively boot-time for this build. Deployment pack
MUST restart dsh.service.

### §6 real-chain proof record (temp-home probe, port 3999, zero prod impact)
Chain verified mechanically END-TO-END: adapter-style task → parent
(switchyard-smart-dsh live via :4000) → translate tool invoked → subagent
spawn → switchyard/thaillm/typhoon child hit (routing.jsonl 18:15:34,
18:17:38, 18:20:33, 18:29:39, 18:32:09 + 13-call run). PROVEN: tool exists
only after boot with insert (HMR negative), tool executed, no fallback.
NOT YET ACCEPTABLE: child outputs across 5 iterations repeatedly returned
conversational/preamble echo instead of the translation - root cause
isolated: the headless child harness prepends a large "Current runtime
context" snapshot to every child task; Typhoon-8B (8B class) answers the
preamble rather than the tiny translate task. Direct minimal-context calls
to the SAME persona+model pass both directions cleanly (EN→TH and TH→EN),
and the 3 sibling ThaiLLM models (openthaigpt/pathumma/thalle) all leak
<think> blocks in the same harness. Decision menu sent to operator (A/B/C/D).

Translation status remains `staged` in capabilities.yaml - NOT advertised live.

## 12. Increment 2c - operator corrections + clean transport (2026-09-05 ~03:30)

Reply semantics corrected: invocation signals INDEPENDENT (mention OR verified
reply; unrelated mentionees no longer cancel a verified reply). Retention
simplified: bounded oldest-first store, NO time TTL (nothing in LINE semantics
requires one); persistence failure = FAIL-CLOSED (real mention path unaffected;
never heuristic). Selftest 92 ALL PASS; translate suite 42/42 (R5 matrix
rewritten to the corrected rule); family 56/56; F3 ALL PASS.

Translation transport defect root cause: dsh-tool-subagent children receive a
synthetic "Current runtime context" snapshot user message (RuntimeContextProjection
in dsh-agent-loop, built from systemPrompt.assemble contexts; suppression exists
via suppressRuntimeContext()/persona.includeRuntimeContext/complete-sections but
NONE is reachable from the tool-subagent config surface; child persona is
installed as a raw section). Verdict: subagent primitive cannot provide clean
translation context by configuration.

Clean transport selected: third already-proven DSH-native pattern -
@deepseek-ai/dsh-mcp-client stdio adapter (same as webgate/discord). New
~200-line dsh_translate_adapter.py (NO credentials, NO daemon, NO new routing
plane): DSH session -> mcp__translate__translate(source_text) -> direct
OpenAI-compatible call to Switchyard :4000 -> switchyard/thaillm/typhoon ->
JSON envelope {ok, result.translation}. Adapter returns errors as
"error: translate_*:" texts; adapter-side validator remains fail-closed
(echo/error/malformed envelope -> concise failure line, never delivered).

REAL-PATH ACCEPTANCE (temp-home probe, port 3999, full production chain):
9/9 cases PASS translation-only, each with EXACTLY 1 tool call and 1 Typhoon
generation (steady state achieved): EN->TH polite, TH->EN polite, names,
URL, numbers+baht, emoji, multiline, mixed proper nouns, awkward mixed.
Representative outputs: 'Good morning, see you at the market.' ->
'สวัสดีตอนเช้า พบกันที่ตลาดนะครับ'; Thai greeting -> 'Good morning, see you
at the market tomorrow at 9:00 AM.'; 'I already tell her แล้ว but she no
reply เลยครับ' -> 'I already told her, but she has not replied yet.'
No runtime-context leakage; no conversational drift; no fallback.

"13-call run" clarification (directive #5): that was ONE parent turn issuing
13 repeated translate tool calls (13 Typhoon child generations) because every
contaminated child result made the parent retry - the anti-pattern the new
transport eliminates (observed 1 call per command across all probe tests).

CANDIDATE HASHES (staged, NOT deployed):
- adapter v7 dsh_line_inbound_v7.py a1a0bf2b8a8b6fc16d5453c021ea8109404dd42e2a0bb8ae495f27633c59d871
- translate MCP adapter dsh_translate_adapter.py 734c68373841a043ace69977515b9163df0588696a0221c7a84cc0cc935d4739
- cordis.patch.translate.yml.new 438e64de52e35aa923325cd71cc02f385b96eaa6136e08a22025c8a942498380
RESTART SCOPE: BOTH dsh.service + dsh-line-inbound (Case B evidence unchanged).

## 13. Rich Menu technical determination (2026-09-05, read-only, nothing created)

API authority: ALL rich menu operations (create/validate/upload-image/list/get/
delete/set-default/per-user link+unlink/bulk link/aliases+switch) are Messaging
API REST calls requiring ONLY the channel access token - the exact credential
class dsh-line-inbound already holds via systemd LoadCredential. No extra
scopes, no new credential, no new service. Image constraints (docs): 8-10MB,
PNG/JPEG, 2500x1686 / 1686x2500 px (full/tuck types per action layout).

Recommended architecture (matches operator preference exactly):
DSH generates menu JSON spec + artwork (its own capability; e.g. via webgate/
canvas)
-> bounded rich-menu operations added to a credential-holding helper (either a
few new ops in the existing dsh-line adapter family, or one tiny sibling
adapter like dsh_translate_adapter) exposing ONLY: list, get, create, upload,
set-default, delete - with dry-run spec validation first.
NO raw token ever reaches DSH.

Surface reality (docs-verified wording): rich menus display on the Official
Account CHAT SCREEN (1:1 chat, "Tap to open" bar); default menu applies to all
friends; per-user override possible. GROUP CHATS: rich menus do NOT render in
group chat surfaces - the family group will never see a menu. Capabilities
that make sense on the menu surface: help/guide, translation helper (opens
template), web-search prompt templates. Group-relevant controls stay
mention/reply-based.

Fallback (operator manual path, LINE Official Account Manager): create menu ->
upload image -> define tappable regions -> assign message/postback actions ->
publish/default -> test on mobile. Kept as operator convenience, not required.

## 14. Hermes round-4 REJECT resolution (run_4c1ab80e) - 2026-09-05 ~02:25

F1 (valid): record_outbound_ids persistence failure was swallowed by
_push_claimed (delivery marked success despite fail-closed state). FIX:
record_outbound_ids returns bool; _push_claimed raises CredError on False ->
deliver_reply treats it like any push failure: pending entry RETAINED for
bounded retry, delivery NOT marked success, STATE_UNAVAILABLE blocks further
dispatch until recovery. Simulated both branches: healthy path completes;
fail path returns False + pending retained + unavailable set. Selftest gains
"record returns True on healthy persistence" (93 checks, ALL PASS).

F2 (process): offline adapter smoke was blocked by the reviewer's approval
gate on its command shape; executed here instead via stdin-file subprocess
(no network): initialize/serverInfo, tools/list [translate], ping,
missing-arg bad_request isError - ALL PASS. Startup/list/error paths
independently verified.

FINAL HASHES: adapter v7 a1a0bf2b (unchanged logic region + return-bool),
translate adapter 734c6837, cordis 438e64de. Suites: translate 42/42,
family 56/56, F3 ALL, selftest 93 ALL PASS.
