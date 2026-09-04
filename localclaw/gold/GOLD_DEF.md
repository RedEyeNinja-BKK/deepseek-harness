# DSH GOLD_DEF — Daily Thai Gold Market Report (product definition, DSH-owned)

**Owner:** DSH executes this entire workflow and owns the daily trigger. (2026-09-04 operator directive: this job is fully DSH-owned; Turnstone no longer triggers it.) Turnstone supervises afterward and never composes report content.
**Trigger:** DSH-owned self-rescheduling daily one-shot schedule created with DSH's native `schedule_create` tool (kind `at`, next 10:00 Asia/Bangkok — always create via the `at` selector with explicit
`time_zone: "Asia/Bangkok"`, never by pre-converting to UTC), in the dedicated gold session. **Re-arm procedure (FIRST action of every fired run, before §0):** (1) `schedule_list` in this session; (2) if a schedule already exists whose target is the next 10:00 BKK with the same run prompt, keep it - do not create a second one (idempotent); (3) otherwise create it; (4) verify via `schedule_list` that exactly one successor is active; (5) if creation fails, continue with the report run and record `rearm_failed` in the receipt `notes` - Turnstone supervision or the operator will re-arm manually. A trigger whose `scheduledAt` is in the future is NEVER fired, consumed, or stale - only the schedule tool state and dispatch events in this session's log are evidence of firing. "Next 10:00 Asia/Bangkok" always means the earliest future 10:00 BKK from the current wall clock; never delete or duplicate an existing future trigger. Because all schedule fires land as turns in this one session, DSH executes them sequentially - concurrent starts are not expected from the schedule itself; the §0 run lock covers any manual double-dispatch.
**Audience:** Thai readers learning gold investment. Report language: 50/50 Thai/English mirrored.
**Model split (2026-09-03 operator-directed revision):** Thai analytical authorship = `thai_analyst` subagent (ThaiLLM Typhoon via Switchyard). Composition, assembly and the English mirror run on the session's own model lane — the dedicated gold session is pinned to `switchyard-smart-bounded-dsh` via `session.selectModel` (pinned once at session creation; verified each run via the session model info). DSH orchestrates, validates, stores, emits, and supervises.

## 0. Idempotency (run start)

**Run lock (atomic, before any fetch or emission-capable work):** run
`mkdir /opt/dsh/workspace/gold-report/locks/<today-BKK>` - `mkdir` is atomic.
If it already exists, another run owns today: read `last-run.json`; if it shows
today with a `PASS*` verdict and `readback_ok`, reply `ALREADY-RUN` and stop;
otherwise wait bounded (≤ 5 min, re-check) for the owner to finish; if the
owner releases the lock and the receipt rules below then permit a run, proceed;
if the owner is STILL present after the bounded wait, record `notes: lock
owner active` and stop WITHOUT emitting (your receipt-free status is the
supervision evidence). Release the lock (`rmdir`) in a finally step at run
end (success OR failure). Never emit while you do not hold today's lock.
Before anything else: if `last-run.json` exists, parses, has `date` == today (Asia/Bangkok), and its `verdict` starts with `PASS` with `readback_ok` true — reply `ALREADY-RUN` with today's message IDs and stop. **Never emit a second Discord report for the same Bangkok date**; a same-day rerun that failed earlier may store/repair files but must not re-emit unless the earlier verdict was `EMIT-FAILED` AND the recovered run is the same date (then emit once and note the recovery in the footer). This does not apply to CANARY mode (§7), which never touches the canonical receipt.

## 1. Storage (this directory is the canonical DSH-owned store)

- `GOLD_DEF.md` — this file (self-copy; DSH-created at bootstrap).
- `reports/YYYY-MM-DD.md` — canonical full report for that date.
- `latest.md` — copy of the most recent report.
- `history.json` — `{"history": [ ... ]}`, one entry appended per **verified report day only** (verdict `PASS` or `PASS-STALE`; never append `DATA-UNAVAILABLE`/`EMIT-FAILED`/`INDETERMINATE` days). Full immutable entry schema:
  `{"date","bar_sell","bar_buy","ornament_sell","ornament_buy","world_gold","usd_thb","as_of","change_prev_day","verdict","key_levels":{"support":[],"resistance":[]}}`
- `last-run.json` — machine receipt for the most recent run (write atomically: write
  `last-run.json.tmp` then rename over the final name; same for `history.json` and
  `latest.md` — a reader must never see a half-written file):
  `{"date","run_started","finished_at","fetch_path","as_of","data_age_min","freshness","thai_author","composer_used","report_path","emitted_message_ids":[],"readback_ok","verdict","macro_research","payload_sha256","notes"}`
  (`macro_research`, `payload_sha256`, `notes`, `charter_status` are additive 2026-09-04 fields - supervisors treat them as informational and never require them for PASS. `notes` is a one-line field holding semicolon-separated items when several apply (e.g. `rearm_failed; mapping: none; repaired bar_sell unit`); `charter_status` = `loaded` / `missing`.)
  (`thai_author` = the tool actually used for the Thai leg, e.g. `ThaiLLM Typhoon (Switchyard)`; `composer_used` = the composition lane actually used, e.g. `switchyard-smart-bounded-dsh (session lane)`.)

Read `history.json` and `latest.md` BEFORE composing (trend context). If history has 5+ entries include week-over-week and month-over-month comparison; otherwise state that accumulation is ongoing and note any observable pattern. If history is missing, start a fresh empty `{"history": []}`.
`history.json` includes backfilled legacy entries (marked `"legacy": true`, 2026-07-29 onward) migrated from the pre-offload store - provenance in `HISTORY-PROVENANCE.md`. Use the FULL series for weekly/monthly comparisons, observed ranges (highs/lows), regime evidence, and support/resistance levels. Legacy entries may lack fields the native schema has - absence means the data was not captured, never fabricate it. See also the Analytical correctness rules in §3. History rows are point-in-time observations at their capture time (native rows carry `as_of`; legacy rows generally do not) - NOT GTA daily closes; describe weekly/monthly context as observed history/range/change, and use prior-close wording only for the GTA `priceChangeFromPrevDayLast` field.

## 2. Data source (simple, reliable, browser-free)

Primary: your `web_fetch` tool on
`https://www.goldtraders.or.th/api/GoldPrices/Latest?readjson=true`
Fallback (only if web_fetch fails): `bash` `curl -sS --max-time 25 -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36" "<same URL>"`

Extract and preserve exactly:
- `bL_BuyPrice`, `bL_SellPrice` (gold bar, THB per baht-weight)
- `oM965_BuyPrice`, `oM965_SellPrice` (ornament 96.5%, THB per baht-weight)
- `goldSpot` (world gold, USD/oz)
- `bahtPerUSD`
- `asTime` (upstream local Thai timestamp), `seq`
- `priceChangeFromPrevDayLast` (change vs previous day close, THB)

**Schema validation before use:** the payload must parse as JSON and every numeric
field above must be present, finite, and numeric, and `asTime` must parse as a
timestamp. Missing/null/non-numeric fields or unparseable `asTime` = treat exactly
like fetch failure (attempt the other path, then `DATA-UNAVAILABLE`, no emission).
Whichever path succeeds - or both fail - the raw response(s) are still saved per
§4 step 0 before any verdict is written.
Plausibility check: bar sell price must be within 20,000–200,000 THB; world gold
within 500–20,000 USD/oz — otherwise treat as invalid data, not a market fact.

**Do not wake or wait for any browser.** General web research is limited to the
single bounded macro window in §3 step 3b (never a substitute for the primary
quote; never used to invent price data).

### Freshness validation
`asTime` is Thai local time (Asia/Bangkok). Compute data age = now − asTime.
- ≤ 90 min → `fresh`; emit normally, footer states age.
- 90 min – 24 h → `stale`; emit with a clear staleness label and reduced conviction wording.
- > 24 h, or both fetch paths fail → **do NOT emit to Discord.** Write `last-run.json` with `verdict: "DATA-UNAVAILABLE"` and stop after recording it.

## 3. Report composition

Compose in this order:
1. Build the data fact sheet (all fields above + computed parity, below).
2. **Load the advisor charter (read-only):** read `ADVISOR_CHARTER.md` next to
   this file. It governs your analyst judgment and voice for this run: the
   five-layer price discipline, evidence labels, freshness honesty, advice
   standards, move-significance tiers, and directionality check. If it is
   missing, continue with the frameworks and record `charter_status: missing` in the receipt (otherwise `loaded`).
3. Read the three analytical frameworks (read-only) and load their anchors — regime classification (trend/volatility/participation/event backdrop), macro-event lens, catalyst mapping:
   - `/opt/dsh/workspace/gold-report/frameworks/gold-market-regime-analysis.md`
   - `/opt/dsh/workspace/gold-report/frameworks/gold-macro-event-analysis.md`
   - `/opt/dsh/workspace/gold-report/frameworks/gold-catalyst-map.md`
   (Use your file-read tool for these — NOT bash, and never request a sandbox
   escalation; they are DSH-owned read-only references adopted 2026-09-04,
   provenance in `frameworks/PROVENANCE.md`. These reads MUST succeed: if one
   fails, retry once, then record `framework_missing: <name>` in the receipt
   `notes` and continue without it — never skip silently.)
3b. **Bounded macro research (conditional, ONE window):** only if data is
   `fresh`, use your `mcp__webgate__web_search` tool ONCE (one search, optionally
   one follow-up `mcp__webgate__web_fetch`) to check for CONFIRMED high/medium-sensitivity gold events
   in the outlook window (Fed/BoT decisions, US CPI/NFP, major geopolitical USD
   or gold drivers). Hard budget: ~2 minutes total, then stop. Confirmed items
   may populate the optional 📅 Macro Outlook / 🔔 Catalyst Watch sections;
   anything unconfirmed is omitted. This window NEVER touches the price-data
   path: the fact sheet, quote fields, FX, and parity math come only from §2
   and history. If the search fails, times out, or finds nothing, omit those
   sections - absence is not content. Record the outcome in the receipt as
   `macro_research`: one of `confirmed: <event list>` / `none found` /
   `skipped (stale)` / `failed`.
4. **Thai authorship - `thai_analyst` (ThaiLLM Typhoon):** call your `thai_analyst`
   tool ONCE with: the fact sheet, the framework anchors you loaded, a 3–5 line
   history digest, and the section/heading spec below. It returns the complete
   Thai report (600–900 Thai words, natural, ไม่เป็นทางการเกินไป, ใช้คำว่า "ท่าน";
   อธิบายศัพท์เทคนิคในประโยค เช่น แนวรับ/แนวต้าน/พาริตี้). **Review its output
   before continuing:** every number must match the fact sheet exactly, units must
   be THB per บาททองคำ (baht-weight), world gold in USD/oz — if the tool introduced
   a wrong number or unit, correct it in your working copy, note the repair in the
   footer, and record the repair in the receipt. If the thai_analyst call itself
   fails, make the ONE allowed retry before falling back, and record the
   failure(s) in the receipt `notes`; falling back without the retry is a
   discipline violation. Pass the no-markdown-tables constraint in the prompt
   you send it (the report renders prices as aligned lists, never tables).
5. **Composition & English mirror (your session lane — switchyard-smart-bounded-dsh):**
   on your own bounded lane, produce the English mirror and assemble the final
   bilingual report from the reviewed Thai report + fact sheet: same section
   headings, same order, every number/currency/unit/date/percentage reproduced
   EXACTLY as in the Thai text (THB per baht-weight stays THB per baht-weight,
   never per-gram), footer included per the spec below. Build the file with the
   chunked bash-append pattern in the discipline rules — never one giant write.
6. **Mirror gate + directionality check before emission:** verify the English section covers every Thai topic and vice versa, with matching section headings and order (50/50 mirror - never EN-first with TH appended). Then apply the charter's DIRECTIONALITY SANITY CHECK: every direction word (up/down, higher/lower, firmer/softer, wider/narrower, premium/discount) in BOTH layers must agree with the underlying numeric deltas and premium math - patch any mismatch before the parity gate. If the mirror or directionality check fails, repair the report; if it still fails, store it, set verdict `INDETERMINATE`, and do NOT emit.
7. **Deterministic bilingual parity gate (required):** with the assembled report
   saved to the working file, run
   `python3 /opt/dsh/workspace/gold-report/bilingual_parity_check.py <working-file>`.
   Exit code 0 = pass. On exit 1, read the printed mismatches, repair the offending
   section(s) with the bash-append pattern, and re-run the checker. If it still
   fails after repair, store the report, set verdict `INDETERMINATE`, and do NOT
   emit. **Never bypass, weaken, or edit the checker to make it pass, and never
   emit with a known bilingual numeric/unit conflict.**

**Analytical correctness rules (mandatory):**
- **Comparison windows must be coherent and labelled.** Thai bar change vs prior close uses the GTA `priceChangeFromPrevDayLast` field (label "vs prior close"); world gold and USD/THB comparisons against stored history are vs yesterday's stored observation (label "vs yesterday's observation") - never mix the two bases in one unlabelled sentence. Weekly/monthly statements only from the history span.
- **Spread is not volatility.** The bar buy/sell spread is transaction cost / quote structure - discuss it as such. Volatility statements require observed price movement/ranges from history; if insufficient data exist, say volatility cannot be robustly characterized.
- **Support/resistance needs evidence.** Derive levels from defensible historical observations (repeated levels, recent highs/lows, stored key_levels). The current bid/ask are NOT support/resistance merely by being the quote. Round numbers (e.g. 70,000) may be described only as explicitly-labelled psychological/reference levels.
- **Participation honesty.** With no volume/order-flow data, state participation is unobserved/unavailable - do not infer it from price location alone.
- **Richness from evidence, never invention.** Use the full history series, spreads, parity premium/discount, and transmission mechanics to go deeper; every number still comes from the API or history.

**Parity math (locked, per advisor charter):** bullion parity (THB per baht-weight) = `goldSpot × bahtPerUSD × (15.244 × 0.965 ÷ 31.1034768)`. The 0.965 is the Thai bar purity factor - never drop it; never use this formula for jewellery. Parity is a **Derived** value: compute at full floating-point precision from the exact captured inputs, then format once (two decimals, thousands separators) and use that exact formatted string in BOTH language layers; label it derived in the report, never as API-sourced. Compare bar sell vs parity; explain premium/discount in plain language, and note in the Trader's Notes rotation (parity day) that this is the 96.5%-adjusted parity.

**Structure (both languages, mirror order):**
- `## 🇹🇭 รายงานทองคำ — YYYY-MM-DD` / `## 🇺🇸 Gold Market Report — YYYY-MM-DD`
- 📊 Price table — render as an aligned list/bullets, NOT markdown tables (Discord does not render tables). All key figures + change vs previous day.
- 📈 Regime summary (four-anchor classification + what would change it)
- 🔍 Key levels & parity (support/resistance with natural-language explanation)
- 🌍 World context & USD/THB
- 💡 Investor insight (translate the day's move into practical implications for a buyer, a holder, and a seller - separate facts, interpretation, uncertainty; probability-based language per charter)
- 🎯 Takeaway (actionable, plain language, cautious per charter advice standards)
- 📚 Trader's Notes (บันทึกสำหรับเทรดเดอร์) — rotate one concept per day (baht-weight, buy/sell spread, ornament premium, the Association, parity, regime, macro catalyst, …)
- Optional (only if data/history actually support): macro outlook, catalyst watch.
- Footer: `**Data source:** Gold Traders Association (goldtraders.or.th) | **Fetched:** HH:MM BKK | **Data age:** X min` + `**Thai analysis:** ThaiLLM Typhoon (Switchyard) | **Composition & English mirror:** switchyard-smart-bounded-dsh` — or the truthful substitution note if a fallback was used (e.g. `**Thai analysis:** DSH (Typhoon unavailable)`).

**Hard rules:** every number comes from the API or history — never invent or round beyond display precision; no cron/schedule/infrastructure meta-talk; no numbered lists; sections separated by `---`; target ≤ 15,000 characters, **hard cap 19,000 characters** (Discord gate limit) — run a pre-send character check; if over the hard cap, shorten the report before emission. Never rely on truncation.

**CHUNK & FILE-TOOL DISCIPLINE (reliability — mandatory):**
(a) Never emit a single generation payload exceeding ~1200 characters. Build every
file (report, latest.md, history.json, last-run.json) as a sequence of
section-sized sequential bash appends — `cat > file` quoted-heredoc for the first
chunk, `cat >> file` for each following chunk, one section per call. Long single
writes have proven to stall the model stream — this rule is not optional.
(b) Your `edit`/`write` file tools require the target file to have been READ in
this session before editing — **never call `edit` on a file you have not read
this session** (that produces `FS_NOT_OBSERVED` errors). For gold-report files,
use the bash append pattern in (a); if you choose `edit`/`write` instead, read
the file first.
(c) Call `thai_analyst` exactly once (plus the single retry on failure) as
specified in step 4. Do not use the bash lane for its work.

## 4. Storage write-back (before emitting)

**Permissions:** every file you create under `gold-report/` must be `0644` — run
`chmod 0644` on `GOLD_DEF.md`, all `reports/*.md`, `latest.md`, `history.json`,
`last-run.json` (your file-write tool defaults to 0600, which locks out supervision).

0. **Evidence preservation (before anything else in this section):** save the
   raw successful quote payload EXACTLY as received - regardless of fetch path
   (web_fetch or curl fallback) - to `raw/YYYY-MM-DD.json` (0644; create `raw/`
   if missing). Save it IMMEDIATELY after the fetch returns, BEFORE schema
   validation, so even invalid/partial payloads are preserved (a preserved
   invalid payload is evidence for the DATA-UNAVAILABLE verdict). Record its
   sha256 in the receipt as `payload_sha256`. The §2 field list is the source-to-field mapping; record
   any deviation in `notes`. The receipt already carries `fetch_path`, `as_of`,
   and `data_age_min` (report time = receipt `finished_at`, source time =
   `as_of`, quote age = `data_age_min`).
1. Save the full report to `reports/YYYY-MM-DD.md`; update `latest.md` (copy).
2. Append today's entry to `history.json` (create if missing). Use the day's verdict and key levels from your own analysis.
3. Write `last-run.json` (all fields; fill `emitted_message_ids` and `readback_ok` after step 5).

## 5. Discord emission + verification

1. Emit with your `discord.send_message` tool: `channel_id = "<your-discord-channel-id>"` (your designated report channel), content = the full report. The gate chunks long content sequentially (≤1900 chars, order preserved, up to 10 chunks) — one call is enough. Never send chunks in separate parallel calls.
2. Verify the tool response: it must succeed (no `isError`) and yield a non-empty set of message IDs; if the response reports an error or yields no IDs, treat emission as failed (`EMIT-FAILED`).
3. Independent read-back: call `discord.read_messages` on the same channel and verify: your messages are present, bot-authored, timestamps in order, count matches the IDs returned, and first/last chunk content matches the report's first/last chunk. Record ALL returned message IDs and the read-back result in `last-run.json`. If emission or read-back fails, set `verdict` to `EMIT-FAILED`/`INDETERMINATE` accordingly — never claim PASS without read-back.

## 6. Failure behavior

- Data unavailable (both paths) → `DATA-UNAVAILABLE`, no emission.
- Stale data → emit with label, verdict `PASS-STALE`.
- Subagent failure after the one retry → self-composed fallback section, truthful footer note, verdict unaffected.
- Macro research window fails or finds nothing → omit the optional sections, record `macro_research`, verdict unaffected.
- Session lane not on `switchyard-smart-bounded-dsh` (check your session model info): record the actual lane truthfully in `composer_used` and continue — never claim the bounded lane was used if it was not.
- Parity gate failure after repair → report stays stored, no emission, verdict `INDETERMINATE`.
- Emission/read-back failure → report stays stored; verdict records the failure for supervision.

## 7. Canary mode (controlled validation only — never the daily run)

Triggered ONLY when the run prompt begins with `CANARY:`. Execute the identical
pipeline (§2 fetch/validation → §3 composition incl. charter load, frameworks, §3 step 3b macro window, `thai_analyst`, `composer`,
mirror gate, parity gate) with these differences:

- **Storage:** write `reports/canary-YYYY-MM-DD.md` and `canary-last-run.json`
  (same receipt fields, plus `"canary": true` and `"deleted_message_ids":[]`).
  Do NOT modify `latest.md`, `history.json`, `last-run.json`, or
  `reports/YYYY-MM-DD.md`. §0 idempotency does not block a canary.
- **Marker:** prepend `[CANARY — validation run, auto-deleted]` to the first chunk.
- **Emission + cleanup:** emit normally (§5), read back, then IMMEDIATELY delete
  every emitted message with your `discord.delete_message` tool
  (`channel_id` + each `message_id`), then verify via `discord.read_messages`
  that none of the IDs remain. Record both the emitted and deleted IDs in
  `canary-last-run.json`.
- **Verdict:** `PASS` requires fresh data, both model lanes actually used
  (`thai_author` + `composer_used` truthful), parity gate exit 0, emission +
  read-back OK, and all messages deleted and verified absent. If deletion fails,
  set verdict `EMIT-CLEANUP-FAILED` and list the surviving IDs for manual cleanup —
  never claim PASS with surviving canary messages.
