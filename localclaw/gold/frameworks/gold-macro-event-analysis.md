---
name: gold-macro-event-analysis
description: Prepare for upcoming macro catalysts that affect Thai gold prices — identify what events matter, map transmission channels through USD/THB and world gold, and surface timing risk for the report's outlook horizon.
---

# Thai Gold Macro Event Analysis

Use this skill when the daily gold report needs to provide context on what macro events could affect gold prices over the coming days to weeks, not just report on today's numbers.

This skill will not:
- predict the exact price reaction to a macro release
- treat every calendar entry as gold-relevant
- substitute macro theater for practical investor guidance

## Role

Act like a macro risk analyst preparing gold readers for event risk. Focus on timing, transmission channels (USD, rate expectations, risk appetite), and scenario awareness.

## When to use it

Use when the daily report needs:
- a forward-looking macro section beyond the current day's data
- to explain why gold is likely to consolidate before a catalyst
- to warn readers about overnight event risk affecting open gold positions
- to contextualize a regime label as "provisional due to upcoming catalyst"

## Data sources

Macro context should draw from:
- Upstream full framework: upstream framework (see NOTICE.md)
- Publicly known macro calendar (Fed, BoJ, ECB, NFP, CPI, PCE)
- Thai-specific: BoT rate decisions, gold import tax policy, holiday demand cycles
- History file for past price reactions to similar events

## Core Assessment Framework

Rank each event against four anchors before calling it important for gold:

### 1. Gold Sensitivity
Does this event historically move gold prices?
- **High:** Fed rate decisions, US CPI/PCE, NFP, USD index moves, major geopolitical risk
- **Medium:** BoJ/ECB decisions (via USD), US retail sales, Chinese gold import data
- **Low:** Second-tier US housing data, regional manufacturing surveys

**Example:** "CPI has high gold sensitivity because it directly drives real rate expectations, which is gold's primary macro driver."

### 2. Surprise Capacity
Is there room between consensus and reality for a gold-relevant surprise?
- Wide estimate dispersion = higher surprise capacity
- Strong consensus with narrow dispersion = lower surprise capacity
- No consensus data available = state explicitly, reduce conviction

**Example:** "CPI consensus at 3.2% with wide dispersion (2.8-3.6%) — meaningful surprise capacity for gold."

### 3. Transmission Speed
How fast does this event transmit to gold?
- **Fast (< 1 hr):** Fed decisions, NFP, CPI — USD and gold react within minutes
- **Medium (1-24 hrs):** BoT policy, Chinese data — gradual transmission through USD/THB
- **Slow (days):** Policy direction changes, geopolitical developments

**Example:** "Fed decision transmits fast — gold can swing 1-2% within an hour. The daily report should flag this clearly."

### 4. Timing Pressure
Does the event compress the reader's decision window?
- Overnight events that BKK readers wake up to
- Events clustering (CPI on Wed, Fed on Thu, NFP on Fri)
- Events during Thai market holidays or low-liquidity windows

**Example:** "CPI Thursday + Fed decision Friday = heavy timing pressure. Readers should avoid oversized positions before Thursday."

## Event classification for the report

| Class | Thai Label | Meaning |
|-------|------------|---------|
| `primary` | เหตุการณ์หลัก | High gold sensitivity, fast transmission, within the report's outlook window |
| `secondary` | เหตุการณ์รอง | Worth monitoring, but less likely to dominate gold price action this week |
| `background` | พื้นหลัง | Context only — useful for the "Trader's Notes" education section |

## Output structure for the report

If confirmed macro context exists (not speculation), add a **Macro Outlook** section:

```
🇹🇭 มาโครสัปดาห์นี้:
• CPI สหรัฐฯ (พุธ) — มีผลต่อทองคำสูง เพราะกระทบอัตราดอกเบี้ยจริง
• ประชุม Fed (พฤหัส) — ส่งผลเร็วผ่านดอลลาร์ ผู้อ่านควรทำความเข้าใจความเสี่ยง
• NFP (ศุกร์) — ทุติยภูมิ แต่อาจเพิ่มความผันผวนช่วงปิดสัปดาห์

🇺🇸 This Week's Macro:
• US CPI (Wed) — High gold sensitivity, drives real rate expectations
• Fed decision (Thu) — Fast transmission through USD. Readers should be aware of overnight risk
• NFP (Fri) — Secondary, but could amplify end-of-week volatility
```

## Best practices
- do not claim a release guarantees a gold price move
- do not bury missing consensus data or stale timestamps
- distinguish macro-event certainty from event importance
- if no confirmed high-sensitivity events exist in the outlook window, say "quiet macro week — gold price action is driven by technical factors and local demand this week"
- the macro section should be optional: only include if there are confirmed events with real gold sensitivity
