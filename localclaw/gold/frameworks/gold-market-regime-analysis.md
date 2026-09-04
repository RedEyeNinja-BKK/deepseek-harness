---
name: gold-market-regime-analysis
description: Analyze Thai gold market context through trend, volatility, participation breadth, and event backdrop so the report can choose framing that fits the environment without relying on black-box regime claims.
---

# Thai Gold Market Regime Analysis

Use this skill when composing the daily gold report to classify the market environment before interpreting price action, key levels, or investor guidance.

This skill will not:
- forecast next-day gold prices from a regime label
- assign fake confidence percentages to limited evidence
- replace instrument-specific gold trade planning

## Role

Act like a gold market structure analyst. Classify the environment conservatively, explain the evidence, and highlight what would invalidate that view — all in 50/50 bilingual format (Thai first, English mirror).

## When to use it

Use when the daily report needs:
- a regime label beyond just "up/down/flat"
- context on whether the trend is healthy, fragile, or transitioning
- to adapt guidance based on volatility or participation conditions
- to pressure-test the report's directional read before language selection

## Data sources

Primary (always):
- Today's GTA data (bar buy/sell, ornament buy/sell, world spot, USD/THB)
- Previous reports in history at `/opt/dsh/workspace/gold-report/history.json`
- Calculated: weekly/monthly/YTD changes, parity, support/resistance levels

Secondary (when available):
- Macro calendar context from upstream `macro-event-analysis` skill
- Recent pattern data from accumulated history

## Core Assessment Framework

Score the gold market on four anchors before choosing a label:

### 1. Trend (แนวโน้ม)
- Bar sell price trajectory: daily change, weekly change, monthly change, YTD change
- Direction and strength: are multi-period trends aligned or conflicting?
- Key level proximity: distance from support, resistance, and parity

**Examples for the report:**
- "Bar sell +0.46% daily, +1.80% weekly, +0.77% monthly — trend is positive but decelerating at the monthly level."
- "Weekly trend positive (+1,100 THB) but daily structure shows intraday pullback (-50 THB from previous quote) — short-term momentum is diverging from intermediate trend."

### 2. Volatility (ความผันผวน)
- Daily range vs recent norm (weekly range, monthly range)
- Gap behavior: how far does today's price range extend relative to recent sessions?
- Ornament-to-bar spread: widening/narrowing as proxy for retail sentiment

**Examples for the report:**
- "Weekly range 64,100–66,150 (2,050 THB) is expanding — volatility is picking up."
- "Ornament spread at 800 THB (sell price bar vs ornament) is within normal range — no panic or euphoria signal."

### 3. Participation / Breadth (การมีส่วนร่วมของตลาด)
- Ornament vs bar: are retail buyers active (ornament volume proxy) or only bar traders?
- Thai vs world divergence: how far is parity gap from normal? A widening premium suggests local demand; a discount suggests local sellers dominate
- Price discovery depth: how many price sequences today (GTA `priceSeq`)?

**Examples for the report:**
- "Premium-to-parity at -3.38% (below fair value) suggests local market is not overheating — buying appetite is measured."
- "Ornament sell price at 65,900 exactly at resistance — retail buyers are transacting at the ceiling, not chasing higher."

### 4. Event Backdrop (บริบทเหตุการณ์)
- Upcoming catalyst proximity: Fed decision, CPI, Thai policy, gold import tax, holidays (Songkran, CNY)
- Whether this week's macro calendar could invalidate the current regime read
- Historical patterns from accumulated data around similar dates

**Examples for the report:**
- "Fed decision this week — gold tends to consolidate before FOMC. The regime read should be treated as provisional."
- "Approaching Chinese New Year — historical data from this period shows seasonal demand support. The current cautious read may shift."

## Regime classification labels

Use these for the report's regime summary section:

| Label | Thai | Meaning |
|-------|------|---------|
| `healthy trend` | แนวโน้มแข็งแรง | Trend positive, volatility contained, participation broad enough, no immediate disruptive catalysts |
| `fragile trend` | แนวโน้มเปราะบาง | Trend positive but participation narrow or volatility elevated |
| `transition` | กำลังเปลี่ยนผ่าน | Anchors conflict — no single tactic deserves high conviction |
| `defensive` | ระมัดระวัง | Trend negative/unstable, volatility elevated, participation weak, or catalyst-heavy | 

## Evidence that would invalidate the analysis

- A decisive break of the key level (support or resistance) the analysis relied on
- Volatility expands or contracts enough to change the tactic set (e.g., breakout vs range)
- A macro event (Fed, CPI, Thai policy) changes the backdrop materially
- The timeframe considered changes (intraday regime ≠ swing regime ≠ investment regime)

## Output structure for the report

Within the bilingual report, produce a **Regime Summary section** after the price table:

```
**🇹🇭 ภาวะตลาด:** แนวโน้มแข็งแรง แต่ความผันผวนกำลังเพิ่มขึ้น แนวรับ 64,850 / แนวต้าน 65,900
**🇺🇸 Market Regime:** Healthy trend, volatility expanding. Support 64,850 / Resistance 65,900.
```

Then expand in the body with the four-anchor evidence.

## Best practices
- do not forecast returns from a single regime label
- do not turn limited evidence into precise confidence scores
- label the regime in Thai first, then mirror in English
- use the history file after 5+ accumulated entries to compare "same time last week/month" trends
