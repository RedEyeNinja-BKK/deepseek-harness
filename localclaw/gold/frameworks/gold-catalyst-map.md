---
name: gold-catalyst-map
description: Build a ranked map of catalysts that could move Thai gold prices over the report's outlook window — showing what matters, when it matters, and how events transmit through USD, world gold, and local demand.
---

# Thai Gold Catalyst Map

Use this skill when the daily report needs a forward-looking catalyst section that connects macro events, seasonal patterns, and policy signals to potential gold price action over the coming days to weeks.

This skill will not:
- predict the market reaction to a catalyst
- pretend every calendar item deserves equal weight in a daily report
- replace deeper macro-event analysis when a high-impact catalyst is imminent

## Role

Act like a cross-asset catalyst planner for gold. Identify the events and patterns that matter, show their transmission path, and distinguish between "watch closely" and "background context."

## Data sources

- Current gold prices and trends (from Phase 1 fact sheet)
- Accumulated history at `/opt/dsh/workspace/gold-report/history.json` for seasonal comparisons
- Upstream full framework: upstream framework (see NOTICE.md)
- Publicly known macro calendar and Thai holiday schedule

## Core Assessment Framework

Assess each catalyst on five anchors:

| Anchor | Gold-specific meaning |
|--------|----------------------|
| **Timing Relevance** | Does the catalyst fall inside the report's outlook window (typically 1-7 days)? |
| **Transmission Strength** | Does it directly affect USD, world gold, USD/THB, or Thai gold demand? |
| **Decision Impact** | Could it change a reader's holding decision or risk tolerance? |
| **Overlap** | Do multiple catalysts converge on the same window? |
| **Preparation Need** | Does the report need to alert readers now, or just note it? |

## Gold-specific catalyst types

| Type | Examples | Transmission to Thai gold |
|------|----------|--------------------------|
| Central bank | Fed rate/dot plot, BoT policy | USD → world gold → THB parity → local price |
| US data | CPI, NFP, PCE, GDP | Real rates → gold spot → THB parity |
| Geopolitical | Middle East, trade war, sanctions | Risk appetite → gold spot (safe haven) → local |
| Thai-specific | Gold import tax, BoT rate, holiday demand | Direct local demand shift |
| Seasonal | Songkran, CNY, Wedding season, Loi Krathong | Retail ornament demand spike |
| Supply-side | Mining output, central bank buying | Structural but slow — background only |

## Classification for the report

| Class | Meaning | What the report says |
|-------|---------|---------------------|
| `primary` | Within window, strong transmission | Dedicated section with explanation |
| `secondary` | Worth awareness, not imminent | Brief note in outlook |
| `background` | Context only | Optional — useful for Trader's Notes education |
| `none` | Quiet week ahead | State explicitly: "No major catalysts — gold driven by technicals and local demand" |

## Output structure

If confirmed catalysts exist in the outlook window, add a **Catalyst Watch** subsection under Macro Outlook:

```
🇹🇭 จับตาเหตุการณ์:
• CPI สหรัฐ (พุธ) → หลัก — ส่งผลผ่านดอลลาร์ สู่ทองคำโลก สู่พาริตี้
• ประชุม Fed (พฤหัส) → หลัก — ตลาดปิดรับความเสี่ยงรอผล
• ไม่มีเหตุการณ์เฉพาะไทยในสัปดาห์นี้

🇺🇸 Catalyst Watch:
• US CPI (Wed) → Primary — transmits through USD → world gold → parity
• Fed decision (Thu) → Primary — markets consolidate ahead
• No Thai-specific catalysts this week
```

## Best practices
- do not confuse more calendar items with better preparation
- do not list catalysts without explaining transmission to GOLD (not just generic market impact)
- where applicable, point to history entries showing past gold reactions to similar events
- if no catalysts exist, say so — "quiet week" is useful information
