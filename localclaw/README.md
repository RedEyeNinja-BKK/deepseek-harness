# LocalClaw DSH extension layer (pinned to DSH 0.1.1-rc.2)

This directory contains an operator's production deployment layer for
[DeepSeek Harness (`dsh`)](https://github.com/deepseek-ai/deepseek-harness) —
companion services, configuration, and analytical tooling that live entirely
**outside** the upstream source tree.

> **Zero core patches.** Every intentional customization in this layer is
> out-of-tree: DSH-native Cordis composition inserts, DSH-native settings, a
> workspace asset directory, and sidecar systemd services. No file outside
> `localclaw/` in this repository is modified by this layer. The installed
> upstream package was verified byte-identical to its published npm tarballs
> (`@deepseek-ai/dsh@0.1.1-rc.2`, 511/511 lockfile packages clean).

## Version qualification

This branch is **`localclaw/dsh-v0.1.1-rc.2`**, rooted at upstream tag
**`dsh-v0.1.1-rc.2`** (commit `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e`).

Everything here was qualified against `@deepseek-ai/dsh@0.1.1-rc.2` — the
published npm package — **not** against current `master`. Future DSH upgrades
should get a new version-qualified `localclaw/dsh-<version>` branch after that
release is qualified, rather than silently rebasing the production baseline
onto moving upstream. `master` is kept as a clean upstream mirror with zero
LocalClaw commits.

## Layout

```
localclaw/
├── config/
│   ├── cordis.patch.yml      # DSH home-level Cordis composition inserts (all profiles)
│   ├── settings.example.yaml # reconstructed example of accepted model/default settings
│   └── env.example           # environment variable NAMES contract (values are operator-staged)
├── install/
│   ├── package.json          # deployment wrapper manifest — installs the PUBLISHED
│   │                         #   @deepseek-ai/dsh@0.1.1-rc.2 package from npm
│   └── package-lock.json     # exact lockfile of that deployment
├── integrations/
│   ├── line/                 # LINE Messaging API webhook listener -> DSH session RPC
│   ├── discord/              # Discord gateway listener + privileged send gate (socket) + MCP adapter
│   └── webgate/              # public-destination-only web gate (CloakBrowser-backed) + MCP adapter
├── plugins/
│   └── schedule-boot-rearm/  # S1 native boot re-arm plugin (replaces the S-01 materializer oneshot)
├── gold/                     # daily Thai gold-market report definitions, charter,
│                             # parity checker, analytical frameworks (MIT, see NOTICE)
├── systemd/dsh.service       # hardened main-service example
├── tests/                    # how to run the shipped selftests
└── requirements.txt          # Python sidecar dependency pins
```

## What each piece does

- **`config/cordis.patch.yml`** — DSH-native composition inserts applied to all
  profiles: a `thai_analyst` subagent (ThaiLLM Typhoon via an OpenAI-compatible
  gateway, maxTokens 16384), a `webgate` MCP client, a `discord` MCP client, the
  upstream `schedule` and `time-context` plugins, and the `schedule-boot-rearm`
  row (S1 native boot re-arm; see `plugins/schedule-boot-rearm/`). No upstream
  files are touched.
- **`plugins/schedule-boot-rearm/`** — in-process replacement for the retired
  `dsh-scheduler-materialize` oneshot: after the `schedule` row, on every DSH
  start it `ctx.agents.resume()`s each configured schedule-bearing session so
  the native schedule plugin re-attaches its tools/runtime and persisted
  schedules re-arm in-DSH. No scheduler semantics, no browser `session.*` RPC,
  no gold special-casing. Ships a self-contained overlay battery
  (`test/run-battery.sh`) for the pre-production proof.
- **`integrations/line/`** — stdlib-only LINE webhook listener: HMAC signature
  verification, persistent conversation→session mapping, dedupe with durable
  fail-closed state, Push-API replies with idempotency keys, 4500-char chunking,
  bounded turn polling, and final-output filtering that strips DSH tool-call
  scaffold blocks from replies (the deployed "F3" behavior). See its
  `DEPLOYMENT.md` for the full operator pack.
- **`integrations/discord/`** — same architecture for Discord: a gateway listener
  (discord.py), a socket-activated privileged send gate holding the bot token via
  systemd `LoadCredential`, and a zero-secret stdio MCP adapter DSH talks to.
- **`integrations/webgate/`** — web authority gate: DSH's `web_search`/`web_fetch`
  MCP calls are validated against an explicit public-destination denylist
  (RFC 6890 families), DNS-rebinding-closed by connection pinning, and executed in
  a hard-killed per-request worker against a pinned browser manager.
- **`gold/`** — product definition (`GOLD_DEF.md`), analyst charter
  (`ADVISOR_CHARTER.md`), a deterministic Thai/English parity gate
  (`bilingual_parity_check.py`), three adapted analytical frameworks
  (MIT-licensed derivatives of [`marian2js/trading-skills`](https://github.com/marian2js/trading-skills)
  — see `gold/frameworks/NOTICE.md`), and the history/provenance *mechanism*.
  Runtime gold data (history, reports, raw payloads, receipts, locks) is
  deployment state and is never committed here.

## Install semantics

`install/package.json` + `install/package-lock.json` reproduce a deployment of
the **published npm package `@deepseek-ai/dsh@0.1.1-rc.2`** (from
registry.npmjs.org). They do **not** build the checked-out Git fork source. If a
real DSH-core patch is ever introduced, the deployment mechanism needs a separate
reviewed change.

Python sidecars (LINE/Discord listeners) install their pinned dependencies from
`requirements.txt`.

## Deployment-local values

Identifiers that differ per deployment are parameterized as placeholders:
`<funnel-host>` (Tailscale Funnel/Serve hostname), `MANAGER_ORIGIN`
(CloakBrowser manager hostname), Discord bot/category IDs, the gold report
Discord channel id, and credential values (never committed — staged by the
operator as root-owned `0600` files consumed via systemd `LoadCredential`).

## Credentials model

No secret ever appears in this repository. All platform credentials (LINE
channel secret/access token, Discord bot token, webgate manager bearer, provider
API keys) are staged by the operator as root-owned `0600` files under `/root/…`
and materialized into services via systemd `LoadCredential` / `EnvironmentFile`.
Sidecar processes hold no credentials unless the unit grants them.
