# Tests shipped with this layer

No test framework is required — the shipped selftests are self-contained.

## F3 final-output filtering regression battery (11 checks)

```bash
python3 localclaw/tests/test_f3_final_output.py localclaw/integrations/line/dsh_line_inbound.py
```

This is the dedicated F3 qualification suite used for deployment qualification
(11/11 PASS against the captured `dsh_line_inbound.py`). It loads the adapter
module read-only (credentials load lazily; state directory is overridden to a
temp dir) and runs `extract_reply_for_dispatch()` against a replica of the
observed multi-step turn shape, including the RED final tool-call-block
suppression case, GREEN extraction cases (ordinary, multi-part, media-bearing),
fail-closed cases (tool-only turn end, unfinished turn, duplicate dispatch
marker, whitespace-only final), and the three review coverage findings:
last-assistant-before-turn/end wins (F1), malformed blocks ignored (F2),
whitespace-only final suppressed (F3). Exit 0 = all pass.
Passing no argument targets the default installed location
`/opt/dsh-line/dsh_line_inbound.py`.

## LINE listener selftest (66 checks)

```bash
python3 localclaw/integrations/line/dsh_line_inbound.py --selftest
```

Covers: webhook signature roundtrip/tamper/missing, chunking boundaries,
reply-marker extraction (ambiguity + splice immunity), dispatch trust gates,
profile paths, delivery claim/finish reservation, stale-claim recovery,
persistence-failure fail-closed, and malformed-state normalization.
Exit code 0 = all pass.

The listener also supports a live smoke cycle without LINE traffic:
`GET /line/health` → 200; unsigned POST → 403; correctly signed POST → 200;
duplicate/oversized bodies rejected. See `integrations/line/DEPLOYMENT.md`.

## Thai/English parity gate

```bash
python3 localclaw/gold/bilingual_parity_check.py <report.md>
```

Exit 0 = parity OK; 1 = critical parity failure (do not emit); 2 = structural
failure. Deterministic, stdlib-only — no LLM judgment.

## Webgate guard unit checks

The gate's public-destination guard is exercised in-process by
`integrations/webgate/webgate_guard.py` (fail-closed denylist classification);
deployment verification steps are documented in the webgate unit file comments.
