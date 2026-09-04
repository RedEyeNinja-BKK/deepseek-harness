# Tests shipped with this layer

No test framework is required — the shipped selftests are self-contained.

## LINE listener selftest (40 checks)

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
