# Gold frameworks — provenance

The three frameworks in this directory are adapted (gold-market-specific)
derivatives of upstream `SKILL.md` frameworks from
[`marian2js/trading-skills`](https://github.com/marian2js/trading-skills) (MIT —
see `NOTICE.md`).

| file | upstream framework lineage | adopted sha256 (post line-reference correction) |
|---|---|---|
| gold-market-regime-analysis.md | market-context/regime-analysis | `19ff784092a13a12d51e72326e6ed4b55d1e31931f53ef56a17828b991f27e07` |
| gold-macro-event-analysis.md | market-context/macro-event-analysis | `bdaa6c54215bd339ba88f05825a88352a6e56a65b78a6409defdf381a8cc220b` |
| gold-catalyst-map.md | idea-discovery/catalyst-map | `b792d15ff0fea2df8538bd7258bd3e4f1f5902e33d138ac96414501f6897be05` |

Adoption record (2026-09-04): the frameworks were adopted into the deploying
agent's local storage so the sandboxed DSH runtime could read them (its
filesystem policy skips reads outside its own home). Copies were hash-verified
byte-exact at adoption; afterward, operational data-source lines inside two
files were corrected to point at the canonical runtime history store (those
lines are informational provenance, not runtime data sources — the report
definition governs actual data sources at runtime). The pre-correction
byte-exact hashes were: regime `c1445fea…` (c1445feada7719e08f097851f6d88c27addf7389cc65216894a0a2bc1c745976),
macro `bdaa6c54…` (unchanged), catalyst `081db7d9…` (081db7d9e499b7625a39f33769fa943387bbffbb6953cfc947d72658bf78bf9a).

For this repository's publication, machine-local path references were replaced
by project-level upstream references; license provenance is preserved in
`NOTICE.md`.
