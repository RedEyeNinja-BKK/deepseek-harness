# History backfill provenance (2026-09-04)

The pre-offload analytical history (2026-07-29 through 2026-09-02, 32 entries
backfilled) was migrated from the pre-offload supervisor-side gold store so that
day/week/month comparisons, observed ranges, and support/resistance evidence
remain available to the DSH-native pipeline. Source: the retired supervisor-side
store's history.json (33 entries; the 2026-09-02 entry was superseded by the
native entry). The runtime history.json itself is deployment state and is never
committed to this repository.
Backfilled entries are marked "legacy": true; they may lack fields the native
schema has (e.g. as_of, ornament_buy, change_prev_day) — absence means the
data was not captured, never fabricate it. Native entries (2026-09-02 onward)
are unmarked and authoritative.
