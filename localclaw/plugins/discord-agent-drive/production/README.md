# S2 production consumer candidate (Discord inbound) — README

NON-PRODUCTION until the S2 live cutover GO. This directory carries the exact
bytes that WOULD be deployed at GO (already reviewed + isolated-tested; see
report/evidence/s2-r4-consumer-candidate-2026-09-09). No production file is
overwritten by this tree.

## Pieces (one helper + one surgical listener patch + tests)
- `dsh_discord_inbound.s2.py` — the ACTUAL production listener candidate =
  current live `/opt/dsh-inbound/dsh_discord_inbound.py` (sha
  528cb84a…) + the S2 pilot consumer delta (~245 added lines; 0 removed):
  - default-off: `S2_PILOT_CONV=""` => historical path exactly as today; the
    seam is never connected, no pilot routing, every conversation unchanged;
  - `S2_PILOT_CONV=<exact conv key>` => that conversation routes through the
    native plugin seam while route control says S2_ACTIVE / QUIESCING_TO_OLD;
    OLD returns new events to the historical path but NEVER resubmits a message
    already S2-attempted (single DSH-driving authority);
  - structural suppression of the pilot old path (no session.create/prompt/
    history, no old _WATCHERS/_backstop_loop/_flush_turn while S2 owns it) with
    explicit `old-path ... conv=<key>` tripwire logs for the live gate;
  - retains external authority: attachment ingest, envelope normalization,
    session map (read-only on the S2 path; NO create), the existing
    inbound-state.json ledger, `_deliver_channel` / `_deliver_media_file`,
    media-identity ledger.
- `s2_seam.py` — the ONE tiny helper (no daemon/process/socket/ledger/framework
  of its own): async AF_UNIX JSONL client to the plugin seam + gate-owned
  route-control reader + minimal delivered-finalization ledger helpers inside
  the EXISTING state file + finalization delivery with the honest
  **at-most-once automatic external send with durable fail-visible
  INDETERMINATE state** semantics (no "exactly once" overclaim).
- `test/s2_consumer_battery.py` — integration battery on the real candidate
  bytes (mocked Discord + fake seam UNIT 13/13; REAL plugin seam INT 7/7;
  flag-empty old-path regression 2/2).

## State schema delta (inside the existing inbound-state.json; NO migration)
`state["s2"] = {
  "delivered_finalizations": {conv_key: {fid: {state: pending|delivered|indeterminate, at, [kind]}}},
  "attempted": {conv_key: {discord_message_id: {state: claimed|ambiguous|quiesced|rejected:<code>|no-*, at}}}
}`
`state["media_delivered"][conv_key]` is reused for S2 media identity suppression.
Route control lives OUTSIDE the state file in a gate-owned file
`STATE_DIR/s2-route.json` = `{"<conv_key>": "OLD"|"S2_ACTIVE"|"QUIESCING_TO_OLD"}`
(the listener only reads it; the gate/operator writes it; missing/corrupt => OLD).

## AF_UNIX boundary (production contract; not yet staged)
Plugin (user dsh 993) creates the socket dir 0770 + socket 0660 with group
`dsh-media` (1001) so the consumer (dsh-discord 988, also a member) connects
without world access; mirrors the existing `/run/dsh-discord/gate.sock` pattern.
Cutover gate must read back owner/group/mode before activating the route.

## Deployment at GO (operator; NOT done)

Use the transactional live gate `dsh_s2_live_gate.sh` (gate-ONLY repair
revision, 2026-09-09 — candidate application bytes unchanged). Modes:

    # operator inputs are env vars (exact binding + captured pins):
    #   S2_PILOT_CONV / S2_PILOT_SID / S2_PROVIDER / S2_MODEL
    #   S2_REASONING_EFFORT / S2_MAX_TOKENS (optional)
    dsh_s2_live_gate.sh preflight          # read-only baseline (no socket req.)
    dsh_s2_live_gate.sh stage              # listener+helper+plugin/composition,
                                           # setgid socket dir, ONE dsh restart,
                                           # hello-only readiness probe; route OLD
    dsh_s2_live_gate.sh activate           # needs PASSED stage; EnvironmentFile
                                           # drop-in + route S2_ACTIVE + seam proof
    dsh_s2_live_gate.sh rollback           # authority -> OLD (quiesce); env fence
    dsh_s2_live_gate.sh restore-baseline   # full byte rollback to pre-S2

Transaction evidence is written under the report evidence dir (per-run txn).
Hermetic regression battery: `test/gate/dsh_s2_gate_regression.sh` (116 checks).

Old manual outline (superseded by the gate):
1. Byte-verify staged == reviewed hashes.
2. `install dsh_discord_inbound.s2.py` as the listener (atomic replace + restart
   of dsh-discord-inbound only) and `install s2_seam.py` beside it; verify the
   live listener sha advances from 528cb84a… to b9907f40….
3. Mount the plugin row + restart dsh.service (plugin-side; the listener bytes
   may land first with S2_PILOT_CONV empty — no behavior change).
4. Write `STATE_DIR/s2-route.json` = S2_ACTIVE for the exact pilot + set
   S2_PILOT_CONV; run the supervised live battery; rollback = route file OLD
   (quiesce) then S2_PILOT_CONV unset on a later restart if desired.
