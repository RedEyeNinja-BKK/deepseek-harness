# S2 live-gate hermetic regression battery

`dsh_s2_gate_regression.sh` proves the corrected `dsh_s2_live_gate.sh`
transaction semantics WITHOUT touching production. Every path is re-pointed into
a fresh overlay under `/tmp/dsh-s2-gate-regression`; `systemctl`,
`journalctl` and `systemd-tmpfiles` are doubles; the "plugin" is a real
AF_UNIX seam server (`mock_plugin_server.py`) that boots when the mocked
`dsh.service` restarts and the plugin file is present.

Run as any user that is a member of the `dsh-media` group (chgrp of the
overlay socket is exercised):

    test/gate/dsh_s2_gate_regression.sh

Requires `python3`, `zstd`, and the REAL reviewed candidate bytes + the REAL
live listener bytes at their current locations (the gate asserts exact hashes
against the reviewed constants, so fixtures must be the real files).

## Coverage (requirement #14 regression list)

| # | scenario | outcome asserted |
|---|----------|------------------|
| 1 | clean baseline preflight with socket absent | PASS, zero mutation, socket NOT required |
| 2 | bad listener source hash | stage refuses, zero mutation |
| 3 | bad binding (session map mismatch) | stage refuses, zero mutation |
| 4 | bad session/pin (duplicate logs; pin mismatch) | stage refuses, zero mutation |
| 5 | invalid operator inputs | stage refuses, zero mutation |
| 6 | listener install/restart failure | full baseline rollback |
| 7 | listener health failure after restart | full baseline rollback |
| 8 | plugin hash mismatch | refused before ANY mutation / dsh restart |
| 9 | plugin mount/readiness failure | composition restored (full baseline) |
| 10 | socket wrong group | full baseline rollback |
| 11 | staged success | route stays OLD; default-off proven; socket/session/pin proofs |
| 12 | activate without staged receipt | refused, zero mutation |
| 13 | activate | only the exact pilot is affected; sibling untouched |
| 13b | activate listener restart crash | auto-rollback to staged OLD |
| 14 | rollback | exact pilot back to OLD; env fence kept; plugin mounted |
| 15 | sibling authority | never changed (state hash + route/env single-key proofs) |
| 14b | restore-baseline | full byte rollback to pre-S2 state |
| 15b | re-stage while row mounted | refused (no double stage) |

## Files

- `mock_systemctl.py` — systemctl double (units, MainPID, daemon-reload,
  EnvironmentFile expansion into `/proc`-style environ).
- `mock_plugin_server.py` — AF_UNIX seam server: hello-ack route echo,
  route-ack, reconcile log lines, socket mode/group fault injection.
- `mock_tmpfiles.py` — type-`d` systemd-tmpfiles double (creates the setgid
  socket dir with the configured owner/group).
- `dsh_s2_gate_regression.sh` — the battery itself.

These doubles are TEST-ONLY tooling; production uses the real binaries and the
real systemd units.
