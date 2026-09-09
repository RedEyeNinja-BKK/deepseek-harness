#!/usr/bin/env python3
"""mock_tmpfiles.py — hermetic systemd-tmpfiles double (type 'd' rules only).

Test-only tooling: parses the gate-written tmpfiles rule
`d <path> <mode> <owner> <group> -` and creates the directory with chown/chmod
(including the setgid bit). Production uses the real systemd-tmpfiles.
"""

from __future__ import annotations

import grp
import os
import pwd
import sys


def main():
    args = sys.argv[1:]
    if "--create" not in args:
        return 0
    conf = None
    for a in args:
        if a == "--create":
            continue
        if a.startswith("-"):
            continue
        conf = a
        break
    if not conf or not os.path.exists(conf):
        return 0
    for line in open(conf, encoding="utf-8"):
        parts = line.split()
        if not parts or parts[0] != "d":
            continue
        if len(parts) < 5:
            continue
        path, mode, owner, group = parts[1], parts[2], parts[3], parts[4]
        try:
            os.makedirs(path, exist_ok=True)
            os.chmod(path, int(mode, 8))
            try:
                uid = pwd.getpwnam(owner).pw_uid
            except KeyError:
                uid = -1
            try:
                gid = grp.getgrnam(group).gr_gid
            except KeyError:
                gid = -1
            os.chown(path, uid, gid)
        except Exception as e:
            print(f"mock tmpfiles: {path}: {e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
