#!/usr/bin/env python3
"""dsh_line_admission.py — operator CLI for the DSH LINE family admission state.

The single operator mechanism for Increment 1: inspect candidates, approve the
family group, revoke it, grant/revoke DM access, and perform explicit
operator-confirmed person binding (cross-channel identity merge). No web
console, no second service — this script + the durable admission-state.json
the service already reads.

Run as root (state dir is service-owned 0700):

    sudo python3 /opt/dsh-line/dsh_line_admission.py list
    sudo python3 /opt/dsh-line/dsh_line_admission.py pending
    sudo python3 /opt/dsh-line/dsh_line_admission.py approve <groupId> [--name "..."]
    sudo python3 /opt/dsh-line/dsh_line_admission.py revoke <groupId> [--no-leave]
    sudo python3 /opt/dsh-line/dsh_line_admission.py grant-dm <lineUserId> [--label "..."]
    sudo python3 /opt/dsh-line/dsh_line_admission.py revoke-dm <lineUserId>
    sudo python3 /opt/dsh-line/dsh_line_admission.py bind <personId> line|discord <channelUserId>
    sudo python3 /opt/dsh-line/dsh_line_admission.py show <groupId>

Review-hardened (Hermes run_d0de92a437474374b73fcfa4d9ecfd65):
- every mutation runs as ONE exclusive-lock read-modify-write transaction
  (load + FULL schema validation + mutate + post-validate + atomic write under
  a single flock) — no lost updates against the service, no partial writes;
- the FULL validator (same rules as the adapter, plus duplicate-binding
  detection) gates every load AND every write — malformed state is never
  mutated or persisted;
- persistence failures print ERROR and exit nonzero BEFORE any success message
  (a command that did not durably land never claims success);
- approve requires an existing PENDING candidate captured from a real webhook
  observation — approval from memory/guessed IDs is refused by design.

Writes are atomic (tmp + fsync + rename + dir fsync) and the file's ownership
is kept on the service identity so the adapter can keep writing.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import pwd
import sys
import time
from pathlib import Path

STATE_DIR = Path(os.environ.get("STATE_DIRECTORY", "/var/lib/dsh-line-inbound")
                 .split(":")[0])
ADMISSION_PATH = STATE_DIR / "admission-state.json"
LOCK_PATH = STATE_DIR / "admission.lock"
SERVICE_USER = "dsh-discord"
ADM_STATES = ("PENDING", "APPROVED", "DECLINED")
DEFAULT = {"version": 1, "persons": {}, "groups": {}, "dmGrants": {"line": {}},
           "dmDenials": {"line": {}}}


def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def _full_validate(adm) -> None:
    """Complete schema validation (review finding 1). Raises ValueError on ANY
    violation — the CLI refuses to read, mutate, or write such state."""
    if not isinstance(adm, dict) or adm.get("version") != 1:
        raise ValueError("version missing/wrong")
    if set(adm) - {"version", "persons", "groups", "dmGrants", "dmDenials"}:
        raise ValueError("unexpected top-level keys")
    persons, groups = adm.get("persons"), adm.get("groups")
    if not isinstance(persons, dict) or not isinstance(groups, dict):
        raise ValueError("persons/groups must be dicts")
    grants = (adm.get("dmGrants") or {}).get("line")
    if grants is not None and not isinstance(grants, dict):
        raise ValueError("dmGrants.line must be a dict")
    denials = (adm.get("dmDenials") or {}).get("line")
    if denials is not None and not isinstance(denials, dict):
        raise ValueError("dmDenials.line must be a dict")
    for uid, d in (denials or {}).items():
        if not isinstance(d, dict) or "deniedAt" not in d:
            raise ValueError(f"dm denial {uid!r}: bad record")
    seen_bindings = {"line": {}, "discord": {}}
    for pid, p in persons.items():
        if not isinstance(p, dict) or not p.get("personId", "").startswith("p-"):
            raise ValueError(f"person {pid!r}: bad record/id")
        b = p.get("bindings")
        if not isinstance(b, dict) or set(b) - {"line", "discord"}:
            raise ValueError(f"person {pid!r}: bad bindings")
        for ch in ("line", "discord"):
            ids = b.get(ch, [])
            if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                raise ValueError(f"person {pid!r}: {ch} bindings must be str list")
            if len(set(ids)) != len(ids):
                raise ValueError(f"person {pid!r}: duplicate {ch} binding")
            for cid in ids:
                if cid in seen_bindings[ch]:
                    raise ValueError(f"{ch} id {args_mask(cid)} bound to multiple "
                                     f"persons ({seen_bindings[ch][cid]}, {pid})")
                seen_bindings[ch][cid] = pid
        if not isinstance(p.get("dmEligible"), dict):
            raise ValueError(f"person {pid!r}: dmEligible missing")
    for gid, g in groups.items():
        if not isinstance(g, dict) or g.get("state") not in ADM_STATES:
            raise ValueError(f"group {gid!r}: bad state record")
        roster = g.get("roster", [])
        if not isinstance(roster, list) or not all(isinstance(x, str) for x in roster):
            raise ValueError(f"group {gid!r}: roster must be str list")
        if len(set(roster)) != len(roster):
            raise ValueError(f"group {gid!r}: duplicate roster entries")
    if grants is not None:
        if not isinstance(grants, dict):
            raise ValueError("dmGrants.line must be a dict")
        for uid, g in grants.items():
            if not isinstance(g, dict) or not isinstance(g.get("personId"), str):
                raise ValueError(f"dm grant {uid!r}: bad record")
            if g.get("personId") not in persons:
                raise ValueError(f"dm grant {uid!r}: references unknown person")


def args_mask(v):
    return v if isinstance(v, str) and len(v) < 60 else "<id>"


def transact(mutator, *, create_ok: bool = True) -> dict:
    """One exclusive-lock RMW transaction. mutator(adm) mutates in place and
    returns nothing. Persistence failure => exit 5 (never reports success).
    Malformed state => exit 3 (never mutated)."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        raw = ADMISSION_PATH.read_text() if ADMISSION_PATH.exists() else ""
        try:
            adm = json.loads(raw) if raw.strip() else json.loads(json.dumps(DEFAULT))
        except Exception as exc:
            fail(f"admission state unreadable ({exc.__class__.__name__}) — NOT "
                 f"modifying; resolve the file manually first", 3)
        try:
            _full_validate(adm)
        except ValueError as exc:
            fail(f"admission state MALFORMED ({exc}) — refusing to mutate; "
                 f"resolve the file manually first", 3)
        mutator(adm)
        try:
            _full_validate(adm)
        except ValueError as exc:
            fail(f"mutation would produce invalid state ({exc}) — refused", 3)
        tmp = STATE_DIR / f"admission.tmp.cli.{os.getpid()}"
        try:
            with open(tmp, "w") as fh:
                fh.write(json.dumps(adm, ensure_ascii=False))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, ADMISSION_PATH)
            dfd = os.open(STATE_DIR, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError as exc:
            fail(f"persistence FAILED ({exc.__class__.__name__}) — admission "
                 f"change did NOT land; service still sees prior state", 5)
        try:
            pw = pwd.getpwnam(SERVICE_USER)
            os.chown(ADMISSION_PATH, pw.pw_uid, pw.pw_gid)
        except (KeyError, PermissionError, OSError):
            pass  # non-root (tests) or unknown service user: keep current owner
        return adm
    finally:
        os.close(lock_fd)


# kept name for the load-only paths (list/pending/show)
def load():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        raw = ADMISSION_PATH.read_text() if ADMISSION_PATH.exists() else ""
    finally:
        os.close(lock_fd)
    try:
        adm = json.loads(raw) if raw.strip() else json.loads(json.dumps(DEFAULT))
    except Exception as exc:
        fail(f"admission state unreadable ({exc.__class__.__name__})", 2)
    try:
        _full_validate(adm)
    except ValueError as exc:
        fail(f"admission state MALFORMED ({exc}) — display only; fix the file "
             f"before any mutation", 3)
    return adm


def cmd_list(args):
    adm = load()
    print("== groups ==")
    for gid, g in sorted(adm["groups"].items()):
        name = (g.get("summary") or {}).get("name") or "?"
        flags = ""
        if g.get("left"):
            flags += " left"
        elif g.get("leaveRequested"):
            flags += " leave-requested"
        print(f"  {gid}  {g.get('state'):9s} kind={g.get('kind')} name={name!r} "
              f"firstSeen={g.get('firstSeen')} roster={len(g.get('roster') or [])}{flags}")
    print("== persons ==")
    for p in adm["persons"].values():
        print(f"  {p['personId']}  label={p.get('label') or '-'}  "
              f"line={p['bindings'].get('line') or '-'}  "
              f"discord={p['bindings'].get('discord') or '-'}  "
              f"dmEligible={p.get('dmEligible')}")
    print("== dm grants ==")
    for uid, g in (adm.get("dmGrants", {}).get("line") or {}).items():
        print(f"  {uid}  person={g.get('personId')}  at={g.get('grantedAt')}")
    if not adm["groups"]:
        print("  (no conversations observed yet)")


def cmd_pending(args):
    adm = load()
    pend = {g: d for g, d in adm["groups"].items() if d.get("state") == "PENDING"}
    if not pend:
        print("no PENDING candidates")
        return
    for gid, g in pend.items():
        name = (g.get("summary") or {}).get("name") or "(summary unavailable)"
        print(f"  {gid}\n    kind={g.get('kind')} name={name!r} "
              f"firstSeen={g.get('firstSeen')} roster={g.get('roster') or []}\n"
              f"    approve: sudo python3 /opt/dsh-line/dsh_line_admission.py approve {gid}")


def cmd_show(args):
    adm = load()
    print(json.dumps({"groups": {args.group_id: adm["groups"].get(args.group_id)}},
                     ensure_ascii=False, indent=1))


def cmd_approve(args):
    gid = args.group_id
    found = {}

    def _m(adm):
        g = adm["groups"].get(gid)
        if not g:
            fail(f"{gid} has no candidate record — the group must send at "
                 f"least one message (or a join event) first so a stable "
                 f"candidate with metadata exists. Do not approve from memory.", 4)
        if g.get("state") == "APPROVED":
            fail(f"{gid} is already APPROVED", 4)
        others = [o for o, d in adm["groups"].items()
                  if o != gid and d.get("state") == "APPROVED"]
        if others:
            fail(f"another group is already APPROVED ({others[0]}) - revoke it "
                 f"first: revoke {others[0]}", 4)
        g["state"] = "APPROVED"
        g["decidedAt"] = now()
        g["decidedBy"] = "operator"
        if args.name:
            g["summary"] = {"name": args.name}
        g["leaveRequested"] = False
        found["ok"] = True
        return adm
    transact(_m)
    print(f"APPROVED {gid}" + (f" as {args.name!r}" if args.name else ""))


def cmd_revoke(args):
    gid = args.group_id

    def _m(adm):
        g = adm["groups"].get(gid)
        if not g:
            fail(f"{gid} has no admission record", 4)
        g["state"] = "DECLINED"
        g["decidedAt"] = now()
        g["decidedBy"] = "operator"
        g["leaveRequested"] = not args.no_leave
        return adm
    transact(_m)
    print(f"DECLINED {gid}"
          + ("" if args.no_leave else " (service will leave the group on its "
                                      "next maintenance pass)"))


def cmd_grant_dm(args):
    uid = args.line_user_id

    def _m(adm):
        existing = [p for p in adm["persons"].values()
                    if uid in (p.get("bindings") or {}).get("line", [])]
        if len(existing) > 1:
            fail("LINE user bound to MULTIPLE persons — corrupt state, refusing", 3)
        if existing:
            person = existing[0]
        else:
            pid = "p-" + os.urandom(16).hex()
            adm["persons"][pid] = {"personId": pid, "label": args.label,
                                   "bindings": {"line": [uid], "discord": []},
                                   "dmEligible": {"line": True, "discord": False},
                                   "createdAt": now(), "updatedAt": now()}
            person = adm["persons"][pid]
        adm.setdefault("dmGrants", {}).setdefault("line", {})[uid] = {
            "personId": person["personId"], "grantedAt": now()}
        # explicit grant-dm is the documented operator reversal of a denial
        (adm.get("dmDenials", {}).get("line") or {}).pop(uid, None)
        return adm
    adm = transact(_m)
    person = [p for p in adm["persons"].values()
              if uid in (p.get("bindings") or {}).get("line", [])][0]
    print(f"DM GRANTED {uid} -> {person['personId']}")


def cmd_revoke_dm(args):
    uid = args.line_user_id

    def _m(adm):
        (adm.get("dmGrants", {}).get("line") or {}).pop(uid, None)
        for g in adm["groups"].values():
            roster = g.get("roster") or []
            while uid in roster:  # eligibility must not silently persist
                roster.remove(uid)
        # durable operator denial: survives later approved-group chatter
        # (rostering) and identity bindings until an explicit grant-dm clears it
        adm.setdefault("dmDenials", {}).setdefault("line", {})[uid] = {
            "deniedAt": now(), "by": "operator"}
        return adm
    transact(_m)
    print(f"DM REVOKED {uid} (durable denial - grant-dm to reverse)")


def cmd_bind(args):
    pid, ch, cid = args.person_id, args.channel, args.channel_user_id

    def _m(adm):
        p = adm["persons"].get(pid)
        if not p:
            fail(f"person {pid} does not exist (see: list)", 4)
        for other in adm["persons"].values():  # binding MOVES (explicit merge)
            bl = (other.get("bindings") or {}).get(ch, [])
            if cid in bl and other["personId"] != pid:
                bl.remove(cid)
                other["updatedAt"] = now()
        bl = p["bindings"].setdefault(ch, [])
        if cid not in bl:
            bl.append(cid)
        p["updatedAt"] = now()
        return adm
    transact(_m)
    print(f"BOUND {ch}:{cid} -> {pid}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    sub.add_parser("pending").set_defaults(fn=cmd_pending)
    a = sub.add_parser("show"); a.add_argument("group_id"); a.set_defaults(fn=cmd_show)
    a = sub.add_parser("approve"); a.add_argument("group_id"); a.add_argument("--name"); a.set_defaults(fn=cmd_approve)
    a = sub.add_parser("revoke"); a.add_argument("group_id"); a.add_argument("--no-leave", action="store_true"); a.set_defaults(fn=cmd_revoke)
    a = sub.add_parser("grant-dm"); a.add_argument("line_user_id"); a.add_argument("--label"); a.set_defaults(fn=cmd_grant_dm)
    a = sub.add_parser("revoke-dm"); a.add_argument("line_user_id"); a.set_defaults(fn=cmd_revoke_dm)
    a = sub.add_parser("bind"); a.add_argument("person_id"); a.add_argument("channel", choices=["line", "discord"]); a.add_argument("channel_user_id"); a.set_defaults(fn=cmd_bind)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()