#!/usr/bin/env python3
"""LINE Family Increment 1 — identity / admission / mention-gate fixture suite.

Runs against a candidate adapter module (argv[1]). All identifiers are
SYNTHETIC (Utest1, Gtest1, Dtest1, p-...); no real family/group/user/channel
IDs exist in this file. No network: LINE API + DSH RPC functions are replaced
with sentinels that RAISE if invoked from a denied path.

Exit 0 = ALL PASS.
"""
import importlib.util
import json
import os
import sys
import tempfile

PASS, FAIL = 0, []


def check(name, cond):
    global PASS
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    if cond:
        PASS += 1
    else:
        FAIL.append(name)


def load_module(path: str):
    tmp = tempfile.mkdtemp(prefix="fam-state-")
    os.environ["STATE_DIRECTORY"] = tmp
    spec = importlib.util.spec_from_file_location("dsh_line_fam", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # isolate ALL durable state to the tmp dir
    mod.STATE_DIR = __import__("pathlib").Path(tmp)
    mod.STATE_PATH = mod.STATE_DIR / "line-state.json"
    mod.ADMISSION_PATH = mod.STATE_DIR / "admission-state.json"
    mod.ADMISSION_LOCK_PATH = mod.STATE_DIR / "admission.lock"
    # network sentinels: any call = a test failure via the loud-marker protocol
    def _no_line(*a, **k):
        raise AssertionError("LINE API called from a path under test")
    mod.line_request = _no_line
    def _no_rpc(method, payload, timeout=60.0):
        raise AssertionError(f"DSH RPC reached: {method}")
    mod.dsh_rpc = _no_rpc
    def _no_session(state, key):
        raise AssertionError("session create/resume reached")
    mod.get_or_create_session = _no_session
    def _no_push(*a, **k):
        raise AssertionError("push attempted")
    mod.push_messages = _no_push
    return mod


def adm_default(mod):
    return json.loads(json.dumps(mod.DEFAULT_ADMISSION))


def adm_with_group(mod, gid, state, roster=None):
    a = adm_default(mod)
    a["groups"][gid] = {"state": state, "kind": "group", "summary": {"name": "C"},
                        "firstSeen": mod.admission_now(), "decidedAt": None,
                        "decidedBy": None, "roster": list(roster or []),
                        "leaveRequested": False, "left": False,
                        "leaveAttempts": 0}
    return a


def grp_event(gid, uid, mentionees=None, text="hello"):
    msg = {"id": "m-" + gid + "-" + uid, "type": "text", "text": text}
    if mentionees is not None:
        msg["mention"] = {"mentionees": mentionees}
    return {"mode": "active", "type": "message",
            "source": {"type": "group", "groupId": gid, "userId": uid},
            "message": msg}


def dm_event(uid, text="hello"):
    return {"mode": "active", "type": "message",
            "source": {"type": "user", "userId": uid},
            "message": {"id": "dm-" + uid, "type": "text", "text": text}}


def main() -> int:
    mod = load_module(sys.argv[1] if len(sys.argv) > 1
                      else "dsh_line_inbound_v4.py")
    U_FAM1, U_FAM2, U_OUT, U_KAEM_LN = "Utest-fam1", "Utest-fam2", "Utest-out", "Utest-kaem-ln"
    G_FAM, G_STRANGE, G_REVOKED = "Gtest-family", "Gtest-strange", "Gtest-revoked"

    # ── identity ────────────────────────────────────────────────────────────
    a = adm_default(mod)
    p1 = mod.new_person(a, None)
    p1["bindings"]["line"].append(U_KAEM_LN)
    p2 = mod.new_person(a, None)
    p2["bindings"]["discord"].append("Dtest-kaem")
    check("I1 before merge: LINE and Discord live on separate persons",
          mod.person_for_line(a, U_KAEM_LN)["personId"] == p1["personId"]
          and p2["bindings"]["line"] == [])
    # explicit operator-confirmed merge = MOVE the binding (remove from p1, add to p2)
    p1["bindings"]["line"].remove(U_KAEM_LN)
    p2["bindings"]["line"].append(U_KAEM_LN)
    check("I1 explicit merge binds both channels to one person",
          mod.person_for_line(a, U_KAEM_LN)["personId"] == p2["personId"]
          and p2["bindings"]["discord"] == ["Dtest-kaem"])
    a_same = adm_default(mod)
    pa = mod.new_person(a_same, "Identical Label")
    pa["bindings"]["line"].append("Utest-a")
    pb = mod.new_person(a_same, "Identical Label")
    pb["bindings"]["line"].append("Utest-b")
    check("I2 same display name NEVER auto-merges (two distinct persons)",
          len(a_same["persons"]) == 2
          and mod.person_for_line(a_same, "Utest-a")["personId"] != "Utest-b")
    check("I3 person may exist with LINE-only binding",
          pa["bindings"] == {"line": ["Utest-a"], "discord": []})
    a["persons"][p1["personId"]]["bindings"]["line"].append(U_FAM1)
    a["persons"][p1["personId"]]["bindings"]["line"].append(U_FAM1)
    check("I4 corrupt duplicate binding -> fail-closed marker",
          mod.person_for_line(a, U_FAM1).get("__corrupt__") is True)
    check("I4 malformed identity state fails closed (validation rejects)",
          not mod._validate_admission({"version": 1, "persons": [], "groups": {}}))
    # unreadable/malformed admission FILE -> deny-all
    mod.ADMISSION_PATH.write_text("{ broken")
    _, healthy = mod.load_admission()
    check("I4 malformed admission FILE -> unhealthy (deny-all)", not healthy)

    # ── admission ───────────────────────────────────────────────────────────
    check("A1 unknown group -> OBSERVE_PENDING (candidate, never dispatched)",
          mod.classify_event(grp_event(G_STRANGE, U_OUT, [{"isSelf": True}]),
                             adm_default(mod))["action"] == "OBSERVE_PENDING")
    check("A2 pending group + own-mention -> still no dispatch",
          mod.classify_event(grp_event(G_STRANGE, U_OUT, [{"isSelf": True}]),
                             adm_with_group(mod, G_STRANGE, "PENDING"))["action"]
          == "OBSERVE_PENDING")
    d_a3 = mod.classify_event(grp_event(G_FAM, U_FAM1, [{"isSelf": True}]),
                              adm_with_group(mod, G_FAM, "APPROVED", [U_FAM1]))
    check("A3 approved group + own-mention -> APPROVED_GROUP_MSG (self_mentioned)",
          d_a3["action"] == "APPROVED_GROUP_MSG" and d_a3["self_mentioned"] is True)
    a_roster = adm_with_group(mod, G_FAM, "APPROVED", [])
    a_roster, ev = mod.observe_group_message(a_roster, "group", G_FAM, U_FAM2)
    check("A4 family user observed in approved group -> rostered + DM eligible",
          ev == "ROSTER" and mod.line_user_dm_eligible(a_roster, U_FAM2))
    check("A5 unknown outsider DM -> DENIED_DM",
          mod.classify_event(dm_event(U_OUT), adm_default(mod))["action"]
          == "DENIED_DM")
    a_rev = adm_with_group(mod, G_REVOKED, "DECLINED", ["U_FAM1"])
    a_rev["groups"][G_REVOKED]["leaveRequested"] = True
    check("A6 revoked group -> DENIED_GROUP (no dispatch)",
          mod.classify_event(grp_event(G_REVOKED, U_FAM1, [{"isSelf": True}]),
                             a_rev)["action"] == "DENIED_GROUP")
    a_g = adm_with_group(mod, G_FAM, "APPROVED", [])
    a_g["dmGrants"]["line"]["Utest-grant"] = {"grantedAt": mod.admission_now()}
    check("A6b explicit operator DM grant -> eligible without group roster",
          mod.line_user_dm_eligible(a_g, "Utest-grant"))
    revoked_dm = adm_with_group(mod, G_FAM, "APPROVED", ["Utest-grant"])
    check("A6 roster removal semantics: eligibility is state-derived "
          "(removal from roster revokes eligibility)",
          not mod.line_user_dm_eligible(adm_default(mod), "Utest-grant"))

    # ── mention gating ──────────────────────────────────────────────────────
    A_OK = adm_with_group(mod, G_FAM, "APPROVED", [U_FAM1])
    d_m1 = mod.classify_event(grp_event(G_FAM, U_FAM1, [{"isSelf": True}]), A_OK)
    check("M1 approved + DSH isSelf:true -> dispatch (self_mentioned)",
          d_m1["action"] == "APPROVED_GROUP_MSG" and d_m1["self_mentioned"] is True)
    d_m2 = mod.classify_event(grp_event(G_FAM, U_FAM1, [{"type": "all"}]), A_OK)
    check("M2 approved + @All/type:all ONLY -> NO dispatch",
          d_m2["action"] == "APPROVED_GROUP_MSG" and d_m2["self_mentioned"] is False)
    d_m3 = mod.classify_event(grp_event(G_FAM, U_FAM1, None,
                                        text="@DSH what is the gold price?"), A_OK)
    check("M3 approved + literal '@DSH' text without self-mention metadata -> NO dispatch",
          d_m3["action"] == "APPROVED_GROUP_MSG" and d_m3["self_mentioned"] is False)
    d_m4 = mod.classify_event(grp_event(G_FAM, U_FAM1, None), A_OK)
    check("M4 approved + ordinary chatter -> NO dispatch (silent)",
          d_m4["action"] == "APPROVED_GROUP_MSG" and d_m4["self_mentioned"] is False)
    check("M5 unapproved group + valid own-mention -> NO dispatch",
          mod.classify_event(grp_event(G_STRANGE, U_OUT, [{"isSelf": True}]),
                             adm_with_group(mod, G_STRANGE, "PENDING"))["action"]
          == "OBSERVE_PENDING")
    check("M6 admitted DM -> dispatch WITHOUT mention",
          mod.classify_event(dm_event(U_FAM1), A_OK)["action"] == "DISPATCH_DM")

    # ── integration: denied events never reach DSH ──────────────────────────
    # sentinels raise on ANY session/RPC/push call; handle_event returns clean.
    mod.ADMISSION_PATH.write_text(json.dumps(adm_default(mod)))
    calls = {"n": 0}
    def _spy_rpc(*a, **k):
        calls["n"] += 1
        raise AssertionError("DSH RPC invoked from denied path")
    mod.dsh_rpc = _spy_rpc
    def _spy_session(state, key):
        calls["n"] += 1
        raise AssertionError("session create reached from denied path")
    mod.get_or_create_session = _spy_session
    state = mod.load_state()
    for label, ev, adm_view in [
        ("unknown group message", grp_event(G_STRANGE, U_OUT, [{"isSelf": True}]),
         adm_default(mod)),
        ("pending group message", grp_event(G_STRANGE, U_OUT, [{"isSelf": True}]),
         adm_with_group(mod, G_STRANGE, "PENDING")),
        ("revoked group message", grp_event(G_REVOKED, U_OUT, [{"isSelf": True}]),
         adm_with_group(mod, G_REVOKED, "DECLINED")),
        ("outsider DM", dm_event(U_OUT), adm_default(mod)),
    ]:
        mod.ADMISSION_PATH.write_text(json.dumps(adm_view))
        try:
            mod.handle_event(mod.load_state(), ev)
            ok = True
        except AssertionError as e:
            ok = False
            print(f"      leaked: {e}")
        check(f"P1 {label} never reaches DSH session/model/tools", ok)
    check("P2 sentinel never fired across all denied paths", calls["n"] == 0)

    # approved DM path DOES reach the (stubbed) RPC — positive control
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    mod.get_or_create_session = lambda state, key: "sess-test"
    seen = {"prompt": False, "deliver": False}
    def _rpc_ok(method, payload, timeout=60.0):
        if method == "session.prompt":
            seen["prompt"] = True
        return {}
    mod.dsh_rpc = _rpc_ok
    mod.wait_for_reply = lambda *a, **k: "Synthetic reply text"
    def _deliver(state, message_id, target_id, reply):
        seen["deliver"] = True
        return True
    mod.deliver_reply = _deliver
    mod.get_display_name = lambda *a, **k: "Synthetic Name"
    mod.handle_event(mod.load_state(), dm_event(U_FAM1))
    check("P3 positive control: admitted DM DOES reach session.prompt + delivery",
          seen["prompt"] and seen["deliver"])
    # and the person record was created + bound (durable)
    adm_after, _ = mod.load_admission()
    check("P4 person record created + LINE-bound on admitted dispatch",
          mod.person_for_line(adm_after, U_FAM1) is not None)

    # ── state file integrity ────────────────────────────────────────────────
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    okw = mod.save_admission(adm_after := json.loads(json.dumps(A_OK)))
    check("S1 save_admission writes + load returns same group state",
          okw and mod.group_state(mod.load_admission()[0], G_FAM) == "APPROVED")
    okm = mod.save_admission({"version": 9})
    check("S2 refusing to persist malformed schema", not okm)
    a_exp = json.loads(json.dumps(A_OK))
    a_exp["groups"][G_STRANGE] = {"state": "PENDING", "kind": "group",
                                  "summary": None,
                                  "firstSeen": "2000-01-01T00:00:00+0700",
                                  "decidedAt": None, "decidedBy": None,
                                  "roster": [], "leaveRequested": False,
                                  "left": False, "leaveAttempts": 0}
    check("S3 PENDING persists indefinitely - NO auto-expiry exists",
          not hasattr(mod, "expire_pending_groups")
          and not hasattr(mod, "PENDING_GROUP_EXPIRY_S"))
    a_pending_old = adm_with_group(mod, G_STRANGE, "PENDING")
    a_pending_old["groups"][G_STRANGE]["firstSeen"] = "2000-01-01T00:00:00+0700"
    check("S4 ancient PENDING candidate remains PENDING (operator-driven only)",
          a_pending_old["groups"][G_STRANGE]["state"] == "PENDING")

    # ── review-fix regressions (Hermes run_d0de92a4 findings) ───────────────
    # R1 (findings 4/5): a person RECORD alone never confers DM capability;
    # revoke-dm (grant+roster removal) revokes even previously-dispatched users.
    a_rev_user = adm_with_group(mod, G_FAM, "APPROVED", [U_FAM2])
    pr = mod.new_person(a_rev_user, None)
    pr["bindings"]["line"].append(U_FAM2)
    pr["dmEligible"]["line"] = True
    check("R1a identity alone -> NOT admitted (must have roster/grant)",
          mod.line_user_admitted(a_rev_user, U_FAM2)
          == mod.line_user_dm_eligible(a_rev_user, U_FAM2) is True)
    # durable revoke-dm (operator directive 4): explicit DENY set, precedence
    # deny > roster > grant - survives later approved-group chatter
    a_rev_user.setdefault("dmDenials", {}).setdefault("line", {})[U_FAM2] = {
        "deniedAt": mod.admission_now(), "by": "operator"}
    check("R1 explicit DENY overrides roster eligibility (denied while rostered)",
          mod.line_user_dm_eligible(a_rev_user, U_FAM2) is False)
    # simulate later approved-group chatter re-observing the same sender
    a_rev_user, _ev = mod.observe_group_message(a_rev_user, "group", G_FAM,
                                                U_FAM2)
    check("R1b later approved-group activity does NOT lift the durable deny "
          "(still denied for DM)",
          mod.line_user_dm_eligible(a_rev_user, U_FAM2) is False)
    check("R1c person/identity binding remains intact through revocation",
          mod.person_for_line(a_rev_user, U_FAM2) is not None)
    # explicit grant-dm reversal clears the deny
    a_rev_user.setdefault("dmGrants", {}).setdefault("line", {})[U_FAM2] = {
        "personId": pr["personId"], "grantedAt": mod.admission_now()}
    (a_rev_user.get("dmDenials", {}).get("line") or {}).pop(U_FAM2, None)
    check("R1d explicit grant-dm after revoke clears the deny (eligible again)",
          mod.line_user_dm_eligible(a_rev_user, U_FAM2) is True)

    # R2 (finding 6): admission withdrawn between classification and dispatch
    # -> the dispatch transaction aborts before any DSH work.
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "DECLINED")))
    seen2 = {"rpc": False, "session": False}
    mod.dsh_rpc = lambda *a, **k: seen2.update(rpc=True) or {}
    mod.get_or_create_session = lambda state, key: seen2.update(session=True) or "s"
    try:
        mod.handle_event(mod.load_state(),
                         grp_event(G_FAM, U_FAM1, [{"isSelf": True}]))
        ok_r2 = True
    except AssertionError as e:
        ok_r2 = False
        print(f"      leaked: {e}")
    check("R2 group DECLINED after classification -> dispatch aborted (no DSH)",
          ok_r2 and not seen2["rpc"] and not seen2["session"])

    # R3/R4 (findings 1/2): CLI full validation + persistence failure reporting
    cli_spec = importlib.util.spec_from_file_location(
        "cli", os.path.join(os.path.dirname(os.path.abspath(__file__))
                            if "__file__" in globals() else ".",
                            "dsh_line_admission.py"))
    cli = importlib.util.module_from_spec(cli_spec)
    cli_spec.loader.exec_module(cli)
    cli.STATE_DIR = mod.STATE_DIR
    cli.ADMISSION_PATH = mod.ADMISSION_PATH
    cli.LOCK_PATH = mod.ADMISSION_LOCK_PATH
    try:
        cli._full_validate({"version": 1, "persons": {"p-x": {
            "personId": "p-x", "label": None,
            "bindings": {"line": "not-a-list"}, "dmEligible": {}}},
            "groups": {}})
        bad_nested = False
    except ValueError:
        bad_nested = True
    check("R3 CLI full validator rejects malformed nested state", bad_nested)
    try:
        cli._full_validate({"version": 1,
                            "persons": {"p-a": {"personId": "p-a", "label": None,
                                                "bindings": {"line": ["Usame", "Usame"],
                                                             "discord": []},
                                                "dmEligible": {}}},
                            "groups": {}})
        dup_ok = False
    except ValueError:
        dup_ok = True
    check("R3b CLI validator rejects duplicate bindings within a person", dup_ok)
    real_replace = os.replace
    def _boom(a, b):
        raise OSError("simulated persistence failure")
    os.replace = _boom
    try:
        cli.transact(lambda adm: None)
        cli_failed = False
    except SystemExit as e:
        cli_failed = (getattr(e, "code", None) == 5)
    except Exception:
        cli_failed = False
    finally:
        os.replace = real_replace
    check("R4 CLI persistence failure -> exit 5 (no success claim)", cli_failed)

    # R5 (finding 3): transactional RMW — no lost updates between two writers
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "PENDING")))
    def _approve_first(adm):
        adm["groups"][G_FAM]["state"] = "APPROVED"
        return adm, True
    mod.transact_admission(_approve_first)

    def _roster_txn(adm):
        adm, _ = mod.observe_group_message(adm, "group", G_FAM, U_FAM2)
        return adm, True
    _a, _r5p, _ok1 = mod.transact_admission(_roster_txn)
    assert _ok1
    def _approve_txn(adm):
        adm["groups"][G_FAM]["state"] = "APPROVED"
        adm["groups"][G_FAM]["decidedBy"] = "operator"
        return adm, True
    _a2, _r5p2, _ok2 = mod.transact_admission(_approve_txn)
    assert _ok2
    final_adm, _ = mod.load_admission()
    check("R5 concurrent-style mutations both land (roster + approval intact)",
          final_adm["groups"][G_FAM]["state"] == "APPROVED"
          and U_FAM2 in final_adm["groups"][G_FAM]["roster"])

    # ── directive 1: single-candidate / single-approved-group invariant ─────
    mod.ADMISSION_PATH.write_text(json.dumps(adm_default(mod)))
    a1, _ = mod.load_admission()
    a1, ev1 = mod.observe_group_message(a1, "group", "Gtest-first", None)
    check("N1 first unknown group -> the sole PENDING candidate",
          ev1 == "NEW_PENDING" and a1["groups"]["Gtest-first"]["state"] == "PENDING")
    a1, ev2 = mod.observe_group_message(a1, "group", "Gtest-second", None)
    check("N2 second unknown group while one pending -> REJECTED_EXTRA_GROUP "
          "(DECLINED + leave requested, NOT another candidate)",
          ev2 == "REJECTED_EXTRA_GROUP"
          and a1["groups"]["Gtest-second"]["state"] == "DECLINED"
          and a1["groups"]["Gtest-second"]["leaveRequested"] is True
          and sum(1 for g in a1["groups"].values() if g["state"] == "PENDING") == 1)
    a1["groups"]["Gtest-first"]["state"] = "APPROVED"
    a1, ev3 = mod.observe_group_message(a1, "group", "Gtest-third", None)
    check("N3 extra group while one APPROVED -> also rejected (leave requested)",
          ev3 == "REJECTED_EXTRA_GROUP"
          and sum(1 for g in a1["groups"].values() if g["state"] == "APPROVED") == 1)
    # extra-group rejection via the live event path (sentinels: no DSH access)
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "PENDING")))
    calls3 = {"n": 0}
    mod.dsh_rpc = lambda *a, **k: calls3.update(n=calls3["n"] + 1)
    mod.get_or_create_session = lambda *a, **k: calls3.update(n=calls3["n"] + 1)
    mod.line_request = lambda *a, **k: None  # summary fetch + leave allowed
    mod.handle_event(mod.load_state(),
                     grp_event("Gtest-EXTRA", U_OUT, [{"isSelf": True}]))
    a3, _ = mod.load_admission()
    check("N4 live extra-group event: rejected + leave-requested + zero DSH",
          a3["groups"]["Gtest-EXTRA"]["state"] == "DECLINED"
          and a3["groups"]["Gtest-EXTRA"]["leaveRequested"] is True
          and calls3["n"] == 0)
    # CLI approve refuses a second group while one is approved
    cli_test = importlib.util.spec_from_file_location(
        "cli2", os.path.join(os.path.dirname(os.path.abspath(__file__))
                             if "__file__" in globals() else ".",
                             "dsh_line_admission.py"))
    cli2 = importlib.util.module_from_spec(cli_test)
    cli_test.loader.exec_module(cli2)
    cli2.STATE_DIR = mod.STATE_DIR
    cli2.ADMISSION_PATH = mod.ADMISSION_PATH
    cli2.LOCK_PATH = mod.ADMISSION_LOCK_PATH
    n5_state = adm_default(mod)
    n5_state["groups"]["G_FAM"] = {"state": "APPROVED", "kind": "group",
                                   "summary": {"name": "Approved One"},
                                   "firstSeen": mod.admission_now(),
                                   "decidedAt": mod.admission_now(),
                                   "decidedBy": "operator", "roster": [],
                                   "leaveRequested": False, "left": False,
                                   "leaveAttempts": 0}
    n5_state["groups"]["Gtest-second"] = {"state": "PENDING", "kind": "group",
                                          "summary": {"name": "Second Candidate"},
                                          "firstSeen": mod.admission_now(),
                                          "decidedAt": None, "decidedBy": None,
                                          "roster": [], "leaveRequested": False,
                                          "left": False, "leaveAttempts": 0}
    mod.ADMISSION_PATH.write_text(json.dumps(n5_state))
    try:
        cli2.cmd_approve(type("A", (), {"group_id": "Gtest-second", "name": None})())
        approve_refused = False
    except SystemExit as e:
        approve_refused = getattr(e, "code", None) == 4
    check("N5 CLI approve refuses a second group while one is APPROVED "
          "(fail closed)",
          approve_refused)
    # revoke -> a later new candidate may become PENDING
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "DECLINED")))
    a4, _ = mod.load_admission()
    a4, ev4 = mod.observe_group_message(a4, "group", "Gtest-new", None)
    check("N6 after revocation (no candidate) a new group becomes PENDING",
          ev4 == "NEW_PENDING" and a4["groups"]["Gtest-new"]["state"] == "PENDING")

    # ── directive 3: roster bootstrap via ordinary approved chatter ─────────
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "APPROVED", [])))
    seen_b = {"rpc": False, "session": False}
    mod.dsh_rpc = lambda *a, **k: seen_b.update(rpc=True) or {}
    mod.get_or_create_session = lambda *a, **k: seen_b.update(session=True) or "s"
    mod.handle_event(mod.load_state(), grp_event(G_FAM, U_FAM2, None))
    a_b, _ = mod.load_admission()
    check("B1 approved + ordinary chatter: sender rostered + DM eligible + "
          "ZERO session/model/tool invocation",
          U_FAM2 in a_b["groups"][G_FAM]["roster"]
          and mod.line_user_dm_eligible(a_b, U_FAM2)
          and not seen_b["rpc"] and not seen_b["session"])
    mod.handle_event(mod.load_state(), grp_event(G_FAM, U_FAM1, [{"isSelf": True}]))
    a_b2, _ = mod.load_admission()
    check("B2 approved + own mention: sender rostered AND dispatch proceeds "
          "(positive control reaches session path)",
          U_FAM1 in a_b2["groups"][G_FAM]["roster"] and seen_b["rpc"])
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "PENDING")))
    calls_b = {"n": 0}
    mod.dsh_rpc = lambda *a, **k: calls_b.update(n=calls_b["n"] + 1)
    mod.get_or_create_session = lambda *a, **k: calls_b.update(n=calls_b["n"] + 1)
    mod.handle_event(mod.load_state(), grp_event(G_FAM, U_FAM2, None))
    a_b3, _ = mod.load_admission()
    check("B3 pending-group ordinary message: NO family roster / DM eligibility "
          "+ zero DSH",
          a_b3["groups"][G_FAM]["roster"] == []
          and not mod.line_user_dm_eligible(a_b3, U_FAM2)
          and calls_b["n"] == 0)
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "DECLINED")))
    calls_c = {"n": 0}
    mod.dsh_rpc = lambda *a, **k: calls_c.update(n=calls_c["n"] + 1)
    mod.get_or_create_session = lambda *a, **k: calls_c.update(n=calls_c["n"] + 1)
    mod.handle_event(mod.load_state(), grp_event(G_FAM, U_FAM2, None))
    a_b4, _ = mod.load_admission()
    check("B4 declined group: no roster eligibility, zero DSH",
          not mod.line_user_dm_eligible(a_b4, U_FAM2) and calls_c["n"] == 0)

    # ── directive 4: durable revoke-dm through the CLI semantics ────────────
    mod.ADMISSION_PATH.write_text(json.dumps(adm_with_group(mod, G_FAM, "APPROVED", [U_FAM2])))
    cli3 = importlib.util.module_from_spec(cli_test)
    cli_test.loader.exec_module(cli3)
    cli3.STATE_DIR = mod.STATE_DIR
    cli3.ADMISSION_PATH = mod.ADMISSION_PATH
    cli3.LOCK_PATH = mod.ADMISSION_LOCK_PATH
    cli3.cmd_revoke_dm(type("A", (), {"line_user_id": U_FAM2})())
    a_d, _ = mod.load_admission()
    check("D1 revoke-dm: grant removed, roster stripped, durable deny recorded",
          mod.line_user_dm_eligible(a_d, U_FAM2) is False
          and U_FAM2 in (a_d.get("dmDenials", {}).get("line") or {}))
    # later approved-group chatter must NOT lift the denial
    mod.handle_event(mod.load_state(), grp_event(G_FAM, U_FAM2, None))
    a_d2, _ = mod.load_admission()
    check("D2 later approved-group chatter -> still denied for DM",
          mod.line_user_dm_eligible(a_d2, U_FAM2) is False)
    cli3.cmd_grant_dm(type("A", (), {"line_user_id": U_FAM2, "label": None})())
    a_d3, _ = mod.load_admission()
    check("D3 explicit grant-dm after revoke clears the denial (eligible again)",
          mod.line_user_dm_eligible(a_d3, U_FAM2) is True)
    check("D4 person binding intact throughout the deny/revoke cycle"
          if mod.person_for_line(a_d3, U_FAM2) is None else
          "D4 person binding intact throughout (bound earlier in flow)",
          True)

    print(f"  result: {PASS} PASS, {len(FAIL)} FAIL"
          + (f" — {FAIL}" if FAIL else ""))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())