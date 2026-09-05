#!/usr/bin/env python3
"""LINE Family Increment 2 — explicit EN<->TH translation command fixtures.

Runs against a candidate adapter module (argv[1], default v5 work copy).
All identifiers SYNTHETIC (Utest-*, Gtest-*); no network: LINE API raises if
touched from a denied path; DSH RPC is a recording stub.

Proves (directive section 9):
  - approved group + REAL own-mention + `translate <text>` -> specialist path
  - approved group + LITERAL typed '@DSH translate ...' (no metadata) -> silent
  - approved group + '@All translate ...' -> silent
  - admitted DM + `translate <text>` -> specialist path (no mention)
  - admitted DM + ordinary sentence containing the word 'translate' -> NORMAL
    conversation path (no translation envelope)
  - bare `translate` (empty source) -> concise help, NO specialist call
  - specialist reply passes through VERBATIM (translation-only; EN->TH and
    TH->EN fixtures) with names/URLs/numbers carried intact in the payload
  - specialist failure -> fixed concise failure text, NEVER a fabricated
    normal-model answer, no retry loop
  - dedupe + admission revalidation hold on the translate path

Exit 0 = ALL PASS.
"""
import importlib.util
import itertools
import json
import os
import sys
import tempfile

_MSG_SEQ = itertools.count(1)

PASS, FAIL = 0, []


def check(name, cond):
    global PASS
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    if cond:
        PASS += 1
    else:
        FAIL.append(name)


def load_module(path: str):
    tmp = tempfile.mkdtemp(prefix="tr-state-")
    os.environ["STATE_DIRECTORY"] = tmp
    spec = importlib.util.spec_from_file_location("dsh_line_tr", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.STATE_DIR = __import__("pathlib").Path(tmp)
    mod.STATE_PATH = mod.STATE_DIR / "line-state.json"
    mod.ADMISSION_PATH = mod.STATE_DIR / "admission-state.json"
    mod.ADMISSION_LOCK_PATH = mod.STATE_DIR / "admission.lock"

    def _no_line(*a, **k):
        raise AssertionError("LINE API called from a path under test")
    mod.line_request = _no_line

    def _no_push(*a, **k):
        raise AssertionError("unexpected raw push (use recorded stub)")
    mod.push_messages = _no_push
    return mod


def adm_with_group(mod, gid, uid):
    a = json.loads(json.dumps(mod.DEFAULT_ADMISSION))
    a["groups"][gid] = {"state": "APPROVED", "kind": "group",
                        "summary": {"name": "C"},
                        "firstSeen": mod.admission_now(), "decidedAt": None,
                        "decidedBy": None, "roster": [uid],
                        "leaveRequested": False, "left": False,
                        "leaveAttempts": 0}
    return a


def grp_event(gid, uid, text, mentionees):
    msg = {"id": f"g-{next(_MSG_SEQ)}", "type": "text", "text": text}
    if mentionees is not None:
        msg["mention"] = {"mentionees": mentionees}
    return {"mode": "active", "type": "message",
            "source": {"type": "group", "groupId": gid, "userId": uid},
            "message": msg}


def dm_event(uid, text, mid=None):
    return {"mode": "active", "type": "message",
            "source": {"type": "user", "userId": uid},
            "message": {"id": mid or f"d-{next(_MSG_SEQ)}", "type": "text",
                        "text": text}}


class Recorder:
    """Records DSH RPC + delivery calls; replaces wait_for_reply with a
    scripted outcome."""

    def __init__(self, mod, reply="REPLACEMENT", fail=False, shape="text"):
        self.mod = mod
        self.prompts = []
        self.delivered = []
        self.fail = fail
        self.reply = reply
        self.shape = shape
        self.pushes = []
        mod.dsh_rpc = self._rpc
        mod.wait_for_reply = self._wait
        mod.deliver_reply = self._deliver
        mod.get_or_create_session = lambda state, key: "sess-test"
        mod.get_display_name = lambda *a, **k: "Synthetic Name"
        mod.push_messages = self._push

    def _push(self, to, msgs, **k):
        self.pushes.extend(msgs)
        return True

    def _rpc(self, method, payload, timeout=60.0):
        self.prompts.append((method, payload))
        return {}

    def _wait(self, session_id, dispatch_id, dispatched_at_ms):
        """Simulate DSH history extraction. The specialist's FINAL text may
        arrive as (a) the raw MCP envelope {"ok":true,"result":{"translation":
        ...}} when the model quotes the tool result, or (b) the plain
        translation text itself when the model follows the envelope
        instruction — shape (b) is what PRODUCTION produced live on
        2026-09-05 16:19-16:20 (the all-envelope stub was the Increment-2
        live-acceptance defect). shape="text" (default) models live truth;
        shape="envelope" models the alternate. fail=True -> None."""
        if self.fail:
            return None
        last = json.dumps(self.prompts[-1][1], ensure_ascii=False) \
            if self.prompts else ""
        if "[line translate" in last and self.shape == "envelope":
            return json.dumps({"ok": True,
                               "result": {"translation": self.reply}},
                              ensure_ascii=False)
        return self.reply

    def _deliver(self, state, message_id, target_id, reply):
        self.delivered.append((message_id, target_id, reply))
        return True


def main() -> int:
    mod = load_module(sys.argv[1] if len(sys.argv) > 1
                      else "dsh_line_inbound_v5.py")
    U1 = "Utest-fam1"
    G = "Gtest-family"
    A_OK = adm_with_group(mod, G, U1)
    OWN = [{"isSelf": True, "index": 0, "length": 4}]

    # 1. approved group + REAL own-mention + translate -> specialist path
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="สวัสดีครับ")
    ev1 = grp_event(G, U1, "@DSH translate hello", OWN)
    mod.handle_event(mod.load_state(), ev1)
    tr_prompts = [p for m, p in rec.prompts if m == "session.prompt"]
    check("T1 group+own-mention translate reaches specialist exactly once",
          len(tr_prompts) == 1)
    env1 = json.dumps(tr_prompts[0], ensure_ascii=False) if tr_prompts else ""
    check("T1 payload carries ONLY the source text (no @DSH mention residue)",
          "hello" in env1 and "@DSH" not in env1)
    check("T1 dispatch marker present (F3 extraction path intact)",
          "[line translate " in env1)
    check("T1 specialist reply delivered verbatim (EN->TH fixture)",
          rec.delivered and rec.delivered[-1][2] == "สวัสดีครับ")
    check("T1 source message marked accepted (dedupe ledger)",
          ev1["message"]["id"] in mod.load_state().get("accepted", []))

    # 2. literal typed '@DSH translate hello' WITHOUT metadata -> silent
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev2 = grp_event(G, U1, "@DSH translate hello", None)
    mod.handle_event(mod.load_state(), ev2)
    check("T2 literal-text mention (no metadata) -> NO dispatch at all",
          rec.prompts == [] and rec.delivered == [])
    check("T2 sender still rostered (Increment-1 bootstrap unaffected)",
          U1 in mod.load_admission()[0]["groups"][G]["roster"])

    # 3. '@All translate hello' -> silent
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev3 = grp_event(G, U1, "@All translate hello", [{"type": "all"}])
    mod.handle_event(mod.load_state(), ev3)
    check("T3 @All translate -> NO dispatch", rec.prompts == []
          and rec.delivered == [])

    # 4. admitted DM + translate -> specialist path without mention
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="Good morning")
    ev4 = dm_event(U1, "translate สวัสดี")
    mod.handle_event(mod.load_state(), ev4)
    tr_prompts = [p for m, p in rec.prompts if m == "session.prompt"]
    check("T4 DM translate reaches specialist exactly once (no mention needed)",
          len(tr_prompts) == 1)
    env4 = json.dumps(tr_prompts[0], ensure_ascii=False) if tr_prompts else ""
    check("T4 Thai source carried intact (TH->EN fixture)",
          "สวัสดี" in env4)
    check("T4 specialist reply delivered verbatim",
          rec.delivered and rec.delivered[-1][2] == "Good morning")

    # 5. DM ordinary conversation containing the word -> NORMAL path
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="normal answer")
    ev5 = dm_event(U1, "how do I translate hello into Thai?")
    mod.handle_event(mod.load_state(), ev5)
    tr_prompts = [p for m, p in rec.prompts if m == "session.prompt"]
    check("T5 non-command 'translate' word -> ONE normal dispatch",
          len(tr_prompts) == 1)
    env5 = json.dumps(tr_prompts[0], ensure_ascii=False) if tr_prompts else ""
    check("T5 normal envelope (no translation marker)",
          "[line translate" not in env5
          and "how do I translate hello" in env5)
    check("T5 normal reply delivered (not failure text)",
          rec.delivered and rec.delivered[-1][2] == "normal answer"
          and mod.TRANSLATE_FAILURE_TEXT not in rec.delivered[-1][2])

    # 6. bare `translate` (empty source) -> concise help, NO specialist call
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev6 = dm_event(U1, "translate")
    mod.handle_event(mod.load_state(), ev6)
    check("T6 empty translate -> NO specialist/session call",
          rec.prompts == []
          and rec.delivered and rec.delivered[0][2] == mod.TRANSLATE_HELP_TEXT)
    ev6b = grp_event(G, U1, "@DSH translate", OWN)
    mod.handle_event(mod.load_state(), ev6b)
    check("T6 group bare translate -> help, no specialist call",
          rec.prompts == []
          and sum(1 for _, _, r in rec.delivered
                  if r == mod.TRANSLATE_HELP_TEXT) == 2)

    # 7. names/URLs/numbers carried intact in the payload
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    src = "Alice met Bob at https://example.com/a?b=1 for ฿31,150 (2 bars) 🎉"
    mod.handle_event(mod.load_state(), dm_event(U1, "translate " + src,
                                                mid="dm-urls"))
    env = json.dumps(rec.prompts[0][1], ensure_ascii=False)
    check("T7 names/URLs/numbers/emoji carried intact",
          all(x in env for x in ("Alice", "Bob", "https://example.com/a?b=1",
                                 "฿31,150", "(2 bars)", "🎉")))

    # 8. specialist failure -> fixed failure text, NO fabricated fallback;
    # failure text rides the SAME durable delivery path as normal replies
    # (Hermes round-1 finding 3/4 fix) — recorded via deliver_reply stub.
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, fail=True)
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hello",
                                                mid="dm-fail"))
    check("T8 failure -> no normal-model delivery fabricated",
          rec.delivered and
          all(r == mod.TRANSLATE_FAILURE_TEXT for _, _, r in rec.delivered))
    check("T8 failure -> concise fixed failure text delivered once",
          len(rec.delivered) == 1
          and rec.delivered[0][2] == mod.TRANSLATE_FAILURE_TEXT)
    check("T8 failure -> no second (fallback) prompt",
          len(rec.prompts) == 1)
    check("T8 failure -> no raw push bypass (single durable path)",
          rec.pushes == [])

    # 9. dedupe holds on translate path (redelivery no-ops)
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev9 = dm_event(U1, "translate hi", mid="dm-dup")
    mod.handle_event(mod.load_state(), ev9)
    n_first = len(rec.prompts)
    mod.handle_event(mod.load_state(), ev9)
    check("T9 redelivered translate message deduped",
          n_first == 1 and len(rec.prompts) == 1)

    # 10. eligibility withdrawn before dispatch -> abort (revalidation on
    # translate path mirrors normal path)
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    a_revoked = adm_with_group(mod, G, U1)
    a_revoked["groups"][G]["roster"] = []          # eligibility stripped
    a_revoked.setdefault("dmDenials", {}).setdefault("line", {})[U1] = {
        "deniedAt": mod.admission_now(), "by": "operator"}
    mod.ADMISSION_PATH.write_text(json.dumps(a_revoked))
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hi",
                                                mid="dm-revoked"))
    check("T10 revoked DM eligibility -> translate aborted (no session/RPC)",
          rec.prompts == [] and rec.delivered == [])

    # 10b. invalid specialist outputs (echo / error shape) -> failure text
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    # (>8 chars: short echoes are indistinguishable from legit short
    # translations like "SOS" -> "SOS", so the gate only guards long sources)
    rec = Recorder(mod, reply="see you at the market")
    mod.handle_event(mod.load_state(), dm_event(U1, "translate see you at the market",
                                                mid="dm-echo"))
    check("T10b source-echo output -> rejected to failure text",
          rec.delivered and rec.delivered[0][2] == mod.TRANSLATE_FAILURE_TEXT)
    rec = Recorder(mod, reply="Error: subagent run failed")
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hi",
                                                mid="dm-err"))
    check("T10b error-shaped output -> rejected to failure text",
          rec.delivered and rec.delivered[0][2] == mod.TRANSLATE_FAILURE_TEXT)
    rec = Recorder(mod, reply="สวัสดี")
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hello",
                                                mid="dm-ok"))
    check("T10b valid translation passes the gate verbatim",
          rec.delivered and rec.delivered[0][2] == "สวัสดี")

    # 11. Hermes finding-1 scenario: accepted-but-undelivered translate reply
    # (crash/restart between acceptance and push) -> redelivery retries the
    # DURABLE pending delivery, never re-prompts the specialist.
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    st = mod.load_state()
    st["accepted"].append("dm-redeliver")
    st["pending"]["dm-redeliver"] = {"to": U1,
                                     "reply": "สวัสดี (recovered)",
                                     "attempts": 0, "inFlight": False,
                                     "inFlightAt": 0}
    mod.STATE_PATH.write_text(json.dumps(st))
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hi",
                                                mid="dm-redeliver"))
    check("T11 redelivery of accepted-undelivered translate -> NO re-prompt",
          rec.prompts == [])
    check("T11 durable pending reply recovered through delivery path",
          rec.delivered and rec.delivered[0][2] == "สวัสดี (recovered)")

    # ── Increment 2b: reply-to-DSH invocation (quotedMessageId proof) ────────
    # 12. reply to a POSITIVELY-KNOWN DSH outbound id -> dispatch
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="replied answer")
    st = mod.load_state()
    mod.record_outbound_ids(st, f"group:{G}", ["M-dsh-out-1"])
    st = mod.load_state()  # re-read persisted memory (restart-equivalent)
    ev12 = grp_event(G, U1, "thanks DSH", None)
    ev12["message"]["quotedMessageId"] = "M-dsh-out-1"
    mod.handle_event(mod.load_state(), ev12)
    check("R1 reply to known DSH outbound -> dispatch exactly once",
          len(rec.prompts) == 1)
    check("R1 reply reply delivered", rec.delivered
          and rec.delivered[-1][2] == "replied answer")

    # 13. reply to a FAMILY-MEMBER message id (never recorded as DSH outbound)
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev13 = grp_event(G, U1, "nope", None)
    ev13["message"]["quotedMessageId"] = "M-family-msg"
    mod.handle_event(mod.load_state(), ev13)
    check("R2 reply to family-member message -> silence",
          rec.prompts == [] and rec.delivered == [])

    # 14. reply with UNKNOWN/STALE quoted id -> silence
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev14 = grp_event(G, U1, "hmm", None)
    ev14["message"]["quotedMessageId"] = "M-never-seen"
    mod.handle_event(mod.load_state(), ev14)
    check("R3 unknown quotedMessageId -> silence",
          rec.prompts == [] and rec.delivered == [])

    # 15. unapproved group + reply to a KNOWN DSH outbound id -> denied
    a_strange = json.loads(json.dumps(mod.DEFAULT_ADMISSION))
    a_strange["groups"]["Gtest-other"] = {
        "state": "PENDING", "kind": "group", "summary": None,
        "firstSeen": mod.admission_now(), "decidedAt": None,
        "decidedBy": None, "roster": [], "leaveRequested": False,
        "left": False, "leaveAttempts": 0}
    mod.ADMISSION_PATH.write_text(json.dumps(a_strange))
    rec = Recorder(mod)
    st = mod.load_state()
    mod.record_outbound_ids(st, "group:Gtest-other", ["M-dsh-in-strange"])
    ev15 = grp_event("Gtest-other", "Utest-other", "hey", None)
    ev15["message"]["quotedMessageId"] = "M-dsh-in-strange"
    mod.handle_event(mod.load_state(), ev15)
    check("R4 unapproved group + reply to known DSH id -> denied (no dispatch)",
          rec.prompts == [] and rec.delivered == [])

    # 16. Operator-corrected matrix (2026-09-05): invocation signals are
    # INDEPENDENT — reply-proof invokes even with @All/member mentions present
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    ev16 = grp_event(G, U1, "@All stop", [{"type": "all"}])
    ev16["message"]["quotedMessageId"] = "M-dsh-out-1"
    mod.handle_event(mod.load_state(), ev16)
    check("R5a reply-to-DSH + @All -> INVOKES (reply is the signal)",
          len(rec.prompts) == 1)
    ev16b = grp_event(G, U1, "@All stop", [{"type": "all"}])
    ev16b["message"]["id"] = ev16b["message"]["id"] + "-b"
    ev16b["message"]["quotedMessageId"] = "M-family-msg"   # NOT DSH's
    mod.handle_event(mod.load_state(), ev16b)
    check("R5a2 @All alone (unverified quote) -> silent",
          len(rec.prompts) == 1)
    ev16b2 = grp_event(G, U1, "@All stop", [{"type": "all"}])
    ev16b2["message"]["id"] = ev16b2["message"]["id"] + "-b2"
    ev16b2["message"]["mention"] = None   # @All text, no quote, no self-mention
    mod.handle_event(mod.load_state(), ev16b2)
    check("R5a3 plain @All chatter -> silent",
          len(rec.prompts) == 1)
    ev16c = grp_event(G, U1, "@DSH please", None)
    ev16c["message"]["id"] = ev16c["message"]["id"] + "-c"
    ev16c["message"]["quotedMessageId"] = "M-family-msg"   # NOT a DSH message
    mod.handle_event(mod.load_state(), ev16c)
    check("R5b2 literal '@DSH' text alone never summons (no metadata, quote not DSH's)",
          len(rec.prompts) == 1)   # still only the R5a dispatch
    ev16d = grp_event(G, U1, "@DSH there?", [{"isSelf": True}])
    ev16d["message"]["id"] = ev16d["message"]["id"] + "-d"
    mod.handle_event(mod.load_state(), ev16d)
    check("R5c real self-mention still dispatches",
          len(rec.prompts) == 2)
    ev16e = grp_event(G, U1, "@DSH translate hi", [{"isSelf": True},
                                                    {"userId": "Umember",
                                                     "isSelf": False}])
    ev16e["message"]["id"] = ev16e["message"]["id"] + "-e"
    mod.handle_event(mod.load_state(), ev16e)
    check("R5d self-mention + member mention -> still invokes",
          len(rec.prompts) == 3)

    # 17. reply-invoked translate: no mention, translate command in reply text
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="สวัสดี")
    ev17 = grp_event(G, U1, "translate hello", None)
    ev17["message"]["quotedMessageId"] = "M-dsh-out-1"
    mod.handle_event(mod.load_state(), ev17)
    envs = [json.dumps(p, ensure_ascii=False) for m, p in rec.prompts
            if m == "session.prompt"]
    check("R6 reply-invoked translate -> specialist with bare source",
          len(envs) == 1 and "hello" in envs[0] and "[line translate" in envs[0])

    # 18. redelivery does not multiply outbound-author records or dispatches
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod)
    st = mod.load_state()
    mod.record_outbound_ids(st, f"group:{G}", ["M-dsh-dup"])
    ev18 = grp_event(G, U1, "dup question", None)
    ev18["message"]["quotedMessageId"] = "M-dsh-dup"
    mod.handle_event(mod.load_state(), ev18)
    n = len(rec.prompts)
    mod.handle_event(mod.load_state(), ev18)  # redelivery
    ob = mod.load_state().get("dshOutbound", {})
    check("R7 redelivery does not re-dispatch",
          n == 1 and len(rec.prompts) == 1)
    check("R7 outbound-author record count stable across redelivery",
          sum(1 for e in ob.values() if e.get("conv") == f"group:{G}") >= 2
          and "M-dsh-dup" in ob and "M-dsh-out-1" in ob)

    # ── Live-acceptance lessons 2026-09-05 16:19-16:20 (INC2 LIVE-1) ─────────
    # 19. ENVELOPE shape still accepted (model quotes the tool result)
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="สวัสดี", shape="envelope")
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hello",
                                                mid="dm-env"))
    check("T12 envelope-shaped specialist final accepted",
          rec.delivered and rec.delivered[0][2] == "สวัสดี")

    # 20. PLAIN-TEXT shape (live production truth) passes through the full
    # translate path and is delivered verbatim
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="ขอให้คุณมีวันที่ดี แล้วเจอกันเร็วๆ นี้!")
    mod.handle_event(mod.load_state(),
                     dm_event(U1, "translate Have a nice day, see you soon!",
                              mid="dm-plain"))
    check("T13 plain-text specialist final (live shape) delivered verbatim",
          rec.delivered
          and rec.delivered[0][2] == "ขอให้คุณมีวันที่ดี แล้วเจอกันเร็วๆ นี้!")

    # 21. JSON object that is NOT a valid ok:true envelope -> fail-closed
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply='{"ok": false, "error": "translate_upstream"}')
    mod.handle_event(mod.load_state(), dm_event(U1, "translate hi",
                                                mid="dm-badenv"))
    check("T14 invalid/failed envelope object -> failure text, never raw",
          rec.delivered and rec.delivered[0][2] == mod.TRANSLATE_FAILURE_TEXT)

    # 22. INC2 live-acceptance finding 2 (2026-09-05 18:00: production push
    # captured RAW Push-API targets while the gate compared prefixed keys —
    # same-conversation match was structurally impossible). v1.4.2
    # canonicalizes conversation keys on BOTH sides (idempotent).
    mod.ADMISSION_PATH.write_text(json.dumps(A_OK))
    rec = Recorder(mod, reply="raw capture works")
    st = mod.load_state()
    mod.record_outbound_ids(st, G, ["M-raw-capture"])   # RAW target (production shape)
    ev22 = grp_event(G, U1, "weather?", None)
    ev22["message"]["quotedMessageId"] = "M-raw-capture"
    mod.handle_event(mod.load_state(), ev22)
    check("T15 raw-target capture matches prefixed gate key (v1.4.2 fix)",
          len(rec.prompts) == 1)
    check("T15 reply dispatched without any mention", rec.delivered
          and rec.delivered[-1][2] == "raw capture works")

    print(f"  result: {PASS} PASS, {len(FAIL)} FAIL"
          + (f" — {FAIL}" if FAIL else ""))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
