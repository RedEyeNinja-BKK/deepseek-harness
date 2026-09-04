#!/usr/bin/env python3
"""DSH LINE F3 final-output filtering — regression fixtures.

Runs extract_reply_for_dispatch() against a module file given as argv[1]
(default: deployed /opt/dsh-line/dsh_line_inbound.py). Import-safe: the module
only defines constants/functions at import; credentials load lazily and
STATE_DIRECTORY is overridden to a tmp dir.

Cases (directive §1):
  1  ordinary one-message answer -> text returned
  2  observed 22-assistant-event multi-step shape -> ONLY final text
  3  tool turn ending without final text -> fail closed (None)
  4  legitimate final multi-part (multiple text blocks) -> joined
  5  media-capable final (text + media block) -> text preserved
  6  turn not finished -> None
  7  duplicate dispatch marker -> None
  8  tool-call leakage attempt in final message -> fail closed (None)

Exit 0 = ALL PASS; exit 1 = failures listed.
"""
import importlib.util
import os
import sys
import tempfile


def load_module(path: str):
    tmp = tempfile.mkdtemp(prefix="f3-state-")
    os.environ["STATE_DIRECTORY"] = tmp
    spec = importlib.util.spec_from_file_location("dsh_line_inbound_f3", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def ev(etype, turn, blocks=None, message=None, text=None):
    data = {"turn": turn}
    if message is not None or blocks is not None or text is not None:
        content = blocks if blocks is not None else (
            message if message is not None else [{"type": "text", "text": text}])
        data["message"] = {"content": content}
    return {"event": {"type": etype, "data": data}}


def build_22_event_turn(dispatch_id: str):
    """Replica of live canary turn 2 shape: user message, 21 scaffold
    assistant/tool steps, final text-only assistant message, turn/end."""
    events = [{"event": {"type": "turn/start", "data": {"turn": 2}}}]
    user_content = [{"type": "text", "text": f"run gold dispatch {dispatch_id}"}]
    events.append({"event": {"type": "user/message",
                             "data": {"turn": 2, "content": user_content}}})
    intermediate = [
        "analyzing latest gold API response...",
        "drafting Thai analytical section...",
        "checking parity between Thai and English...",
        "waiting for emit tool result...",
    ]
    for i in range(21):
        events.append({"event": {"type": "assistant/message",
                                 "data": {"turn": 2,
                                          "message": {"content": [
                                              {"type": "tool-call",
                                               "callId": f"c{i}",
                                               "name": "emit",
                                               "arguments": "{}"}]}}}})
        if i % 3 == 0:  # some intermediates also carry working text (leak risk)
            events[-1]["event"]["data"]["message"]["content"].append(
                {"type": "text", "text": f"WORKING-NOTE-{i} should never reach LINE"})
        events.append({"event": {"type": "tool/result",
                                 "data": {"turn": 2, "result": "ok"}}})
        events.append({"event": {"type": "step/end", "data": {"turn": 2, "step": i + 1}}})
    final_text = ("GOLD REPORT 2026-09-04\nราคาทองคำแท่ง ซื้อ 43,400 บาท\n"
                  "Gold bar sell 43,400 THB")
    events.append({"event": {"type": "assistant/message",
                             "data": {"turn": 2, "message": {"content": [
                                 {"type": "text", "text": final_text}]}}}})
    events.append({"event": {"type": "turn/end", "data": {"turn": 2}}})
    return events, final_text


def main() -> int:
    mod = load_module(sys.argv[1] if len(sys.argv) > 1
                      else "/opt/dsh-line/dsh_line_inbound.py")
    f = mod.extract_reply_for_dispatch
    failures = []

    def check(name, got, want):
        ok = got == want
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        if not ok:
            failures.append(name)
            print(f"      got:  {got!r}")
            print(f"      want: {want!r}")

    # 1. ordinary one-message answer
    d1 = "D-ID-1"
    ev1 = [{"event": {"type": "turn/start", "data": {"turn": 1}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 1, "content": [{"type": "text",
                               "text": f"hello {d1}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 1, "message": {"content": [
                          {"type": "text", "text": "Hi! How can I help?"}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 1}}}]
    check("1 ordinary one-message answer", f(ev1, d1), "Hi! How can I help?")

    # 2. observed 22-assistant-event multi-step shape
    d2 = "D-ID-2"
    ev2, final2 = build_22_event_turn(d2)
    check("2 multi-step 22-event shape -> final only", f(ev2, d2), final2)

    # 3. tool turn ending without final text -> fail closed
    d3 = "D-ID-3"
    ev3 = [{"event": {"type": "turn/start", "data": {"turn": 3}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 3, "content": [{"type": "text",
                               "text": f"do thing {d3}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 3, "message": {"content": [
                          {"type": "tool-call", "callId": "c1",
                           "name": "tool", "arguments": "{}"}]}}}},
           {"event": {"type": "tool/result", "data": {"turn": 3, "result": "ok"}}},
           {"event": {"type": "turn/end", "data": {"turn": 3}}}]
    check("3 tool-only turn end -> fail closed", f(ev3, d3), None)

    # 4. legitimate final multi-part delivery
    d4 = "D-ID-4"
    ev4 = [{"event": {"type": "turn/start", "data": {"turn": 4}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 4, "content": [{"type": "text",
                               "text": f"parts {d4}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 4, "message": {"content": [
                          {"type": "text", "text": "Part 1: summary."},
                          {"type": "text", "text": "Part 2: details."}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 4}}}]
    check("4 final multi-part joined", f(ev4, d4),
          "Part 1: summary.\nPart 2: details.")

    # 5. media-capable final structure (text + non-tool media block)
    d5 = "D-ID-5"
    ev5 = [{"event": {"type": "turn/start", "data": {"turn": 5}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 5, "content": [{"type": "text",
                               "text": f"media {d5}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 5, "message": {"content": [
                          {"type": "text", "text": "Here is your image."},
                          {"type": "media", "url": "https://x/y.png"}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 5}}}]
    check("5 media final -> text preserved", f(ev5, d5), "Here is your image.")

    # 6. turn not finished -> None
    d6 = "D-ID-6"
    ev6 = [{"event": {"type": "turn/start", "data": {"turn": 6}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 6, "content": [{"type": "text",
                               "text": f"pending {d6}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 6, "message": {"content": [
                          {"type": "text", "text": "working..."}]}}}}]
    check("6 turn not finished -> fail closed", f(ev6, d6), None)

    # 7. duplicate dispatch marker -> None
    d7 = "D-ID-7"
    ev7 = [{"event": {"type": "turn/start", "data": {"turn": 7}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 7, "content": [{"type": "text",
                               "text": f"one {d7}"}]}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 7, "content": [{"type": "text",
                               "text": f"two {d7}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 7, "message": {"content": [
                          {"type": "text", "text": "answer"}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 7}}}]
    check("7 duplicate marker -> None", f(ev7, d7), None)

    # 8. final message carrying tool-call block -> fail closed
    d8 = "D-ID-8"
    ev8 = [{"event": {"type": "turn/start", "data": {"turn": 8}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 8, "content": [{"type": "text",
                               "text": f"mixed {d8}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 8, "message": {"content": [
                          {"type": "text", "text": "answer text"},
                          {"type": "tool-call", "callId": "c9",
                           "name": "tool", "arguments": "{}"}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 8}}}]
    check("8 mixed final w/ tool-call -> fail closed", f(ev8, d8), None)

    # 9. (review F1) multiple assistant messages with intervening non-assistant
    #    events; the LAST assistant/message immediately before turn/end is canonical
    d9 = "D-ID-9"
    ev9 = [{"event": {"type": "turn/start", "data": {"turn": 9}}},
           {"event": {"type": "user/message",
                      "data": {"turn": 9, "content": [{"type": "text",
                               "text": f"multi {d9}"}]}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 9, "message": {"content": [
                          {"type": "text", "text": "intermediate scaffold"}]}}}},
           {"event": {"type": "step/end", "data": {"turn": 9, "step": 1}}},
           {"event": {"type": "assistant/message",
                      "data": {"turn": 9, "message": {"content": [
                          {"type": "text", "text": "canonical final"}]}}}},
           {"event": {"type": "turn/end", "data": {"turn": 9}}}]
    check("9 last assistant before turn/end wins", f(ev9, d9), "canonical final")

    # 10. (review F2) malformed / non-dictionary blocks in the final message:
    #     unknown shapes are ignored (only the proven "tool-call" kind fails
    #     closed); usable text is still extracted
    d10 = "D-ID-10"
    ev10 = [{"event": {"type": "turn/start", "data": {"turn": 10}}},
            {"event": {"type": "user/message",
                       "data": {"turn": 10, "content": [{"type": "text",
                                "text": f"weird {d10}"}]}}},
            {"event": {"type": "assistant/message",
                       "data": {"turn": 10, "message": {"content": [
                           "not-a-dict-block",
                           {"callId": "c1", "arguments": "{}"},
                           {"type": "text", "text": "usable text"}]}}}},
            {"event": {"type": "turn/end", "data": {"turn": 10}}}]
    check("10 malformed blocks ignored, text kept", f(ev10, d10), "usable text")

    # 11. (review F3) empty / whitespace-only final text -> fail closed
    d11 = "D-ID-11"
    ev11 = [{"event": {"type": "turn/start", "data": {"turn": 11}}},
            {"event": {"type": "user/message",
                       "data": {"turn": 11, "content": [{"type": "text",
                                "text": f"blank {d11}"}]}}},
            {"event": {"type": "assistant/message",
                       "data": {"turn": 11, "message": {"content": [
                           {"type": "text", "text": ""},
                           {"type": "text", "text": "   "}]}}}},
            {"event": {"type": "turn/end", "data": {"turn": 11}}}]
    check("11 whitespace-only final -> None", f(ev11, d11), None)

    print(f"  result: {'ALL PASS' if not failures else f'FAILURES: {failures}'}")
    return 0 if not failures else 1

if __name__ == "__main__":
    sys.exit(main())