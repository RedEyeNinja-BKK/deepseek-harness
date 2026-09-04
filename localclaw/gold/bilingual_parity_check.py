#!/usr/bin/env python3
"""bilingual_parity_check.py — deterministic Thai/English numeric & unit parity gate
for the DSH gold report (2026-09-03 operator-directed reliability fix 9A).

Usage: python3 bilingual_parity_check.py <report.md>
Exit 0 = parity OK. Exit 1 = critical parity failure (do NOT emit).
Exit 2 = usage/structure failure (sections not found).

Hard-fail scope (the drift classes that mislead readers):
  1. baht-weight -> "per gram" unit drift in the English mirror.
  2. The 📊 price-table fact blocks: the numeric facts (prices, deltas,
     currencies, percentages, data time) must be IDENTICAL between the Thai
     and English tables — these are the canonical data, not prose.
  3. Material data time (HH:MM) mismatch between the two tables.

Everything else (support/resistance mentions, yesterday references, historical
asides appearing in one language only) is reported as a WARNING — legitimate
mirrored prose may carry asymmetric analytical detail, and this gate must not
block an honest report. Pure stdlib; deterministic; no LLM judgment.
"""
import re
import sys

THAI_DIGITS = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\b")
NUM_RE = re.compile(r"[+\-−–—]?\d[\d,]*(?:\.\d+)?")
PER_GRAM_EN = re.compile(r"per\s+gram", re.IGNORECASE)
GRAM_TH = re.compile(r"กรัม")
TH_CHARS = re.compile(r"[\u0e00-\u0e7f]")


def normalize(tok: str) -> str:
    tok = tok.translate(THAI_DIGITS)
    tok = tok.replace("−", "-").replace("–", "-").replace("—", "-")
    tok = tok.replace(",", "").replace("+", "")
    if tok.endswith("."):
        tok = tok[:-1]
    try:
        f = float(tok)
        if f == int(f):
            return str(int(f))
        return ("%.4f" % f).rstrip("0").rstrip(".")
    except ValueError:
        return tok


def split_sections(text: str):
    """Split on '## ' headings; classify each section Thai/English."""
    sections = re.split(r"(?m)^## ", text)
    th_secs, en_secs = [], []
    for s in sections[1:]:
        head = s.split("\n", 1)[0]
        is_th = bool(TH_CHARS.search(head)) or bool(TH_CHARS.search(s[:200]))
        (th_secs if is_th else en_secs).append(s)
    return th_secs, en_secs


def table_secs(secs):
    return [s for s in secs if ("📊" in s.split("\n", 1)[0]) or ("ราคา" in s.split("\n", 1)[0]) or ("Price" in s.split("\n", 1)[0])]


def numbers(body: str):
    """Normalized numeric tokens, excluding ISO dates and clock times."""
    body = ISO_DATE.sub(" ", body)
    body = TIME_RE.sub(" ", body)
    return {normalize(m.group(0)) for m in NUM_RE.finditer(body) if normalize(m.group(0)) not in ("", "-")}


def times(body: str):
    return sorted(TIME_RE.findall(body))


def fmt_diff(a, b):
    return sorted(a - b, key=lambda x: (len(x), x)) or sorted(b - a, key=lambda x: (len(x), x))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: bilingual_parity_check.py <report.md>")
        return 2
    text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
    th_secs, en_secs = split_sections(text)
    if len(TH_CHARS.findall("".join(th_secs))) < 50 or len(TH_CHARS.findall("".join(en_secs))) > len("".join(en_secs)) * 0.5:
        print("STRUCTURE-FAIL: could not separate a Thai and an English section")
        return 2

    failures, warnings = [], []

    # 1. Unit drift: EN "per gram" while TH carries no gram pricing.
    en_all, th_all = "\n".join(en_secs), "\n".join(th_secs)
    if PER_GRAM_EN.search(en_all) and not GRAM_TH.search(th_all):
        failures.append("UNIT-DRIFT: English uses 'per gram' but Thai has no กรัม pricing (baht-weight/gram confusion)")

    # 2. Price-table fact blocks must match 1:1.
    tt, te = table_secs(th_secs), table_secs(en_secs)
    if not tt or not te:
        print("STRUCTURE-FAIL: price-table section not found in one language")
        return 2
    th_nums, en_nums = numbers("".join(tt)), numbers("".join(te))
    if th_nums != en_nums:
        failures.append(
            "TABLE-FACTS-MISMATCH — thai-only: [" + ", ".join(sorted(th_nums - en_nums)) +
            "]  english-only: [" + ", ".join(sorted(en_nums - th_nums)) + "]")

    # 3. Material data time in the tables.
    if times("".join(tt)) != times("".join(te)):
        failures.append(f"DATA-TIME-MISMATCH: thai table {times(''.join(tt))} vs english table {times(''.join(te))}")

    # Asymmetric numbers outside the tables are warnings, not failures.
    th_rest, en_rest = numbers(th_all) - th_nums, numbers(en_all) - en_nums
    if th_rest - en_rest:
        warnings.append("thai-only numbers outside tables (informational): " + ", ".join(sorted(th_rest - en_rest)))
    if en_rest - th_rest:
        warnings.append("english-only numbers outside tables (informational): " + ", ".join(sorted(en_rest - th_rest)))

    print(f"price-table facts: thai={len(th_nums)} english={len(en_nums)}; data times: thai={times(''.join(tt))} english={times(''.join(te))}")
    for w in warnings:
        print("WARNING: " + w)
    if failures:
        print("PARITY-FAIL:")
        for f in failures:
            print("  - " + f)
        return 1
    print("PARITY-OK: price-table facts, data times and units match across Thai/English")
    return 0


if __name__ == "__main__":
    sys.exit(main())
