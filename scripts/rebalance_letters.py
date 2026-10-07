"""Rebalance correct-answer letters in data/verified/examN_q*.json to ~15 per letter per exam.

Swaps the correct option with an option at an under-used letter (option text and optExpl move together).
Only touches questions whose text never refers to options by letter, so explanations stay valid.
"""
import json, pathlib, re
from collections import Counter

DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "verified"
LETTER_REF = re.compile(r"\b(?i:option|answer|choice)s? [A-D]\b|\([A-D]\)|\b(?i:unlike|like|than|vs\.?) [A-D]\b")


def mentions_letters(q):
    text = " ".join([q["stem"], q["explanation"], *q["optExpl"].values(), *q["options"].values()])
    return bool(LETTER_REF.search(text))


for n in (1, 2):
    parts = sorted(DATA.glob(f"exam{n}_q*.json"))
    docs = {p: json.loads(p.read_text(encoding="utf-8")) for p in parts}
    qs = sorted((q for d in docs.values() for q in d["questions"]), key=lambda q: q["id"])
    target = len(qs) / 4
    counts = Counter(q["answer"] for q in qs)
    before = dict(sorted(counts.items()))
    skipped = 0
    for q in qs:
        over = q["answer"]
        if counts[over] <= target:
            continue
        under = min("ABCD", key=lambda L: counts[L])
        if counts[under] >= target:
            break
        if mentions_letters(q):
            skipped += 1
            continue
        for field in ("options", "optExpl"):
            q[field][over], q[field][under] = q[field][under], q[field][over]
        q["answer"] = under
        counts[over] -= 1
        counts[under] += 1
    for p, d in docs.items():
        p.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"exam{n}: {before} -> {dict(sorted(counts.items()))} | skipped (letter refs): {skipped}")
