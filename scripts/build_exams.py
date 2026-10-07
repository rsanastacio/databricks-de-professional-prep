"""Merge data/examN_part*.json, validate, and write data/examN.json, docs/PRACTICE_EXAM_N.md and desktop/questions.js.

Usage: python3 scripts/build_exams.py [path/to/docs_urls.txt]
The optional allowlist (one docs URL per line, e.g. from https://docs.databricks.com/aws/en/sitemap.xml)
makes every reference URL be checked against it.
"""
import json, pathlib, re, sys
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA, DOCS = ROOT / "data", ROOT / "docs"
OUT_JS = ROOT / "desktop" / "questions.js"

# Weights of the new exam (Oct 9, 2026) for 60 questions.
DOMAINS = {
    "Developing Code": 14, "Data Ingestion & Acquisition": 7, "Data Manipulation": 7,
    "Monitoring and Alerting": 6, "Cost & Performance Optimization": 9,
    "Data Security and Compliance": 5, "Data Governance": 3,
    "Debugging and Deploying": 6, "Data Modeling": 3,
}
# New storage keys: the questions changed, so old saved answers must not carry over.
EXAMS = {"exam1": "Practice Exam 1", "exam2": "Practice Exam 2"}
BANNED = re.compile(r"\b(all of the above|none of the above|both [a-d] and [a-d])\b", re.I)
LETTER_REF = re.compile(r"\b(?i:option|answer|choice)s? [A-D]\b|\([A-D]\)|\b(?i:unlike|like|than|vs\.?) [A-D]\b")
LEVEL = {"exam1": "", "exam2": " Level: harder, more code-reading."}

allow = None
if len(sys.argv) > 1:
    allow = {l.strip().rstrip("/") for l in pathlib.Path(sys.argv[1]).read_text().splitlines() if l.strip()}


def load(key):
    """Prefer the evidence-checked batches in data/verified/; fall back to data/examN.json."""
    n = key[-1]
    batches = sorted((DATA / "verified").glob(f"exam{n}_q*.json"))
    if not batches:
        return sorted(json.loads((DATA / f"exam{n}.json").read_text(encoding="utf-8"))["questions"], key=lambda q: q["id"])
    qs = [q for p in batches for q in json.loads(p.read_text(encoding="utf-8"))["questions"]]
    return sorted(qs, key=lambda q: q["id"])


def validate(qs):
    p = []
    if [q["id"] for q in qs] != list(range(1, 61)):
        p.append("ids are not exactly 1..60")
    dist = Counter(q["domain"] for q in qs)
    for d, n in DOMAINS.items():
        if dist.get(d, 0) != n:
            p.append(f"domain {d!r}: {dist.get(d, 0)} (expected {n})")
    for d in dist:
        if d not in DOMAINS:
            p.append(f"unknown domain {d!r}")
    for q in qs:
        i = q["id"]
        if sorted(q.get("options", {})) != list("ABCD"):
            p.append(f"Q{i}: options must be A-D")
        if q.get("answer") not in list("ABCD"):
            p.append(f"Q{i}: invalid answer")
        oe = q.get("optExpl", {})
        if sorted(oe) != list("ABCD"):
            p.append(f"Q{i}: optExpl must be A-D")
        elif [L for L in "ABCD" if oe[L].startswith("Correct")] != [q.get("answer")]:
            p.append(f"Q{i}: 'Correct:' must appear exactly once, on the answer letter")
        for L, txt in q.get("options", {}).items():
            if BANNED.search(txt):
                p.append(f"Q{i}{L}: banned option wording")
        if not q.get("stem") or not q.get("explanation"):
            p.append(f"Q{i}: empty stem or explanation")
        refs = q.get("refs") or []
        if not 1 <= len(refs) <= 3:
            p.append(f"Q{i}: needs 1-3 refs")
        for u in refs:
            if not u.startswith("https://docs.databricks.com/"):
                p.append(f"Q{i}: non-docs ref {u}")
            elif allow is not None and u.rstrip("/") not in allow:
                p.append(f"Q{i}: ref not in allowlist {u}")
        c = q.get("code")
        if c is not None and (not isinstance(c, dict) or not c.get("src")):
            p.append(f"Q{i}: malformed code")
        if LETTER_REF.search(q["explanation"] + " " + " ".join(oe.values())):
            p.append(f"Q{i}: explanation refers to options by letter")
    return p, dist, Counter(q["answer"] for q in qs)


def to_md(key, qs):
    lines = [f"# {EXAMS[key]} — Databricks Certified Data Engineer Professional (New Exam)", "",
             "> Original practice questions, not official exam questions. Answers are based on the Databricks "
             "documentation linked under each answer; verify in the docs. Time yourself: 120 minutes."
             + LEVEL[key], "", "## Questions", ""]
    for q in qs:
        lines += [f"### {q['id']}. [{q['domain']} — {q['topic']}]", "", q["stem"], ""]
        if q.get("code"):
            lines += [f"```{q['code'].get('lang', '')}", q["code"]["src"].rstrip(), "```", ""]
        lines += [f"{L}) {q['options'][L]}  " for L in "ABCD"] + ["", "---", ""]
    lines += ["## Answer Key and Explanations", ""]
    for q in qs:
        lines += [f"**{q['id']}. Answer: {q['answer']}**", "", q["explanation"], ""]
        lines += [f"- **{L}:** {q['optExpl'][L]}" for L in "ABCD"] + [""]
        lines += ["References: " + " · ".join(f"<{u}>" for u in q["refs"]), ""]
    return "\n".join(lines)


out, ok = {}, True
for key, title in EXAMS.items():
    qs = load(key)
    problems, dist, letters = validate(qs)
    print(f"== {title}: {len(qs)} questions | letters {dict(sorted(letters.items()))} | "
          f"code snippets {sum(1 for q in qs if q.get('code'))} | refs {sum(len(q['refs']) for q in qs)}")
    for x in problems:
        ok = False
        print("   PROBLEM:", x)
    (DATA / f"{key}.json").write_text(json.dumps({"questions": qs}, ensure_ascii=False, indent=1), encoding="utf-8")
    (DOCS / f"PRACTICE_EXAM_{key[-1]}.md").write_text(to_md(key, qs), encoding="utf-8")
    out[key] = {"title": title, "questions": qs}

OUT_JS.write_text("window.SIMULADOS = " + json.dumps(out, ensure_ascii=False, indent=1) + ";\n", encoding="utf-8")
print("questions.js:", OUT_JS.stat().st_size, "bytes")
sys.exit(0 if ok else 2)
