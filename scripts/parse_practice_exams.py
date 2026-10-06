"""Convert docs/PRACTICE_EXAM_*.md + data/option_rationales_*.json into desktop/questions.js."""
import re, json, sys, pathlib
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
DATA = ROOT / "data"
OUT = ROOT / "desktop" / "questions.js"

Q_HEADER = re.compile(r'^(?:###\s+(\d+)\.\s+\[(.+?)\]|\*\*(\d+)\.\s+\[(.+?)\]\*\*)\s*$')
OPT = re.compile(r'^([A-D])\)\s+(.*?)\s*$')
ANS = re.compile(r'^\*\*(\d+)\.\s*Answer:\s*\**([A-D])\**\.?\s*\**\s*$')

# Keys must stay stable: the app stores progress in localStorage under them.
EXAMS = {
    "simulado1": ("Practice Exam 1", "PRACTICE_EXAM_1.md", "option_rationales_1.json"),
    "simulado2": ("Practice Exam 2", "PRACTICE_EXAM_2.md", "option_rationales_2.json"),
}


def split_domain_topic(label):
    for sep in [' — ', ' – ', ' - ']:
        if sep in label:
            d, t = label.split(sep, 1)
            return d.strip(), t.strip()
    return label.strip(), ""


def clean(s):
    return s.replace('**', '').strip().rstrip('\\').strip()


def parse(path):
    lines = path.read_text(encoding='utf-8').splitlines()
    key_idx = next((i for i, l in enumerate(lines) if l.strip().startswith('## Answer Key')), len(lines))
    body, key = lines[:key_idx], lines[key_idx:]

    questions = {}
    i = 0
    while i < len(body):
        m = Q_HEADER.match(body[i])
        if not m:
            i += 1
            continue
        qid = int(m.group(1) or m.group(3))
        dom, topic = split_domain_topic(m.group(2) or m.group(4))
        i += 1
        text_lines, opts = [], {}
        while i < len(body):
            l = body[i]
            if Q_HEADER.match(l):
                break
            if l.strip() == '---':
                i += 1
                break
            om = OPT.match(l)
            if om:
                opts[om.group(1)] = clean(om.group(2))
            elif l.strip() and not opts:
                text_lines.append(l.strip())
            i += 1
        questions[qid] = {"id": qid, "domain": dom, "topic": topic,
                          "text": " ".join(text_lines).strip(),
                          "options": opts, "answer": None, "explanation": ""}

    j = 0
    while j < len(key):
        m = ANS.match(key[j].strip())
        if not m:
            j += 1
            continue
        qid, letter = int(m.group(1)), m.group(2)
        j += 1
        expl = []
        while j < len(key):
            s = key[j].strip()
            if ANS.match(s) or s.startswith(('## ', '---', '> ')):
                break
            if s:
                expl.append(s.replace('**', ''))
            elif expl:
                break
            j += 1
        if qid in questions:
            questions[qid]["answer"] = letter
            questions[qid]["explanation"] = " ".join(expl).strip()

    return [questions[k] for k in sorted(questions)]


def validate(qs):
    problems = []
    if len(qs) != 60:
        problems.append(f"{len(qs)} questions (expected 60)")
    if [q["id"] for q in qs] != list(range(1, len(qs) + 1)):
        problems.append("question IDs are not sequential 1..N")
    for q in qs:
        missing = [L for L in "ABCD" if L not in q["options"]]
        if missing:
            problems.append(f"Q{q['id']}: missing options {missing}")
        if q["answer"] not in "ABCD" or q["answer"] not in q["options"]:
            problems.append(f"Q{q['id']}: invalid answer key")
        if not q["explanation"]:
            problems.append(f"Q{q['id']}: no explanation")
        if not q["text"]:
            problems.append(f"Q{q['id']}: empty question text")
    return problems, Counter(q["domain"] for q in qs)


out, ok = {}, True
for key, (title, md, rationales) in EXAMS.items():
    qs = parse(DOCS / md)
    problems, dist = validate(qs)
    rp = DATA / rationales
    if rp.exists():
        r = json.loads(rp.read_text(encoding="utf-8"))
        for q in qs:
            oe = r.get(str(q["id"]))
            if oe:
                q["optExpl"] = {k: oe[k] for k in "ABCD" if k in oe}
        missing = [q["id"] for q in qs if "optExpl" not in q]
        if missing:
            problems.append(f"no per-option rationale: {missing}")
    out[key] = {"title": title, "questions": qs}
    print(f"== {title}: {len(qs)} questions | {dict(sorted(dist.items(), key=lambda x: -x[1]))}")
    for p in problems:
        ok = False
        print("   PROBLEM:", p)

OUT.write_text("window.SIMULADOS = " + json.dumps(out, ensure_ascii=False, indent=1) + ";\n", encoding="utf-8")
print("questions.js:", OUT.stat().st_size, "bytes")
sys.exit(0 if ok else 2)
