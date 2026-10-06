"""Converte docs/SIMULADO_*.md + data/just_*.json em desktop/questions.js."""
import re, json, sys, pathlib
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
DATA = ROOT / "data"
OUT = ROOT / "desktop" / "questions.js"

Q_HEADER = re.compile(r'^(?:###\s+(\d+)\.\s+\[(.+?)\]|\*\*(\d+)\.\s+\[(.+?)\]\*\*)\s*$')
OPT = re.compile(r'^([A-D])\)\s+(.*?)\s*$')
ANS = re.compile(r'^\*\*(\d+)\.\s*Resposta:\s*\**([A-D])\**\.?\s*\**\s*$')

SIMS = {
    "simulado1": ("Simulado 1", "SIMULADO_1.md", "just_1.json"),
    "simulado2": ("Simulado 2", "SIMULADO_2.md", "just_2.json"),
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
    gab_idx = next((i for i, l in enumerate(lines) if l.strip().startswith('## Gabarito')), len(lines))
    body, gabar = lines[:gab_idx], lines[gab_idx:]

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
    while j < len(gabar):
        m = ANS.match(gabar[j].strip())
        if not m:
            j += 1
            continue
        qid, letter = int(m.group(1)), m.group(2)
        j += 1
        expl = []
        while j < len(gabar):
            s = gabar[j].strip()
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
    probs = []
    if len(qs) != 60:
        probs.append(f"{len(qs)} questões (esperado 60)")
    if [q["id"] for q in qs] != list(range(1, len(qs) + 1)):
        probs.append("IDs não sequenciais 1..N")
    for q in qs:
        miss = [L for L in "ABCD" if L not in q["options"]]
        if miss:
            probs.append(f"Q{q['id']}: faltam alternativas {miss}")
        if q["answer"] not in "ABCD" or q["answer"] not in q["options"]:
            probs.append(f"Q{q['id']}: gabarito inválido")
        if not q["explanation"]:
            probs.append(f"Q{q['id']}: sem explicação")
        if not q["text"]:
            probs.append(f"Q{q['id']}: enunciado vazio")
    return probs, Counter(q["domain"] for q in qs)


out, ok = {}, True
for sid, (title, md, just) in SIMS.items():
    qs = parse(DOCS / md)
    probs, dist = validate(qs)
    jp = DATA / just
    if jp.exists():
        j = json.loads(jp.read_text(encoding="utf-8"))
        for q in qs:
            oe = j.get(str(q["id"]))
            if oe:
                q["optExpl"] = {k: oe[k] for k in "ABCD" if k in oe}
        missing = [q["id"] for q in qs if "optExpl" not in q]
        if missing:
            probs.append(f"sem justificativa por alternativa: {missing}")
    out[sid] = {"title": title, "questions": qs}
    print(f"== {title}: {len(qs)} questões | {dict(sorted(dist.items(), key=lambda x: -x[1]))}")
    for p in probs:
        ok = False
        print("   PROBLEMA:", p)

OUT.write_text("window.SIMULADOS = " + json.dumps(out, ensure_ascii=False, indent=1) + ";\n", encoding="utf-8")
print("questions.js:", OUT.stat().st_size, "bytes")
sys.exit(0 if ok else 2)
