"""Check every verbatim evidence quote in data/verified/audit_*.json against the live doc page.

A quote passes if, after normalising whitespace, case, quotes and markdown/code markers, it appears
as a contiguous substring of the page text. Exit code 1 if any quote fails.
"""
import html, json, pathlib, re, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

VER = pathlib.Path(__file__).resolve().parent.parent / "data" / "verified"


def norm(s):
    s = html.unescape(s)
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = s.replace("—", "-").replace("–", "-").replace(" ", " ")
    s = re.sub(r"[`*_]", "", s)
    return re.sub(r"\s+", " ", s).strip().lower()


def page_text(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 evidence-check"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8", "replace")
    raw = re.sub(r"(?is)<(script|style|noscript)\b.*?</\1>", " ", raw)
    raw = re.sub(r"(?s)<[^>]+>", " ", raw)
    return norm(raw)


files = [pathlib.Path(p) for p in sys.argv[1:]] or sorted(VER.glob("audit_*.json"))
entries, bad_files = [], []
for f in files:
    d = json.loads(f.read_text(encoding="utf-8"))
    if not isinstance(d, dict) or not isinstance(d.get("audit"), list):
        bad_files.append(f.name)
        continue
    for a in d["audit"]:
        entries.append((f.name, a))

def fetch(url):
    try:
        return url, page_text(url), None
    except Exception as e:  # an unreachable page fails its quotes; it must not pass silently
        return url, "", repr(e)


urls = sorted({a["evidence_url"] for _, a in entries})
with ThreadPoolExecutor(10) as ex:
    fetched = list(ex.map(fetch, urls))
pages = {u: t for u, t, _ in fetched}
for u, _, err in fetched:
    if err:
        print(f"  FETCH ERROR {u}: {err}")

fail = []
for name, a in entries:
    ev = norm(a.get("evidence", ""))
    if len(ev.split()) < 6 or ev not in pages.get(a["evidence_url"], ""):
        fail.append((name, a["id"], a["evidence_url"], a.get("evidence", "")[:90]))

print(f"{len(entries)} evidence quotes | {len(entries) - len(fail)} found verbatim on the live page | {len(fail)} not found")
for name, i, u, ev in fail:
    print(f"  FAIL {name} Q{i}: {u}\n       \"{ev}...\"")
for name in bad_files:
    print(f"  FAIL {name}: not in the {{\"audit\": [...]}} format, so none of its quotes can be checked")
sys.exit(1 if fail or bad_files else 0)
