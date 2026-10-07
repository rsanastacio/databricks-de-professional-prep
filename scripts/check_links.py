"""Check that every reference URL in data/exam*.json answers HTTP 200."""
import json, pathlib, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

DATA = pathlib.Path(__file__).resolve().parent.parent / "data"
urls = sorted({u for f in DATA.glob("exam[0-9].json")
               for q in json.loads(f.read_text(encoding="utf-8"))["questions"] for u in q["refs"]})


def status(u):
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 link-check"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return u, r.status, r.geturl()
    except Exception as e:  # report every failure; nothing is skipped silently
        return u, getattr(e, "code", repr(e)), None


with ThreadPoolExecutor(12) as ex:
    results = list(ex.map(status, urls))
bad = [(u, s) for u, s, _ in results if s != 200]
moved = [(u, f) for u, s, f in results if s == 200 and f and f.rstrip("/") != u.rstrip("/")]
print(f"{len(urls)} unique URLs | {len(urls) - len(bad)} OK | {len(bad)} failing | {len(moved)} redirected")
for u, s in bad:
    print("  FAIL", s, u)
for u, f in moved:
    print("  REDIRECT", u, "->", f)
sys.exit(1 if bad else 0)
