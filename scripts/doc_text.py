"""Print the raw text of a Databricks docs page (no summarisation).

  python3 scripts/doc_text.py <url>                 # whole page text, one block per line
  python3 scripts/doc_text.py <url> <regex> [...]   # only lines matching any regex (case-insensitive)

Use it to copy verbatim evidence: the text printed here is what verify_evidence.py matches against.
"""
import html, re, sys, urllib.request


def lines(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 doc-text"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8", "replace")
    main = re.search(r"(?is)<article\b.*?</article>", raw)
    raw = main.group(0) if main else raw
    raw = re.sub(r"(?is)<(script|style|noscript|svg)\b.*?</\1>", " ", raw)
    raw = re.sub(r"(?i)</(p|li|h[1-6]|tr|pre|div|td|th|dt|dd)>|<br\s*/?>", "\n", raw)
    raw = html.unescape(re.sub(r"(?s)<[^>]+>", " ", raw))
    out = []
    for l in raw.split("\n"):
        l = re.sub(r"[ \t ]+", " ", l).strip()
        if len(l) > 2:
            out.append(l)
    return out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    pats = [re.compile(p, re.I) for p in sys.argv[2:]]
    for l in lines(sys.argv[1]):
        if not pats or any(p.search(l) for p in pats):
            print(l)
