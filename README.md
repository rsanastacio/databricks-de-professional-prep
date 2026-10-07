# Databricks Data Engineer Professional — study kit

Final-review kit for the **Databricks Certified Data Engineer Professional** exam, version live **from Oct 9, 2026** (60 questions, 120 minutes, English, 9 sections).

> The practice exam questions are **original practice questions**, not official exam questions. Every answer links to the Databricks documentation that supports it; the source of truth is the [official exam guide](https://www.databricks.com/learn/certification/data-engineer-professional) and the [documentation](https://docs.databricks.com). Verify features before the exam.

## Practice exams (app)

**Open:** https://rsanastacio.github.io/databricks-de-professional-prep/

- 2 practice exams × 60 questions, weighted like the official exam domains (14/7/7/6/9/5/3/6/3).
- Exam-style questions: multi-constraint scenarios, code-reading (PySpark, SQL, declarative pipelines, `databricks.yml`) and troubleshooting from symptoms. Exam 2 is harder and more code-heavy.
- **Instant feedback**: verdict, explanation, a rationale for **every option**, and links to the docs that back the answer.
- **Exam mode**: with instant feedback off, grading appears only at the end, with a score per domain against the ~70% pass mark.
- 120-minute timer, "review incorrect", progress saved on the device.

### On iPhone
1. Open the URL in **Safari**. Opening the HTML file from the Files or Mail preview does not work, because that preview does not run JavaScript.
2. Share → **Add to Home Screen**.
3. Always launch it from the icon. After the first visit it **works offline** (the doc links need a connection).

Progress is stored on the device, per context: the Home Screen icon and a Safari tab do not share answers.

Desktop version (every question on one page): [`desktop/`](https://rsanastacio.github.io/databricks-de-professional-prep/desktop/).

## Study material

| File | Contents |
|---|---|
| [docs/STUDY_PLAN.md](docs/STUDY_PLAN.md) | 14-day plan ordered by domain weight + cheat sheet + score tracker |
| [docs/STUDY_GUIDE.md](docs/STUDY_GUIDE.md) | Recap of the 9 domains, decision heuristics, AUTO CDC/SCD deep dive |
| [docs/PRACTICE_EXAM_1.md](docs/PRACTICE_EXAM_1.md) · [docs/PRACTICE_EXAM_2.md](docs/PRACTICE_EXAM_2.md) | Questions + answer key with per-option rationale and doc links (generated) |
| [docs/study_plan.ics](docs/study_plan.ics) | The 14-day schedule, to import into a calendar |

## Rebuilding

The source of truth is `data/exam1.json` and `data/exam2.json`. After editing them:

```bash
python3 scripts/build_exams.py [docs_urls.txt]  # validates; writes docs/PRACTICE_EXAM_*.md and desktop/questions.js
python3 scripts/check_links.py                  # every reference URL must answer HTTP 200
python3 scripts/build_mobile.py                 # writes index.html, sw.js, manifest and icons
```

The optional `docs_urls.txt` is an allowlist of documentation URLs (for example extracted from `https://docs.databricks.com/aws/en/sitemap.xml`). `sw.js` gets a new version on every build, so installed copies pick up the update the next time they open with a connection.
