# Databricks Data Engineer Professional — study kit

Final-review kit for the **Databricks Certified Data Engineer Professional** exam, version live **from Oct 9, 2026** (60 questions, 120 minutes, English, 9 sections).

> The practice exam questions are **original practice questions**, not official exam questions. Answers follow the Databricks documentation, but the source of truth is the [official exam guide](https://www.databricks.com/learn/certification/data-engineer-professional) and the [documentation](https://docs.databricks.com). Verify features before the exam.

## Practice exams (app)

**Open:** https://rsanastacio.github.io/databricks-de-professional-prep/

- 2 practice exams × 60 questions, weighted like the official exam domains.
- **Instant feedback**: shows the verdict, explains why, and gives a rationale for **every option** (why the correct one is right and why each distractor is wrong).
- **Exam mode**: with instant feedback off, grading appears only at the end, with a score per domain against the ~70% pass mark.
- 120-minute timer, "review incorrect", progress saved on the device.

### On iPhone
1. Open the URL in **Safari**. Opening the HTML file from the Files or Mail preview does not work, because that preview does not run JavaScript.
2. Share → **Add to Home Screen**.
3. Always launch it from the icon. After the first visit it **works offline**.

Progress is stored on the device, per context: the Home Screen icon and a Safari tab do not share answers.

Desktop version (every question on one page): [`desktop/`](https://rsanastacio.github.io/databricks-de-professional-prep/desktop/).

## Study material

| File | Contents |
|---|---|
| [docs/STUDY_PLAN.md](docs/STUDY_PLAN.md) | 14-day plan ordered by domain weight + cheat sheet + score tracker |
| [docs/STUDY_GUIDE.md](docs/STUDY_GUIDE.md) | Recap of the 9 domains, decision heuristics, AUTO CDC/SCD deep dive |
| [docs/PRACTICE_EXAM_1.md](docs/PRACTICE_EXAM_1.md) · [docs/PRACTICE_EXAM_2.md](docs/PRACTICE_EXAM_2.md) | Questions + answer key with explanations (source for the app) |
| [docs/study_plan.ics](docs/study_plan.ics) | The 14-day schedule, to import into a calendar |

## Rebuilding the app

After editing `docs/PRACTICE_EXAM_*.md` or `data/option_rationales_*.json`:

```bash
python3 scripts/parse_practice_exams.py   # validates and writes desktop/questions.js
python3 scripts/build_mobile.py      # writes index.html, sw.js, manifest and icons
```

`sw.js` gets a new version on every build, so installed copies pick up the update the next time they open with a connection.
