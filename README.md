# Job Automation AI

Local LinkedIn-focused job-search automation for Ermias:

- high-concurrency LinkedIn job scraping across your target roles and European markets
- OpenAI-based fit scoring and application tailoring
- local output artifacts for each strong match
- Google Sheets export
- optional email summaries
- optional FastAPI scraper server for n8n Cloud

## What this replaces

This project is the local alternative to the earlier n8n + Bright Data flow. It keeps the high-performance scraping idea, but removes the paid Bright Data dependency and the brittle workflow JSON issues. The default configuration is now LinkedIn-only.

## Project layout

- `main.py`: run the scraper only or the full automation pipeline
- `main.py phd-run`: run PhD automation (multi-source university discovery -> professor leads -> AI matching -> tailored artifacts -> sheet export)
- `scheduler.py`: daily scheduled run
- `job_scraper_server.py`: exposes `/health` and `/scrape` for n8n Cloud if you still want that path
- `job_automation/`: core package
- `job_automation/reports/job_automation.py`: job-automation report module (rows + headers)
- `job_automation/reports/phd_roles.py`: PhD-role research report module (rows + headers)
- `job_automation/university_scraper.py`: multi-source university collector (seed file, PhDPortal, fallback)
- `job_automation/professor_finder.py`: professor lead discovery from university names
- `job_automation/phd_pipeline.py`: end-to-end PhD workflow
- `job_automation/phd_email_automation.py`: optional outreach email sender
- `profiles/candidate_profile.md`: edit this to improve AI matching quality
- `apps_script/Code.gs`: optional Apps Script sink for Google Sheets

## Setup

1. Create a virtual environment and install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy the environment file and fill in your values.

```bash
cp .env.example .env
```

3. Edit `profiles/candidate_profile.md` with your real background, stack, and role priorities.

4. Set at least:

- `OPENAI_API_KEY`
- `RESUME_PDF_PATH`

5. For Google Sheets, choose one export path:

- Service account:
  - create a Google Cloud service account
  - enable Google Sheets API
  - enable Google Drive API (required for resume upload links)
  - download the JSON key
  - set `GOOGLE_SERVICE_ACCOUNT_JSON`
  - share the target spreadsheet with the service-account email
- Apps Script:
  - create or open an Apps Script project
  - paste `apps_script/Code.gs`
  - deploy it as a web app
  - set `GOOGLE_APPS_SCRIPT_WEBAPP_URL`
  - set `GOOGLE_SHEETS_SPREADSHEET_ID`

Optional sheet names:

- `GOOGLE_SHEETS_WORKSHEET` (default: `Jobs`)
- `GOOGLE_SHEETS_PHD_REPORT_WORKSHEET` (default: `phd-research-report`)
- `GOOGLE_DRIVE_UPLOAD_ENABLED` (default: `true`, uploads PhD `resume.txt` files to Drive and writes Drive links into `Resume Path`)
- `GOOGLE_DRIVE_ROOT_FOLDER_ID` (optional existing Drive folder ID; if empty, pipeline creates/uses `GOOGLE_DRIVE_ROOT_FOLDER_NAME`)
- `GOOGLE_DRIVE_ROOT_FOLDER_NAME` (default: `JobAutomationAI-PhD-Applications`)
- `GOOGLE_DRIVE_PUBLIC_LINKS` (default: `true`, tries to set `anyone with link` read access)
- `GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_JSON` (optional; if set, Drive uploader can fall back to user OAuth when service-account uploads fail with storage quota limits)
- `GOOGLE_DRIVE_OAUTH_TOKEN_JSON` (optional token cache path, default `credentials/google-drive-oauth-token.json`)

PhD pipeline settings:

- `PHD_PORTAL_UNIVERSITIES_URL` (default: `https://www.phdportal.com/search/universities/phd/rankings/computer-science-it`)
- `PHD_UNIVERSITY_SOURCE_ORDER` (default: `seed_file,phdportal,fallback`)
- `PHD_UNIVERSITY_SEED_FILE` (default: `profiles/phd_universities_seed.csv`)
- `PHD_MAX_UNIVERSITIES`
- `PHD_PROFESSORS_PER_UNIVERSITY`
- `PHD_SUBJECT_KEYWORDS`
- `PHD_SEND_EMAILS` (set `true` to send outreach emails to leads that include an email address)

By default, the pipeline uses a local university seed file first to avoid PhDPortal anti-bot blocks, then optionally tries PhDPortal, then falls back to a built-in list.

Note: the Apps Script ID you pasted earlier is not a spreadsheet ID. The spreadsheet ID is the value between `/d/` and `/edit` in the Google Sheets URL.

## Usage

Run the full pipeline:

```bash
python3 main.py run
```

Run without OpenAI calls:

```bash
python3 main.py run --no-ai
```

Run only the scraper:

```bash
python3 main.py scrape --pretty
```

Run AI recommendation and CV tailoring on an existing scrape result:

```bash
python3 main.py recommend --input outputs/<run_id>/raw_jobs.json
```

Or use the heuristic path only:

```bash
python3 main.py recommend --input outputs/<run_id>/raw_jobs.json --no-ai
```

Run the PhD pipeline:

```bash
python3 main.py phd-run
```

Heuristic-only PhD run:

```bash
python3 main.py phd-run --no-ai
```

For LinkedIn, start with a very small test matrix in `.env` because LinkedIn rate-limits aggressively:

```env
JOB_TITLES=Software Engineer
MAX_SEARCH_TARGETS=3
SCRAPER_MAX_WORKERS=1
SCRAPER_BATCH_PAUSE_SECONDS=10
LINKEDIN_FETCH_DESCRIPTION=false
```

Then expand gradually once you confirm your IP is not immediately returning `429`.

Run the daily scheduler:

```bash
python3 scheduler.py
```

Run the scraper API for n8n Cloud:

```bash
python3 job_scraper_server.py
```

## Outputs

Each run creates a folder under `outputs/<run_id>/` with:

- `raw_jobs.json`
- `matched_jobs.csv`
- `phd_research_report.csv`
- `run_summary.json`
- `applications/<company-role>/resume.txt`
- `applications/<company-role>/cover_letter.txt`
- `applications/<company-role>/email_intro.txt`

## Performance notes

The scraper keeps the parallel design, but LinkedIn-specific defaults are intentionally throttled:

- batched `ThreadPoolExecutor` execution instead of one 144-request burst
- randomized per-request jitter
- pause between batches to reduce `429` responses
- thread-safe dedup removes duplicate postings
- the pipeline limits OpenAI scoring to the best candidate jobs instead of sending every raw result
- application tailoring is also concurrent, with a lower worker count to avoid API throttling

## Caveats

- `python-jobspy` can be sensitive to site-side changes. This project defaults to `JOB_SITES=linkedin`.
- LinkedIn guest scraping is rate-limited. A 144-search run from one IP will often fail if you try to do it too fast. The defaults now bias toward reliability over raw speed.
- Email delivery requires SMTP credentials. Gmail usually requires an App Password, not your regular account password.
- The tailored outputs are markdown drafts. They are designed to be reviewed before submission.
