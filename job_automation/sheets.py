from __future__ import annotations

import csv
import logging
from pathlib import Path

from job_automation.config import Settings
from job_automation.defaults import DEFAULT_SHEET_HEADERS
from job_automation.models import MatchResult

logger = logging.getLogger(__name__)


class SheetExporter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def export(self, run_id: str, searched_at: str, matches: list[MatchResult], output_dir: Path) -> dict:
        rows = [self._row(run_id, searched_at, match) for match in matches]
        csv_path = output_dir / "matched_jobs.csv"
        self._write_local_csv(csv_path, rows)

        remote_status = "skipped"
        if rows:
            remote_status = self._export_remote(rows)

        return {"csv_path": str(csv_path), "remote_status": remote_status}

    def _export_remote(self, rows: list[dict]) -> str:
        # Prefer service account because it is more reliable than public Apps Script endpoints.
        can_use_service_account = (
            self.settings.google_sheets_spreadsheet_id
            and self.settings.google_service_account_json
            and self.settings.google_service_account_json.exists()
        )
        can_use_apps_script = bool(self.settings.google_apps_script_webapp_url)

        if can_use_service_account:
            try:
                self._append_via_service_account(rows)
                return "service_account"
            except Exception:
                logger.exception("Service-account export failed; trying Apps Script fallback")
                if can_use_apps_script:
                    try:
                        self._append_via_apps_script(rows)
                        return "apps_script_fallback"
                    except Exception as exc:
                        logger.exception("Apps Script fallback also failed; local CSV was still written")
                        return f"error:{exc.__class__.__name__}"
                return "error:ServiceAccountExportFailed"

        if can_use_apps_script:
            try:
                self._append_via_apps_script(rows)
                return "apps_script"
            except Exception as exc:
                logger.exception("Apps Script export failed; local CSV was still written")
                return f"error:{exc.__class__.__name__}"

        return "skipped"

    def _write_local_csv(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DEFAULT_SHEET_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _append_via_apps_script(self, rows: list[dict]) -> None:
        import requests

        response = requests.post(
            self.settings.google_apps_script_webapp_url,
            json={
                "spreadsheetId": self.settings.google_sheets_spreadsheet_id,
                "worksheet": self.settings.google_sheets_worksheet,
                "headers": DEFAULT_SHEET_HEADERS,
                "rows": rows,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Apps Script export failed with HTTP {response.status_code}: {response.text[:300]}"
            )
        response.raise_for_status()

    def _append_via_service_account(self, rows: list[dict]) -> None:
        import gspread

        client = gspread.service_account(
            filename=str(self.settings.google_service_account_json)
        )
        spreadsheet = client.open_by_key(self.settings.google_sheets_spreadsheet_id)
        worksheet = self._get_or_create_worksheet(spreadsheet)
        headers = self._ensure_headers(worksheet)

        worksheet.append_rows(
            [[row.get(header, "") for header in headers] for row in rows],
            value_input_option="USER_ENTERED",
        )

    def _get_or_create_worksheet(self, spreadsheet):
        try:
            return spreadsheet.worksheet(self.settings.google_sheets_worksheet)
        except Exception:
            return spreadsheet.add_worksheet(
                title=self.settings.google_sheets_worksheet,
                rows=1000,
                cols=len(DEFAULT_SHEET_HEADERS),
            )

    def _ensure_headers(self, worksheet) -> list[str]:
        existing_headers = [value for value in worksheet.row_values(1) if value]
        if not existing_headers:
            worksheet.append_row(DEFAULT_SHEET_HEADERS)
            return list(DEFAULT_SHEET_HEADERS)

        missing_headers = [
            header for header in DEFAULT_SHEET_HEADERS if header not in existing_headers
        ]
        if not missing_headers:
            return existing_headers

        merged_headers = existing_headers + missing_headers
        worksheet.update("A1", [merged_headers])
        return merged_headers

    @staticmethod
    def _row(run_id: str, searched_at: str, match: MatchResult) -> dict:
        summary = match.job.job_summary or match.job.job_description_formatted
        resume_doc = (
            match.artifacts.resume_doc_title
            if match.artifacts and match.artifacts.resume_doc_title
            else f"Resume - {match.job.job_title} @ {match.job.company_name}"
        )
        return {
            "Job Title": match.job.job_title,
            "Company": match.job.company_name,
            "Location": match.job.job_location,
            "Employment Type": match.job.job_employment_type,
            "Seniority": match.job.job_seniority_level,
            "Salary Range": match.job.job_base_pay_range,
            "Applicants": match.job.job_num_applicants,
            "Posted": match.job.job_posted_time,
            "Apply Link": match.job.apply_link,
            "Company URL": match.job.company_url,
            "Job Summary": summary,
            "AI Fit": "Yes" if match.assessment.ai_fit else "No",
            "Resume Doc": resume_doc,
            "Fit Score": match.assessment.fit_score,
            "Recommendation": match.assessment.recommendation,
            "Decision": match.assessment.decision,
            "Reasoning": match.assessment.reasoning,
            "Missing Skills": " | ".join(match.assessment.missing_skills),
            "Candidate Highlights": " | ".join(match.assessment.candidate_highlights),
            "Resume Focus": " | ".join(match.assessment.resume_focus),
            "Resume Summary": match.artifacts.resume_summary if match.artifacts else "",
            "Resume Path": str(match.artifacts.resume_path) if match.artifacts and match.artifacts.resume_path else "",
            "Cover Letter Path": str(match.artifacts.cover_letter_path) if match.artifacts and match.artifacts.cover_letter_path else "",
            "Email Intro Path": str(match.artifacts.email_intro_path) if match.artifacts and match.artifacts.email_intro_path else "",
            "Source Site": match.job.source_site,
            "Search Title": match.job.search_title,
            "Search Country": match.job.search_country,
            "Run ID": run_id,
            "Searched At": searched_at,
        }
