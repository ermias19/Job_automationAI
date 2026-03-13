from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from job_automation.config import Settings
from job_automation.defaults import DEFAULT_PHD_REPORT_HEADERS, DEFAULT_SHEET_HEADERS
from job_automation.models import MatchResult
from job_automation.reports import build_job_automation_rows, build_phd_role_rows

logger = logging.getLogger(__name__)


class SheetExporter:
    """Coordinator that exports two independent report streams:
    1) Jobs sheet
    2) PhD roles sheet
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.job_exporter = JobAutomationWorksheetExporter(settings)
        self.phd_exporter = PhdRoleWorksheetExporter(settings)

    def export(
        self,
        run_id: str,
        searched_at: str,
        matches: list[MatchResult],
        output_dir: Path,
    ) -> dict:
        job_rows = build_job_automation_rows(
            run_id=run_id,
            searched_at=searched_at,
            matches=matches,
        )
        csv_path = output_dir / "matched_jobs.csv"
        self._write_local_csv(csv_path, job_rows, DEFAULT_SHEET_HEADERS)
        xlsx_path = output_dir / "matched_jobs.xlsx"
        xlsx_written = self._write_local_xlsx(
            xlsx_path,
            job_rows,
            DEFAULT_SHEET_HEADERS,
            worksheet_title="job-automation",
        )

        remote_status = "skipped"
        if job_rows:
            remote_status = self._export_jobs_only_remote(job_rows=job_rows)

        return {
            "csv_path": str(csv_path),
            "xlsx_path": str(xlsx_path) if xlsx_written else "",
            "phd_report_csv_path": "",
            "phd_report_xlsx_path": "",
            "remote_status": remote_status,
        }

    def export_phd_only(
        self,
        run_id: str,
        searched_at: str,
        matches: list[MatchResult],
        output_dir: Path,
    ) -> dict:
        phd_rows = build_phd_role_rows(
            run_id=run_id,
            searched_at=searched_at,
            matches=matches,
        )
        phd_report_csv_path = output_dir / "phd_research_report.csv"
        self._write_local_csv(phd_report_csv_path, phd_rows, DEFAULT_PHD_REPORT_HEADERS)
        phd_report_xlsx_path = output_dir / "phd_research_report.xlsx"
        xlsx_written = self._write_local_xlsx(
            phd_report_xlsx_path,
            phd_rows,
            DEFAULT_PHD_REPORT_HEADERS,
            worksheet_title="phd-research-report",
        )

        # Safeguard: PhD report writes are full refreshes so stale rows never accumulate.
        remote_status = self._export_phd_only_remote(phd_rows)

        return {
            "csv_path": "",
            "xlsx_path": "",
            "phd_report_csv_path": str(phd_report_csv_path),
            "phd_report_xlsx_path": str(phd_report_xlsx_path) if xlsx_written else "",
            "remote_status": remote_status,
        }

    def _export_jobs_only_remote(self, job_rows: list[dict]) -> str:
        can_use_service_account = (
            self.settings.google_sheets_spreadsheet_id
            and self.settings.google_service_account_json
            and self.settings.google_service_account_json.exists()
        )
        can_use_apps_script = bool(self.settings.google_apps_script_webapp_url)

        if can_use_service_account:
            try:
                self.job_exporter.append_via_service_account(job_rows)
                return "service_account"
            except Exception:
                logger.exception("Service-account export failed; trying Apps Script fallback")
                if can_use_apps_script:
                    try:
                        self.job_exporter.append_via_apps_script(job_rows)
                        return "apps_script_fallback"
                    except Exception as exc:
                        logger.exception("Apps Script fallback also failed; local CSV was still written")
                        return f"error:{exc.__class__.__name__}"
                return "error:ServiceAccountExportFailed"

        if can_use_apps_script:
            try:
                self.job_exporter.append_via_apps_script(job_rows)
                return "apps_script"
            except Exception as exc:
                logger.exception("Apps Script export failed; local CSV was still written")
                return f"error:{exc.__class__.__name__}"

        return "skipped"

    def _export_phd_only_remote(self, phd_rows: list[dict]) -> str:
        can_use_service_account = (
            self.settings.google_sheets_spreadsheet_id
            and self.settings.google_service_account_json
            and self.settings.google_service_account_json.exists()
        )
        can_use_apps_script = bool(self.settings.google_apps_script_webapp_url)

        if can_use_service_account:
            try:
                self.phd_exporter.replace_via_service_account(phd_rows)
                return "service_account"
            except Exception:
                logger.exception("Service-account export failed for PhD sheet; trying Apps Script fallback")
                if can_use_apps_script:
                    try:
                        self.phd_exporter.replace_via_apps_script(phd_rows)
                        return "apps_script_fallback"
                    except Exception as exc:
                        logger.exception("Apps Script fallback also failed for PhD sheet")
                        return f"error:{exc.__class__.__name__}"
                return "error:ServiceAccountExportFailed"

        if can_use_apps_script:
            try:
                self.phd_exporter.replace_via_apps_script(phd_rows)
                return "apps_script"
            except Exception as exc:
                logger.exception("Apps Script export failed for PhD sheet")
                return f"error:{exc.__class__.__name__}"

        return "skipped"

    @staticmethod
    def _write_local_csv(path: Path, rows: list[dict], headers: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    @staticmethod
    def _write_local_xlsx(
        path: Path,
        rows: list[dict],
        headers: list[str],
        worksheet_title: str,
    ) -> bool:
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill
        except ImportError:
            logger.warning("openpyxl is not installed; skipping XLSX export for %s", path.name)
            return False

        path.parent.mkdir(parents=True, exist_ok=True)

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = worksheet_title[:31] if worksheet_title else "report"

        for col_index, header in enumerate(headers, start=1):
            cell = sheet.cell(row=1, column=col_index, value=header)
            cell.fill = PatternFill(fill_type="solid", fgColor="339966")
            cell.font = Font(color="FFFFFF", bold=True)

        for row_index, row in enumerate(rows, start=2):
            for col_index, header in enumerate(headers, start=1):
                value: Any = row.get(header, "")
                sheet.cell(row=row_index, column=col_index, value=value)

        workbook.save(path)
        return True


class BaseWorksheetExporter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def worksheet_name(self) -> str:
        raise NotImplementedError

    @property
    def headers(self) -> list[str]:
        raise NotImplementedError

    def append_via_apps_script(self, rows: list[dict]) -> None:
        import requests

        response = requests.post(
            self.settings.google_apps_script_webapp_url,
            json={
                "spreadsheetId": self.settings.google_sheets_spreadsheet_id,
                "worksheet": self.worksheet_name,
                "headers": self.headers,
                "rows": rows,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Apps Script export failed with HTTP {response.status_code}: {response.text[:300]}"
            )
        response.raise_for_status()

    def replace_via_apps_script(self, rows: list[dict]) -> None:
        import requests

        response = requests.post(
            self.settings.google_apps_script_webapp_url,
            json={
                "spreadsheetId": self.settings.google_sheets_spreadsheet_id,
                "worksheet": self.worksheet_name,
                "headers": self.headers,
                "rows": rows,
                "clearExisting": True,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Apps Script export failed with HTTP {response.status_code}: {response.text[:300]}"
            )
        response.raise_for_status()

    def append_via_service_account(self, rows: list[dict]) -> None:
        import gspread

        client = gspread.service_account(filename=str(self.settings.google_service_account_json))
        spreadsheet = client.open_by_key(self.settings.google_sheets_spreadsheet_id)
        worksheet = self._get_or_create_worksheet(spreadsheet)
        resolved_headers, table_range = self._ensure_headers(worksheet)
        self._style_header_row(worksheet, len(resolved_headers))
        self._after_header_ready(worksheet=worksheet, headers=resolved_headers)

        append_kwargs = {"value_input_option": "USER_ENTERED"}
        if table_range:
            append_kwargs["table_range"] = table_range

        worksheet.append_rows(
            [[row.get(header, "") for header in resolved_headers] for row in rows],
            **append_kwargs,
        )

    def replace_via_service_account(self, rows: list[dict]) -> None:
        import gspread

        client = gspread.service_account(filename=str(self.settings.google_service_account_json))
        spreadsheet = client.open_by_key(self.settings.google_sheets_spreadsheet_id)
        worksheet = self._get_or_create_worksheet(spreadsheet)
        self._clear_worksheet_data(worksheet)

        resolved_headers, _ = self._ensure_headers(worksheet)
        self._style_header_row(worksheet, len(resolved_headers))
        self._after_header_ready(worksheet=worksheet, headers=resolved_headers)

        if not rows:
            return

        worksheet.append_rows(
            [[row.get(header, "") for header in resolved_headers] for row in rows],
            value_input_option="USER_ENTERED",
        )

    def _after_header_ready(self, worksheet, headers: list[str]) -> None:
        """Hook for subclass-specific worksheet setup after headers are ready."""
        return

    def _get_or_create_worksheet(self, spreadsheet):
        try:
            return spreadsheet.worksheet(self.worksheet_name)
        except Exception:
            return spreadsheet.add_worksheet(
                title=self.worksheet_name,
                rows=1000,
                cols=len(self.headers),
            )

    def _ensure_headers(self, worksheet) -> tuple[list[str], str | None]:
        existing_headers = [value for value in worksheet.row_values(1) if value]
        if not existing_headers:
            try:
                worksheet.append_row(self.headers)
                return list(self.headers), None
            except Exception:
                logger.warning("Header row could not be written; appending data starting from A2")
                return list(self.headers), "A2"

        extra_headers = [header for header in existing_headers if header not in self.headers]
        target_headers = list(self.headers) + extra_headers

        if existing_headers == target_headers:
            self._repair_legacy_rows_if_needed(worksheet=worksheet, target_headers=target_headers)
            return existing_headers, None

        try:
            self._migrate_existing_rows_to_target_headers(
                worksheet=worksheet,
                existing_headers=existing_headers,
                target_headers=target_headers,
            )
            worksheet.update(range_name="A1", values=[target_headers])
            return target_headers, None
        except Exception:
            logger.warning("Could not update header row; using existing headers as-is")
            return existing_headers, None

    def _repair_legacy_rows_if_needed(self, worksheet, target_headers: list[str]) -> None:
        """Default: no-op. Subclass can override."""
        return

    def _migrate_existing_rows_to_target_headers(
        self,
        worksheet,
        existing_headers: list[str],
        target_headers: list[str],
    ) -> None:
        used_last_row = worksheet.get_all_values()
        if not used_last_row:
            return

        used_last_row_index = len(used_last_row)
        if used_last_row_index <= 1:
            return

        old_end_col = self._column_index_to_letter(len(existing_headers))
        current_rows = worksheet.get(f"A2:{old_end_col}{used_last_row_index}")
        if not current_rows:
            return

        normalized_rows: list[list[str]] = []
        for row in current_rows:
            row_padded = row + [""] * (len(existing_headers) - len(row))
            row_dict = {header: row_padded[idx] for idx, header in enumerate(existing_headers)}
            normalized_rows.append([row_dict.get(header, "") for header in target_headers])

        new_end_col = self._column_index_to_letter(len(target_headers))
        worksheet.update(
            range_name=f"A2:{new_end_col}{used_last_row_index}",
            values=normalized_rows,
        )

    def _clear_worksheet_data(self, worksheet) -> None:
        try:
            worksheet.clear()
            return
        except Exception:
            logger.warning(
                "Could not fully clear worksheet %s; trying row-content clear fallback",
                self.worksheet_name,
            )
        try:
            worksheet.batch_clear(["A2:ZZZ"])
        except Exception:
            logger.warning(
                "Could not clear worksheet row contents for %s; continuing",
                self.worksheet_name,
            )

    def _style_header_row(self, worksheet, header_count: int) -> None:
        if header_count <= 0:
            return

        end_col = self._column_index_to_letter(header_count)
        header_range = f"A1:{end_col}1"
        try:
            worksheet.format(
                header_range,
                {
                    "backgroundColor": {"red": 0.20, "green": 0.60, "blue": 0.20},
                    "textFormat": {
                        "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        "bold": True,
                    },
                },
            )
        except Exception:
            logger.warning("Could not style header row; continuing without formatting")

    @staticmethod
    def _column_index_to_letter(index: int) -> str:
        letters: list[str] = []
        while index > 0:
            index, rem = divmod(index - 1, 26)
            letters.append(chr(65 + rem))
        return "".join(reversed(letters))


class JobAutomationWorksheetExporter(BaseWorksheetExporter):
    @property
    def worksheet_name(self) -> str:
        return self.settings.google_sheets_worksheet

    @property
    def headers(self) -> list[str]:
        return DEFAULT_SHEET_HEADERS

    def _after_header_ready(self, worksheet, headers: list[str]) -> None:
        from gspread.utils import ValidationConditionType

        if "Application Status" not in headers:
            return

        status_col_index = headers.index("Application Status") + 1
        status_col_letter = self._column_index_to_letter(status_col_index)
        status_range = f"{status_col_letter}2:{status_col_letter}"
        try:
            worksheet.add_validation(
                status_range,
                ValidationConditionType.one_of_list,
                ["Applied", "Interview", "Rejected"],
                strict=True,
                showCustomUi=True,
            )
        except Exception:
            logger.warning("Could not apply status dropdown validation; continuing without validation")

    def _repair_legacy_rows_if_needed(self, worksheet, target_headers: list[str]) -> None:
        used_values = worksheet.get_all_values()
        if len(used_values) <= 1:
            return

        yes_no_values = {"Yes", "No"}
        target_len = len(target_headers)
        job_summary_index = target_headers.index("Job Summary")
        app_status_index = target_headers.index("Application Status")
        ai_fit_index = target_headers.index("AI Fit")
        resume_doc_index = target_headers.index("Resume Doc")
        fit_score_index = target_headers.index("Fit Score")

        repaired_rows: list[list[str]] = []
        changed = False

        for row in used_values[1:]:
            row_padded = (row + [""] * target_len)[:target_len]
            app_status_value = row_padded[app_status_index].strip()
            tail_status_value = row_padded[-1].strip()
            ai_fit_value = row_padded[ai_fit_index].strip()
            job_summary_value = row_padded[job_summary_index].strip()
            resume_doc_value = row_padded[resume_doc_index].strip()
            fit_score_value = row_padded[fit_score_index].strip()

            # Case 1: Resume title in Application Status and shift from Resume Doc onward.
            if self._looks_like_resume_doc_text(app_status_value) and ai_fit_value in yes_no_values:
                corrected = list(row_padded)
                normalized_status = self._normalize_application_status(
                    app_status_value=app_status_value,
                    tail_status_value=tail_status_value,
                )

                for idx in range(target_len - 1, resume_doc_index, -1):
                    corrected[idx] = row_padded[idx - 1]

                corrected[resume_doc_index] = app_status_value
                corrected[app_status_index] = normalized_status
                repaired_rows.append(corrected)
                changed = True
                continue

            # Case 2: Legacy rows from pre-Application-Status schema.
            looks_like_old_layout = (
                ai_fit_value not in yes_no_values
                and (
                    job_summary_value in yes_no_values
                    or self._looks_like_resume_doc_text(ai_fit_value)
                    or (
                        self._looks_like_fit_score(resume_doc_value)
                        and self._looks_like_recommendation(fit_score_value)
                    )
                )
            )
            if looks_like_old_layout:
                corrected = [""] * target_len
                normalized_status = self._normalize_application_status(
                    app_status_value=app_status_value,
                    tail_status_value=tail_status_value,
                )
                col3_status = self._normalize_status_value(row_padded[2])

                corrected[0] = row_padded[0]
                corrected[1] = row_padded[1]
                corrected[app_status_index] = normalized_status
                corrected[3] = row_padded[2] if not col3_status else ""

                for new_idx in range(4, target_len):
                    corrected[new_idx] = row_padded[new_idx - 1]

                repaired_rows.append(corrected)
                changed = True
                continue

            repaired_rows.append(row_padded)

        if not changed:
            return

        end_col = self._column_index_to_letter(target_len)
        worksheet.update(
            range_name=f"A2:{end_col}{len(used_values)}",
            values=repaired_rows,
        )

    @staticmethod
    def _looks_like_resume_doc_text(value: str) -> bool:
        if not value:
            return False
        lowered = value.lower()
        return value.startswith("Resume - ") or "/applications/" in lowered or lowered.endswith(".txt")

    @staticmethod
    def _looks_like_fit_score(value: str) -> bool:
        if not value:
            return False
        try:
            parsed = float(value)
        except ValueError:
            return False
        return 0 <= parsed <= 100

    @staticmethod
    def _looks_like_recommendation(value: str) -> bool:
        return value.strip().lower() in {"strong_match", "good_match", "stretch", "skip"}

    @classmethod
    def _normalize_status_value(cls, value: str) -> str:
        mapping = {
            "applied": "Applied",
            "interview": "Interview",
            "rejected": "Rejected",
        }
        return mapping.get(value.strip().lower(), "")

    @classmethod
    def _normalize_application_status(cls, app_status_value: str, tail_status_value: str) -> str:
        from_status_col = cls._normalize_status_value(app_status_value)
        if from_status_col:
            return from_status_col
        from_tail_col = cls._normalize_status_value(tail_status_value)
        if from_tail_col:
            return from_tail_col
        return ""


class PhdRoleWorksheetExporter(BaseWorksheetExporter):
    @property
    def worksheet_name(self) -> str:
        return self.settings.google_sheets_phd_report_worksheet

    @property
    def headers(self) -> list[str]:
        return DEFAULT_PHD_REPORT_HEADERS
