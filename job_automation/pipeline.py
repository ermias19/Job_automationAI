from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import json
import logging
from pathlib import Path

from job_automation.ai import OpenAIOrHeuristicEngine
from job_automation.config import Settings
from job_automation.emailer import Emailer
from job_automation.models import JobListing, MatchResult
from job_automation.resume import load_candidate_background
from job_automation.scraper import JobScraper
from job_automation.sheets import SheetExporter

logger = logging.getLogger(__name__)


class AutomationPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def run(self) -> dict:
        started_at = datetime.now().astimezone()
        run_id = started_at.strftime("%Y%m%d-%H%M%S")
        output_dir = self.settings.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Starting pipeline run %s", run_id)

        scraper = JobScraper(self.settings)
        scraped_jobs = scraper.scrape_listings()
        logger.info("Scraper returned %s jobs", len(scraped_jobs))
        self._write_json(output_dir / "raw_jobs.json", [asdict(job) for job in scraped_jobs])
        return self._process_jobs(scraped_jobs, run_id, output_dir, started_at)

    def run_from_file(self, input_path: Path) -> dict:
        started_at = datetime.now().astimezone()
        run_id = started_at.strftime("%Y%m%d-%H%M%S") + "-recommend"
        output_dir = self.settings.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Starting recommendation-only run %s from %s", run_id, input_path)

        payload = json.loads(input_path.read_text(encoding="utf-8"))
        items = payload["jobs"] if isinstance(payload, dict) and "jobs" in payload else payload
        scraped_jobs = [JobListing(**self._normalize_job_item(item)) for item in items]
        self._write_json(output_dir / "raw_jobs.json", [asdict(job) for job in scraped_jobs])
        return self._process_jobs(scraped_jobs, run_id, output_dir, started_at)

    def _process_jobs(self, scraped_jobs, run_id: str, output_dir: Path, started_at) -> dict:
        background = load_candidate_background(self.settings)
        logger.info("Loaded candidate background")

        jobs_for_ai = self._rank_jobs(scraped_jobs)[: self.settings.max_jobs_for_ai]
        logger.info("Selected %s jobs for evaluation", len(jobs_for_ai))
        engine = OpenAIOrHeuristicEngine(self.settings, background)
        assessments = engine.evaluate_jobs(jobs_for_ai)
        logger.info("Completed fit evaluation for %s jobs", len(assessments))

        matches = [
            MatchResult(job=job, assessment=assessments[job.dedupe_key()])
            for job in jobs_for_ai
            if job.dedupe_key() in assessments
            and assessments[job.dedupe_key()].ai_fit
        ]
        matches.sort(key=lambda item: item.assessment.fit_score, reverse=True)
        logger.info("Kept %s matches after scoring", len(matches))

        tailored_matches = engine.tailor_matches(matches[: self.settings.tailor_top_n])
        logger.info("Generated tailored artifacts for %s matches", len(tailored_matches))
        artifact_index = {
            match.job.dedupe_key(): match.artifacts for match in tailored_matches
        }
        for match in matches:
            match.artifacts = artifact_index.get(match.job.dedupe_key())
            if match.artifacts:
                self._persist_artifacts(output_dir, match)

        searched_at = started_at.isoformat()
        exporter = SheetExporter(self.settings)
        export_info = exporter.export(run_id, searched_at, matches, output_dir)
        logger.info("Export finished via %s", export_info["remote_status"])

        summary = (
            f"Run {run_id}: scraped {len(scraped_jobs)} jobs, evaluated {len(jobs_for_ai)}, "
            f"kept {len(matches)} matches."
        )
        email_sent = Emailer(self.settings).send_summary(run_id, matches, summary)
        logger.info("Email sent: %s", email_sent)

        result = {
            "run_id": run_id,
            "searched_at": searched_at,
            "scraped_jobs": len(scraped_jobs),
            "evaluated_jobs": len(jobs_for_ai),
            "matches": len(matches),
            "csv_path": export_info["csv_path"],
            "phd_report_csv_path": export_info.get("phd_report_csv_path", ""),
            "remote_export": export_info["remote_status"],
            "email_sent": email_sent,
            "summary": summary,
            "output_dir": str(output_dir),
        }
        self._write_json(output_dir / "run_summary.json", result)
        logger.info("Pipeline run %s complete", run_id)
        return result

    def _persist_artifacts(self, output_dir: Path, match: MatchResult) -> None:
        artifact_dir = output_dir / "applications" / match.job.storage_slug()
        artifact_dir.mkdir(parents=True, exist_ok=True)

        resume_path = artifact_dir / "resume.txt"
        cover_letter_path = artifact_dir / "cover_letter.txt"
        email_intro_path = artifact_dir / "email_intro.txt"

        resume_path.write_text(match.artifacts.resume_markdown, encoding="utf-8")
        cover_letter_path.write_text(match.artifacts.cover_letter_markdown, encoding="utf-8")
        email_intro_path.write_text(match.artifacts.email_intro, encoding="utf-8")

        match.artifacts.resume_path = resume_path
        match.artifacts.cover_letter_path = cover_letter_path
        match.artifacts.email_intro_path = email_intro_path

    @staticmethod
    def _rank_jobs(jobs):
        priority = {"linkedin": 0, "glassdoor": 1, "indeed": 2}
        return sorted(
            jobs,
            key=lambda job: (
                priority.get(job.source_site.lower(), 10),
                0 if job.job_description_formatted else 1,
                job.company_name.lower(),
                job.job_title.lower(),
            ),
        )

    @staticmethod
    def _normalize_job_item(item: dict) -> dict:
        normalized = dict(item)
        normalized["job_title"] = AutomationPipeline._clean_text(normalized.get("job_title", ""))
        normalized["company_name"] = AutomationPipeline._clean_text(normalized.get("company_name", ""))
        normalized["job_location"] = AutomationPipeline._clean_text(normalized.get("job_location", ""))
        normalized["job_employment_type"] = AutomationPipeline._clean_text(normalized.get("job_employment_type", ""))
        normalized["job_seniority_level"] = AutomationPipeline._clean_text(normalized.get("job_seniority_level", ""))
        normalized["job_base_pay_range"] = AutomationPipeline._clean_text(normalized.get("job_base_pay_range", ""))
        normalized["job_num_applicants"] = AutomationPipeline._clean_text(normalized.get("job_num_applicants", ""))
        normalized["job_posted_time"] = AutomationPipeline._clean_text(normalized.get("job_posted_time", ""))
        normalized["company_url"] = AutomationPipeline._clean_text(normalized.get("company_url", ""))
        normalized["job_summary"] = AutomationPipeline._clean_text(normalized.get("job_summary", ""))
        normalized["job_description_formatted"] = AutomationPipeline._clean_text(normalized.get("job_description_formatted", ""))
        normalized["source_site"] = AutomationPipeline._clean_text(normalized.get("source_site", ""))
        normalized.setdefault("raw", {})
        return normalized

    @staticmethod
    def _clean_text(value) -> str:
        text = str(value).strip()
        if text.lower() in {"none", "nan", "nat"}:
            return ""
        return text

    @staticmethod
    def _write_json(path: Path, payload) -> None:
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True, default=str),
            encoding="utf-8",
        )
