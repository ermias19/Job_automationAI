from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import json
import logging
from pathlib import Path

from job_automation.ai import OpenAIOrHeuristicEngine
from job_automation.config import Settings
from job_automation.models import JobListing, MatchResult, ProfessorLead
from job_automation.phd_email_automation import PhdEmailAutomation
from job_automation.professor_finder import ProfessorFinder
from job_automation.resume import load_candidate_background
from job_automation.sheets import SheetExporter
from job_automation.university_scraper import UniversityScraper

logger = logging.getLogger(__name__)


class PhdAutomationPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def run(self) -> dict:
        started_at = datetime.now().astimezone()
        run_id = started_at.strftime("%Y%m%d-%H%M%S") + "-phd"
        output_dir = self.settings.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Starting PhD pipeline run %s", run_id)

        universities = UniversityScraper(self.settings).scrape_universities()
        logger.info("University scraper returned %s universities", len(universities))
        self._write_json(output_dir / "universities.json", [asdict(item) for item in universities])

        professor_leads = ProfessorFinder(self.settings).find_professors(universities)
        logger.info("Professor finder returned %s leads", len(professor_leads))
        self._write_json(output_dir / "professor_leads.json", [asdict(item) for item in professor_leads])

        opportunities = [self._lead_to_job_listing(item) for item in professor_leads]
        self._write_json(output_dir / "raw_jobs.json", [asdict(job) for job in opportunities])

        background = load_candidate_background(self.settings)
        logger.info("Loaded candidate background")

        engine = OpenAIOrHeuristicEngine(self.settings, background)
        assessments = engine.evaluate_jobs(opportunities)
        logger.info("Completed fit evaluation for %s opportunities", len(assessments))

        matches = [
            MatchResult(job=job, assessment=assessments[job.dedupe_key()])
            for job in opportunities
            if job.dedupe_key() in assessments and assessments[job.dedupe_key()].ai_fit
        ]
        matches.sort(key=lambda item: item.assessment.fit_score, reverse=True)
        logger.info("Kept %s PhD matches after scoring", len(matches))

        tailored_matches = engine.tailor_matches(matches[: self.settings.tailor_top_n])
        logger.info("Generated tailored artifacts for %s PhD matches", len(tailored_matches))

        artifact_index = {
            match.job.dedupe_key(): match.artifacts for match in tailored_matches
        }
        for match in matches:
            match.artifacts = artifact_index.get(match.job.dedupe_key())
            if match.artifacts:
                self._persist_artifacts(output_dir, match)

        searched_at = started_at.isoformat()
        export_info = SheetExporter(self.settings).export_phd_only(
            run_id=run_id,
            searched_at=searched_at,
            matches=matches,
            output_dir=output_dir,
        )
        logger.info("PhD export finished via %s", export_info["remote_status"])

        email_result = PhdEmailAutomation(self.settings).send_applications(
            run_id=run_id,
            matches=matches,
        )
        logger.info(
            "PhD outreach emails: enabled=%s sent=%s skipped=%s",
            email_result.get("enabled"),
            email_result.get("sent"),
            email_result.get("skipped"),
        )

        summary = (
            f"PhD run {run_id}: scraped {len(universities)} universities, "
            f"found {len(professor_leads)} professor leads, kept {len(matches)} matches."
        )
        result = {
            "run_id": run_id,
            "searched_at": searched_at,
            "universities": len(universities),
            "professor_leads": len(professor_leads),
            "evaluated_leads": len(opportunities),
            "matches": len(matches),
            "phd_report_csv_path": export_info.get("phd_report_csv_path", ""),
            "remote_export": export_info["remote_status"],
            "emails_sent": email_result.get("sent", 0),
            "emails_skipped": email_result.get("skipped", 0),
            "summary": summary,
            "output_dir": str(output_dir),
        }
        self._write_json(output_dir / "run_summary.json", result)
        logger.info("PhD pipeline run %s complete", run_id)
        return result

    def _lead_to_job_listing(self, lead: ProfessorLead) -> JobListing:
        title_topic = lead.research_topics[0] if lead.research_topics else "Computer Science"
        title = f"PhD Research Opportunity - {title_topic}"
        summary = (
            f"University: {lead.university_name}. "
            f"Professor: {lead.professor_name}. "
            f"Lab: {lead.lab_name}. "
            f"Research topics: {', '.join(lead.research_topics)}."
        )
        return JobListing(
            job_title=title,
            company_name=lead.university_name,
            job_location=lead.country or "Unknown",
            job_employment_type="PhD",
            job_seniority_level="PhD",
            job_base_pay_range="",
            job_num_applicants="",
            job_posted_time="",
            apply_link=lead.source_url,
            company_url=lead.source_url,
            job_summary=summary,
            job_description_formatted=summary,
            source_site="phdportal",
            search_title="PhD Computer Science",
            search_country=lead.country or "",
            raw={
                "professor_name": lead.professor_name,
                "professor_email": lead.professor_email,
                "lab_name": lead.lab_name,
                "research_topics": lead.research_topics,
                "rank_hint": lead.rank_hint,
                "metadata": lead.metadata,
            },
        )

    @staticmethod
    def _persist_artifacts(output_dir: Path, match: MatchResult) -> None:
        artifact_dir = output_dir / "phd_applications" / match.job.storage_slug()
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
    def _write_json(path: Path, payload) -> None:
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True, default=str),
            encoding="utf-8",
        )
