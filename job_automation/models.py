from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re


@dataclass(frozen=True)
class SearchTarget:
    job_title: str
    country_code: str
    country_name: str
    city: str
    remote: bool = False

    @property
    def location(self) -> str:
        if self.remote:
            return self.country_name
        return f"{self.city}, {self.country_name}"


@dataclass
class JobListing:
    job_title: str
    company_name: str
    job_location: str
    job_employment_type: str
    job_seniority_level: str
    job_base_pay_range: str
    job_num_applicants: str
    job_posted_time: str
    apply_link: str
    company_url: str
    job_summary: str
    job_description_formatted: str
    source_site: str
    search_title: str
    search_country: str
    raw: dict = field(default_factory=dict)

    def dedupe_key(self) -> str:
        link = self.apply_link.strip().lower()
        if link:
            return link

        parts = [
            self.job_title.strip().lower(),
            self.company_name.strip().lower(),
            self.job_location.strip().lower(),
            self.source_site.strip().lower(),
        ]
        return "::".join(parts)

    def short_description(self, limit: int = 2200) -> str:
        text = self.job_description_formatted or self.job_summary or ""
        return text[:limit]

    def storage_slug(self) -> str:
        raw = f"{self.company_name}-{self.job_title}".strip().lower()
        slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
        return slug or "job"

    def ai_payload(self) -> dict:
        return {
            "job_key": self.dedupe_key(),
            "job_title": self.job_title,
            "company_name": self.company_name,
            "job_location": self.job_location,
            "job_employment_type": self.job_employment_type,
            "job_posted_time": self.job_posted_time,
            "apply_link": self.apply_link,
            "source_site": self.source_site,
            "search_title": self.search_title,
            "search_country": self.search_country,
            "job_summary": self.job_summary[:800],
            "job_description": self.short_description(),
        }


@dataclass
class FitAssessment:
    ai_fit: bool
    fit_score: int
    decision: str
    recommendation: str
    reasoning: str
    missing_skills: list[str] = field(default_factory=list)
    resume_focus: list[str] = field(default_factory=list)
    candidate_highlights: list[str] = field(default_factory=list)


@dataclass
class TailoredArtifacts:
    resume_markdown: str
    cover_letter_markdown: str
    email_intro: str
    resume_summary: str = ""
    resume_doc_title: str = ""
    resume_text_path: Path | None = None
    resume_pdf_path: Path | None = None
    resume_path: Path | None = None
    cover_letter_path: Path | None = None
    email_intro_path: Path | None = None
    resume_drive_url: str = ""
    email_intro_drive_url: str = ""


@dataclass
class MatchResult:
    job: JobListing
    assessment: FitAssessment
    artifacts: TailoredArtifacts | None = None


@dataclass
class UniversityLead:
    university_name: str
    country: str
    source_url: str
    rank_hint: str = ""


@dataclass
class ProfessorLead:
    university_name: str
    country: str
    professor_name: str
    lab_name: str
    research_topics: list[str]
    source_url: str
    professor_email: str = ""
    rank_hint: str = ""
    metadata: dict = field(default_factory=dict)
