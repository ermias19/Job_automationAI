from __future__ import annotations

from job_automation.models import MatchResult
from job_automation.reports.links import local_path_hyperlink


JOB_AUTOMATION_HEADERS = [
    "Job Title",
    "Company",
    "Application Status",
    "Location",
    "Employment Type",
    "Seniority",
    "Salary Range",
    "Applicants",
    "Posted",
    "Apply Link",
    "Company URL",
    "Job Summary",
    "AI Fit",
    "Resume Doc",
    "Fit Score",
    "Recommendation",
    "Decision",
    "Reasoning",
    "Missing Skills",
    "Candidate Highlights",
    "Resume Focus",
    "Resume Summary",
    "Resume Path",
    "Cover Letter Path",
    "Email Intro Path",
    "Source Site",
    "Search Title",
    "Search Country",
    "Run ID",
    "Searched At",
]


def build_job_automation_rows(
    run_id: str,
    searched_at: str,
    matches: list[MatchResult],
) -> list[dict]:
    return [_build_job_automation_row(run_id, searched_at, match) for match in matches]


def _build_job_automation_row(run_id: str, searched_at: str, match: MatchResult) -> dict:
    summary = match.job.job_summary or match.job.job_description_formatted
    resume_doc = (
        match.artifacts.resume_doc_title
        if match.artifacts and match.artifacts.resume_doc_title
        else f"Resume - {match.job.job_title} @ {match.job.company_name}"
    )
    resume_path = (
        str(match.artifacts.resume_path) if match.artifacts and match.artifacts.resume_path else ""
    )
    cover_letter_path = (
        str(match.artifacts.cover_letter_path)
        if match.artifacts and match.artifacts.cover_letter_path
        else ""
    )
    email_intro_path = (
        str(match.artifacts.email_intro_path)
        if match.artifacts and match.artifacts.email_intro_path
        else ""
    )
    return {
        "Job Title": match.job.job_title,
        "Company": match.job.company_name,
        "Application Status": "",
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
        "Resume Path": local_path_hyperlink(resume_path, "Open Resume"),
        "Cover Letter Path": local_path_hyperlink(cover_letter_path, "Open Cover Letter"),
        "Email Intro Path": local_path_hyperlink(email_intro_path, "Open Email Intro"),
        "Source Site": match.job.source_site,
        "Search Title": match.job.search_title,
        "Search Country": match.job.search_country,
        "Run ID": run_id,
        "Searched At": searched_at,
    }
