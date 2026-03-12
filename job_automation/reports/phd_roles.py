from __future__ import annotations

from job_automation.models import MatchResult


PHD_ROLE_HEADERS = [
    "University",
    "Professor",
    "Lab / Group",
    "PhD / Research Role",
    "Research Area",
    "Match Score",
    "Relevant Skills",
    "Missing Skills",
    "Generated Resume",
    "Email Draft",
    "Resume Path",
    "Cover Letter Path",
    "Email Intro Path",
    "Apply Link",
    "Source Site",
    "Search Country",
    "Run ID",
    "Searched At",
    "Notes",
]


def build_phd_role_rows(
    run_id: str,
    searched_at: str,
    matches: list[MatchResult],
) -> list[dict]:
    return [_build_phd_role_row(run_id, searched_at, match) for match in matches]


def _build_phd_role_row(run_id: str, searched_at: str, match: MatchResult) -> dict:
    summary = match.job.job_summary or match.job.job_description_formatted
    raw = match.job.raw or {}
    resume_doc = (
        match.artifacts.resume_doc_title
        if match.artifacts and match.artifacts.resume_doc_title
        else f"Resume - {match.job.job_title} @ {match.job.company_name}"
    )
    relevant_skills = (
        match.assessment.resume_focus
        if match.assessment.resume_focus
        else match.assessment.candidate_highlights
    )

    email_draft = (
        str(match.artifacts.email_intro_path)
        if match.artifacts and match.artifacts.email_intro_path
        else ""
    )

    topics = raw.get("research_topics")
    if isinstance(topics, list):
        topic_list = [str(item).strip() for item in topics if str(item).strip()]
    else:
        topic_list = []

    return {
        "University": match.job.company_name,
        "Professor": str(raw.get("professor_name", "")).strip(),
        "Lab / Group": str(raw.get("lab_name", "")).strip() or match.job.company_name,
        "PhD / Research Role": match.job.job_title,
        "Research Area": ", ".join(topic_list)
        if topic_list
        else infer_research_area(job_title=match.job.job_title, summary=summary),
        "Match Score": match.assessment.fit_score,
        "Relevant Skills": " | ".join(relevant_skills),
        "Missing Skills": " | ".join(match.assessment.missing_skills),
        "Generated Resume": resume_doc,
        "Email Draft": email_draft,
        "Resume Path": str(match.artifacts.resume_path) if match.artifacts and match.artifacts.resume_path else "",
        "Cover Letter Path": str(match.artifacts.cover_letter_path) if match.artifacts and match.artifacts.cover_letter_path else "",
        "Email Intro Path": email_draft,
        "Apply Link": str(raw.get("opportunity_url", "")).strip() or match.job.apply_link,
        "Source Site": match.job.source_site,
        "Search Country": match.job.search_country,
        "Run ID": run_id,
        "Searched At": searched_at,
        "Notes": match.assessment.reasoning,
    }


def infer_research_area(job_title: str, summary: str) -> str:
    text = f"{job_title} {summary}".lower()
    topic_map = {
        "High Performance Computing": ["hpc", "high performance", "gpu", "parallel"],
        "Distributed Systems": ["distributed", "microservices", "cluster", "scalable"],
        "Cloud Infrastructure": ["cloud", "kubernetes", "docker", "devops"],
        "Networking / Wireless": ["network", "wireless", "5g", "teletraffic"],
        "AI / Data": ["ai", "machine learning", "nlp", "data"],
        "Software Engineering": ["software", "backend", "frontend", "full stack"],
    }
    topics = [
        topic
        for topic, keywords in topic_map.items()
        if any(keyword in text for keyword in keywords)
    ]
    if not topics:
        return "General Research Software Engineering"
    return ", ".join(topics[:3])
