from __future__ import annotations

import logging
import re

import requests

from job_automation.config import Settings
from job_automation.models import ProfessorLead, UniversityLead

logger = logging.getLogger(__name__)

OPENALEX_INSTITUTIONS_URL = "https://api.openalex.org/institutions"
OPENALEX_AUTHORS_URL = "https://api.openalex.org/authors"


class ProfessorFinder:
    """Finds professor-like leads for each university using OpenAlex."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "JobAutomationAI/1.0 (PhD outreach; contact: ermiasmulu19@gmail.com)"
                )
            }
        )

    def find_professors(self, universities: list[UniversityLead]) -> list[ProfessorLead]:
        leads: list[ProfessorLead] = []

        for university in universities:
            university_leads = self._find_for_university(university)
            if not university_leads:
                university_leads = [self._fallback_contact(university)]
            leads.extend(university_leads[: self.settings.phd_professors_per_university])

        logger.info("Professor finder produced %s leads", len(leads))
        return leads

    def _find_for_university(self, university: UniversityLead) -> list[ProfessorLead]:
        institution = self._search_openalex_institution(university.university_name)
        if not institution:
            return []

        institution_id = institution.get("id", "")
        if not institution_id:
            return []

        authors = self._search_openalex_authors(institution_id=institution_id)
        if not authors:
            return []

        ranked = sorted(
            (
                self._author_to_lead(author=author, university=university)
                for author in authors
            ),
            key=lambda lead: (
                self._topic_relevance(lead.research_topics),
                int(lead.metadata.get("works_count", 0)),
            ),
            reverse=True,
        )
        return ranked

    def _search_openalex_institution(self, university_name: str) -> dict:
        params = {"search": university_name, "per-page": 1}
        try:
            response = self.session.get(OPENALEX_INSTITUTIONS_URL, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", [])
            if not results:
                return {}
            return results[0]
        except Exception:
            logger.exception("Institution lookup failed for %s", university_name)
            return {}

    def _search_openalex_authors(self, institution_id: str) -> list[dict]:
        params = {
            "filter": f"last_known_institutions.id:{institution_id}",
            "sort": "works_count:desc",
            "per-page": 25,
        }
        try:
            response = self.session.get(OPENALEX_AUTHORS_URL, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            return payload.get("results", [])
        except Exception:
            logger.exception("Author lookup failed for institution %s", institution_id)
            return []

    def _author_to_lead(self, author: dict, university: UniversityLead) -> ProfessorLead:
        topics = self._extract_topics(author)
        lab_name = self._extract_lab_name(author) or "Computer Science Department"
        source_url = author.get("id", university.source_url) or university.source_url
        works_count = int(author.get("works_count") or 0)

        return ProfessorLead(
            university_name=university.university_name,
            country=university.country,
            professor_name=author.get("display_name", "").strip() or "Unknown Researcher",
            lab_name=lab_name,
            research_topics=topics,
            source_url=source_url,
            professor_email="",
            rank_hint=university.rank_hint,
            metadata={"works_count": works_count},
        )

    @staticmethod
    def _extract_lab_name(author: dict) -> str:
        institution = author.get("last_known_institution") or {}
        display_name = institution.get("display_name", "")
        return str(display_name).strip()

    @staticmethod
    def _extract_topics(author: dict) -> list[str]:
        concepts = author.get("x_concepts") or []
        topics: list[str] = []
        for concept in concepts:
            name = str(concept.get("display_name", "")).strip()
            score = concept.get("score", 0)
            if not name:
                continue
            if isinstance(score, (int, float)) and score < 0.15:
                continue
            topics.append(name)
            if len(topics) >= 8:
                break
        return topics

    def _topic_relevance(self, topics: list[str]) -> int:
        if not topics:
            return 0
        keywords = [k.lower() for k in self.settings.phd_subject_keywords]
        score = 0
        for topic in topics:
            lowered = topic.lower()
            for keyword in keywords:
                if keyword in lowered or lowered in keyword:
                    score += 1
                    break
        return score

    def _fallback_contact(self, university: UniversityLead) -> ProfessorLead:
        return ProfessorLead(
            university_name=university.university_name,
            country=university.country,
            professor_name="Graduate Admissions Contact",
            lab_name="Computer Science Department",
            research_topics=list(self.settings.phd_subject_keywords[:5]),
            source_url=university.source_url,
            professor_email="",
            rank_hint=university.rank_hint,
            metadata={"fallback": True},
        )


def sanitize_topic_list(topics: list[str]) -> list[str]:
    cleaned: list[str] = []
    for topic in topics:
        value = re.sub(r"\s+", " ", str(topic)).strip()
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned
