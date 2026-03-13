from __future__ import annotations

import json
import logging
import re
import time
from urllib.parse import urlparse

import requests

from job_automation.config import Settings
from job_automation.models import ProfessorLead, UniversityLead

logger = logging.getLogger(__name__)

OPENALEX_INSTITUTIONS_URL = "https://api.openalex.org/institutions"
OPENALEX_AUTHORS_URL = "https://api.openalex.org/authors"
EMAIL_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "email": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reasoning": {"type": "string"},
    },
    "required": ["email", "confidence", "reasoning"],
}


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
        self._openai_client = None
        self._openai_available: bool | None = None
        self._email_cache: dict[str, str] = {}

    def find_professors(self, universities: list[UniversityLead]) -> list[ProfessorLead]:
        leads: list[ProfessorLead] = []

        for university in universities:
            university_leads = self._find_for_university(university)
            if not university_leads:
                university_leads = [self._fallback_contact(university)]
            leads.extend(university_leads[: self.settings.phd_professors_per_university])

        leads_with_email = sum(1 for lead in leads if lead.professor_email)
        logger.info(
            "Professor finder produced %s leads (%s with email)",
            len(leads),
            leads_with_email,
        )
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

        institution_domain = self._extract_domain(
            str(institution.get("homepage_url", "")).strip()
            or university.source_url
        )
        ranked = sorted(
            (
                self._author_to_lead(
                    author=author,
                    university=university,
                    institution_domain=institution_domain,
                )
                for author in authors
            ),
            key=lambda lead: (
                self._topic_relevance(lead.research_topics),
                int(lead.metadata.get("works_count", 0)),
            ),
            reverse=True,
        )
        selected = ranked[: self.settings.phd_professors_per_university]
        for lead in selected:
            lead.professor_email = self._resolve_professor_email(lead, institution_domain)
        return selected

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

    def _author_to_lead(
        self,
        author: dict,
        university: UniversityLead,
        institution_domain: str,
    ) -> ProfessorLead:
        topics = self._extract_topics(author)
        lab_name = self._extract_lab_name(author) or "Computer Science Department"
        openalex_author_url = str(author.get("id", "")).strip()
        source_url = self._build_university_contact_url(
            university_url=university.source_url,
            professor_name=str(author.get("display_name", "")).strip() or "contact",
        )
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
            metadata={
                "works_count": works_count,
                "openalex_author_url": openalex_author_url,
                "university_url": university.source_url,
                "institution_domain": institution_domain,
            },
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

    @staticmethod
    def _build_university_contact_url(university_url: str, professor_name: str) -> str:
        base = (university_url or "").strip()
        if not base:
            return ""
        anchor = re.sub(r"[^a-z0-9]+", "-", professor_name.lower()).strip("-")
        if not anchor:
            return base
        return f"{base}#prof-{anchor}"

    def _resolve_professor_email(self, lead: ProfessorLead, institution_domain: str) -> str:
        cache_key = f"{lead.university_name}|{lead.professor_name}|{institution_domain}".lower()
        if cache_key in self._email_cache:
            return self._email_cache[cache_key]

        if not institution_domain or not lead.professor_name.strip():
            self._email_cache[cache_key] = ""
            return ""

        heuristic_candidates = self._build_candidate_emails(
            full_name=lead.professor_name,
            domain=institution_domain,
        )
        if not heuristic_candidates:
            self._email_cache[cache_key] = ""
            return ""

        resolved = ""
        if self.settings.openai_api_key:
            resolved = self._infer_email_with_openai(
                professor_name=lead.professor_name,
                university_name=lead.university_name,
                domain=institution_domain,
                candidates=heuristic_candidates,
            )
        if not resolved:
            resolved = self._fallback_email_guess(heuristic_candidates)
        self._email_cache[cache_key] = resolved
        return resolved

    def _infer_email_with_openai(
        self,
        professor_name: str,
        university_name: str,
        domain: str,
        candidates: list[str],
    ) -> str:
        if not self.settings.openai_api_key:
            return ""
        if not self._has_openai_sdk():
            return ""

        system_prompt = (
            "You infer likely university email addresses for researchers. "
            "Return an empty string when uncertain. "
            "Rules: use only the provided domain, only choose from provided candidates, "
            "never invent a new domain, never output personal email providers, and be conservative."
        )
        payload = {
            "professor_name": professor_name,
            "university_name": university_name,
            "domain": domain,
            "candidate_emails": candidates,
        }

        try:
            response = self._call_json(
                schema_name="professor_email_guess",
                schema=EMAIL_SCHEMA,
                system_prompt=system_prompt,
                payload=payload,
            )
        except Exception as exc:
            logger.warning(
                "OpenAI email inference failed for %s at %s: %s",
                professor_name,
                university_name,
                exc,
            )
            return ""

        email = str(response.get("email", "")).strip().lower()
        confidence = float(response.get("confidence", 0) or 0)
        if confidence < 0.6:
            return ""
        if email not in candidates:
            return ""
        if not self._looks_like_email(email):
            return ""
        if not email.endswith(f"@{domain}"):
            return ""
        return email

    def _has_openai_sdk(self) -> bool:
        if self._openai_available is not None:
            return self._openai_available

        try:
            import openai  # noqa: F401
        except Exception:
            logger.warning(
                "OpenAI SDK is not installed in this environment. "
                "Install it with `pip install openai` to enable AI email inference."
            )
            self._openai_available = False
            return False

        self._openai_available = True
        return True

    @staticmethod
    def _build_candidate_emails(full_name: str, domain: str) -> list[str]:
        ignored = {"prof", "professor", "dr", "mr", "mrs", "ms"}
        name_parts = [
            token.lower()
            for token in re.findall(r"[A-Za-z]+", full_name)
            if token and token.lower() not in ignored
        ]
        if len(name_parts) < 2:
            return []

        first = name_parts[0]
        last = name_parts[-1]
        first_initial = first[0]
        middle_initial = name_parts[1][0] if len(name_parts) > 2 else ""
        local_parts = [
            f"{first}.{last}",
            f"{first_initial}.{last}",
            f"{first}{last}",
            f"{first}_{last}",
            f"{first}-{last}",
            f"{last}.{first}",
            f"{first}.{last[0]}",
            f"{first_initial}{last}",
        ]
        if middle_initial:
            local_parts.extend(
                [
                    f"{first}.{middle_initial}.{last}",
                    f"{first_initial}{middle_initial}{last}",
                ]
            )

        deduped: list[str] = []
        seen: set[str] = set()
        for local in local_parts:
            normalized = re.sub(r"[^a-z0-9._-]", "", local)
            if not normalized:
                continue
            email = f"{normalized}@{domain}"
            if email not in seen:
                seen.add(email)
                deduped.append(email)
        return deduped

    @staticmethod
    def _extract_domain(url_or_host: str) -> str:
        value = (url_or_host or "").strip()
        if not value:
            return ""

        parsed = urlparse(value if "://" in value else f"https://{value}")
        host = (parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host

    @staticmethod
    def _looks_like_email(value: str) -> bool:
        return bool(re.fullmatch(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", value))

    @staticmethod
    def _fallback_email_guess(candidates: list[str]) -> str:
        if not candidates:
            return ""
        # Conservative deterministic preference for common academic patterns.
        preferred_order = [
            re.compile(r"^[a-z]+\.[a-z]+@"),      # first.last
            re.compile(r"^[a-z]\.[a-z]+@"),       # f.last
            re.compile(r"^[a-z][a-z]+@[a-z0-9.-]+$"),  # flast / firstlast
            re.compile(r"^[a-z]+_[a-z]+@"),       # first_last
            re.compile(r"^[a-z]+-[a-z]+@"),       # first-last
            re.compile(r"^[a-z]+\.[a-z]\@"),      # first.l
        ]

        for pattern in preferred_order:
            for candidate in candidates:
                if pattern.search(candidate):
                    return candidate
        return candidates[0]

    def _client_or_raise(self):
        if self._openai_client is not None:
            return self._openai_client

        from openai import OpenAI

        self._openai_client = OpenAI(api_key=self.settings.openai_api_key)
        return self._openai_client

    def _call_json(self, schema_name: str, schema: dict, system_prompt: str, payload: dict) -> dict:
        client = self._client_or_raise()

        for attempt in range(3):
            try:
                if hasattr(client, "responses"):
                    response = client.responses.create(
                        model=self.settings.openai_model,
                        input=[
                            {
                                "role": "system",
                                "content": [{"type": "input_text", "text": system_prompt}],
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "input_text",
                                        "text": json.dumps(payload, ensure_ascii=True),
                                    }
                                ],
                            },
                        ],
                        text={
                            "format": {
                                "type": "json_schema",
                                "name": schema_name,
                                "schema": schema,
                                "strict": True,
                            }
                        },
                    )
                    return json.loads(response.output_text)

                completion = client.chat.completions.create(
                    model=self.settings.openai_model,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {
                            "role": "user",
                            "content": json.dumps(payload, ensure_ascii=True),
                        },
                    ],
                )
                return json.loads(completion.choices[0].message.content or "{}")
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))

        return {}


def sanitize_topic_list(topics: list[str]) -> list[str]:
    cleaned: list[str] = []
    for topic in topics:
        value = re.sub(r"\s+", " ", str(topic)).strip()
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned
