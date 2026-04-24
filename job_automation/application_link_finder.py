from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import re
import threading
import time
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter

from job_automation.config import Settings
from job_automation.models import ProfessorLead

logger = logging.getLogger(__name__)

APPLICATION_LINK_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "url": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reasoning": {"type": "string"},
    },
    "required": ["url", "confidence", "reasoning"],
}


@dataclass
class LinkCandidate:
    url: str
    anchor_text: str
    score: int
    source_page: str


class ApplicationLinkFinder:
    """Resolves likely real PhD application URLs from university websites."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        # Prevent urllib3 pool-overflow warnings under concurrent link-resolution workers.
        pool_size = max(32, self.settings.phd_link_finder_max_workers * 4)
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            pool_block=False,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._resolved_cache: dict[str, str] = {}
        self._persistent_cache_lock = threading.Lock()
        self._persistent_cache_path = (
            self.settings.output_dir / ".cache" / "application_link_cache.json"
        )
        self._persistent_cache: dict[str, str] = self._load_persistent_cache()
        self._persistent_cache_dirty = False
        self._openai_client = None
        self._openai_available: bool | None = None
        self._openai_client_lock = threading.Lock()

    def enrich_professor_leads(self, leads: list[ProfessorLead]) -> list[ProfessorLead]:
        if not self.settings.phd_resolve_application_links:
            return leads

        enriched_count = 0
        max_workers = max(1, self.settings.phd_link_finder_max_workers)

        if len(leads) <= 1 or max_workers == 1:
            for lead in leads:
                url = self._resolve_for_lead(lead)
                if not url:
                    continue
                lead.metadata["opportunity_url"] = url
                if url != lead.source_url:
                    enriched_count += 1
        else:
            worker_count = min(max_workers, len(leads))
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                futures = {pool.submit(self._resolve_for_lead, lead): lead for lead in leads}
                for future in as_completed(futures):
                    lead = futures[future]
                    try:
                        url = future.result()
                    except Exception:
                        logger.exception(
                            "Application link resolution crashed for %s @ %s",
                            lead.professor_name,
                            lead.university_name,
                        )
                        continue
                    if not url:
                        continue
                    lead.metadata["opportunity_url"] = url
                    if url != lead.source_url:
                        enriched_count += 1

        self._save_persistent_cache()
        logger.info("Application link finder enriched %s/%s leads", enriched_count, len(leads))
        return leads

    def _resolve_for_lead(self, lead: ProfessorLead) -> str:
        base_url = self._base_url_for_lead(lead)
        if not base_url:
            return lead.source_url

        persistent_key = self._persistent_cache_key(lead=lead, base_url=base_url)
        if persistent_key:
            with self._persistent_cache_lock:
                cached_url = self._persistent_cache.get(persistent_key, "")
            if cached_url:
                return cached_url

        cache_key = f"{lead.university_name.strip().lower()}::{base_url.lower()}"
        if cache_key in self._resolved_cache:
            return self._resolved_cache[cache_key]

        candidates = self._collect_candidates(base_url, lead)
        selected = self._pick_best_candidate(candidates, lead, base_url)
        result = selected or base_url
        self._resolved_cache[cache_key] = result
        if persistent_key:
            with self._persistent_cache_lock:
                if self._persistent_cache.get(persistent_key) != result:
                    self._persistent_cache[persistent_key] = result
                    self._persistent_cache_dirty = True
        return result

    @staticmethod
    def _base_url_for_lead(lead: ProfessorLead) -> str:
        metadata = lead.metadata or {}
        for key in ("university_url",):
            value = str(metadata.get(key, "")).strip()
            normalized = ApplicationLinkFinder._normalize_base_url(value)
            if normalized:
                return normalized

        domain = str(metadata.get("institution_domain", "")).strip().lower()
        if domain:
            return f"https://{domain}"

        return ApplicationLinkFinder._normalize_base_url(lead.source_url)

    @staticmethod
    def _normalize_base_url(value: str) -> str:
        if not value:
            return ""
        parsed = urlparse(value if "://" in value else f"https://{value}")
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "https"
        return f"{scheme}://{parsed.netloc}"

    def _collect_candidates(self, base_url: str, lead: ProfessorLead) -> list[LinkCandidate]:
        seed_urls = self._build_seed_urls(base_url)
        all_candidates: dict[str, LinkCandidate] = {}
        for seed_url in seed_urls:
            html = self._download(seed_url)
            if not html:
                continue
            page_candidates = self._extract_candidates_from_page(
                html=html,
                page_url=seed_url,
                base_url=base_url,
                lead=lead,
            )
            for candidate in page_candidates:
                existing = all_candidates.get(candidate.url)
                if existing is None or candidate.score > existing.score:
                    all_candidates[candidate.url] = candidate

            # Early cutoff: once we have a strong, likely-apply URL, skip deeper crawling.
            best = max(all_candidates.values(), key=lambda item: item.score, default=None)
            if best and self._is_high_confidence_candidate(best):
                break

        ranked = sorted(all_candidates.values(), key=lambda item: item.score, reverse=True)
        return ranked[: self.settings.phd_application_link_max_candidates]

    def _build_seed_urls(self, base_url: str) -> list[str]:
        paths = [
            "",
            "/admissions",
            "/admissions/graduate",
            "/graduate-admissions",
            "/graduate",
            "/study/graduate",
            "/study/postgraduate",
            "/phd",
            "/doctoral",
            "/academics/graduate-studies",
            "/computer-science",
            "/computer-science/graduate",
            "/computer-science/phd",
            "/cs/graduate",
            "/cs/phd",
            "/engineering/graduate",
        ]
        seen: set[str] = set()
        seed_urls: list[str] = []
        for path in paths:
            url = urljoin(base_url + "/", path.lstrip("/")) if path else base_url
            if url in seen:
                continue
            seen.add(url)
            seed_urls.append(url)
            if len(seed_urls) >= max(1, self.settings.phd_application_link_max_seed_pages):
                break
        return seed_urls

    def _download(self, url: str) -> str:
        try:
            response = self.session.get(
                url,
                timeout=(5, max(5.0, self.settings.phd_link_request_timeout_seconds)),
            )
            if response.status_code in {401, 403, 429}:
                return ""
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "<html" not in response.text.lower():
                return ""
            return response.text
        except requests.RequestException:
            return ""

    def _is_high_confidence_candidate(self, candidate: LinkCandidate) -> bool:
        if candidate.score < max(1, self.settings.phd_link_early_cutoff_score):
            return False
        text = f"{candidate.url} {candidate.anchor_text}".lower()
        strong_tokens = ("apply", "application", "admission", "graduate", "phd", "doctoral")
        return any(token in text for token in strong_tokens)

    def _extract_candidates_from_page(
        self,
        html: str,
        page_url: str,
        base_url: str,
        lead: ProfessorLead,
    ) -> list[LinkCandidate]:
        try:
            from bs4 import BeautifulSoup  # type: ignore

            soup = BeautifulSoup(html, "html.parser")
            links: list[tuple[str, str]] = []
            for anchor in soup.find_all("a"):
                href = str(anchor.get("href", "")).strip()
                text = anchor.get_text(" ", strip=True)
                if href:
                    links.append((href, text))
        except Exception:
            pattern = re.compile(
                r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<text>.*?)</a>',
                re.IGNORECASE | re.DOTALL,
            )
            links = []
            for match in pattern.finditer(html):
                href = match.group("href").strip()
                text = re.sub(r"<[^>]+>", " ", match.group("text")).strip()
                if href:
                    links.append((href, text))

        candidates: list[LinkCandidate] = []
        for href, text in links:
            full_url = urljoin(page_url, href)
            if not full_url.startswith(("http://", "https://")):
                continue
            if self._is_excluded_url(full_url):
                continue

            score = self._score_candidate(
                url=full_url,
                text=text,
                base_url=base_url,
                lead=lead,
            )
            if score <= 0:
                continue
            candidates.append(
                LinkCandidate(
                    url=full_url,
                    anchor_text=text[:180],
                    score=score,
                    source_page=page_url,
                )
            )
        return candidates

    @staticmethod
    def _is_excluded_url(url: str) -> bool:
        lowered = url.lower()
        if lowered.startswith(("mailto:", "javascript:", "tel:")):
            return True
        excluded_tokens = [
            "openalex.org",
            "linkedin.com",
            "twitter.com",
            "x.com",
            "facebook.com",
            "instagram.com",
            "youtube.com",
            "github.com",
            "/privacy",
            "/terms",
            "/cookies",
            "/news",
            "/events",
            "/people",
            "/faculty",
            "/staff",
            "#",
        ]
        return any(token in lowered for token in excluded_tokens)

    def _score_candidate(self, url: str, text: str, base_url: str, lead: ProfessorLead) -> int:
        haystack = f"{url} {text}".lower()
        score = 0

        positive_weights = {
            "apply": 10,
            "application": 10,
            "how to apply": 12,
            "admissions": 9,
            "graduate admissions": 12,
            "prospective": 5,
            "phd": 8,
            "doctoral": 8,
            "doctorate": 8,
            "postgraduate": 6,
            "funding": 3,
            "deadline": 3,
        }
        for token, weight in positive_weights.items():
            if token in haystack:
                score += weight

        for keyword in self.settings.phd_subject_keywords:
            token = keyword.strip().lower()
            if token and token in haystack:
                score += 2

        negative_tokens = [
            "open day",
            "seminar",
            "lecture",
            "publication",
            "research profile",
            "directory",
            "faculty",
            "staff",
            "alumni",
            "news",
            "event",
            "blog",
        ]
        for token in negative_tokens:
            if token in haystack:
                score -= 5

        base_host = urlparse(base_url).netloc.lower()
        target_host = urlparse(url).netloc.lower()
        if target_host and base_host and target_host != base_host:
            if any(token in target_host for token in ("apply", "admission", "grad", "slate")):
                score += 2
            else:
                score -= 2

        path = urlparse(url).path.lower()
        if path in {"", "/"}:
            score -= 4
        if path.endswith((".jpg", ".png", ".svg", ".css", ".js")):
            score -= 10

        return score

    def _pick_best_candidate(
        self,
        candidates: list[LinkCandidate],
        lead: ProfessorLead,
        base_url: str,
    ) -> str:
        if not candidates:
            return ""

        heuristic_choice = ""
        for candidate in candidates:
            if candidate.score >= 10:
                heuristic_choice = candidate.url
                break
        if not heuristic_choice:
            heuristic_choice = candidates[0].url

        if self.settings.openai_api_key and self._has_openai_sdk() and len(candidates) >= 3:
            ai_choice = self._pick_with_openai(candidates, lead)
            if ai_choice:
                return ai_choice
        return heuristic_choice or base_url

    def _pick_with_openai(self, candidates: list[LinkCandidate], lead: ProfessorLead) -> str:
        payload = {
            "university_name": lead.university_name,
            "professor_name": lead.professor_name,
            "lab_name": lead.lab_name,
            "research_topics": lead.research_topics,
            "candidate_links": [
                {
                    "url": item.url,
                    "anchor_text": item.anchor_text,
                    "score": item.score,
                    "source_page": item.source_page,
                }
                for item in candidates[:20]
            ],
        }
        system_prompt = (
            "Pick the single best official university link where a student can apply for a PhD "
            "or view official PhD application instructions. "
            "Prefer direct application portal or graduate admissions apply pages. "
            "Do not select researcher profiles, news pages, or general homepages when better options exist."
        )
        try:
            response = self._call_json(
                schema_name="phd_application_link",
                schema=APPLICATION_LINK_SCHEMA,
                system_prompt=system_prompt,
                payload=payload,
            )
        except Exception as exc:
            logger.warning(
                "OpenAI application-link selection failed for %s: %s",
                lead.university_name,
                exc,
            )
            return ""

        url = str(response.get("url", "")).strip()
        confidence = float(response.get("confidence", 0) or 0)
        if confidence < 0.55:
            return ""
        allowed = {candidate.url for candidate in candidates}
        if url not in allowed:
            return ""
        return url

    def _has_openai_sdk(self) -> bool:
        if self._openai_available is not None:
            return self._openai_available
        try:
            import openai  # noqa: F401
        except Exception:
            self._openai_available = False
            return False
        self._openai_available = True
        return True

    def _client_or_raise(self):
        if self._openai_client is not None:
            return self._openai_client
        with self._openai_client_lock:
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
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
                    ],
                )
                content = completion.choices[0].message.content or "{}"
                return json.loads(content)
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1.2 + attempt)
        return {}

    def _persistent_cache_key(self, lead: ProfessorLead, base_url: str) -> str:
        domain = urlparse(base_url).netloc.strip().lower()
        professor = re.sub(r"[^a-z0-9]+", " ", lead.professor_name.lower()).strip()
        professor = re.sub(r"\s+", "-", professor)
        if not domain or not professor:
            return ""
        return f"{domain}::{professor}"

    def _load_persistent_cache(self) -> dict[str, str]:
        path = self._persistent_cache_path
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {}
            return {
                str(key): str(value)
                for key, value in payload.items()
                if isinstance(key, str) and isinstance(value, str) and value.strip()
            }
        except Exception:
            logger.warning("Could not read application-link cache at %s", path)
            return {}

    def _save_persistent_cache(self) -> None:
        if not self._persistent_cache_dirty:
            return
        path: Path = self._persistent_cache_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with self._persistent_cache_lock:
                payload = dict(self._persistent_cache)
                self._persistent_cache_dirty = False
            path.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.warning("Could not persist application-link cache at %s", path)
