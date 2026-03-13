from __future__ import annotations

import csv
from html import unescape
import logging
import re
from urllib.parse import urljoin, urlparse

import requests

from job_automation.config import Settings
from job_automation.models import UniversityLead

logger = logging.getLogger(__name__)


FALLBACK_CS_UNIVERSITIES: list[tuple[str, str, str]] = [
    ("Massachusetts Institute of Technology", "US", "https://www.mit.edu"),
    ("Stanford University", "US", "https://www.stanford.edu"),
    ("Carnegie Mellon University", "US", "https://www.cmu.edu"),
    ("University of Oxford", "GB", "https://www.ox.ac.uk"),
    ("University of Cambridge", "GB", "https://www.cam.ac.uk"),
    ("ETH Zurich", "CH", "https://ethz.ch"),
    ("EPFL", "CH", "https://www.epfl.ch"),
    ("Technical University of Munich", "DE", "https://www.tum.de"),
    ("University College London", "GB", "https://www.ucl.ac.uk"),
    ("Imperial College London", "GB", "https://www.imperial.ac.uk"),
    ("National University of Singapore", "SG", "https://www.nus.edu.sg"),
    ("University of Toronto", "CA", "https://www.utoronto.ca"),
    ("University of California, Berkeley", "US", "https://www.berkeley.edu"),
    ("University of Washington", "US", "https://www.washington.edu"),
    ("University of Edinburgh", "GB", "https://www.ed.ac.uk"),
]


class UniversityScraper:
    """Collects university leads from configured sources for the PhD workflow."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def scrape_universities(self) -> list[UniversityLead]:
        ordered_sources = self._normalized_source_order(
            self.settings.phd_university_source_order
        )
        if not ordered_sources:
            ordered_sources = ["seed_file", "phdportal", "fallback"]

        combined: list[UniversityLead] = []
        seen_names: set[str] = set()

        for source in ordered_sources:
            if len(combined) >= self.settings.phd_max_universities:
                break

            source_leads = self._collect_source(source)
            if not source_leads:
                continue

            added = 0
            for lead in source_leads:
                key = lead.university_name.strip().lower()
                if not key or key in seen_names:
                    continue
                seen_names.add(key)
                combined.append(lead)
                added += 1
                if len(combined) >= self.settings.phd_max_universities:
                    break

            logger.info(
                "University source %s produced %s leads (%s added after dedupe)",
                source,
                len(source_leads),
                added,
            )

        if combined:
            return combined[: self.settings.phd_max_universities]

        logger.warning(
            "All configured university sources failed. Using built-in CS fallback universities."
        )
        return self._fallback_university_leads(
            source_url=self.settings.phd_portal_universities_url
        )

    @staticmethod
    def _normalized_source_order(raw_sources: list[str]) -> list[str]:
        if not raw_sources:
            return []

        normalized: list[str] = []
        for source in raw_sources:
            token = source.strip().lower().replace("-", "_")
            if token in {"seed", "builtin", "built_in"}:
                token = "fallback"
            if token not in {"seed_file", "phdportal", "fallback"}:
                logger.warning("Ignoring unknown PHD university source: %s", source)
                continue
            if token not in normalized:
                normalized.append(token)
        return normalized

    def _collect_source(self, source: str) -> list[UniversityLead]:
        if source == "seed_file":
            return self._load_seed_file()
        if source == "phdportal":
            return self._load_from_phdportal()
        if source == "fallback":
            return self._fallback_university_leads(
                source_url=self.settings.phd_portal_universities_url
            )
        return []

    def _load_from_phdportal(self) -> list[UniversityLead]:
        url = self.settings.phd_portal_universities_url
        logger.info("Scraping PhD universities from %s", url)
        html = self._download(url)
        if not html:
            logger.warning("PhD ranking page could not be downloaded from %s", url)
            return []

        leads = self._extract_university_leads(html=html, source_url=url)
        if not leads:
            logger.warning(
                "Could not parse university leads from %s. The site markup likely changed.",
                url,
            )
            return []
        return leads

    def _load_seed_file(self) -> list[UniversityLead]:
        path = self.settings.phd_university_seed_file
        if path is None:
            return []
        if not path.exists():
            logger.warning("University seed file does not exist: %s", path)
            return []

        leads: list[UniversityLead] = []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for index, row in enumerate(reader, start=1):
                    name = (
                        row.get("university_name")
                        or row.get("name")
                        or row.get("university")
                        or ""
                    ).strip()
                    if not name:
                        continue
                    country = (
                        row.get("country")
                        or row.get("country_code")
                        or ""
                    ).strip()
                    source_url = (
                        row.get("source_url")
                        or row.get("url")
                        or row.get("website")
                        or ""
                    ).strip()
                    rank_hint = (row.get("rank_hint") or "").strip() or f"seedfile#{index}"
                    leads.append(
                        UniversityLead(
                            university_name=name,
                            country=country,
                            source_url=source_url or self.settings.phd_portal_universities_url,
                            rank_hint=rank_hint,
                        )
                    )
        except Exception:
            logger.exception("Failed reading university seed file %s", path)
            return []
        return leads

    @staticmethod
    def _download(url: str) -> str:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.phdportal.com/",
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
            if response.status_code in {401, 403, 429}:
                logger.warning(
                    "PhD portal blocked automated access (HTTP %s) for %s",
                    response.status_code,
                    url,
                )
                return ""
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            logger.warning("Failed to download %s (%s)", url, exc)
            return ""

    def _extract_university_leads(self, html: str, source_url: str) -> list[UniversityLead]:
        try:
            from bs4 import BeautifulSoup  # type: ignore

            return self._extract_with_bs4(html=html, source_url=source_url, soup_cls=BeautifulSoup)
        except Exception:
            # Fallback parser when BeautifulSoup is unavailable.
            return self._extract_with_regex(html=html, source_url=source_url)

    def _extract_with_bs4(self, html: str, source_url: str, soup_cls) -> list[UniversityLead]:
        soup = soup_cls(html, "html.parser")
        seen: set[str] = set()
        leads: list[UniversityLead] = []
        rank_counter = 1

        for anchor in soup.select('a[href*="/universities/"]'):
            href = anchor.get("href", "").strip()
            name = anchor.get_text(" ", strip=True)
            if not href or not name:
                continue
            if len(name) < 3 or len(name) > 120:
                continue

            full_url = urljoin(source_url, href)
            key = f"{name.lower()}::{full_url.lower()}"
            if key in seen:
                continue
            seen.add(key)

            leads.append(
                UniversityLead(
                    university_name=name,
                    country=self._extract_country_from_text(name) or "",
                    source_url=full_url,
                    rank_hint=f"#{rank_counter}",
                )
            )
            rank_counter += 1

        if not leads:
            for name, href in self._extract_name_url_pairs_from_json_blobs(html):
                full_url = urljoin(source_url, href)
                key = f"{name.lower()}::{full_url.lower()}"
                if key in seen:
                    continue
                seen.add(key)
                leads.append(
                    UniversityLead(
                        university_name=name,
                        country=self._extract_country_from_text(name) or "",
                        source_url=full_url,
                        rank_hint=f"#{rank_counter}",
                    )
                )
                rank_counter += 1

        return leads

    def _extract_with_regex(self, html: str, source_url: str) -> list[UniversityLead]:
        pattern = re.compile(
            r'href="(?P<href>/universities/[^"]+)"[^>]*>(?P<name>[^<]{3,120})</a>',
            re.IGNORECASE,
        )
        seen: set[str] = set()
        leads: list[UniversityLead] = []
        rank_counter = 1

        for match in pattern.finditer(html):
            href = match.group("href").strip()
            name = unescape(match.group("name")).strip()
            if not href or not name:
                continue
            full_url = urljoin(source_url, href)
            key = f"{name.lower()}::{full_url.lower()}"
            if key in seen:
                continue
            seen.add(key)
            leads.append(
                UniversityLead(
                    university_name=name,
                    country=self._extract_country_from_text(name) or "",
                    source_url=full_url,
                    rank_hint=f"#{rank_counter}",
                )
            )
            rank_counter += 1

        if not leads:
            for name, href in self._extract_name_url_pairs_from_json_blobs(html):
                full_url = urljoin(source_url, href)
                key = f"{name.lower()}::{full_url.lower()}"
                if key in seen:
                    continue
                seen.add(key)
                leads.append(
                    UniversityLead(
                        university_name=name,
                        country=self._extract_country_from_text(name) or "",
                        source_url=full_url,
                        rank_hint=f"#{rank_counter}",
                    )
                )
                rank_counter += 1

        return leads

    @staticmethod
    def _extract_name_url_pairs_from_json_blobs(html: str) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        pattern = re.compile(
            r'"name"\s*:\s*"(?P<name>[^"]{3,120})"[\s\S]{0,240}?"url"\s*:\s*"(?P<url>(?:https?:)?//[^"]+/universities/[^"]+|/universities/[^"]+)"',
            re.IGNORECASE,
        )
        for match in pattern.finditer(html):
            name = unescape(match.group("name")).strip()
            url = match.group("url").strip()
            if name and url:
                pairs.append((name, url))
        return pairs

    @staticmethod
    def _extract_country_from_text(value: str) -> str:
        # The ranking page often does not include explicit country per row.
        # We keep it blank when not obvious.
        text = value.strip()
        if "," in text:
            return text.split(",")[-1].strip()
        return ""

    def _fallback_university_leads(self, source_url: str) -> list[UniversityLead]:
        leads: list[UniversityLead] = []
        for rank, (name, country, website) in enumerate(FALLBACK_CS_UNIVERSITIES, start=1):
            leads.append(
                UniversityLead(
                    university_name=name,
                    country=country,
                    source_url=website or source_url,
                    rank_hint=f"seed#{rank}",
                )
            )
        return leads[: self.settings.phd_max_universities]


def normalize_phdportal_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url.lstrip('/')}"
    return url
