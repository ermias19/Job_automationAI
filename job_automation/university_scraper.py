from __future__ import annotations

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
    """Scrapes university leads from the PhDPortal ranking page."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def scrape_universities(self) -> list[UniversityLead]:
        url = self.settings.phd_portal_universities_url
        logger.info("Scraping PhD universities from %s", url)
        html = self._download(url)
        if not html:
            logger.warning(
                "PhD ranking page could not be downloaded. Using fallback CS university seed list."
            )
            return self._fallback_university_leads(source_url=url)

        leads = self._extract_university_leads(html=html, source_url=url)
        if not leads:
            logger.warning(
                "Could not extract university leads from the ranking page. "
                "The site likely changed markup or blocked scraping. "
                "Using fallback CS university seed list."
            )
            return self._fallback_university_leads(source_url=url)

        return leads[: self.settings.phd_max_universities]

    @staticmethod
    def _download(url: str) -> str:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception:
            logger.exception("Failed to download %s", url)
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
