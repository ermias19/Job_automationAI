from __future__ import annotations

import csv
from html import unescape
import json
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
            ordered_sources = ["euraxess", "findaphd", "scholarshipdb", "fallback"]

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
            source_url=self._primary_source_url()
        )

    @staticmethod
    def _normalized_source_order(raw_sources: list[str]) -> list[str]:
        if not raw_sources:
            return []

        normalized: list[str] = []
        for source in raw_sources:
            token = source.strip().lower().replace("-", "_")
            if token in {"phdportal", "phd_portal", "phd-portal"}:
                token = "findaphd"
            if token in {"seed", "builtin", "built_in"}:
                token = "fallback"
            if token not in {
                "seed_file",
                "findaphd",
                "scholarshipdb",
                "euraxess",
                "fallback",
            }:
                logger.warning("Ignoring unknown PHD university source: %s", source)
                continue
            if token not in normalized:
                normalized.append(token)
        return normalized

    def _collect_source(self, source: str) -> list[UniversityLead]:
        if source == "seed_file":
            return self._load_seed_file()
        if source == "findaphd":
            return self._load_from_findaphd()
        if source == "scholarshipdb":
            return self._load_from_scholarshipdb()
        if source == "euraxess":
            return self._load_from_euraxess()
        if source == "fallback":
            return self._fallback_university_leads(
                source_url=self._primary_source_url()
            )
        return []

    def _primary_source_url(self) -> str:
        for candidate in (
            self.settings.phd_findaphd_url,
            self.settings.phd_scholarshipdb_url,
            self.settings.phd_euraxess_api_url,
        ):
            if candidate:
                return candidate
        return "https://www.findaphd.com/phds/computer-science/"

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
                            source_url=source_url or self._primary_source_url(),
                            rank_hint=rank_hint,
                        )
                    )
        except Exception:
            logger.exception("Failed reading university seed file %s", path)
            return []
        return leads

    def _load_from_findaphd(self) -> list[UniversityLead]:
        url = self.settings.phd_findaphd_url
        logger.info("Scraping universities from FindAPhD: %s", url)
        html = self._download(url)
        if not html:
            return []
        leads = self._extract_generic_university_leads(
            html=html,
            source_url=url,
            rank_prefix="findaphd",
        )
        if not leads:
            logger.warning("No university leads parsed from FindAPhD page %s", url)
        return leads

    def _load_from_scholarshipdb(self) -> list[UniversityLead]:
        url = self.settings.phd_scholarshipdb_url
        logger.info("Scraping universities from ScholarshipDB: %s", url)
        html = self._download(url)
        if not html:
            return []
        leads = self._extract_generic_university_leads(
            html=html,
            source_url=url,
            rank_prefix="scholarshipdb",
        )
        if not leads:
            logger.warning("No university leads parsed from ScholarshipDB page %s", url)
        return leads

    def _load_from_euraxess(self) -> list[UniversityLead]:
        url = self.settings.phd_euraxess_api_url
        logger.info("Collecting universities from EURAXESS source: %s", url)

        payload = self._download_json(url)
        if payload is not None:
            api_leads = self._extract_euraxess_university_leads(payload=payload, source_url=url)
            if api_leads:
                return api_leads
            logger.warning("EURAXESS JSON source returned no university leads: %s", url)

        html = self._download(url)
        if not html:
            return []
        html_leads = self._extract_generic_university_leads(
            html=html,
            source_url=url,
            rank_prefix="euraxess",
        )
        if not html_leads:
            logger.warning("No university leads parsed from EURAXESS HTML page %s", url)
        return html_leads

    def _download_json(self, url: str) -> dict | list | None:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/json,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
            if response.status_code in {401, 403, 429}:
                logger.warning(
                    "Source blocked automated access (HTTP %s) for %s",
                    response.status_code,
                    url,
                )
                return None
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()
            body = response.text.strip()
            if "json" not in content_type and not (body.startswith("{") or body.startswith("[")):
                return None
            return response.json()
        except requests.RequestException as exc:
            logger.warning("Failed to download JSON %s (%s)", url, exc)
            return None
        except ValueError:
            logger.warning("Could not parse JSON response from %s", url)
            return None

    def _extract_euraxess_university_leads(
        self,
        payload: dict | list,
        source_url: str,
    ) -> list[UniversityLead]:
        records = self._extract_records(payload)
        seen: set[str] = set()
        leads: list[UniversityLead] = []
        for index, record in enumerate(records, start=1):
            name = self._extract_university_name_from_record(record)
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            country = self._extract_country_from_record(record)
            link = self._extract_link_from_record(record) or source_url
            leads.append(
                UniversityLead(
                    university_name=name,
                    country=country,
                    source_url=link,
                    rank_hint=f"euraxess#{index}",
                )
            )
        return leads

    def _extract_generic_university_leads(
        self,
        html: str,
        source_url: str,
        rank_prefix: str,
    ) -> list[UniversityLead]:
        names_and_urls: list[tuple[str, str]] = []
        try:
            from bs4 import BeautifulSoup  # type: ignore

            soup = BeautifulSoup(html, "html.parser")
            for anchor in soup.find_all("a"):
                href = str(anchor.get("href", "")).strip()
                text = anchor.get_text(" ", strip=True)
                name = self._extract_university_name_from_text(text)
                if not name:
                    continue
                full_url = urljoin(source_url, href) if href else source_url
                names_and_urls.append((name, full_url))

            for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                raw = script.string or script.text or ""
                for name in self._extract_university_names_from_json_text(raw):
                    names_and_urls.append((name, source_url))
        except Exception:
            pass

        if not names_and_urls:
            for name in self._extract_university_names_from_text_blob(html):
                names_and_urls.append((name, source_url))

        seen: set[str] = set()
        leads: list[UniversityLead] = []
        rank_counter = 1
        for name, url in names_and_urls:
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            leads.append(
                UniversityLead(
                    university_name=name,
                    country=self._extract_country_from_text(name) or "",
                    source_url=url or source_url,
                    rank_hint=f"{rank_prefix}#{rank_counter}",
                )
            )
            rank_counter += 1
        return leads

    @staticmethod
    def _extract_records(payload: dict | list) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []

        queue: list[dict | list] = [payload]
        records: list[dict] = []
        while queue:
            current = queue.pop(0)
            if isinstance(current, list):
                if current and all(isinstance(item, dict) for item in current):
                    records.extend(item for item in current if isinstance(item, dict))
                else:
                    queue.extend(item for item in current if isinstance(item, (dict, list)))
            elif isinstance(current, dict):
                for value in current.values():
                    if isinstance(value, list):
                        queue.append(value)
                    elif isinstance(value, dict):
                        queue.append(value)
        return records

    def _extract_university_name_from_record(self, record: dict) -> str:
        possible_values = [
            record.get("organizationName"),
            record.get("organisationName"),
            record.get("institutionName"),
            record.get("university"),
            record.get("employerName"),
            record.get("organization"),
            record.get("organisation"),
            record.get("institution"),
            record.get("hiringOrganization"),
            record.get("hostInstitution"),
        ]
        for value in possible_values:
            if isinstance(value, dict):
                value = value.get("name") or value.get("display_name")
            if isinstance(value, str):
                extracted = self._extract_university_name_from_text(value)
                if extracted:
                    return extracted
        return ""

    @staticmethod
    def _extract_country_from_record(record: dict) -> str:
        for key in (
            "countryCode",
            "country_code",
            "country",
            "countryName",
            "country_name",
        ):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                for sub in ("code", "name"):
                    sub_value = value.get(sub)
                    if isinstance(sub_value, str) and sub_value.strip():
                        return sub_value.strip()
        return ""

    @staticmethod
    def _extract_link_from_record(record: dict) -> str:
        for key in (
            "applyUrl",
            "applicationUrl",
            "url",
            "link",
            "vacancyUrl",
            "vacancy_url",
            "detailUrl",
            "jobUrl",
            "externalUrl",
        ):
            value = record.get(key)
            if isinstance(value, str) and value.strip().startswith(("http://", "https://")):
                return value.strip()
        return ""

    def _extract_university_names_from_json_text(self, raw: str) -> list[str]:
        names: list[str] = []
        if not raw.strip():
            return names
        try:
            payload = json.loads(raw)
            for record in self._extract_records(payload):
                name = self._extract_university_name_from_record(record)
                if name:
                    names.append(name)
            return names
        except Exception:
            pass

        names.extend(self._extract_university_names_from_text_blob(raw))
        return names

    def _extract_university_names_from_text_blob(self, text: str) -> list[str]:
        normalized = unescape(text or "")
        pattern = re.compile(
            r"([A-Z][A-Za-z&'’().,\- ]{2,100}"
            r"(?:University|Institute|College|School|Polytechnic|Universitat|Universität|Universite|Universidad)"
            r"[A-Za-z&'’().,\- ]{0,80})"
        )
        names: list[str] = []
        for match in pattern.finditer(normalized):
            candidate = self._extract_university_name_from_text(match.group(1))
            if candidate:
                names.append(candidate)
        return names

    def _extract_university_name_from_text(self, value: str) -> str:
        text = re.sub(r"\s+", " ", (value or "").strip())
        if not text or len(text) < 4 or len(text) > 140:
            return ""
        lowered = text.lower()
        noise_tokens = [
            "phd",
            "scholarship",
            "position",
            "postdoc",
            "apply",
            "deadline",
            "research assistant",
            "funding",
        ]
        if any(token in lowered for token in noise_tokens):
            if "university" not in lowered and "institute" not in lowered:
                return ""
        if " at " in lowered:
            maybe_name = text.split(" at ")[-1].strip(" -|")
            if maybe_name:
                text = maybe_name
                lowered = text.lower()
        if " | " in text:
            text = text.split(" | ")[0].strip()
            lowered = text.lower()

        keyword_tokens = [
            "university",
            "institute",
            "college",
            "school",
            "polytechnic",
            "universitat",
            "universität",
            "universite",
            "universidad",
            "academy",
        ]
        has_keyword = any(token in lowered for token in keyword_tokens)
        acronym_like = bool(re.fullmatch(r"[A-Z]{2,8}", text))
        if not has_keyword and not acronym_like:
            return ""
        return text

    @staticmethod
    def _download(url: str) -> str:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        referer = f"{parsed.scheme or 'https'}://{parsed.netloc}/" if parsed.netloc else "https://www.google.com/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
            if response.status_code in {401, 403, 429}:
                logger.warning(
                    "Source blocked automated access (HTTP %s) for %s",
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


def normalize_source_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url.lstrip('/')}"
    return url
