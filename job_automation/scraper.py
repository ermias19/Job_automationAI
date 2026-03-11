from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import asdict
import logging
import random
from threading import Lock
import time

from job_automation.config import Settings
from job_automation.models import JobListing, SearchTarget

logger = logging.getLogger(__name__)


def _load_scrape_jobs():
    try:
        from jobspy import scrape_jobs

        return scrape_jobs
    except ImportError:
        try:
            from python_jobspy.jobspy import scrape_jobs
        except ImportError as exc:
            raise RuntimeError(
                "Missing dependency: python-jobspy/jobspy. Install requirements first with "
                "`pip install -r requirements.txt`."
            ) from exc

        return scrape_jobs


class JobScraper:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._seen_lock = Lock()
        self._seen_keys: set[str] = set()

    def scrape_all(self) -> dict:
        targets = self.settings.build_targets()
        random.shuffle(targets)
        start = time.time()
        all_jobs: list[JobListing] = []
        batch_size = self.settings.effective_batch_size()
        total_batches = (len(targets) + batch_size - 1) // batch_size

        logger.info(
            "Starting scrape: %s targets, %s workers, %s batch size",
            len(targets),
            self.settings.effective_scraper_workers(),
            batch_size,
        )

        for offset in range(0, len(targets), batch_size):
            batch = targets[offset : offset + batch_size]
            batch_number = (offset // batch_size) + 1
            batch_targets = ", ".join(
                f"{target.job_title} @ {target.city}/{target.country_code}" for target in batch
            )
            logger.info(
                "Batch %s/%s: %s",
                batch_number,
                total_batches,
                batch_targets,
            )
            with ThreadPoolExecutor(
                max_workers=self.settings.effective_scraper_workers()
            ) as pool:
                futures = {
                    pool.submit(self._scrape_one, target): target
                    for target in batch
                }

                for future in as_completed(
                    futures,
                    timeout=self.settings.scraper_timeout_seconds + 30,
                ):
                    try:
                        all_jobs.extend(
                            future.result(timeout=self.settings.scraper_timeout_seconds)
                        )
                    except TimeoutError:
                        logger.warning("Batch %s timed out for %s", batch_number, futures[future])
                        continue
                    except Exception:
                        logger.exception(
                            "Batch %s failed for %s",
                            batch_number,
                            futures[future],
                        )
                        continue

            is_last_batch = offset + batch_size >= len(targets)
            if not is_last_batch and self.settings.scraper_batch_pause_seconds > 0:
                logger.info(
                    "Pausing %.1fs before next batch",
                    self.settings.scraper_batch_pause_seconds,
                )
                time.sleep(self.settings.scraper_batch_pause_seconds)

        elapsed = round(time.time() - start, 1)
        logger.info(
            "Scrape complete: %s jobs found across %s targets in %ss",
            len(all_jobs),
            len(targets),
            elapsed,
        )
        return {
            "jobs": [self._serialize_job(job) for job in all_jobs],
            "total_jobs_found": len(all_jobs),
            "elapsed_seconds": elapsed,
            "combinations_searched": len(targets),
        }

    def scrape_listings(self) -> list[JobListing]:
        payload = self.scrape_all()
        return [JobListing(**item) for item in payload["jobs"]]

    def _scrape_one(self, target: SearchTarget) -> list[JobListing]:
        scrape_jobs = _load_scrape_jobs()

        try:
            jitter_min = self.settings.scraper_request_jitter_min_seconds
            jitter_max = self.settings.scraper_request_jitter_max_seconds
            if jitter_max > 0 and jitter_max >= jitter_min:
                delay = random.uniform(jitter_min, jitter_max)
                logger.info(
                    "Waiting %.1fs before LinkedIn request for %s in %s",
                    delay,
                    target.job_title,
                    target.country_code,
                )
                time.sleep(delay)

            params = {
                "site_name": self.settings.sites,
                "search_term": target.job_title,
                "location": target.location,
                "results_wanted": self.settings.results_per_search,
                "hours_old": self.settings.hours_old,
                "country_indeed": target.country_name,
                "linkedin_fetch_description": self.settings.linkedin_fetch_description,
                "verbose": 0,
            }

            if self.settings.job_type:
                params["job_type"] = self.settings.job_type

            if target.remote:
                params["is_remote"] = True

            dataframe = scrape_jobs(**params)
            if dataframe is None or len(dataframe) == 0:
                logger.info(
                    "No jobs returned for %s in %s",
                    target.job_title,
                    target.country_code,
                )
                return []

            results: list[JobListing] = []
            for _, row in dataframe.iterrows():
                url = str(row.get("job_url", "")).strip()
                if not url:
                    continue

                listing = JobListing(
                    job_title=self._clean_text(row.get("title", "")),
                    company_name=self._clean_text(row.get("company", "")),
                    job_location=self._clean_text(row.get("location", "")),
                    job_employment_type=self._clean_text(row.get("job_type", "")),
                    job_seniority_level=self._clean_text(row.get("job_level", "")),
                    job_base_pay_range=self._format_salary(row),
                    job_num_applicants=self._clean_text(row.get("num_applicants", "")),
                    job_posted_time=self._clean_text(row.get("date_posted", "")),
                    apply_link=url,
                    company_url=self._clean_text(row.get("company_url", "")),
                    job_summary=self._clean_text(row.get("description", ""))[:500],
                    job_description_formatted=self._clean_text(row.get("description", "")),
                    source_site=self._clean_text(row.get("site", "")),
                    search_title=target.job_title,
                    search_country=target.country_code,
                    raw=row.to_dict(),
                )

                dedupe_key = listing.dedupe_key()
                with self._seen_lock:
                    if dedupe_key in self._seen_keys:
                        continue
                    self._seen_keys.add(dedupe_key)

                results.append(listing)
            logger.info(
                "Found %s jobs for %s in %s",
                len(results),
                target.job_title,
                target.country_code,
            )
            return results
        except Exception:
            logger.exception(
                "Scrape failed for %s in %s",
                target.job_title,
                target.country_code,
            )
            return []

    @staticmethod
    def _format_salary(row) -> str:
        min_amount = row.get("min_amount", "")
        max_amount = row.get("max_amount", "")
        currency = row.get("currency", "")
        if min_amount or max_amount:
            return f"{min_amount} - {max_amount} {currency}".strip(" -").strip()
        return ""

    @staticmethod
    def _serialize_job(job: JobListing) -> dict:
        payload = asdict(job)
        payload["raw"] = {}
        return payload

    @staticmethod
    def _clean_text(value) -> str:
        text = str(value).strip()
        if text.lower() in {"none", "nan", "nat"}:
            return ""
        return text
