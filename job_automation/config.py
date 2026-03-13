from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os

from job_automation.defaults import COUNTRIES, DEFAULT_SITES, JOB_TITLES
from job_automation.models import SearchTarget

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    if load_dotenv is not None:
        load_dotenv(PROJECT_ROOT / ".env")


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, fallback: list[str]) -> list[str]:
    if not value:
        return fallback
    return [item.strip() for item in value.split(",") if item.strip()]


def _path(value: str | None, default: Path | None = None) -> Path | None:
    if value:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        return candidate
    return default


@dataclass
class Settings:
    project_root: Path = PROJECT_ROOT
    output_dir: Path = PROJECT_ROOT / "outputs"
    resume_pdf_path: Path | None = None
    candidate_profile_path: Path = PROJECT_ROOT / "profiles" / "candidate_profile.md"

    job_titles: list[str] = field(default_factory=lambda: list(JOB_TITLES))
    countries: list[SearchTarget] = field(default_factory=list)
    sites: list[str] = field(default_factory=lambda: list(DEFAULT_SITES))
    include_remote: bool = False
    results_per_search: int = 5
    hours_old: int = 168
    job_type: str | None = "fulltime"
    linkedin_fetch_description: bool = False
    scraper_max_workers: int = 20
    scraper_timeout_seconds: int = 45
    scraper_batch_pause_seconds: float = 8.0
    scraper_request_jitter_min_seconds: float = 1.5
    scraper_request_jitter_max_seconds: float = 4.0
    max_search_targets: int | None = None

    openai_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"
    ai_batch_size: int = 5
    ai_max_workers: int = 4
    max_jobs_for_ai: int = 48
    minimum_fit_score: int = 70
    tailor_top_n: int = 10

    google_sheets_spreadsheet_id: str | None = None
    google_sheets_worksheet: str = "Jobs"
    google_sheets_phd_report_worksheet: str = "phd-research-report"
    google_service_account_json: Path | None = None
    google_apps_script_webapp_url: str | None = None
    google_drive_upload_enabled: bool = True
    google_drive_root_folder_id: str | None = None
    google_drive_root_folder_name: str = "JobAutomationAI-PhD-Applications"
    google_drive_public_links: bool = True
    google_drive_oauth_client_secret_json: Path | None = None
    google_drive_oauth_token_json: Path = PROJECT_ROOT / "credentials" / "google-drive-oauth-token.json"

    email_to: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True

    daily_run_hour: int = 8
    daily_run_minute: int = 0

    phd_portal_universities_url: str = (
        "https://www.phdportal.com/search/universities/phd/rankings/computer-science-it"
    )
    phd_university_source_order: list[str] = field(
        default_factory=lambda: ["seed_file", "phdportal", "fallback"]
    )
    phd_university_seed_file: Path | None = PROJECT_ROOT / "profiles" / "phd_universities_seed.csv"
    phd_max_universities: int = 30
    phd_professors_per_university: int = 3
    phd_subject_keywords: list[str] = field(
        default_factory=lambda: [
            "computer science",
            "distributed systems",
            "high performance computing",
            "networking",
            "artificial intelligence",
        ]
    )
    phd_send_emails: bool = False

    def effective_scraper_workers(self) -> int:
        if self.sites == ["linkedin"]:
            return max(1, min(self.scraper_max_workers, 2))
        return max(1, self.scraper_max_workers)

    def effective_batch_size(self) -> int:
        if self.sites == ["linkedin"]:
            return self.effective_scraper_workers()
        return max(1, self.effective_scraper_workers())

    def build_targets(self) -> list[SearchTarget]:
        targets: list[SearchTarget] = []
        for job_title in self.job_titles:
            for country in self.countries:
                targets.append(
                    SearchTarget(
                        job_title=job_title,
                        country_code=country.country_code,
                        country_name=country.country_name,
                        city=country.city,
                        remote=False,
                    )
                )
                if self.include_remote:
                    targets.append(
                        SearchTarget(
                            job_title=job_title,
                            country_code=country.country_code,
                            country_name=country.country_name,
                            city=country.city,
                            remote=True,
                        )
                    )
        if self.max_search_targets is not None:
            return targets[: self.max_search_targets]
        return targets


def load_settings() -> Settings:
    _load_dotenv()

    countries = [
        SearchTarget(
            job_title="",
            country_code=item["code"],
            country_name=item["full"],
            city=item["city"],
        )
        for item in COUNTRIES
    ]

    sites = _csv(os.getenv("JOB_SITES"), list(DEFAULT_SITES))
    raw_workers = os.getenv("SCRAPER_MAX_WORKERS")
    default_workers = "2" if sites == ["linkedin"] else "20"

    return Settings(
        output_dir=_path(os.getenv("OUTPUT_DIR"), PROJECT_ROOT / "outputs") or PROJECT_ROOT / "outputs",
        resume_pdf_path=_path(os.getenv("RESUME_PDF_PATH")),
        candidate_profile_path=_path(
            os.getenv("CANDIDATE_PROFILE_PATH"),
            PROJECT_ROOT / "profiles" / "candidate_profile.md",
        )
        or PROJECT_ROOT / "profiles" / "candidate_profile.md",
        job_titles=_csv(os.getenv("JOB_TITLES"), list(JOB_TITLES)),
        countries=countries,
        sites=sites,
        include_remote=_as_bool(os.getenv("INCLUDE_REMOTE")),
        results_per_search=int(os.getenv("RESULTS_PER_SEARCH", "5")),
        hours_old=int(os.getenv("HOURS_OLD", "168")),
        job_type=os.getenv("JOB_TYPE", "fulltime") or None,
        linkedin_fetch_description=_as_bool(
            os.getenv("LINKEDIN_FETCH_DESCRIPTION"),
            default=False,
        ),
        scraper_max_workers=int(raw_workers or default_workers),
        scraper_timeout_seconds=int(os.getenv("SCRAPER_TIMEOUT_SECONDS", "45")),
        scraper_batch_pause_seconds=float(os.getenv("SCRAPER_BATCH_PAUSE_SECONDS", "8")),
        scraper_request_jitter_min_seconds=float(
            os.getenv("SCRAPER_REQUEST_JITTER_MIN_SECONDS", "1.5")
        ),
        scraper_request_jitter_max_seconds=float(
            os.getenv("SCRAPER_REQUEST_JITTER_MAX_SECONDS", "4.0")
        ),
        max_search_targets=(
            int(os.getenv("MAX_SEARCH_TARGETS"))
            if os.getenv("MAX_SEARCH_TARGETS")
            else None
        ),
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        ai_batch_size=int(os.getenv("AI_BATCH_SIZE", "5")),
        ai_max_workers=int(os.getenv("AI_MAX_WORKERS", "4")),
        max_jobs_for_ai=int(os.getenv("MAX_JOBS_FOR_AI", "48")),
        minimum_fit_score=int(os.getenv("MINIMUM_FIT_SCORE", "70")),
        tailor_top_n=int(os.getenv("TAILOR_TOP_N", "10")),
        google_sheets_spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID") or None,
        google_sheets_worksheet=os.getenv("GOOGLE_SHEETS_WORKSHEET", "Jobs"),
        google_sheets_phd_report_worksheet=os.getenv(
            "GOOGLE_SHEETS_PHD_REPORT_WORKSHEET",
            "phd-research-report",
        ),
        google_service_account_json=_path(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
        google_apps_script_webapp_url=os.getenv("GOOGLE_APPS_SCRIPT_WEBAPP_URL") or None,
        google_drive_upload_enabled=_as_bool(os.getenv("GOOGLE_DRIVE_UPLOAD_ENABLED"), default=True),
        google_drive_root_folder_id=os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID") or None,
        google_drive_root_folder_name=os.getenv(
            "GOOGLE_DRIVE_ROOT_FOLDER_NAME",
            "JobAutomationAI-PhD-Applications",
        ),
        google_drive_public_links=_as_bool(os.getenv("GOOGLE_DRIVE_PUBLIC_LINKS"), default=True),
        google_drive_oauth_client_secret_json=_path(
            os.getenv("GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_JSON")
        ),
        google_drive_oauth_token_json=_path(
            os.getenv("GOOGLE_DRIVE_OAUTH_TOKEN_JSON"),
            PROJECT_ROOT / "credentials" / "google-drive-oauth-token.json",
        )
        or PROJECT_ROOT / "credentials" / "google-drive-oauth-token.json",
        email_to=os.getenv("EMAIL_TO") or None,
        smtp_host=os.getenv("SMTP_HOST") or None,
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_username=os.getenv("SMTP_USERNAME") or None,
        smtp_password=os.getenv("SMTP_PASSWORD") or None,
        smtp_use_tls=_as_bool(os.getenv("SMTP_USE_TLS"), default=True),
        daily_run_hour=int(os.getenv("DAILY_RUN_HOUR", "8")),
        daily_run_minute=int(os.getenv("DAILY_RUN_MINUTE", "0")),
        phd_portal_universities_url=os.getenv(
            "PHD_PORTAL_UNIVERSITIES_URL",
            "https://www.phdportal.com/search/universities/phd/rankings/computer-science-it",
        ),
        phd_university_source_order=_csv(
            os.getenv("PHD_UNIVERSITY_SOURCE_ORDER"),
            ["seed_file", "phdportal", "fallback"],
        ),
        phd_university_seed_file=_path(
            os.getenv("PHD_UNIVERSITY_SEED_FILE"),
            PROJECT_ROOT / "profiles" / "phd_universities_seed.csv",
        ),
        phd_max_universities=int(os.getenv("PHD_MAX_UNIVERSITIES", "30")),
        phd_professors_per_university=int(
            os.getenv("PHD_PROFESSORS_PER_UNIVERSITY", "3")
        ),
        phd_subject_keywords=_csv(
            os.getenv("PHD_SUBJECT_KEYWORDS"),
            [
                "computer science",
                "distributed systems",
                "high performance computing",
                "networking",
                "artificial intelligence",
            ],
        ),
        phd_send_emails=_as_bool(os.getenv("PHD_SEND_EMAILS"), default=False),
    )
