#!/usr/bin/env python3

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from job_automation.config import load_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape jobs, score fit, tailor application assets, and export results."
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser("run", help="Run the full automation pipeline.")
    run_parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Skip OpenAI scoring and use the heuristic fallback only.",
    )

    scrape_parser = subparsers.add_parser("scrape", help="Run only the scraper.")
    scrape_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the scraped JSON payload.",
    )

    recommend_parser = subparsers.add_parser(
        "recommend",
        help="Run AI recommendation and CV tailoring on an existing scraped-jobs JSON file.",
    )
    recommend_parser.add_argument(
        "--input",
        required=True,
        help="Path to a JSON file containing either a top-level jobs array or a {\"jobs\": [...]} payload.",
    )
    recommend_parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Skip OpenAI scoring and use the heuristic fallback only.",
    )

    phd_parser = subparsers.add_parser(
        "phd-run",
        help=(
            "Run PhD automation pipeline: university scraper -> professor finder -> "
            "AI relevance -> resume/email generation -> sheet export."
        ),
    )
    phd_parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Skip OpenAI scoring and use the heuristic fallback only.",
    )

    return parser


def run_pipeline(no_ai: bool = False) -> dict[str, Any]:
    from job_automation.pipeline import AutomationPipeline

    settings = load_settings()
    if no_ai:
        settings.openai_api_key = None
    pipeline = AutomationPipeline(settings)
    return pipeline.run()


def run_scrape(pretty: bool = False) -> None:
    from job_automation.scraper import JobScraper

    settings = load_settings()
    scraper = JobScraper(settings)
    payload = scraper.scrape_all()
    indent = 2 if pretty else None
    print(json.dumps(payload, indent=indent, ensure_ascii=True, default=str))


def run_recommend(input_path: str, no_ai: bool = False) -> dict[str, Any]:
    from job_automation.pipeline import AutomationPipeline

    settings = load_settings()
    if no_ai:
        settings.openai_api_key = None
    pipeline = AutomationPipeline(settings)
    return pipeline.run_from_file(Path(input_path))


def run_phd_pipeline(no_ai: bool = False) -> dict[str, Any]:
    from job_automation.phd_pipeline import PhdAutomationPipeline

    settings = load_settings()
    if no_ai:
        settings.openai_api_key = None
    pipeline = PhdAutomationPipeline(settings)
    return pipeline.run()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    # Suppress verbose transport logs from OpenAI/httpx while keeping app logs at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    parser = build_parser()
    args = parser.parse_args()
    command = args.command or "run"

    if command == "run":
        result = run_pipeline(no_ai=getattr(args, "no_ai", False))
        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
        return

    if command == "scrape":
        run_scrape(pretty=getattr(args, "pretty", False))
        return

    if command == "recommend":
        result = run_recommend(
            input_path=getattr(args, "input"),
            no_ai=getattr(args, "no_ai", False),
        )
        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
        return

    if command == "phd-run":
        result = run_phd_pipeline(no_ai=getattr(args, "no_ai", False))
        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
        return

    parser.error(f"Unsupported command: {command}")


if __name__ == "__main__":
    main()
