#!/usr/bin/env python3

from job_automation.config import load_settings


def main() -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler

    from job_automation.pipeline import AutomationPipeline

    settings = load_settings()
    scheduler = BlockingScheduler(timezone="Europe/Rome")

    @scheduler.scheduled_job(
        "cron",
        hour=settings.daily_run_hour,
        minute=settings.daily_run_minute,
    )
    def scheduled_run() -> None:
        pipeline = AutomationPipeline(load_settings())
        result = pipeline.run()
        print(result["summary"])

    print(
        f"Scheduler active. Daily run at {settings.daily_run_hour:02d}:{settings.daily_run_minute:02d} Europe/Rome"
    )
    scheduler.start()


if __name__ == "__main__":
    main()
