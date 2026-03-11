#!/usr/bin/env python3

from job_automation.config import load_settings
from job_automation.scraper import JobScraper

def create_app():
    try:
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "Missing dependency: fastapi. Install requirements first with "
            "`pip install -r requirements.txt`."
        ) from exc

    app = FastAPI(title="Ermias Job Scraper API")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "message": "Job scraper is running"}

    @app.get("/scrape")
    def scrape() -> JSONResponse:
        scraper = JobScraper(load_settings())
        return JSONResponse(content=scraper.scrape_all())

    return app


app = create_app()


if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "Missing dependency: uvicorn. Install requirements first with "
            "`pip install -r requirements.txt`."
        ) from exc

    print("Starting scraper API at http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
