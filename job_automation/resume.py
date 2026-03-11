from __future__ import annotations

from pathlib import Path

from job_automation.config import Settings


def _read_markdown(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _read_pdf_text(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""

    try:
        from PyPDF2 import PdfReader
    except ImportError:
        return ""

    reader = PdfReader(str(path))
    parts: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            parts.append(text.strip())
    return "\n\n".join(parts).strip()


def load_candidate_background(settings: Settings) -> str:
    sections: list[str] = []

    profile_text = _read_markdown(settings.candidate_profile_path)
    if profile_text:
        sections.append("Candidate profile:\n" + profile_text)

    resume_text = _read_pdf_text(settings.resume_pdf_path)
    if resume_text:
        sections.append("Resume text:\n" + resume_text[:14000])

    return "\n\n".join(section for section in sections if section).strip()
