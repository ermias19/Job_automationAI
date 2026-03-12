from __future__ import annotations

from email.message import EmailMessage
import logging
import smtplib

from job_automation.config import Settings
from job_automation.models import MatchResult

logger = logging.getLogger(__name__)


class PhdEmailAutomation:
    """Sends personalized outreach emails for PhD leads when enabled."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send_applications(self, run_id: str, matches: list[MatchResult]) -> dict:
        if not self.settings.phd_send_emails:
            return {"enabled": False, "sent": 0, "skipped": len(matches)}

        if not all(
            [
                self.settings.smtp_host,
                self.settings.smtp_username,
                self.settings.smtp_password,
            ]
        ):
            logger.warning("SMTP credentials missing; PhD email automation skipped")
            return {"enabled": True, "sent": 0, "skipped": len(matches)}

        sent = 0
        skipped = 0
        for match in matches:
            recipient = self._extract_recipient(match)
            if not recipient:
                skipped += 1
                continue
            try:
                self._send_one(run_id=run_id, match=match, recipient=recipient)
                sent += 1
            except Exception:
                skipped += 1
                logger.exception(
                    "Failed to send PhD outreach email for %s at %s",
                    match.job.job_title,
                    match.job.company_name,
                )
        return {"enabled": True, "sent": sent, "skipped": skipped}

    @staticmethod
    def _extract_recipient(match: MatchResult) -> str:
        raw = match.job.raw or {}
        recipient = str(raw.get("professor_email", "")).strip()
        return recipient

    def _send_one(self, run_id: str, match: MatchResult, recipient: str) -> None:
        professor_name = str((match.job.raw or {}).get("professor_name", "")).strip()
        greeting = f"Dear Prof. {professor_name}," if professor_name else "Dear Professor,"
        intro = (
            match.artifacts.email_intro
            if match.artifacts and match.artifacts.email_intro
            else (
                f"I am writing to express my interest in the research direction around "
                f"{match.job.job_title} at {match.job.company_name}."
            )
        )
        body = (
            f"{greeting}\n\n"
            f"{intro}\n\n"
            "I would be grateful for the opportunity to discuss potential PhD supervision.\n\n"
            "Best regards,\n"
            "Ermias Mulugeta Teklehaimanot\n"
        )

        message = EmailMessage()
        message["Subject"] = f"Prospective PhD Applicant - {match.job.company_name} ({run_id})"
        message["From"] = self.settings.smtp_username
        message["To"] = recipient
        message.set_content(body)

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port) as client:
            if self.settings.smtp_use_tls:
                client.starttls()
            client.login(self.settings.smtp_username, self.settings.smtp_password)
            client.send_message(message)
