from __future__ import annotations

from email.message import EmailMessage
import smtplib

from job_automation.config import Settings
from job_automation.models import MatchResult


class Emailer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send_summary(self, run_id: str, matches: list[MatchResult], summary: str) -> bool:
        if not all(
            [
                self.settings.email_to,
                self.settings.smtp_host,
                self.settings.smtp_username,
                self.settings.smtp_password,
            ]
        ):
            return False

        body_lines = [summary, "", "Top matches:"]
        for match in matches[:10]:
            body_lines.append(
                f"- [{match.assessment.fit_score}] {match.job.job_title} at "
                f"{match.job.company_name} ({match.job.job_location})"
            )
            body_lines.append(f"  {match.job.apply_link}")

        message = EmailMessage()
        message["Subject"] = f"Job automation summary - {run_id}"
        message["From"] = self.settings.smtp_username
        message["To"] = self.settings.email_to
        message.set_content("\n".join(body_lines))

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port) as client:
            if self.settings.smtp_use_tls:
                client.starttls()
            client.login(self.settings.smtp_username, self.settings.smtp_password)
            client.send_message(message)
        return True
