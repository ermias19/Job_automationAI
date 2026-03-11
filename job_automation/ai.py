from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

from job_automation.config import Settings
from job_automation.models import FitAssessment, JobListing, MatchResult, TailoredArtifacts


FIT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "assessments": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "job_key": {"type": "string"},
                    "ai_fit": {"type": "boolean"},
                    "fit_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "decision": {"type": "string"},
                    "recommendation": {"type": "string"},
                    "reasoning": {"type": "string"},
                    "missing_skills": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "resume_focus": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "candidate_highlights": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": [
                    "job_key",
                    "ai_fit",
                    "fit_score",
                    "decision",
                    "recommendation",
                    "reasoning",
                    "missing_skills",
                    "resume_focus",
                    "candidate_highlights",
                ],
            },
        }
    },
    "required": ["assessments"],
}

TAILOR_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "resume_markdown": {"type": "string"},
        "cover_letter_markdown": {"type": "string"},
        "email_intro": {"type": "string"},
        "resume_summary": {"type": "string"},
    },
    "required": ["resume_markdown", "cover_letter_markdown", "email_intro", "resume_summary"],
}


class OpenAIOrHeuristicEngine:
    def __init__(self, settings: Settings, candidate_background: str) -> None:
        self.settings = settings
        self.candidate_background = candidate_background or "Candidate profile not provided."
        self._client = None

    def evaluate_jobs(self, jobs: list[JobListing]) -> dict[str, FitAssessment]:
        if not jobs:
            return {}

        if not self.settings.openai_api_key:
            return {job.dedupe_key(): self._heuristic_fit(job) for job in jobs}

        batches = [
            jobs[index : index + self.settings.ai_batch_size]
            for index in range(0, len(jobs), self.settings.ai_batch_size)
        ]
        results: dict[str, FitAssessment] = {}

        with ThreadPoolExecutor(max_workers=self.settings.ai_max_workers) as pool:
            futures = {
                pool.submit(self._evaluate_batch, batch): batch
                for batch in batches
            }
            for future in as_completed(futures):
                try:
                    results.update(future.result())
                except Exception:
                    for job in futures[future]:
                        results[job.dedupe_key()] = self._heuristic_fit(job)

        return results

    def tailor_matches(self, matches: list[MatchResult]) -> list[MatchResult]:
        if not matches:
            return []

        if not self.settings.openai_api_key:
            for match in matches:
                match.artifacts = self._heuristic_tailor(match)
            return matches

        with ThreadPoolExecutor(max_workers=min(self.settings.ai_max_workers, len(matches))) as pool:
            futures = {
                pool.submit(self._tailor_one, match): match
                for match in matches
            }
            for future in as_completed(futures):
                match = futures[future]
                try:
                    match.artifacts = future.result()
                except Exception:
                    match.artifacts = self._heuristic_tailor(match)
        return matches

    def _evaluate_batch(self, jobs: list[JobListing]) -> dict[str, FitAssessment]:
        payload = {
            "candidate_background": self.candidate_background[:16000],
            "jobs": [job.ai_payload() for job in jobs],
        }
        system_prompt = (
            "You are the local equivalent of an n8n OpenAI fit-filter node. "
            "For each job, first decide the strict n8n-style answer: would the candidate be a good fit, yes or no. "
            "Set ai_fit to true only when the role is a clear match for the candidate's profile, target markets, and seniority. "
            "The candidate is Ermias Mulugeta, a backend and full-stack software engineer with 4+ years of experience "
            "in Python, Django, Flask, FastAPI, Node.js, React, Next.js, Docker, Kubernetes, CI/CD, GitHub Actions, "
            "PostgreSQL, MySQL, MongoDB, microservices, Linux, cybersecurity, QA automation, and cloud-native delivery. "
            "He is based in Pisa, Italy, open to remote, EU-based, and UK relocation roles, with no sponsorship required. "
            "Prefer software engineer, backend, full-stack, Python, platform, DevOps, cloud, and early-career engineering roles. "
            "Penalize obvious mismatches such as spontaneous applications, internship-only roles with no engineering depth, "
            "or roles clearly unrelated to software engineering. "
            "Use decision values keep, review, or skip. "
            "Use recommendation values strong_match, good_match, stretch, or skip. "
            "Return only the JSON schema provided."
        )
        response = self._call_json(
            schema_name="job_fit_batch",
            schema=FIT_SCHEMA,
            system_prompt=system_prompt,
            payload=payload,
        )

        results: dict[str, FitAssessment] = {}
        for item in response.get("assessments", []):
            results[item["job_key"]] = FitAssessment(
                ai_fit=bool(item.get("ai_fit", False)),
                fit_score=int(item.get("fit_score", 0)),
                decision=str(item.get("decision", "review")).strip().lower(),
                recommendation=str(item.get("recommendation", "review")).strip().lower(),
                reasoning=str(item.get("reasoning", "")).strip(),
                missing_skills=[str(value) for value in item.get("missing_skills", [])],
                resume_focus=[str(value) for value in item.get("resume_focus", [])],
                candidate_highlights=[
                    str(value) for value in item.get("candidate_highlights", [])
                ],
            )

        for job in jobs:
            results.setdefault(job.dedupe_key(), self._heuristic_fit(job))
        return results

    def _tailor_one(self, match: MatchResult) -> TailoredArtifacts:
        payload = {
            "candidate_background": self.candidate_background[:16000],
            "job": match.job.ai_payload(),
            "fit_assessment": {
                "fit_score": match.assessment.fit_score,
                "ai_fit": match.assessment.ai_fit,
                "decision": match.assessment.decision,
                "recommendation": match.assessment.recommendation,
                "reasoning": match.assessment.reasoning,
                "missing_skills": match.assessment.missing_skills,
                "resume_focus": match.assessment.resume_focus,
                "candidate_highlights": match.assessment.candidate_highlights,
            },
        }
        system_prompt = (
            "You are the local equivalent of an n8n OpenAI resume-customization node. "
            "Create concise, credible, ATS-friendly application materials for the candidate. "
            "Do not invent degrees, certifications, employers, locations, or metrics. "
            "The resume output must be clean plain text only: no markdown, no bullets that invent content, no backticks, no HTML. "
            "Reorder and reword the existing background to emphasize the strongest matching skills and experience for this specific job. "
            "Use the candidate background exactly as the source of truth. "
            "Return only the JSON schema provided."
        )
        response = self._call_json(
            schema_name="tailored_assets",
            schema=TAILOR_SCHEMA,
            system_prompt=system_prompt,
            payload=payload,
        )
        return TailoredArtifacts(
            resume_markdown=response.get("resume_markdown", "").strip(),
            cover_letter_markdown=response.get("cover_letter_markdown", "").strip(),
            email_intro=response.get("email_intro", "").strip(),
            resume_summary=response.get("resume_summary", "").strip(),
            resume_doc_title=self._resume_doc_title(match),
        )

    def _heuristic_fit(self, job: JobListing) -> FitAssessment:
        score = 45
        title = f"{job.job_title} {job.search_title}".lower()
        highlights: list[str] = []

        if any(
            token in title
            for token in [
                "software",
                "backend",
                "frontend",
                "python",
                "full stack",
                "fullstack",
                "devops",
                "cloud",
                "sre",
                "api",
                "web developer",
                "developer",
            ]
        ):
            score += 20
            highlights.append("Strong title alignment with software engineering targets.")

        if any(token in title for token in ["graduate", "junior", "early career", "new grad"]):
            score += 10
            highlights.append("Early-career positioning matches your current market profile.")

        if any(token in title for token in ["internship", "apprenticeship", "spontanea", "spontaneous"]):
            score -= 20

        if job.source_site.lower() == "linkedin":
            score += 5

        description = (job.job_description_formatted or job.job_summary or "").lower()
        if description and description not in {"none", "nan"}:
            score += 5
            if any(token in description for token in ["python", "django", "fastapi", "docker", "kubernetes", "react", "ci/cd", "postgres"]):
                score += 10
                highlights.append("Description mentions stack areas already present in your CV.")

        recommendation = "good_match"
        decision = "keep"
        if score >= 85:
            recommendation = "strong_match"
        elif score >= self.settings.minimum_fit_score:
            recommendation = "good_match"
        elif score >= 55:
            recommendation = "stretch"
            decision = "review"
        else:
            recommendation = "skip"
            decision = "skip"
        ai_fit = decision == "keep" and score >= self.settings.minimum_fit_score

        reasoning = (
            "Heuristic fallback used because OpenAI was not configured or failed. "
            "The score was based on title alignment, seniority cues, and any matching technical keywords."
        )
        return FitAssessment(
            ai_fit=ai_fit,
            fit_score=min(score, 100),
            decision=decision,
            recommendation=recommendation,
            reasoning=reasoning,
            missing_skills=[],
            resume_focus=["Highlight directly relevant engineering experience."],
            candidate_highlights=highlights,
        )

    def _heuristic_tailor(self, match: MatchResult) -> TailoredArtifacts:
        bullet_block = "\n".join(
            f"- {item}"
            for item in (
                match.assessment.resume_focus
                or ["Align experience to the job requirements."]
            )
        )
        resume_markdown = (
            f"Ermias Mulugeta Teklehaimanot\n"
            f"Target role: {match.job.job_title} at {match.job.company_name}\n\n"
            f"Professional summary:\n"
            f"Software Engineer with 4+ years of experience across backend, full-stack, DevOps, and QA-oriented roles. "
            f"Strong in Python, Django, FastAPI, React, Docker, Kubernetes, CI/CD, and scalable systems.\n\n"
            f"Focus areas for this application:\n{bullet_block}\n"
        )
        cover_letter_markdown = (
            f"Dear Hiring Team,\n\n"
            f"I am applying for the {match.job.job_title} role at {match.job.company_name}. "
            f"My background aligns with the technical focus of the role, and I would "
            f"value the opportunity to contribute.\n\n"
            f"Best regards,\nErmias Mulugeta Teklehaimanot\n"
        )
        email_intro = (
            f"Hello, I am interested in the {match.job.job_title} opportunity at "
            f"{match.job.company_name}. I have attached tailored application materials."
        )
        return TailoredArtifacts(
            resume_markdown=resume_markdown,
            cover_letter_markdown=cover_letter_markdown,
            email_intro=email_intro,
            resume_summary="Heuristic tailored resume draft generated from title alignment.",
            resume_doc_title=self._resume_doc_title(match),
        )

    @staticmethod
    def _resume_doc_title(match: MatchResult) -> str:
        location = match.job.job_location or "Location unavailable"
        return f"Resume - {match.job.job_title} @ {match.job.company_name} ({location})"

    def _client_or_raise(self):
        if self._client is not None:
            return self._client

        from openai import OpenAI

        self._client = OpenAI(api_key=self.settings.openai_api_key)
        return self._client

    def _call_json(self, schema_name: str, schema: dict, system_prompt: str, payload: dict) -> dict:
        client = self._client_or_raise()

        for attempt in range(3):
            try:
                if hasattr(client, "responses"):
                    response = client.responses.create(
                        model=self.settings.openai_model,
                        input=[
                            {
                                "role": "system",
                                "content": [{"type": "input_text", "text": system_prompt}],
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "input_text",
                                        "text": json.dumps(payload, ensure_ascii=True),
                                    }
                                ],
                            },
                        ],
                        text={
                            "format": {
                                "type": "json_schema",
                                "name": schema_name,
                                "schema": schema,
                                "strict": True,
                            }
                        },
                    )
                    return json.loads(response.output_text)

                completion = client.chat.completions.create(
                    model=self.settings.openai_model,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {
                            "role": "user",
                            "content": json.dumps(payload, ensure_ascii=True),
                        },
                    ],
                )
                return json.loads(completion.choices[0].message.content or "{}")
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))

        return {}
