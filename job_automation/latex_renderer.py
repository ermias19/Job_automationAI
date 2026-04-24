from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from job_automation.config import Settings

logger = logging.getLogger(__name__)

_LATEX_SPECIAL_CHARS = {
    "\\": r"\textbackslash{}",
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}


class LatexResumeRenderer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.compilers = self._detect_compilers()
        self.disabled_compilers: set[str] = set()
        self.template_header = self._extract_template_header(settings.resume_pdf_path)

    def render_resume_pdf(self, artifact_dir: Path, doc_title: str, resume_text: str) -> Path | None:
        if not resume_text.strip():
            return None

        tex_path = artifact_dir / "resume.tex"
        pdf_path = artifact_dir / "resume.pdf"
        tex_source = self._build_resume_tex(doc_title=doc_title, resume_text=resume_text)
        tex_path.write_text(tex_source, encoding="utf-8")

        if not self.compilers:
            logger.info("No LaTeX compiler found. Created %s without PDF compilation.", tex_path)
            return None

        if self._compile_tex(tex_path=tex_path, output_dir=artifact_dir):
            if pdf_path.exists():
                return pdf_path
            logger.warning(
                "LaTeX compiler finished but PDF not found at %s",
                pdf_path,
            )
        return None

    @staticmethod
    def _detect_compilers() -> list[str]:
        # Keep rendering deterministic and avoid sandbox/permission issues from alternate engines.
        return ["pdflatex"] if shutil.which("pdflatex") else []

    def _compile_tex(self, tex_path: Path, output_dir: Path) -> bool:
        active_compilers = [item for item in self.compilers if item not in self.disabled_compilers]
        for compiler in active_compilers:
            if compiler == "tectonic":
                command = [
                    "tectonic",
                    tex_path.name,
                    "--outdir",
                    str(output_dir),
                    "--keep-logs",
                ]
            else:
                command = [
                    "pdflatex",
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    "-output-directory",
                    str(output_dir),
                    tex_path.name,
                ]

            try:
                subprocess.run(
                    command,
                    cwd=output_dir,
                    check=True,
                    capture_output=True,
                )
                return True
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or b"").decode("utf-8", errors="replace").strip()
                stdout = (exc.stdout or b"").decode("utf-8", errors="replace").strip()
                logger.warning(
                    "Could not compile %s with %s: %s",
                    tex_path,
                    compiler,
                    stderr or stdout or str(exc),
                )
                reason = f"{stderr} {stdout} {exc}".lower()
                if "operation not permitted" in reason:
                    self.disabled_compilers.add(compiler)
                    logger.warning(
                        "Disabling %s for the rest of this run due to permission errors.",
                        compiler,
                    )
            except OSError as exc:
                logger.warning(
                    "Could not run %s for %s: %s",
                    compiler,
                    tex_path,
                    exc,
                )
                self.disabled_compilers.add(compiler)
        return False

    def _build_resume_tex(self, doc_title: str, resume_text: str) -> str:
        title = self._escape(doc_title or "Tailored Resume")
        header_block = ""
        if self.template_header:
            header_line = self._escape(self.template_header)
            header_block = f"\\textit{{Template base: {header_line}}}\\\\[0.6em]\n"

        body = self._render_text_to_latex(resume_text)
        return (
            r"\documentclass[11pt]{article}" "\n"
            r"\usepackage[utf8]{inputenc}" "\n"
            r"\usepackage[T1]{fontenc}" "\n"
            r"\usepackage[a4paper,margin=1in]{geometry}" "\n"
            r"\usepackage[hidelinks]{hyperref}" "\n"
            r"\setlength{\parindent}{0pt}" "\n"
            r"\setlength{\parskip}{0.6em}" "\n"
            r"\begin{document}" "\n"
            rf"\textbf{{\LARGE {title}}}\\[0.8em]" "\n"
            f"{header_block}"
            f"{body}\n"
            r"\end{document}" "\n"
        )

    def _render_text_to_latex(self, text: str) -> str:
        lines = [line.rstrip() for line in text.splitlines()]
        blocks: list[str] = []
        paragraph_buffer: list[str] = []
        list_buffer: list[str] = []

        def flush_paragraph() -> None:
            nonlocal paragraph_buffer
            if not paragraph_buffer:
                return
            escaped = [self._escape(item) for item in paragraph_buffer if item.strip()]
            if escaped:
                blocks.append(r"\\ ".join(escaped))
            paragraph_buffer = []

        def flush_list() -> None:
            nonlocal list_buffer
            if not list_buffer:
                return
            items = "\n".join(f"\\item {self._escape(item)}" for item in list_buffer if item.strip())
            if items:
                blocks.append("\\begin{itemize}\n" + items + "\n\\end{itemize}")
            list_buffer = []

        for raw_line in lines:
            stripped = raw_line.strip()
            if not stripped:
                flush_paragraph()
                flush_list()
                continue

            if stripped.startswith(("- ", "* ", "• ")):
                flush_paragraph()
                list_buffer.append(stripped[2:].strip())
                continue

            flush_list()
            paragraph_buffer.append(stripped)

        flush_paragraph()
        flush_list()
        return "\n\n".join(blocks) if blocks else self._escape(text.strip())

    @staticmethod
    def _escape(text: str) -> str:
        return "".join(_LATEX_SPECIAL_CHARS.get(char, char) for char in text)

    @staticmethod
    def _extract_template_header(path: Path | None) -> str:
        if path is None or not path.exists():
            return ""

        try:
            from PyPDF2 import PdfReader
        except ImportError:
            return ""

        try:
            reader = PdfReader(str(path))
            if not reader.pages:
                return ""
            first_page = (reader.pages[0].extract_text() or "").strip()
            lines = [line.strip() for line in first_page.splitlines() if line.strip()]
            return " | ".join(lines[:3])
        except Exception:
            return ""
