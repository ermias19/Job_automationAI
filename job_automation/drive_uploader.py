from __future__ import annotations

import json
import logging
from pathlib import Path
import shutil

from job_automation.config import Settings
from job_automation.models import MatchResult

logger = logging.getLogger(__name__)


class DriveResumePublisher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def publish_phd_resumes(self, run_id: str, matches: list[MatchResult]) -> dict:
        if not self.settings.google_drive_upload_enabled:
            return {"enabled": False, "uploaded": 0, "failed": 0, "folder_url": ""}
        service_account_available = bool(
            self.settings.google_service_account_json
            and self.settings.google_service_account_json.exists()
        )
        if (
            (not service_account_available)
            and not (
                self.settings.google_drive_oauth_client_secret_json
                and self.settings.google_drive_oauth_client_secret_json.exists()
            )
        ):
            logger.warning(
                "Drive upload enabled but neither service-account nor OAuth credentials are configured."
            )
            return {"enabled": True, "uploaded": 0, "failed": 0, "folder_url": ""}

        using_oauth = False
        try:
            service = self._build_drive_service()
        except Exception:
            logger.exception("Could not initialize Google Drive API client for resume publishing.")
            return {"enabled": True, "uploaded": 0, "failed": len(matches), "folder_url": ""}

        if service_account_available and not self._oauth_configured():
            if not self._service_account_target_is_supported(service):
                return {"enabled": True, "uploaded": 0, "failed": len(matches), "folder_url": ""}

        try:
            root_id = self._ensure_root_folder(service)
            run_folder_id = self._create_folder(service, f"phd-{run_id}", parent_id=root_id)
        except Exception as exc:
            if self._is_storage_quota_error(exc) and self._oauth_configured():
                logger.warning(
                    "Service-account Drive quota issue detected while creating folder; retrying with OAuth user."
                )
                using_oauth = True
                try:
                    service = self._build_drive_service(use_oauth=True)
                    root_id = self._ensure_root_folder(service)
                    run_folder_id = self._create_folder(service, f"phd-{run_id}", parent_id=root_id)
                except Exception:
                    logger.exception("Could not create Drive folder structure via OAuth for run %s", run_id)
                    return {"enabled": True, "uploaded": 0, "failed": len(matches), "folder_url": ""}
            elif self._is_storage_quota_error(exc):
                logger.error(
                    "Drive upload blocked by storageQuotaExceeded. "
                    "Service accounts cannot upload into personal My Drive. "
                    "Configure GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_JSON or use a Shared Drive."
                )
                return {"enabled": True, "uploaded": 0, "failed": len(matches), "folder_url": ""}
            else:
                logger.exception("Could not create Drive folder structure for run %s", run_id)
                return {"enabled": True, "uploaded": 0, "failed": len(matches), "folder_url": ""}

        resume_uploaded = 0
        resume_failed = 0
        email_uploaded = 0
        email_failed = 0
        quota_blocked = False

        try:
            resumes_folder_id = self._create_folder(service, "resumes", parent_id=run_folder_id)
            email_drafts_folder_id = self._create_folder(service, "email-drafts", parent_id=run_folder_id)
        except Exception:
            logger.warning(
                "Could not create Drive subfolders for resumes/email drafts. Using run folder directly."
            )
            resumes_folder_id = run_folder_id
            email_drafts_folder_id = run_folder_id

        def _switch_to_oauth_and_recreate_folders() -> None:
            nonlocal service, using_oauth, run_folder_id, resumes_folder_id, email_drafts_folder_id
            using_oauth = True
            service = self._build_drive_service(use_oauth=True)
            root_id = self._ensure_root_folder(service)
            run_folder_id = self._create_folder(service, f"phd-{run_id}", parent_id=root_id)
            resumes_folder_id = self._create_folder(service, "resumes", parent_id=run_folder_id)
            email_drafts_folder_id = self._create_folder(service, "email-drafts", parent_id=run_folder_id)

        for match in matches:
            if quota_blocked:
                break

            artifact = match.artifacts
            if not artifact:
                continue

            slug = match.job.storage_slug()

            if artifact.resume_path:
                resume_local_path = Path(artifact.resume_path)
                if resume_local_path.exists():
                    try:
                        file_id = self._upload_file(
                            service=service,
                            local_path=resume_local_path,
                            file_name=f"{slug}-resume.txt",
                            parent_id=resumes_folder_id,
                        )
                        if self.settings.google_drive_public_links:
                            self._ensure_public_read(service, file_id)
                        artifact.resume_drive_url = (
                            f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                        )
                        resume_uploaded += 1
                    except Exception as exc:
                        if self._is_storage_quota_error(exc) and not using_oauth and self._oauth_configured():
                            logger.warning(
                                "Service-account Drive quota issue detected while uploading; switching to OAuth user."
                            )
                            try:
                                _switch_to_oauth_and_recreate_folders()
                                file_id = self._upload_file(
                                    service=service,
                                    local_path=resume_local_path,
                                    file_name=f"{slug}-resume.txt",
                                    parent_id=resumes_folder_id,
                                )
                                if self.settings.google_drive_public_links:
                                    self._ensure_public_read(service, file_id)
                                artifact.resume_drive_url = (
                                    f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                                )
                                resume_uploaded += 1
                            except Exception:
                                logger.exception("OAuth fallback resume upload failed for %s", slug)
                                resume_failed += 1
                        elif self._is_storage_quota_error(exc):
                            logger.error(
                                "Drive upload blocked by storageQuotaExceeded for %s. "
                                "Configure GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_JSON or use a Shared Drive. "
                                "Stopping further Drive uploads for this run.",
                                slug,
                            )
                            resume_failed += 1
                            quota_blocked = True
                        else:
                            logger.exception("Failed uploading resume for %s", slug)
                            resume_failed += 1
                else:
                    resume_failed += 1

            if quota_blocked:
                break

            if artifact.email_intro_path:
                email_local_path = Path(artifact.email_intro_path)
                if email_local_path.exists():
                    try:
                        file_id = self._upload_file(
                            service=service,
                            local_path=email_local_path,
                            file_name=f"{slug}-email-draft.txt",
                            parent_id=email_drafts_folder_id,
                        )
                        if self.settings.google_drive_public_links:
                            self._ensure_public_read(service, file_id)
                        artifact.email_intro_drive_url = (
                            f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                        )
                        email_uploaded += 1
                    except Exception as exc:
                        if self._is_storage_quota_error(exc) and not using_oauth and self._oauth_configured():
                            logger.warning(
                                "Service-account Drive quota issue detected while uploading email draft; "
                                "switching to OAuth user."
                            )
                            try:
                                _switch_to_oauth_and_recreate_folders()
                                file_id = self._upload_file(
                                    service=service,
                                    local_path=email_local_path,
                                    file_name=f"{slug}-email-draft.txt",
                                    parent_id=email_drafts_folder_id,
                                )
                                if self.settings.google_drive_public_links:
                                    self._ensure_public_read(service, file_id)
                                artifact.email_intro_drive_url = (
                                    f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                                )
                                email_uploaded += 1
                            except Exception:
                                logger.exception("OAuth fallback email draft upload failed for %s", slug)
                                email_failed += 1
                        elif self._is_storage_quota_error(exc):
                            logger.error(
                                "Drive upload blocked by storageQuotaExceeded for email draft %s. "
                                "Stopping further Drive uploads for this run.",
                                slug,
                            )
                            email_failed += 1
                            quota_blocked = True
                        else:
                            logger.exception("Failed uploading email draft for %s", slug)
                            email_failed += 1
                else:
                    email_failed += 1

        folder_url = f"https://drive.google.com/drive/folders/{run_folder_id}" if run_folder_id else ""
        return {
            "enabled": True,
            "uploaded": resume_uploaded,
            "failed": resume_failed,
            "email_uploaded": email_uploaded,
            "email_failed": email_failed,
            "folder_url": folder_url,
        }

    def _build_drive_service(self, *, use_oauth: bool = False):
        if use_oauth:
            return self._build_drive_service_via_oauth()

        if self.settings.google_service_account_json and self.settings.google_service_account_json.exists():
            return self._build_drive_service_via_service_account()

        if self._oauth_configured():
            return self._build_drive_service_via_oauth()

        raise RuntimeError("No Drive authentication configuration available.")

    def _build_drive_service_via_service_account(self):
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        credentials = Credentials.from_service_account_file(
            str(self.settings.google_service_account_json),
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=credentials, cache_discovery=False)

    def _build_drive_service_via_oauth(self):
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/drive"]
        token_path = self.settings.google_drive_oauth_token_json
        client_secret_path = self._resolve_oauth_client_secret_path()
        credentials = None

        if token_path.exists():
            credentials = Credentials.from_authorized_user_file(str(token_path), scopes=scopes)

        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())

        if not credentials or not credentials.valid:
            if not client_secret_path or not client_secret_path.exists():
                raise RuntimeError(
                    "OAuth client secret JSON not found. "
                    "Download OAuth Desktop client credentials from Google Cloud and place the file at "
                    f"{self.settings.google_drive_oauth_client_secret_json} "
                    "or keep it in ~/Downloads and rerun."
                )

            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path),
                scopes=scopes,
            )
            credentials = flow.run_local_server(port=0)
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(credentials.to_json(), encoding="utf-8")

        return build("drive", "v3", credentials=credentials, cache_discovery=False)

    def _ensure_root_folder(self, service) -> str:
        if self.settings.google_drive_root_folder_id:
            return self.settings.google_drive_root_folder_id

        folder_name = self.settings.google_drive_root_folder_name.strip()
        query = (
            "mimeType='application/vnd.google-apps.folder' "
            f"and name='{self._escape_query(folder_name)}' and trashed=false"
        )
        response = (
            service.files()
            .list(
                q=query,
                fields="files(id,name)",
                pageSize=1,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            return files[0]["id"]

        return self._create_folder(service, folder_name, parent_id=None)

    @staticmethod
    def _create_folder(service, folder_name: str, parent_id: str | None) -> str:
        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            metadata["parents"] = [parent_id]
        created = (
            service.files()
            .create(body=metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        return created["id"]

    @staticmethod
    def _upload_file(service, local_path: Path, file_name: str, parent_id: str) -> str:
        from googleapiclient.http import MediaFileUpload

        media = MediaFileUpload(str(local_path), mimetype="text/plain", resumable=False)
        metadata = {"name": file_name, "parents": [parent_id]}
        created = (
            service.files()
            .create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return created["id"]

    @staticmethod
    def _ensure_public_read(service, file_id: str) -> None:
        try:
            service.permissions().create(
                fileId=file_id,
                body={"role": "reader", "type": "anyone"},
                fields="id",
                supportsAllDrives=True,
            ).execute()
        except Exception:
            logger.warning(
                "Could not make Drive file public: %s. File may require account access.",
                file_id,
            )

    @staticmethod
    def _escape_query(value: str) -> str:
        return value.replace("'", "\\'")

    def _oauth_configured(self) -> bool:
        configured = self.settings.google_drive_oauth_client_secret_json
        if configured and configured.exists():
            return True
        return self._locate_oauth_client_secret_path() is not None

    @staticmethod
    def _is_storage_quota_error(exc: Exception) -> bool:
        text = str(exc).lower()
        if "storagequotaexceeded" in text:
            return True
        if "service accounts do not have storage quota" in text:
            return True

        details = getattr(exc, "error_details", None)
        if isinstance(details, list):
            for item in details:
                try:
                    if str(item.get("reason", "")).lower() == "storagequotaexceeded":
                        return True
                except Exception:
                    continue

        try:
            content = getattr(exc, "content", None)
            if content:
                parsed = json.loads(content.decode("utf-8") if isinstance(content, bytes) else str(content))
                errors = parsed.get("error", {}).get("errors", [])
                for err in errors:
                    if str(err.get("reason", "")).lower() == "storagequotaexceeded":
                        return True
        except Exception:
            pass
        return False

    def _service_account_target_is_supported(self, service) -> bool:
        root_id = (self.settings.google_drive_root_folder_id or "").strip()
        if not root_id:
            # Without explicit root, cannot determine upfront. Let normal flow decide.
            return True

        try:
            metadata = (
                service.files()
                .get(
                    fileId=root_id,
                    fields="id,name,driveId,owners(emailAddress)",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except Exception:
            logger.warning("Could not inspect Drive root folder metadata. Continuing with upload attempt.")
            return True

        drive_id = str(metadata.get("driveId", "")).strip()
        if drive_id:
            return True

        owners = metadata.get("owners") or []
        owner_email = ""
        if owners and isinstance(owners[0], dict):
            owner_email = str(owners[0].get("emailAddress", "")).strip()

        logger.error(
            "GOOGLE_DRIVE_ROOT_FOLDER_ID points to personal My Drive folder '%s' (owner=%s). "
            "Service-account uploads cannot store resume files there. "
            "Use OAuth (set GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_JSON) or switch to a Shared Drive folder.",
            metadata.get("name", root_id),
            owner_email or "unknown",
        )
        return False

    def _resolve_oauth_client_secret_path(self) -> Path | None:
        configured = self.settings.google_drive_oauth_client_secret_json
        if configured and configured.exists():
            return configured

        discovered = self._locate_oauth_client_secret_path()
        if not discovered:
            return None

        if configured:
            try:
                configured.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(discovered, configured)
                logger.info(
                    "Copied OAuth client secret from %s to %s",
                    discovered,
                    configured,
                )
                return configured
            except Exception:
                logger.warning(
                    "Could not copy OAuth client secret to configured path. Using discovered file directly: %s",
                    discovered,
                )
        return discovered

    @staticmethod
    def _locate_oauth_client_secret_path() -> Path | None:
        downloads = Path.home() / "Downloads"
        if not downloads.exists():
            return None

        try:
            candidates = sorted(
                downloads.glob("*.json"),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
        except Exception:
            return None

        for candidate in candidates:
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if "installed" in payload or "web" in payload:
                return candidate
        return None
