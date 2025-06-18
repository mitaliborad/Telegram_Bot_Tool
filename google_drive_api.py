# google_drive_api.py – OAuth-user version (free Gmail)
# --------------------------------------------------------------
# Auth flow now uses an OAuth2 refresh-token tied to your personal
# Gmail account instead of a service account, so uploads count
# against your main Drive quota.  Service-account imports have
# been removed and _get_drive_service() now builds a Credentials
# object from CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN.

import os
import io
import logging
import mimetypes
from typing import Optional, Tuple, Generator, Dict, Any, Union

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import (
    MediaFileUpload,
    MediaIoBaseUpload,
    MediaIoBaseDownload,
)
from google_auth_httplib2 import AuthorizedHttp
import httplib2

# ---------------------------------------------------------------------------
# Environment & constants
# ---------------------------------------------------------------------------
load_dotenv()

GDRIVE_CLIENT_ID = os.getenv("GDRIVE_CLIENT_ID")
GDRIVE_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET")
GDRIVE_REFRESH_TOKEN = os.getenv("GDRIVE_REFRESH_TOKEN")

DRIVE_TEMP_FOLDER_ID = os.getenv("GDRIVE_TEMP_FOLDER_ID")  # optional target folder

SCOPES = ["https://www.googleapis.com/auth/drive"]
ONE_MB = 8 * 1024 * 1024

try:
    from extensions import upload_progress_data  # noqa: F401  (optional)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Public helpers – upload / download / delete
# ---------------------------------------------------------------------------

def upload_to_gdrive_with_progress(
    source: str | io.BytesIO,
    filename_in_gdrive: str,
    operation_id_for_log: str,
) -> Generator[Dict[str, Any], None, Tuple[Optional[str], Optional[str]]]:
    """Upload *source* into your Drive with progress events."""

    log_prefix = f"[GDriveUpload-{operation_id_for_log}-{filename_in_gdrive[:20]}]"

    try:
        service = _get_drive_service()
        file_metadata = {"name": filename_in_gdrive}
        if DRIVE_TEMP_FOLDER_ID:
            file_metadata["parents"] = [DRIVE_TEMP_FOLDER_ID]

        # Choose correct media wrapper
        if isinstance(source, str) and os.path.exists(source):
            file_size = os.path.getsize(source)
            media_body = MediaFileUpload(
                source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0]
                or "application/octet-stream",
                resumable=True,
                chunksize=ONE_MB,
            )
        elif isinstance(source, io.BytesIO):
            source.seek(0)
            file_size = len(source.getvalue())
            source.seek(0)
            media_body = MediaIoBaseUpload(
                fd=source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0]
                or "application/octet-stream",
                resumable=True,
                chunksize=ONE_MB,
            )
        else:
            err = "Invalid source type for GDrive upload."
            logging.error(f"{log_prefix} {err}")
            yield {"type": "error", "message": err}
            return None, err

        # Edge-case: zero-byte placeholder
        if file_size == 0:
            empty = service.files().create(body=file_metadata, fields="id").execute()
            gid = empty.get("id")
            if gid:
                yield {"type": "progress", "percentage": 100, "bytes_sent": 0}
                return gid, None
            err = "Failed to create empty file placeholder."
            yield {"type": "error", "message": err}
            return None, err

        request = service.files().create(
            body=file_metadata,
            media_body=media_body,
            fields="id,name",
        )

        response = None
        last_pct = -1
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if pct > last_pct:
                    logging.info(f"{log_prefix} {pct}%")
                    yield {
                        "type": "progress",
                        "percentage": pct,
                        "bytes_sent": int(status.resumable_progress),
                    }
                    last_pct = pct

        gid = response.get("id") if response else None
        if gid:
            logging.info(f"{log_prefix} Upload successful. ID: {gid}")
            return gid, None

        err = "Upload finished but no Drive ID returned."
        logging.error(f"{log_prefix} {err}")
        yield {"type": "error", "message": err}
        return None, err

    except HttpError as e:
        reason = getattr(e, "_get_reason", lambda: str(e))()
        err = f"Google Drive API HTTP error: {reason}"
        logging.error(f"{log_prefix} {err}")
        yield {"type": "error", "message": err}
        return None, err
    except Exception as e:
        err = f"Unexpected upload error: {str(e)}"
        logging.error(f"{log_prefix} {err}", exc_info=True)
        yield {"type": "error", "message": err}
        return None, err


def download_from_gdrive(gid: str) -> Tuple[Optional[io.BytesIO], Optional[str]]:
    """Download a file by ID from Drive."""
    try:
        service = _get_drive_service()
        request = service.files().get_media(fileId=gid)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"Download {gid}: {int(status.progress() * 100)}%")
        buf.seek(0)
        return buf, None
    except HttpError as e:
        if e.resp.status == 404:
            return None, f"File ID {gid} not found."
        return None, f"Drive HTTP error {e.resp.status}: {e._get_reason()}"
    except Exception as e:
        return None, f"Unexpected error downloading {gid}: {str(e)}"


def delete_from_gdrive(gid: str) -> Tuple[bool, Optional[str]]:
    """Delete file permanently."""
    try:
        service = _get_drive_service()
        service.files().delete(fileId=gid).execute()
        return True, None
    except HttpError as e:
        if e.resp.status == 404:
            return True, None  # already gone
        return False, f"Drive HTTP error {e.resp.status}: {e._get_reason()}"
    except Exception as e:
        return False, f"Unexpected delete error: {str(e)}"

# ---------------------------------------------------------------------------
# Private – OAuth credentials helper
# ---------------------------------------------------------------------------

def _get_drive_service():
    """Return a Drive service authenticated with user-refresh token."""
    if not (GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET and GDRIVE_REFRESH_TOKEN):
        raise ValueError("OAuth env vars missing (CLIENT_ID/SECRET/REFRESH_TOKEN).")

    creds = Credentials(
        token=None,
        refresh_token=GDRIVE_REFRESH_TOKEN,
        client_id=GDRIVE_CLIENT_ID,
        client_secret=GDRIVE_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )

    http = httplib2.Http(timeout=600)
    http.follow_redirects = False
    authed_http = AuthorizedHttp(creds, http=http)
    service = build("drive", "v3", http=authed_http, cache_discovery=False)
    return service


logging.info("Google Drive API module loaded – OAuth user mode.")
