import os
import io
import logging
from typing import Optional, Tuple, Generator, Dict, Any
from googleapiclient.http import MediaIoBaseUpload, MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import mimetypes

# This try/except block is kept for safety, but this module should no longer
# be directly manipulating the upload_progress_data dictionary.
try:
    from extensions import upload_progress_data
except ImportError:
    logging.warning("Could not import upload_progress_data from extensions in google_drive_api.py.")
    upload_progress_data = {}

load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv('GDRIVE_SERVICE_ACCOUNT_FILE')
DRIVE_TEMP_FOLDER_ID = os.getenv('GDRIVE_TEMP_FOLDER_ID')
SCOPES = ['https://www.googleapis.com/auth/drive']

ONE_MB = 8 * 1024 * 1024

def upload_to_gdrive_with_progress(
    source: str | io.BytesIO,
    filename_in_gdrive: str,
    operation_id_for_log: str
) -> Generator[Dict[str, Any], None, Tuple[Optional[str], Optional[str]]]:
    """
    Uploads a file to Google Drive, yielding progress events.
    """
    log_prefix = f"[GDriveUpload-{operation_id_for_log}-{filename_in_gdrive[:20]}]"
    
    try:
        service = _get_drive_service()
        if not service:
            error_message = "Google Drive service not available."
            logging.error(f"{log_prefix} {error_message}")
            yield {"type": "error", "message": error_message}
            return None, error_message

        file_metadata = {'name': filename_in_gdrive, 'parents': [DRIVE_TEMP_FOLDER_ID]}
        media_body = None
        file_size = 0

        if isinstance(source, str) and os.path.exists(source):
            file_size = os.path.getsize(source)
            media_body = MediaFileUpload(
                source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                resumable=True,
                chunksize=ONE_MB
            )
        elif isinstance(source, io.BytesIO):
            source.seek(0)
            file_size = len(source.getvalue())
            source.seek(0)
            media_body = MediaIoBaseUpload(
                fd=source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                resumable=True,
                chunksize=ONE_MB
            )
        else:
            error_message = "Invalid source type for GDrive upload."
            logging.error(f"{log_prefix} {error_message}")
            yield {"type": "error", "message": error_message}
            return None, error_message

        if file_size == 0:
            logging.warning(f"{log_prefix} Source is empty. Creating an empty file placeholder in Drive.")
            empty_file = service.files().create(body=file_metadata, fields='id').execute()
            gdrive_file_id = empty_file.get('id')
            if gdrive_file_id:
                yield {"type": "progress", "percentage": 100, "bytes_sent": 0}
                return gdrive_file_id, None
            else:
                error_message = "Failed to create empty file placeholder."
                yield {"type": "error", "message": error_message}
                return None, error_message

        gdrive_request = service.files().create(
            body=file_metadata,
            media_body=media_body,
            fields='id, name'
        )

        response = None
        last_reported_percentage = -1
        while response is None:
            status, response = gdrive_request.next_chunk()
            if status:
                progress_percentage = int(status.progress() * 100)
                bytes_sent = int(status.resumable_progress)
                
                if progress_percentage > last_reported_percentage:
                    logging.info(f"{log_prefix} GDrive Upload Progress: {progress_percentage}%")
                    yield {
                        "type": "progress",
                        "percentage": progress_percentage,
                        "bytes_sent": bytes_sent
                    }
                    last_reported_percentage = progress_percentage

        if response and response.get('id'):
            gdrive_file_id = response.get('id')
            logging.info(f"{log_prefix} Upload successful. GDrive ID: {gdrive_file_id}")
            return gdrive_file_id, None
        else:
            error_message = "GDrive upload finished but no file ID was returned."
            logging.error(f"{log_prefix} {error_message} Response: {response}")
            yield {"type": "error", "message": error_message}
            return None, error_message

    except HttpError as error:
        reason = getattr(error, '_get_reason', lambda: str(error))()
        error_message = f"Google Drive API HTTP error: {reason}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield {"type": "error", "message": error_message}
        return None, error_message
    except Exception as e:
        error_message = f"An unexpected error occurred during GDrive upload: {str(e)}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield {"type": "error", "message": error_message}
        return None, error_message

def download_from_gdrive(gdrive_file_id: str) -> Tuple[Optional[io.BytesIO], Optional[str]]:
    """
    Downloads a file from Google Drive by its ID.
    """
    try:
        service = _get_drive_service()
        if not service:
            return None, "Google Drive service not available."

        request = service.files().get_media(fileId=gdrive_file_id)
        file_content_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content_stream, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"GDrive Download Progress for '{gdrive_file_id}': {int(status.progress() * 100)}%")
        
        file_content_stream.seek(0)
        return file_content_stream, None

    except HttpError as error:
        err_reason = error._get_reason() if hasattr(error, '_get_reason') else str(error)
        if error.resp.status == 404:
            return None, f"File not found on Google Drive (ID: {gdrive_file_id})."
        return None, f"Google Drive API HTTP error: {error.resp.status} - {err_reason}"
    except Exception as e:
        return None, f"Unexpected error downloading from Google Drive: {str(e)}"

def delete_from_gdrive(gdrive_file_id: str) -> Tuple[bool, Optional[str]]:
    """
    Deletes a file from Google Drive by its ID.
    """
    try:
        service = _get_drive_service()
        if not service:
            return False, "Google Drive service not available."

        service.files().delete(fileId=gdrive_file_id).execute()
        return True, None

    except HttpError as error:
        if error.resp.status == 404:
            logging.warning(f"File '{gdrive_file_id}' not found for deletion (already deleted?).")
            return True, None # Treat as success
        err_reason = error._get_reason() if hasattr(error, '_get_reason') else str(error)
        return False, f"Google Drive API HTTP error: {error.resp.status} - {err_reason}"
    except Exception as e:
        return False, f"Unexpected error deleting from Google Drive: {str(e)}"

def _get_drive_service():
    """
    Authenticates and returns a new Google Drive API service object for each call.
    This is thread-safe.
    """
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise ValueError("Service account file not configured or not found.")
    if not DRIVE_TEMP_FOLDER_ID:
        raise ValueError("Google Drive temporary folder ID not configured.")
        
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        # Use a new httplib2.Http object for each service instance
        http_client_with_timeout = httplib2.Http(timeout=600)
        # ====================================================================
        # === THE FIX IS HERE: Disable automatic redirects in httplib2     ===
        # ====================================================================
        http_client_with_timeout.follow_redirects = False
        
        authed_http = AuthorizedHttp(creds, http=http_client_with_timeout)
        service = build('drive', 'v3', http=authed_http, cache_discovery=False)
        
        logging.info("Google Drive API service initialized successfully (Thread-Safe Mode).")
        return service
    except Exception as e:
        logging.error(f"Failed to initialize Google Drive service: {e}", exc_info=True)
        raise ConnectionError(f"Could not connect to Google Drive API: {e}")

logging.info("Google Drive API module (google_drive_api.py) loaded.")