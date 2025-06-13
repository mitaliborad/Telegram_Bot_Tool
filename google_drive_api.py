# google_drive_api.py
import os
import io
import logging
from typing import Optional, Tuple
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from typing import Dict, Any, Tuple, Optional, List, Generator
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import mimetypes

try:
    from extensions import upload_progress_data
except ImportError:
    logging.warning("Could not import upload_progress_data from extensions in google_drive_api.py. "
                    "GDrive ID might not be stored correctly in shared state.")
    upload_progress_data = {}

load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv('GDRIVE_SERVICE_ACCOUNT_FILE')
DRIVE_TEMP_FOLDER_ID = os.getenv('GDRIVE_TEMP_FOLDER_ID')
SCOPES = ['https://www.googleapis.com/auth/drive']

# Global variable to cache the Drive service object
_drive_service = None

def upload_to_gdrive_with_progress(
    source: str | io.BytesIO,
    filename_in_gdrive: str,
    operation_id_for_log: str
) -> Generator[Dict[str, Any], None, Tuple[Optional[str], Optional[str]]]:
    """
    Uploads a file (from path or stream) to Google Drive, yielding progress.

    Args:
        source: File path (str) or BytesIO stream.
        filename_in_gdrive: Name for the file in Google Drive.
        operation_id_for_log: ID for logging.

    Yields:
        Progress dictionaries: {'type': 'progress', 'percentage': int}

    Returns (via StopIteration):
        A tuple (gdrive_file_id, error_message).
    """
    log_prefix = f"[GDriveUpload-{operation_id_for_log}-{filename_in_gdrive[:20]}]"
    service = None
    try:
        service = _get_drive_service()
        if not service:
            logging.error(f"{log_prefix} Google Drive service not available for upload.")
            # To make it a generator that signals error, we yield an error dict and then return
            yield {"type": "error", "message": "Google Drive service not available."}
            return None, "Google Drive service not available."

        file_metadata = {
            'name': filename_in_gdrive,
            'parents': [DRIVE_TEMP_FOLDER_ID]
        }
        
        media_body = None
        file_size = 0

        if isinstance(source, str) and os.path.exists(source): # It's a file path
            file_size = os.path.getsize(source)
            media_body = MediaFileUpload(
                source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                resumable=True,
                chunksize=1024 * 1024 * 1 # 1MB chunk for more frequent progress for smaller files
            )
            logging.info(f"{log_prefix} Prepared MediaFileUpload from path: {source}, size: {file_size}")
        elif isinstance(source, io.BytesIO):
            source.seek(0)
            file_size = len(source.getvalue())
            source.seek(0)
            media_body = MediaIoBaseUpload(
                fd=source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                chunksize=1024 * 1024 * 1,
                resumable=True
            )
            logging.info(f"{log_prefix} Prepared MediaIoBaseUpload from stream, size: {file_size}")
        else:
            err_msg = "Invalid source type for GDrive upload. Must be file path or BytesIO."
            logging.error(f"{log_prefix} {err_msg}")
            yield {"type": "error", "message": err_msg}
            return None, err_msg
        
        if file_size == 0:
            logging.warning(f"{log_prefix} Source file/stream is empty. Skipping actual GDrive upload call, but will create an empty file placeholder if needed by logic.")
            placeholder_metadata = {'name': filename_in_gdrive, 'parents': [DRIVE_TEMP_FOLDER_ID]}
            # Create an empty file using just metadata
            empty_file = service.files().create(body=placeholder_metadata, fields='id,name').execute()
            gdrive_id = empty_file.get('id')
            if gdrive_id:
                logging.info(f"{log_prefix} Empty source. Created empty file placeholder in GDrive. ID: {gdrive_id}")
                yield {"type": "progress", "percentage": 100}
                return gdrive_id, None
            else:
                return None, "Failed to create empty file placeholder on GDrive."


        logging.info(f"{log_prefix} Initiating resumable upload to Google Drive...")
        
        gdrive_request = service.files().create(
            body=file_metadata,
            media_body=media_body,
            fields='id, name' # Only need id and name for confirmation
        )
        gdrive_request = service.files().create(body=file_metadata, media_body=media_body, fields='id, name')
        
        response = None
        while response is None:
            status, response = gdrive_request.next_chunk()
            if status:
                progress_percentage = int(status.progress() * 100)
                logging.info(f"{log_prefix} GDrive Upload Progress: {progress_percentage}%")
                yield {"type": "progress", "percentage": progress_percentage}
        
        # When loop finishes, response contains the completed file resource
        uploaded_file_info = response

        if uploaded_file_info and uploaded_file_info.get('id'):
            gdrive_file_id = uploaded_file_info.get('id')
            name = uploaded_file_info.get('name')
            logging.info(f"{log_prefix} File '{name}' uploaded successfully to GDrive. ID: {gdrive_file_id}")
            # Ensure 100% is yielded if not already
            yield {"type": "progress", "percentage": 100} 
            return gdrive_file_id, None # Return via StopIteration implicitly
        else:
            err_msg = "GDrive upload finished but no file ID returned in final response."
            logging.error(f"{log_prefix} {err_msg} Response: {uploaded_file_info}")
            yield {"type": "error", "message": err_msg}
            return None, err_msg

    except HttpError as error:
        reason = getattr(error, '_get_reason', lambda: str(error))()
        status_code = error.resp.status if hasattr(error, 'resp') else 'N/A'
        logging.error(f"{log_prefix} GDrive API HTTP error: {status_code} - {reason}", exc_info=True)
        yield {"type": "error", "message": f"Google Drive API HTTP error: {status_code} - {reason}"}
        return None, f"Google Drive API HTTP error: {status_code} - {reason}"
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error during GDrive upload: {e}", exc_info=True)
        yield {"type": "error", "message": f"Unexpected error uploading to GDrive: {str(e)}"}
        return None, f"Unexpected error uploading to Google Drive: {str(e)}"
    finally:
        if isinstance(source, io.BytesIO): # Close stream only if we created it here conceptually
            # source.close() # Caller of this generator should handle closing the source stream if it was passed in
            pass

def upload_to_gdrive_with_progress(
    source: str | io.BytesIO,
    filename_in_gdrive: str,
    operation_id_for_log: str # This is the key for upload_progress_data
) -> Generator[Dict[str, Any], None, Tuple[Optional[str], Optional[str]]]:
    """
    Uploads a file (from path or stream) to Google Drive, yielding progress.
    Crucially, it updates upload_progress_data with the gdrive_file_id on success.

    Args:
        source: File path (str) or BytesIO stream.
        filename_in_gdrive: Name for the file in Google Drive.
        operation_id_for_log: ID for logging and for accessing upload_progress_data.

    Yields:
        Progress dictionaries: {'type': 'progress', 'percentage': int}
        Error dictionaries: {'type': 'error', 'message': str}

    Returns (via StopIteration value, implicitly, Python 3.3+):
        A tuple (gdrive_file_id, error_message).
    """
    log_prefix = f"[GDriveUpload-{operation_id_for_log}-{filename_in_gdrive[:20]}]"
    service = None
    gdrive_file_id_final = None # To store the ID before returning
    error_message_final = None  # To store error before returning

    try:
        service = _get_drive_service()
        if not service:
            error_message_final = "Google Drive service not available."
            logging.error(f"{log_prefix} {error_message_final}")
            yield {"type": "error", "message": error_message_final}
            return None, error_message_final # Python 3.3+ implicit return value for generator

        file_metadata = {
            'name': filename_in_gdrive,
            'parents': [DRIVE_TEMP_FOLDER_ID]
        }

        media_body = None
        file_size = 0

        if isinstance(source, str) and os.path.exists(source):
            file_size = os.path.getsize(source)
            media_body = MediaFileUpload(
                source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                resumable=True,
                chunksize=1024 * 1024 * 1
            )
            logging.info(f"{log_prefix} Prepared MediaFileUpload from path: {source}, size: {file_size}")
        elif isinstance(source, io.BytesIO):
            source.seek(0)
            file_size = len(source.getvalue())
            source.seek(0)
            media_body = MediaIoBaseUpload(
                fd=source,
                mimetype=mimetypes.guess_type(filename_in_gdrive)[0] or 'application/octet-stream',
                chunksize=1024 * 1024 * 1,
                resumable=True
            )
            logging.info(f"{log_prefix} Prepared MediaIoBaseUpload from stream, size: {file_size}")
        else:
            error_message_final = "Invalid source type for GDrive upload. Must be file path or BytesIO."
            logging.error(f"{log_prefix} {error_message_final}")
            yield {"type": "error", "message": error_message_final}
            return None, error_message_final

        if file_size == 0:
            logging.warning(f"{log_prefix} Source file/stream is empty. Creating empty file placeholder.")
            placeholder_metadata = {'name': filename_in_gdrive, 'parents': [DRIVE_TEMP_FOLDER_ID]}
            empty_file = service.files().create(body=placeholder_metadata, fields='id,name').execute()
            gdrive_file_id_final = empty_file.get('id')
            if gdrive_file_id_final:
                logging.info(f"{log_prefix} Empty source. Created empty file placeholder in GDrive. ID: {gdrive_file_id_final}")
                yield {"type": "progress", "percentage": 100}
                # Store the ID in upload_progress_data
                # Check if upload_progress_data was imported correctly
                if 'upload_progress_data' in globals() and isinstance(upload_progress_data, dict):
                    if operation_id_for_log in upload_progress_data:
                        upload_progress_data[operation_id_for_log]["gdrive_file_id_temp_result"] = gdrive_file_id_final
                        logging.info(f"{log_prefix} Stored gdrive_file_id_temp_result: {gdrive_file_id_final} for op: {operation_id_for_log}")
                    else:
                        logging.error(f"{log_prefix} upload_progress_data key '{operation_id_for_log}' not found for storing GDrive ID (empty file).")
                else:
                    logging.error(f"{log_prefix} upload_progress_data not available/usable in google_drive_api.py (empty file).")

                return gdrive_file_id_final, None
            else:
                error_message_final = "Failed to create empty file placeholder on GDrive."
                logging.error(f"{log_prefix} {error_message_final}")
                yield {"type": "error", "message": error_message_final}
                return None, error_message_final

        logging.info(f"{log_prefix} Initiating resumable upload to Google Drive...")
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
                if progress_percentage > last_reported_percentage:
                    logging.info(f"{log_prefix} GDrive Upload Progress: {progress_percentage}%")
                    yield {"type": "progress", "percentage": progress_percentage}
                    last_reported_percentage = progress_percentage

        uploaded_file_info = response
        if uploaded_file_info and uploaded_file_info.get('id'):
            gdrive_file_id_final = uploaded_file_info.get('id')
            name = uploaded_file_info.get('name')
            logging.info(f"{log_prefix} File '{name}' uploaded successfully to GDrive. ID: {gdrive_file_id_final}")
            
            # --- CRITICAL FIX: Store the gdrive_file_id in upload_progress_data ---
            if 'upload_progress_data' in globals() and isinstance(upload_progress_data, dict):
                if operation_id_for_log in upload_progress_data:
                    upload_progress_data[operation_id_for_log]["gdrive_file_id_temp_result"] = gdrive_file_id_final
                    logging.info(f"{log_prefix} Stored gdrive_file_id_temp_result: {gdrive_file_id_final} for op: {operation_id_for_log}")
                else:
                    # This case should ideally not happen if initiate_upload sets up the entry correctly
                    logging.error(f"{log_prefix} upload_progress_data key '{operation_id_for_log}' not found. Cannot store GDrive ID.")
                    # Yield an error because the next stage will fail
                    error_message_final = "Internal state error: upload session data missing for GDrive ID storage."
                    yield {"type": "error", "message": error_message_final}
                    return None, error_message_final # This indicates a problem in the handoff
            else:
                logging.critical(f"{log_prefix} upload_progress_data is not available or not a dict in google_drive_api.py. GDrive ID cannot be stored.")
                error_message_final = "Server configuration error: shared upload state unavailable."
                yield {"type": "error", "message": error_message_final}
                return None, error_message_final


            yield {"type": "progress", "percentage": 100}
            return gdrive_file_id_final, None
        else:
            error_message_final = "GDrive upload finished but no file ID returned in final response."
            logging.error(f"{log_prefix} {error_message_final} Response: {uploaded_file_info}")
            yield {"type": "error", "message": error_message_final}
            return None, error_message_final

    except HttpError as error:
        reason = getattr(error, '_get_reason', lambda: str(error))()
        status_code = error.resp.status if hasattr(error, 'resp') else 'N/A'
        error_message_final = f"Google Drive API HTTP error: {status_code} - {reason}"
        logging.error(f"{log_prefix} GDrive API HTTP error: {status_code} - {reason}", exc_info=True)
        yield {"type": "error", "message": error_message_final}
        return None, error_message_final
    except Exception as e:
        error_message_final = f"Unexpected error uploading to Google Drive: {str(e)}"
        logging.error(f"{log_prefix} Unexpected error during GDrive upload: {e}", exc_info=True)
        yield {"type": "error", "message": error_message_final}
        return None, error_message_final

def download_from_gdrive(gdrive_file_id: str) -> Tuple[Optional[io.BytesIO], Optional[str]]:
    """
    Downloads a file from Google Drive by its ID.

    Args:
        gdrive_file_id: The ID of the file in Google Drive.

    Returns:
        A tuple (file_content_stream, error_message).
        file_content_stream is a BytesIO object or None if download fails.
        error_message is None if download succeeds.
    """
    try:
        service = _get_drive_service()
        if not service:
            return None, "Google Drive service not available."

        logging.info(f"Requesting download for Google Drive file ID: {gdrive_file_id}")
        request = service.files().get_media(fileId=gdrive_file_id)
        
        file_content_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content_stream, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"GDrive Download Progress for '{gdrive_file_id}': {int(status.progress() * 100)}%")
        
        file_content_stream.seek(0) # Reset stream position to the beginning for reading
        logging.info(f"File '{gdrive_file_id}' downloaded successfully from Google Drive. Size: {len(file_content_stream.getvalue())} bytes.")
        return file_content_stream, None

    except HttpError as error:
        err_reason = error._get_reason() if hasattr(error, '_get_reason') else str(error)
        logging.error(f"An HTTP error occurred downloading GDrive file '{gdrive_file_id}': {error.resp.status} - {err_reason}", exc_info=True)
        # Check for specific 404 error
        if error.resp.status == 404:
            return None, f"File not found on Google Drive (ID: {gdrive_file_id}). It may have been deleted or the ID is incorrect."
        return None, f"Google Drive API HTTP error: {error.resp.status} - {err_reason}"
    except Exception as e:
        logging.error(f"An unexpected error occurred downloading GDrive file '{gdrive_file_id}': {e}", exc_info=True)
        return None, f"Unexpected error downloading from Google Drive: {str(e)}"


def delete_from_gdrive(gdrive_file_id: str) -> Tuple[bool, Optional[str]]:
    """
    Deletes a file from Google Drive by its ID.

    Args:
        gdrive_file_id: The ID of the file in Google Drive.

    Returns:
        A tuple (success_status, error_message).
        success_status is True if deletion was successful or file didn't exist.
        error_message is None if successful or file didn't exist, otherwise contains the error.
    """
    try:
        service = _get_drive_service()
        if not service:
            return False, "Google Drive service not available."

        logging.info(f"Requesting deletion of Google Drive file ID: {gdrive_file_id}")
        service.files().delete(fileId=gdrive_file_id).execute()
        logging.info(f"File '{gdrive_file_id}' deleted successfully from Google Drive.")
        return True, None

    except HttpError as error:
        err_reason = error._get_reason() if hasattr(error, '_get_reason') else str(error)
        if error.resp.status == 404:
            logging.warning(f"File '{gdrive_file_id}' not found on Google Drive for deletion (already deleted?).")
            return True, None # Treat as success if file is already gone
        logging.error(f"An HTTP error occurred deleting GDrive file '{gdrive_file_id}': {error.resp.status} - {err_reason}", exc_info=True)
        return False, f"Google Drive API HTTP error: {error.resp.status} - {err_reason}"
    except Exception as e:
        logging.error(f"An unexpected error occurred deleting GDrive file '{gdrive_file_id}': {e}", exc_info=True)
        return False, f"Unexpected error deleting from Google Drive: {str(e)}"

logging.info("Google Drive API module (google_drive_api.py) loaded.")





def _get_drive_service():
    """
    Authenticates and returns a new Google Drive API service object for each call.
    This version is thread-safe.
    """
    # global _drive_service  <-- REMOVE OR COMMENT OUT THIS LINE
    # if _drive_service:     <-- REMOVE OR COMMENT OUT THIS LINE
    #     return _drive_service <-- REMOVE OR COMMENT OUT THIS LINE

    if not SERVICE_ACCOUNT_FILE:
        logging.error("Google Drive API: SERVICE_ACCOUNT_FILE path is not set in environment variables.")
        raise ValueError("Service account file path not configured.")
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logging.error(f"Google Drive API: Service account file not found at '{SERVICE_ACCOUNT_FILE}'.")
        raise FileNotFoundError(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")
    if not DRIVE_TEMP_FOLDER_ID:
        logging.error("Google Drive API: DRIVE_TEMP_FOLDER_ID is not set in environment variables.")
        raise ValueError("Google Drive temporary folder ID not configured.")

    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        http_client_with_timeout = httplib2.Http(timeout=600)
        http_client_with_timeout.follow_redirects = False

        authed_http = AuthorizedHttp(creds, http=http_client_with_timeout)
        service = build('drive', 'v3', http=authed_http, cache_discovery=False)
        
        # We no longer cache the service object in _drive_service
        logging.info("Google Drive API service initialized successfully (Thread-Safe Mode).")
        return service
    except Exception as e:
        logging.error(f"Failed to initialize Google Drive service: {e}", exc_info=True)
        raise ConnectionError(f"Could not connect to Google Drive API: {e}")