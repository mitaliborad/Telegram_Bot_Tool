# google_drive_api.py
import os
import io
import logging
from typing import Optional, Tuple
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Path to your service account key file (JSON)
# Ensure this path is correct relative to where your script/app runs, or use an absolute path.
# For Flask apps, relative to the project root is usually fine if .env is loaded correctly.
SERVICE_ACCOUNT_FILE = os.getenv('GDRIVE_SERVICE_ACCOUNT_FILE')

# The ID of the Google Drive folder where temporary files will be stored.
# You get this from the URL of the folder in Google Drive.
DRIVE_TEMP_FOLDER_ID = os.getenv('GDRIVE_TEMP_FOLDER_ID')

# Define the scopes required by the Drive API.
# For file operations (upload, download, delete), 'https://www.googleapis.com/auth/drive' is comprehensive.
# You could use more granular scopes if needed (e.g., drive.file for per-file access created by the app).
SCOPES = ['https://www.googleapis.com/auth/drive']

# Global variable to cache the Drive service object
_drive_service = None

def _get_drive_service():
    """
    Authenticates and returns a Google Drive API service object.
    Caches the service object for efficiency.
    """
    global _drive_service
    if _drive_service:
        return _drive_service

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
        
        # Build the Drive API service object
        # cache_discovery=False can be useful in environments where caching causes issues (e.g. serverless)
        # but generally not needed for typical server apps.
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        _drive_service = service
        logging.info("Google Drive API service initialized successfully.")
        return service
    except Exception as e:
        logging.error(f"Failed to initialize Google Drive service: {e}", exc_info=True)
        raise ConnectionError(f"Could not connect to Google Drive API: {e}")


# def upload_to_gdrive(file_stream: io.BytesIO, filename: str) -> Tuple[Optional[str], Optional[str]]:
#     """
#     Uploads a file stream to the configured Google Drive temporary folder.

#     Args:
#         file_stream: A file-like object (BytesIO) containing the file content.
#         filename: The desired name for the file in Google Drive.

#     Returns:
#         A tuple (gdrive_file_id, error_message).
#         gdrive_file_id is None if upload fails.
#         error_message is None if upload succeeds.
#     """
#     try:
#         service = _get_drive_service()
#         if not service:
#             return None, "Google Drive service not available."

#         file_stream.seek(0) # Ensure stream is at the beginning

#         file_metadata = {
#             'name': filename,
#             'parents': [DRIVE_TEMP_FOLDER_ID]  # Specify the parent folder ID
#         }
        
#         media = MediaFileUpload(
#             filename, # This is a dummy filename for MediaFileUpload, actual content is from file_stream
#             mimetype='application/octet-stream', # Or guess mimetype if important
#             resumable=True,
#             chunksize=1024*1024*5 # 5MB chunk size for resumable uploads (adjust as needed)
#         )
#         # We need to set the stream for MediaFileUpload manually since we are not passing a filepath
#         media._stream = file_stream
#         media._size = len(file_stream.getvalue())


#         logging.info(f"Uploading '{filename}' to Google Drive folder ID '{DRIVE_TEMP_FOLDER_ID}'...")
        
#         request = service.files().create(
#             body=file_metadata,
#             media_body=media,
#             fields='id, name, webViewLink, webContentLink' # Fields to retrieve in the response
#         )
        
#         response = None
#         uploaded_file = None
#         # Loop for resumable upload progress (optional but good for large files)
#         while response is None:
#             status, response = request.next_chunk()
#             if status:
#                 logging.info(f"GDrive Upload Progress for '{filename}': {int(status.progress() * 100)}%")
        
#         uploaded_file = response

#         if uploaded_file and uploaded_file.get('id'):
#             gdrive_file_id = uploaded_file.get('id')
#             name = uploaded_file.get('name')
#             # web_view_link = uploaded_file.get('webViewLink') # Can be used to view in browser
#             # web_content_link = uploaded_file.get('webContentLink') # Can be used for direct download (permissions apply)

#             logging.info(f"File '{name}' uploaded successfully to Google Drive with ID: {gdrive_file_id}")
#             return gdrive_file_id, None
#         else:
#             logging.error(f"Google Drive upload failed for '{filename}'. Response: {uploaded_file}")
#             return None, "Upload to Google Drive failed (no file ID returned)."

#     except HttpError as error:
#         logging.error(f"An HTTP error occurred during Google Drive upload for '{filename}': {error.resp.status} - {error._get_reason()}", exc_info=True)
#         return None, f"Google Drive API HTTP error: {error.resp.status} - {error._get_reason()}"
#     except Exception as e:
#         logging.error(f"An unexpected error occurred during Google Drive upload for '{filename}': {e}", exc_info=True)
#         return None, f"Unexpected error uploading to Google Drive: {str(e)}"

# def upload_to_gdrive(file_stream: io.BytesIO, filename: str) -> Tuple[Optional[str], Optional[str]]:
#     """
#     Uploads a file stream to the configured Google Drive temporary folder.

#     Args:
#         file_stream: A file-like object (BytesIO) containing the file content.
#         filename: The desired name for the file in Google Drive.

#     Returns:
#         A tuple (gdrive_file_id, error_message).
#         gdrive_file_id is None if upload fails.
#         error_message is None if upload succeeds.
#     """
#     try:
#         service = _get_drive_service()
#         if not service:
#             return None, "Google Drive service not available."

#         file_stream.seek(0) # Ensure stream is at the beginning

#         file_metadata = {
#             'name': filename,
#             'parents': [DRIVE_TEMP_FOLDER_ID]
#         }
        
#         # --- CORRECTED MediaFileUpload instantiation for in-memory streams ---
#         # We pass the file_stream directly as the 'fd' (file descriptor) argument
#         # to MediaIoBaseUpload, which MediaFileUpload inherits from.
#         # The 'filename' argument here is just for metadata in the HTTP request,
#         # not for opening a local file.
#         media = MediaFileUpload(
#             filename, # This acts as the filename for the Content-Disposition header
#             mimetype='application/octet-stream', # Or guess mimetype
#             resumable=True,
#             chunksize=1024 * 1024 * 5  # 5MB chunk size
#         )
#         # Manually set the stream and its size
#         media._fd = file_stream
#         media._size = len(file_stream.getvalue())
#         # --- END OF CORRECTION ---

#         logging.info(f"Uploading '{filename}' to Google Drive folder ID '{DRIVE_TEMP_FOLDER_ID}' (size: {media._size} bytes)...")
        
#         gdrive_request = service.files().create( # Renamed variable for clarity
#             body=file_metadata,
#             media_body=media,
#             fields='id, name, webViewLink, webContentLink'
#         )
        
#         response = None
#         # Loop for resumable upload progress
#         while response is None:
#             status, response = gdrive_request.next_chunk() # Use gdrive_request
#             if status:
#                 logging.info(f"GDrive Upload Progress for '{filename}': {int(status.progress() * 100)}%")
        
#         uploaded_file_info = response # response is the file resource on completion

#         if uploaded_file_info and uploaded_file_info.get('id'):
#             gdrive_file_id = uploaded_file_info.get('id')
#             name = uploaded_file_info.get('name')
#             logging.info(f"File '{name}' uploaded successfully to Google Drive with ID: {gdrive_file_id}")
#             return gdrive_file_id, None
#         else:
#             logging.error(f"Google Drive upload failed for '{filename}'. Response: {uploaded_file_info}")
#             return None, "Upload to Google Drive failed (no file ID returned in final response)."

#     except HttpError as error:
#         logging.error(f"An HTTP error occurred during Google Drive upload for '{filename}': {error.resp.status} - {error._get_reason()}", exc_info=True)
#         return None, f"Google Drive API HTTP error: {error.resp.status} - {error._get_reason()}"
#     except Exception as e:
#         # This will catch the FileNotFoundError if MediaFileUpload tries to open the dummy filename
#         logging.error(f"An unexpected error occurred during Google Drive upload for '{filename}': {e}", exc_info=True)
#         return None, f"Unexpected error uploading to Google Drive: {str(e)}"

def upload_to_gdrive(file_stream: io.BytesIO, filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Uploads a file stream to the configured Google Drive temporary folder.

    Args:
        file_stream: A file-like object (BytesIO) containing the file content.
        filename: The desired name for the file in Google Drive.

    Returns:
        A tuple (gdrive_file_id, error_message).
        gdrive_file_id is None if upload fails.
        error_message is None if upload succeeds.
    """
    try:
        service = _get_drive_service()
        if not service:
            return None, "Google Drive service not available."

        file_stream.seek(0) # Ensure stream is at the beginning
        file_size = len(file_stream.getvalue())

        file_metadata = {
            'name': filename,
            'parents': [DRIVE_TEMP_FOLDER_ID]
        }
        
        # --- CORRECTED Way to handle in-memory streams ---
        # Use MediaIoBaseUpload for in-memory streams directly.
        # Provide the stream (fd) and mimetype.
        media_body = MediaIoBaseUpload(
            fd=file_stream,
            mimetype='application/octet-stream', # Or guess mimetype
            chunksize=1024 * 1024 * 5,  # 5MB chunk size, adjust as needed
            resumable=True
        )
        # --- END OF CORRECTION ---

        logging.info(f"Uploading '{filename}' to Google Drive folder ID '{DRIVE_TEMP_FOLDER_ID}' (size: {file_size} bytes)...")
        
        gdrive_request = service.files().create(
            body=file_metadata,
            media_body=media_body, # Pass the MediaIoBaseUpload object here
            fields='id, name, webViewLink, webContentLink'
        )
        
        response = None
        # Loop for resumable upload progress
        while response is None:
            status, response = gdrive_request.next_chunk()
            if status:
                logging.info(f"GDrive Upload Progress for '{filename}': {int(status.progress() * 100)}%")
        
        uploaded_file_info = response

        if uploaded_file_info and uploaded_file_info.get('id'):
            gdrive_file_id = uploaded_file_info.get('id')
            name = uploaded_file_info.get('name')
            logging.info(f"File '{name}' uploaded successfully to Google Drive with ID: {gdrive_file_id}")
            return gdrive_file_id, None
        else:
            logging.error(f"Google Drive upload failed for '{filename}'. Response: {uploaded_file_info}")
            return None, "Upload to Google Drive failed (no file ID returned in final response)."

    except HttpError as error:
        # Ensure we get the reason string correctly
        reason = "Unknown HTTP error reason"
        if hasattr(error, '_get_reason'):
            reason = error._get_reason()
        elif hasattr(error, 'reason'): # some HttpError instances might have 'reason'
            reason = error.reason
        
        logging.error(f"An HTTP error occurred during Google Drive upload for '{filename}': {error.resp.status} - {reason}", exc_info=True)
        return None, f"Google Drive API HTTP error: {error.resp.status} - {reason}"
    except Exception as e:
        logging.error(f"An unexpected error occurred during Google Drive upload for '{filename}': {e}", exc_info=True)
        return None, f"Unexpected error uploading to Google Drive: {str(e)}"

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