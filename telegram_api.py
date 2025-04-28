"""Handles interactions with the Telegram Bot API for file uploads and downloads."""

# --- Imports ---
import requests
import logging
import json
import time
from typing import Tuple, Optional, Dict, Any, Union, IO

# --- Import config variables ---
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_API_TIMEOUTS,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY
)

# --- Type Aliases ---
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] # success, message, response_json

# --- Module Level Requests Session ---
# Use a session object for connection pooling and potential performance gains.
# This helps reuse TCP connections, reducing overhead for multiple API calls.
session = requests.Session()
logging.info("Initialized requests.Session for Telegram API calls.")

# --- Telegram API Interaction ---

def send_file_to_telegram(
    file_object: Union[IO[bytes], bytes],
    filename: str,
    target_chat_id: Union[str, int]
) -> ApiResult:
    """
    Sends a file (from buffer or bytes) to a specific Telegram chat ID.
    Implements retries for transient network errors.

    Args:
        file_object: A file-like object opened in binary mode or raw bytes.
        filename: The desired filename for the uploaded document.
        target_chat_id: The target chat ID (string or integer).

    Returns:
        A tuple: (success: bool, message: str, response_json: Optional[dict])
    """
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
    files_payload = {'document': (filename, file_object)}
    data_payload = {'chat_id': str(target_chat_id)} # Ensure chat_id is string
    log_prefix = f"ChatID {target_chat_id}, File '{filename}'"
    logging.info(f"[{log_prefix}] Attempting send.")
    last_exception: Optional[Exception] = None
    response: Optional[requests.Response] = None

    for attempt in range(API_RETRY_ATTEMPTS + 1):
        response = None # Reset response for each attempt
        try:
            response = session.post(
                api_url,
                data=data_payload,
                files=files_payload,
                timeout=(
                    TELEGRAM_API_TIMEOUTS['connect'],
                    TELEGRAM_API_TIMEOUTS.get('send_document', TELEGRAM_API_TIMEOUTS['read'])
                )
            )
            response.raise_for_status() # Check for HTTP 4xx/5xx errors
            response_json = response.json() # Decode JSON response

            if response_json.get('ok'):
                logging.info(f"[{log_prefix}] API success (Attempt {attempt+1}).")
                return True, f"File '{filename}' sent successfully!", response_json
            else:
                # Telegram API reported an error (e.g., invalid chat_id, bot permissions)
                error_desc = response_json.get('description', 'Unknown Telegram error')
                logging.error(f"[{log_prefix}] API Error (Attempt {attempt+1}): {error_desc} (Resp: {response.text})")
                return False, f"Telegram API Error: {error_desc}", None # Don't retry logic errors

        except requests.exceptions.Timeout as e:
            last_exception = e; logging.warning(f"[{log_prefix}] Timeout attempt {attempt+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception = e; logging.warning(f"[{log_prefix}] Connection error attempt {attempt+1}: {e}")
        except requests.exceptions.RequestException as e: # Includes HTTP errors from raise_for_status
            last_exception = e
            error_details = str(e)
            current_response = e.response if e.response is not None else response
            if current_response is not None:
                error_details += f" | Status: {current_response.status_code} | Response: {current_response.text}"
                logging.error(f"[{log_prefix}] Network/Request Error (Attempt {attempt+1}): {error_details}", exc_info=True)
                # Do not retry HTTP errors (4xx/5xx) automatically here
                return False, f"Network/Request Error: {error_details}", None
            else: # Error without response (DNS etc.) - potentially retryable
                logging.warning(f"[{log_prefix}] Network/Request Error no response (Attempt {attempt+1}): {error_details}", exc_info=True)
                # Fall through to retry logic

        except json.JSONDecodeError as e:
             # Should only happen if raise_for_status() passes but response is not JSON
             status = response.status_code if response else 'N/A'
             body = response.text if response else 'N/A'
             logging.error(f"[{log_prefix}] Invalid JSON response. Status: {status}, Body: {body}", exc_info=True)
             return False, "Error: Received invalid JSON response from Telegram.", None # Don't retry

        except Exception as e: # Catch any other unexpected error
             logging.error(f"[{log_prefix}] Unexpected error send attempt {attempt+1}: {e}", exc_info=True)
             return False, f"An unexpected error occurred: {e}", None # Don't retry

        # --- Retry logic for Timeout/ConnectionError/RequestException without response ---
        if last_exception and attempt < API_RETRY_ATTEMPTS:
             logging.info(f"[{log_prefix}] Retrying in {API_RETRY_DELAY}s...")
             time.sleep(API_RETRY_DELAY)
             last_exception = None # Reset for next attempt
             # Reset file-like object stream position if applicable
             if hasattr(file_object, 'seek') and callable(file_object.seek):
                 try:
                     file_object.seek(0); logging.debug(f"[{log_prefix}] Reset file obj pos for retry.")
                 except Exception as seek_err:
                     logging.error(f"[{log_prefix}] Failed reset file pos for retry: {seek_err}")
                     return False, f"Error resetting file stream: {seek_err}", None
             continue # Go to next attempt
        elif last_exception: # Retries exhausted or non-retryable RequestException occurred without response
             logging.error(f"[{log_prefix}] Send failed after {attempt+1} attempts.", exc_info=last_exception)
             return False, f"Failed after multiple attempts: {last_exception}", None

    # Fallback if loop finishes unexpectedly (should not happen)
    logging.error(f"[{log_prefix}] Send file logic exited loop unexpectedly.")
    return False, "Unknown error during file sending.", None

def download_telegram_file_content(file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Gets download URL and downloads content for a file_id, with retries.

    Args:
        file_id: The file_id from Telegram.

    Returns:
        Tuple (content_bytes, None) on success, (None, error_message) on failure.
    """
    log_prefix = f"FileID {file_id}"
    logging.info(f"[{log_prefix}] Attempting download.")
    get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
    params = {'file_id': file_id}
    direct_download_url: Optional[str] = None
    last_exception_getfile: Optional[Exception] = None
    response_getfile: Optional[requests.Response] = None

    # --- Step 1: Get file path (with retries) ---
    for attempt in range(API_RETRY_ATTEMPTS + 1):
        response_getfile = None
        try:
            response_getfile = session.get(
                get_file_url, params=params,
                timeout=(TELEGRAM_API_TIMEOUTS['connect'], TELEGRAM_API_TIMEOUTS.get('get_file', TELEGRAM_API_TIMEOUTS['read']))
            )
            response_getfile.raise_for_status()
            response_json = response_getfile.json()

            if response_json.get('ok'):
                file_path = response_json.get('result', {}).get('file_path')
                if file_path:
                    direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                    logging.info(f"[{log_prefix}] Got DL URL (Attempt {attempt+1}): {direct_download_url}")
                    last_exception_getfile = None; break # Success
                else: # OK=True but no file_path
                    logging.error(f"[{log_prefix}] getFile OK but no path. Resp: {response_json}")
                    return None, "Telegram API OK but no file path received."
            else: # OK=False
                error_desc = response_json.get('description', 'Unknown TG error (getFile)')
                logging.error(f"[{log_prefix}] API error getFile (Attempt {attempt+1}): {error_desc}. Resp: {response_json}")
                return None, f"API error getting file path: {error_desc}"

        except requests.exceptions.Timeout as e:
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Timeout attempt {attempt+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Connection error attempt {attempt+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_getfile = e; err_details = str(e)
            current_response = e.response if e.response is not None else response_getfile
            if current_response is not None:
                err_details += f" | Status: {current_response.status_code} | Response: {current_response.text}"
                logging.error(f"[{log_prefix}] getFile Net/Req Error (Attempt {attempt+1}): {err_details}", exc_info=True)
                return None, f"Network error getting download URL: {err_details}" # Don't retry
            else:
                logging.warning(f"[{log_prefix}] getFile Net/Req Error no response (Attempt {attempt+1}): {err_details}", exc_info=True)
                # Fall through to retry

        except json.JSONDecodeError as e:
             status = response_getfile.status_code if response_getfile else 'N/A'
             body = response_getfile.text if response_getfile else 'N/A'
             logging.error(f"[{log_prefix}] Invalid JSON (getFile). Status: {status}, Body: {body}", exc_info=True)
             return None, "Invalid response from Telegram (getFile)." # Don't retry

        except Exception as e:
             logging.error(f"[{log_prefix}] Unexpected error getFile attempt {attempt+1}: {e}", exc_info=True)
             return None, f"Unexpected error getting download URL: {e}" # Don't retry

        # --- Retry logic ---
        if last_exception_getfile and attempt < API_RETRY_ATTEMPTS:
            logging.info(f"[{log_prefix}] Retrying getFile in {API_RETRY_DELAY}s...")
            time.sleep(API_RETRY_DELAY); last_exception_getfile = None
        elif last_exception_getfile: # Retries exhausted
            logging.error(f"[{log_prefix}] getFile failed after {attempt+1} attempts.", exc_info=last_exception_getfile)
            return None, f"Failed get DL URL: {last_exception_getfile}"
    # Check if loop finished without success (only possible if retries exhausted)
    if not direct_download_url:
         if last_exception_getfile:
             logging.error(f"[{log_prefix}] getFile failed after {API_RETRY_ATTEMPTS+1} attempts (final check).", exc_info=last_exception_getfile)
             return None, f"Failed get DL URL: {last_exception_getfile}"
         else: # Should be impossible if logic is correct
              logging.error(f"[{log_prefix}] getFile loop finished unexpectedly."); return None, "Unknown error getting DL URL."

    # --- Step 2: Download content (with retries) ---
    logging.info(f"[{log_prefix}] Attempting content download from URL.")
    last_exception_download: Optional[Exception] = None
    response_download: Optional[requests.Response] = None

    for attempt in range(API_RETRY_ATTEMPTS + 1):
        response_download = None
        try:
            response_download = session.get(
                direct_download_url,
                stream=True, # Allows efficient reading of large files if needed later
                timeout=(
                    TELEGRAM_API_TIMEOUTS['connect'],
                    TELEGRAM_API_TIMEOUTS.get('download_file', TELEGRAM_API_TIMEOUTS['read'])
                )
            )
            response_download.raise_for_status()
            file_content = response_download.content # Read entire content
            logging.info(f"[{log_prefix}] Downloaded {len(file_content)} bytes (Attempt {attempt+1}).")
            return file_content, None # Success

        except requests.exceptions.Timeout as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Timeout attempt {attempt+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Connection error attempt {attempt+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_download = e; err_details = str(e)
            current_response = e.response if e.response is not None else response_download
            if current_response is not None:
                 err_details += f" | Status: {current_response.status_code} | Response: {current_response.text}"
                 logging.error(f"[{log_prefix}] Download Net/Req Error (Attempt {attempt+1}): {err_details}", exc_info=True)
                 return None, f"Network error downloading content: {err_details}" # Don't retry
            else:
                 logging.warning(f"[{log_prefix}] Download Net/Req Error no response (Attempt {attempt+1}): {err_details}", exc_info=True)
                 # Fall through to retry

        # No JSONDecodeError expected for content download
        except Exception as e:
             logging.error(f"[{log_prefix}] Unexpected error download attempt {attempt+1}: {e}", exc_info=True)
             return None, f"Unexpected error downloading content: {e}" # Don't retry

        # --- Retry logic ---
        if last_exception_download and attempt < API_RETRY_ATTEMPTS:
            logging.info(f"[{log_prefix}] Retrying download in {API_RETRY_DELAY}s...")
            time.sleep(API_RETRY_DELAY); last_exception_download = None
        elif last_exception_download: # Retries exhausted
            logging.error(f"[{log_prefix}] Download failed after {attempt+1} attempts.", exc_info=last_exception_download)
            return None, f"Failed download content: {last_exception_download}"
    # Check if loop finished without success
    if last_exception_download: # Should be set if loop finished due to retries
         logging.error(f"[{log_prefix}] Download failed after {API_RETRY_ATTEMPTS+1} attempts (final check).", exc_info=last_exception_download)
         return None, f"Failed download content: {last_exception_download}"
    else: # Should be impossible
         logging.error(f"[{log_prefix}] Download loop finished unexpectedly."); return None, "Unknown error downloading content."

logging.info("Telegram API functions defined with Session and Retries.")