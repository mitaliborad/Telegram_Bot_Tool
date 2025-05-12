"""Handles interactions with the Telegram Bot API for file uploads and downloads."""
import requests
import logging
import json
import time
from typing import Tuple, Optional, Dict, Any, Union, IO

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_API_TIMEOUTS,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    format_bytes
)

# --- Type Aliases ---
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] # success, message, response_json

# --- Module Level Requests Session ---
session = requests.Session()
logging.info("Initialized requests.Session for Telegram API calls.")

# --- Telegram API Interaction ---
def send_file_to_telegram(
    file_handle: IO[bytes],
    #file_object: Union[IO[bytes], bytes],
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
    files_payload = {'document': (filename, file_handle)}
    data_payload = {'chat_id': str(target_chat_id)} 
    log_prefix = f"ChatID {target_chat_id}, File '{filename}'"
    logging.info(f"[{log_prefix}] Attempting send.")
    last_exception: Optional[Exception] = None
    response: Optional[requests.Response] = None

    for attempt in range(API_RETRY_ATTEMPTS + 1):
        response = None 
        try:
            if attempt > 0:
                try:
                    file_handle.seek(0)
                    logging.debug(f"[{log_prefix}] Reset file handle position for retry {attempt+1}.")
                except Exception as seek_err:
                    logging.error(f"[{log_prefix}] Failed reset file pos for retry: {seek_err}")
                    return False, f"Error resetting file stream: {seek_err}", None
            response = session.post(
                api_url,
                data=data_payload,
                files=files_payload,
                timeout=(
                    TELEGRAM_API_TIMEOUTS['connect'],
                    TELEGRAM_API_TIMEOUTS.get('send_document', TELEGRAM_API_TIMEOUTS['read'])
                )
            )
            response.raise_for_status() 
            response_json = response.json() 

            if response_json.get('ok'):
                logging.info(f"[{log_prefix}] API success (Attempt {attempt+1}).")
                return True, f"File '{filename}' sent successfully!", response_json
            else:
                error_desc = response_json.get('description', 'Unknown Telegram error')
                logging.error(f"[{log_prefix}] API Error (Attempt {attempt+1}): {error_desc} (Resp: {response.text})")
                return False, f"Telegram API Error: {error_desc}", None 

        except requests.exceptions.Timeout as e:
            last_exception = e; logging.warning(f"[{log_prefix}] Timeout attempt {attempt+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception = e; logging.warning(f"[{log_prefix}] Connection error attempt {attempt+1}: {e}")
        except requests.exceptions.RequestException as e: 
            last_exception = e
            error_details = str(e)
            current_response = e.response if e.response is not None else response
            if current_response is not None:
                error_details += f" | Status: {current_response.status_code} | Response: {current_response.text}"
                logging.error(f"[{log_prefix}] Network/Request Error (Attempt {attempt+1}): {error_details}", exc_info=True)
                return False, f"Network/Request Error: {error_details}", None
            else: 
                logging.warning(f"[{log_prefix}] Network/Request Error no response (Attempt {attempt+1}): {error_details}", exc_info=True)

        except json.JSONDecodeError as e:
             status = response.status_code if response else 'N/A'
             body = response.text if response else 'N/A'
             logging.error(f"[{log_prefix}] Invalid JSON response. Status: {status}, Body: {body}", exc_info=True)
             return False, "Error: Received invalid JSON response from Telegram.", None 

        except Exception as e: 
             logging.error(f"[{log_prefix}] Unexpected error send attempt {attempt+1}: {e}", exc_info=True)
             return False, f"An unexpected error occurred: {e}", None 

        # --- Retry logic for Timeout/ConnectionError/RequestException without response ---
        if last_exception and attempt < API_RETRY_ATTEMPTS:
             logging.info(f"[{log_prefix}] Retrying in {API_RETRY_DELAY}s...")
             time.sleep(API_RETRY_DELAY)
             last_exception = None 
             continue
             # Reset file-like object stream position if applicable
             if hasattr(file_object, 'seek') and callable(file_object.seek):
                 try:
                     file_object.seek(0); logging.debug(f"[{log_prefix}] Reset file obj pos for retry.")
                 except Exception as seek_err:
                     logging.error(f"[{log_prefix}] Failed reset file pos for retry: {seek_err}")
                     return False, f"Error resetting file stream: {seek_err}", None
              
        elif last_exception: 
             logging.error(f"[{log_prefix}] Send failed after {attempt+1} attempts.", exc_info=last_exception)
             return False, f"Failed after multiple attempts: {last_exception}", None

    # Fallback if loop finishes unexpectedly (should not happen)
    logging.error(f"[{log_prefix}] Send file logic exited loop unexpectedly.")
    return False, "Unknown error during file sending.", None

import requests
import logging
import json
import time
from typing import Tuple, Optional, Dict, Any, Union, IO

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_API_TIMEOUTS,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    format_bytes # Import format_bytes
)

# ... (ApiResult, session initialization remain the same) ...

# --- Telegram API Interaction ---
# ... (send_file_to_telegram remains the same for now) ...

def download_telegram_file_content(file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Gets download URL and downloads content for a file_id, with retries.
    Handles specific "file is too big" error during content download.

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

    # --- Step 1: Get file path (with retries) ---
    for attempt_getf in range(API_RETRY_ATTEMPTS + 1):
        response_getfile: Optional[requests.Response] = None
        try:
            if attempt_getf > 0:
                logging.info(f"[{log_prefix}] Retrying getFile (Attempt {attempt_getf + 1}/{API_RETRY_ATTEMPTS + 1})...")
                time.sleep(API_RETRY_DELAY)

            response_getfile = session.get(
                get_file_url, params=params,
                timeout=(TELEGRAM_API_TIMEOUTS['connect'], TELEGRAM_API_TIMEOUTS.get('get_file', TELEGRAM_API_TIMEOUTS['read']))
            )

            # Check for specific 400 error in JSON *before* raise_for_status
            if response_getfile.status_code == 400:
                 try:
                     err_json = response_getfile.json()
                     description = err_json.get("description", "Unknown 400 error from Telegram")
                     logging.error(f"[{log_prefix}] Telegram API Bad Request (getFile): {description} (Full Response: {response_getfile.text})")
                     # Errors like "file_id doesn't correspond to a file" are not retryable here.
                     return None, f"Telegram error (getFile): {description}"
                 except json.JSONDecodeError:
                     logging.error(f"[{log_prefix}] Got 400 from getFile but failed to parse JSON: {response_getfile.text}")
                     # Treat as generic HTTPError if JSON parsing fails
                     response_getfile.raise_for_status() # Let HTTPError handler catch it

            response_getfile.raise_for_status() # For other non-200 codes (401, 404, 5xx etc.)
            response_json_getfile = response_getfile.json()

            if response_json_getfile.get('ok'):
                file_path = response_json_getfile.get('result', {}).get('file_path')
                if file_path:
                    direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                    logging.info(f"[{log_prefix}] Got DL URL (Attempt {attempt_getf+1}).")
                    last_exception_getfile = None # Clear exception on success
                    break # Exit getFile loop
                else:
                    logging.error(f"[{log_prefix}] getFile OK but no path. Resp: {response_json_getfile}")
                    return None, "Telegram API OK but no file path received."
            else: # Should technically be caught by raise_for_status, but defensive check
                error_desc = response_json_getfile.get('description', 'Unknown TG error (getFile, ok=false)')
                logging.error(f"[{log_prefix}] API error getFile (ok=false, Attempt {attempt_getf+1}): {error_desc}. Resp: {response_json_getfile}")
                return None, f"API error getting file path: {error_desc}"

        except requests.exceptions.HTTPError as e:
            last_exception_getfile = e
            err_details = str(e)
            if e.response is not None:
                 err_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
            logging.error(f"[{log_prefix}] getFile HTTP Error (Attempt {attempt_getf+1}): {err_details}", exc_info=False)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                # Non-retryable client errors (401, 403, 404 etc.)
                return None, f"Client error getting download URL: {err_details}"

        except requests.exceptions.Timeout as e:
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Timeout attempt {attempt_getf+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Connection error attempt {attempt_getf+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_getfile = e; logging.error(f"[{log_prefix}] getFile RequestException (Attempt {attempt_getf+1}): {e}", exc_info=True)
            return None, f"Request error getting download URL: {str(e)}"
        except json.JSONDecodeError as e:
             status = response_getfile.status_code if response_getfile else 'N/A'
             body = response_getfile.text if response_getfile else 'N/A'
             logging.error(f"[{log_prefix}] Invalid JSON (getFile). Status: {status}, Body: {body}", exc_info=True)
             return None, "Invalid response from Telegram (getFile)."
        except Exception as e:
             logging.error(f"[{log_prefix}] Unexpected error getFile attempt {attempt_getf+1}: {e}", exc_info=True)
             return None, f"Unexpected error getting download URL: {e}"

        # --- Retry logic for getFile ---
        if attempt_getf == API_RETRY_ATTEMPTS and direct_download_url is None: # All retries done, still no URL
            final_error_msg = f"Failed to get download URL after {API_RETRY_ATTEMPTS + 1} attempts."
            if last_exception_getfile: final_error_msg += f" Last error: {str(last_exception_getfile)}"
            logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_getfile if last_exception_getfile else False)
            return None, final_error_msg

    if not direct_download_url:
         # This case should be covered by the retry logic's final check, but as a safeguard:
         logging.error(f"[{log_prefix}] Exited getFile loop without URL and without final error. This should not happen.")
         return None, "Unknown error obtaining download URL."

    # --- Step 2: Download content (with retries) ---
    logging.info(f"[{log_prefix}] Attempting content download from URL.")
    last_exception_download: Optional[Exception] = None

    for attempt_dl in range(API_RETRY_ATTEMPTS + 1):
        response_dl: Optional[requests.Response] = None
        try:
            if attempt_dl > 0:
                logging.info(f"[{log_prefix}] Retrying content download (Attempt {attempt_dl + 1}/{API_RETRY_ATTEMPTS + 1})...")
                time.sleep(API_RETRY_DELAY)

            response_dl = session.get(
                direct_download_url,
                stream=True, # Important for potentially large files, though we read all at once with .content later
                timeout=(
                    TELEGRAM_API_TIMEOUTS['connect'],
                    TELEGRAM_API_TIMEOUTS.get('download_file', TELEGRAM_API_TIMEOUTS['read'])
                )
            )

            # --- >>> Check for "file is too big" error specifically <<< ---
            if response_dl.status_code == 400 and response_dl.request.method == 'GET': # Check method for safety
                 # Need to read *some* content to check the error message for TG downloads
                 try:
                     error_text_sample = response_dl.text # Read the error body
                     if "file is too big" in error_text_sample.lower():
                         logging.error(f"[{log_prefix}] Telegram Error: File is too big to be downloaded (Status 400). Response: {error_text_sample[:200]}")
                         return None, "Telegram error: File is too big to be downloaded." # Non-retryable specific error
                     else:
                         # It's another 400 error, let raise_for_status handle it
                         logging.warning(f"[{log_prefix}] Received 400 during content download, but not 'file too big'. Response: {error_text_sample[:200]}")
                         response_dl.raise_for_status() # Raise for generic HTTPError handling
                 except Exception as read_err:
                      logging.error(f"[{log_prefix}] Error reading response body for 400 error check: {read_err}")
                      response_dl.raise_for_status() # Raise anyway

            response_dl.raise_for_status() # Check for other errors (404, 5xx, etc.)

            # Read content (potential memory issue for huge files, but required by current design)
            file_content = response_dl.content
            if not file_content:
                logging.warning(f"[{log_prefix}] Downloaded content is empty (Attempt {attempt_dl+1}). Might retry.")
                last_exception_download = ValueError("Downloaded empty content") # Treat as potentially transient
                if attempt_dl == API_RETRY_ATTEMPTS:
                    return None, "Downloaded empty content after multiple attempts."
                continue # Retry

            logging.info(f"[{log_prefix}] Downloaded {format_bytes(len(file_content))} (Attempt {attempt_dl+1}).")
            return file_content, None # Success

        except requests.exceptions.HTTPError as e:
            last_exception_download = e
            err_details = str(e)
            if e.response is not None:
                 err_details += f" | Status: {e.response.status_code} | Response: {e.response.text[:200]}" # Limit response log
            logging.error(f"[{log_prefix}] Download HTTP Error (Attempt {attempt_dl+1}): {err_details}", exc_info=False)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                 # Specific client errors (e.g., 404 Not Found on download URL) - non-retryable
                 return None, f"Client error downloading content: {err_details}"

        except requests.exceptions.Timeout as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Timeout attempt {attempt_dl+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Connection error attempt {attempt_dl+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_download = e; logging.error(f"[{log_prefix}] Download RequestException (Attempt {attempt_dl+1}): {e}", exc_info=True)
            return None, f"Request error downloading content: {str(e)}"
        except Exception as e:
             logging.error(f"[{log_prefix}] Unexpected error download content attempt {attempt_dl+1}: {e}", exc_info=True)
             return None, f"Unexpected error downloading content: {e}"

        # --- Retry logic for download ---
        if attempt_dl == API_RETRY_ATTEMPTS: # All retries for download content exhausted
            final_error_msg = f"Failed to download content after {API_RETRY_ATTEMPTS + 1} attempts."
            if last_exception_download: final_error_msg += f" Last error: {str(last_exception_download)}"
            logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_download if last_exception_download else False)
            return None, final_error_msg

    # Fallback if loop finishes unexpectedly
    logging.error(f"[{log_prefix}] Download content loop finished unexpectedly.")
    return None, "Unknown error during content download."

# --- Logging Confirmation ---
logging.info("Telegram API functions updated with specific 'file too big' handling and format_bytes logging.")

logging.info("Telegram API functions defined with Session and Retries.")


# # telegram_api.py

# """Handles interactions with the Telegram Bot API for file uploads and downloads."""
# import requests
# import logging
# import json
# import time
# from typing import Tuple, Optional, Dict, Any, Union, IO

# from config import (
#     TELEGRAM_BOT_TOKEN,
#     TELEGRAM_API_TIMEOUTS,
#     API_RETRY_ATTEMPTS,
#     API_RETRY_DELAY
# )

# # --- Type Aliases ---
# ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] # success, message, response_json

# # --- Module Level Requests Session ---
# session = requests.Session()
# logging.info("Initialized requests.Session for Telegram API calls.")

# # --- Telegram API Interaction ---
# def send_file_to_telegram(
#     file_handle: IO[bytes],
#     filename: str,
#     target_chat_id: Union[str, int]
# ) -> ApiResult:
#     api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
#     files_payload = {'document': (filename, file_handle)}
#     data_payload = {'chat_id': str(target_chat_id)}
#     log_prefix = f"ChatID {target_chat_id}, File '{filename}'"
#     logging.info(f"[{log_prefix}] Attempting send.")
#     last_exception: Optional[Exception] = None
#     response: Optional[requests.Response] = None

#     for attempt in range(API_RETRY_ATTEMPTS + 1): # Retries + initial attempt
#         current_response_for_logging: Optional[requests.Response] = None # Define for wider scope
#         try:
#             if attempt > 0: # For retries
#                 logging.info(f"[{log_prefix}] Retrying send (Attempt {attempt + 1}/{API_RETRY_ATTEMPTS + 1})...")
#                 time.sleep(API_RETRY_DELAY)
#                 # Reset file handle position for retry
#                 # Ensure file_handle is the correct variable from arguments
#                 if hasattr(file_handle, 'seek') and callable(file_handle.seek):
#                     try:
#                         file_handle.seek(0)
#                         logging.debug(f"[{log_prefix}] Reset file handle position for retry.")
#                     except Exception as seek_err:
#                         logging.error(f"[{log_prefix}] Failed to reset file handle position for retry: {seek_err}")
#                         return False, f"Error resetting file stream for retry: {seek_err}", None
#                 else:
#                     logging.warning(f"[{log_prefix}] File handle not seekable for retry.")
#                     # This is a critical issue if the stream was consumed and cannot be reset.
#                     return False, "File stream not seekable for retry after partial send.", None

#             response = session.post( # Assign to outer scope 'response'
#                 api_url,
#                 data=data_payload,
#                 files=files_payload,
#                 timeout=(
#                     TELEGRAM_API_TIMEOUTS['connect'],
#                     TELEGRAM_API_TIMEOUTS.get('send_document', TELEGRAM_API_TIMEOUTS['read'])
#                 )
#             )
#             current_response_for_logging = response # For use in exception blocks
#             response.raise_for_status()
#             response_json = response.json()

#             if response_json.get('ok'):
#                 logging.info(f"[{log_prefix}] API success (Attempt {attempt+1}).")
#                 return True, f"File '{filename}' sent successfully!", response_json
#             else:
#                 error_desc = response_json.get('description', 'Unknown Telegram error')
#                 logging.error(f"[{log_prefix}] API Error (Attempt {attempt+1}): {error_desc} (Resp: {response.text})")
#                 # This is an API level error from Telegram, usually not retryable unless specified.
#                 return False, f"Telegram API Error: {error_desc}", None

#         except requests.exceptions.HTTPError as e: # From raise_for_status
#             last_exception = e
#             error_details = str(e)
#             if e.response is not None:
#                 error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
#             logging.error(f"[{log_prefix}] HTTP Error (Attempt {attempt+1}): {error_details}", exc_info=True)
#             # Decide if retryable, e.g. 5xx server errors could be. 4xx usually not.
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
#                 return False, f"Client-side HTTP error: {error_details}", None

#         except requests.exceptions.Timeout as e:
#             last_exception = e; logging.warning(f"[{log_prefix}] Timeout attempt {attempt+1}: {e}")
#         except requests.exceptions.ConnectionError as e:
#             last_exception = e; logging.warning(f"[{log_prefix}] Connection error attempt {attempt+1}: {e}")
#         except requests.exceptions.RequestException as e: # Other network or request level errors
#             last_exception = e
#             error_details = str(e)
#             # Use current_response_for_logging which should be set if post() was reached
#             if current_response_for_logging is not None:
#                 error_details += f" | Status: {current_response_for_logging.status_code} | Response: {current_response_for_logging.text}"
#             elif e.response is not None: # Fallback if current_response_for_logging wasn't set
#                  error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"

#             logging.error(f"[{log_prefix}] Network/Request Error (Attempt {attempt+1}): {error_details}", exc_info=True)
#             # If it's a client error from RequestException (e.g., bad URL before even sending)
#             if e.response is not None and 400 <= e.response.status_code < 500:
#                  return False, f"Network/Request Error (Client): {error_details}", None

#         except json.JSONDecodeError as e:
#              status_code_for_json_error = current_response_for_logging.status_code if current_response_for_logging else 'N/A'
#              body_for_json_error = current_response_for_logging.text if current_response_for_logging else 'N/A'
#              logging.error(f"[{log_prefix}] Invalid JSON response. Status: {status_code_for_json_error}, Body: {body_for_json_error}", exc_info=True)
#              return False, "Error: Received invalid JSON response from Telegram.", None

#         except Exception as e:
#              logging.error(f"[{log_prefix}] Unexpected error send attempt {attempt+1}: {e}", exc_info=True)
#              return False, f"An unexpected error occurred: {e}", None

#         if attempt < API_RETRY_ATTEMPTS:
#              # Check if last_exception suggests no retry (e.g., it was a return False)
#              # This part of the original logic for retry was a bit off, simplified now
#              if last_exception is not None: # Only retry if a retryable exception was caught
#                  logging.info(f"[{log_prefix}] Retrying in {API_RETRY_DELAY}s due to: {type(last_exception).__name__}")
#                  # time.sleep(API_RETRY_DELAY) # Delay is now at the start of the retry attempt
#                  last_exception = None # Reset for next attempt
#                  continue
#              else: # No exception, but also no success (e.g. if API said 'ok': false but not an HTTP error)
#                   # This case is now handled by returning False from the 'ok':false block.
#                   pass # Should not be reached if logic above is correct
#         elif last_exception:
#              logging.error(f"[{log_prefix}] Send failed after {attempt+1} attempts. Last error: {last_exception}", exc_info=last_exception)
#              return False, f"Failed after multiple attempts: {last_exception}", None

#     logging.error(f"[{log_prefix}] Send file logic exited loop unexpectedly (all attempts made or error).")
#     final_msg = "Unknown error during file sending after all attempts."
#     if last_exception:
#         final_msg = f"Failed after {API_RETRY_ATTEMPTS + 1} attempts: {str(last_exception)}"
#     return False, final_msg, None


# def download_telegram_file_content(file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
#     log_prefix = f"FileID {file_id}"
#     logging.info(f"[{log_prefix}] Attempting to get file path for download.")
#     get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
#     params = {'file_id': file_id}
#     direct_download_url: Optional[str] = None
#     last_exception_getfile: Optional[Exception] = None
    
#     for attempt_getf in range(API_RETRY_ATTEMPTS + 1):
#         response_getfile: Optional[requests.Response] = None
#         try:
#             if attempt_getf > 0:
#                 logging.info(f"[{log_prefix}] Retrying getFile (Attempt {attempt_getf + 1}/{API_RETRY_ATTEMPTS + 1})...")
#                 time.sleep(API_RETRY_DELAY)

#             response_getfile = session.get(
#                 get_file_url, params=params,
#                 timeout=(TELEGRAM_API_TIMEOUTS['connect'], TELEGRAM_API_TIMEOUTS.get('get_file', TELEGRAM_API_TIMEOUTS['read']))
#             )
            
#             # Explicitly check for "file is too big" or other 400 errors from Telegram's JSON response
#             if response_getfile.status_code == 400:
#                 try:
#                     err_json = response_getfile.json()
#                     description = err_json.get("description", "Unknown 400 error from Telegram")
#                     logging.error(f"[{log_prefix}] Telegram API Bad Request (getFile): {description} (Full Response: {response_getfile.text})")
#                     # "file is too big" or other client errors on getFile are typically not retryable.
#                     return None, f"Telegram error (getFile): {description}"
#                 except json.JSONDecodeError:
#                     # If JSON parsing fails on a 400, treat as a generic HTTPError
#                     logging.error(f"[{log_prefix}] Got 400 from getFile but failed to parse JSON response: {response_getfile.text}")
#                     response_getfile.raise_for_status() # Let it be caught by HTTPError

#             response_getfile.raise_for_status() # For other HTTP errors (e.g., 401, 404, 5xx)
#             response_json_getfile = response_getfile.json()

#             if response_json_getfile.get('ok'):
#                 file_path = response_json_getfile.get('result', {}).get('file_path')
#                 if file_path:
#                     direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
#                     logging.info(f"[{log_prefix}] Got DL URL (Attempt {attempt_getf+1}): {direct_download_url}")
#                     last_exception_getfile = None # Clear last exception on success
#                     break # Successfully got URL, exit getFile loop
#                 else: 
#                     logging.error(f"[{log_prefix}] getFile API call OK but no file_path in response. Resp: {response_json_getfile}")
#                     return None, "Telegram API OK but no file path received." # Non-retryable
#             else: # Should be caught by raise_for_status if not ok, but for safety
#                 error_desc = response_json_getfile.get('description', 'Unknown TG error (getFile)')
#                 logging.error(f"[{log_prefix}] API error getFile (Attempt {attempt_getf+1}): {error_desc}. Resp: {response_json_getfile}")
#                 last_exception_getfile = requests.exceptions.HTTPError(f"Telegram API Error (ok=false): {error_desc}", response=response_getfile)
#                 # If Telegram explicitly says "ok": false, it's likely not a transient network issue.
#                 if attempt_getf == API_RETRY_ATTEMPTS: # If it's the last attempt, return this error
#                     return None, f"API error getting file path: {error_desc}"
        
#         except requests.exceptions.HTTPError as e:
#             last_exception_getfile = e
#             err_details = str(e)
#             if e.response is not None:
#                  err_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
#             logging.error(f"[{log_prefix}] getFile HTTP Error (Attempt {attempt_getf+1}): {err_details}", exc_info=False) # exc_info=False if details are in err_details
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429 and e.response.status_code != 408:
#                 return None, f"Client error getting download URL: {err_details}" # Non-retryable client error

#         except requests.exceptions.Timeout as e:
#             last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Timeout attempt {attempt_getf+1}: {e}")
#         except requests.exceptions.ConnectionError as e:
#             last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Connection error attempt {attempt_getf+1}: {e}")
#         except requests.exceptions.RequestException as e: # Other request errors
#             last_exception_getfile = e; logging.error(f"[{log_prefix}] getFile RequestException (Attempt {attempt_getf+1}): {e}", exc_info=True)
#             return None, f"Request error getting download URL: {str(e)}"
#         except json.JSONDecodeError as e: # If response.json() fails
#              status_code_json_err = response_getfile.status_code if response_getfile else 'N/A'
#              body_json_err = response_getfile.text if response_getfile else 'N/A'
#              logging.error(f"[{log_prefix}] Invalid JSON response from getFile. Status: {status_code_json_err}, Body: {body_json_err}", exc_info=True)
#              return None, "Invalid JSON response from Telegram (getFile)."
#         except Exception as e: # Catch-all for unexpected
#              logging.error(f"[{log_prefix}] Unexpected error during getFile attempt {attempt_getf+1}: {e}", exc_info=True)
#              return None, f"Unexpected error getting download URL: {e}"

#         if attempt_getf == API_RETRY_ATTEMPTS and direct_download_url is None: # All retries done, still no URL
#             final_error_msg_getfile = f"Failed to get download URL after {API_RETRY_ATTEMPTS + 1} attempts."
#             if last_exception_getfile: final_error_msg_getfile += f" Last error: {str(last_exception_getfile)}"
#             logging.error(f"[{log_prefix}] {final_error_msg_getfile}", exc_info=last_exception_getfile if last_exception_getfile else False)
#             return None, final_error_msg_getfile

#     if not direct_download_url: # Should be caught above, but defensive check
#         return None, "Failed to obtain direct download URL after all attempts."

#     # --- Step 2: Download content (with retries for network issues) ---
#     logging.info(f"[{log_prefix}] Attempting content download from URL: {direct_download_url}")
#     last_exception_download: Optional[Exception] = None

#     for attempt_dl in range(API_RETRY_ATTEMPTS + 1):
#         response_dl: Optional[requests.Response] = None
#         try:
#             if attempt_dl > 0:
#                 logging.info(f"[{log_prefix}] Retrying content download (Attempt {attempt_dl + 1}/{API_RETRY_ATTEMPTS + 1})...")
#                 time.sleep(API_RETRY_DELAY)

#             response_dl = session.get(
#                 direct_download_url, stream=True,
#                 timeout=(TELEGRAM_API_TIMEOUTS['connect'], TELEGRAM_API_TIMEOUTS.get('download_file', TELEGRAM_API_TIMEOUTS['read']))
#             )
#             response_dl.raise_for_status()
            
#             file_content = response_dl.content
#             if not file_content:
#                 logging.warning(f"[{log_prefix}] Downloaded content is empty (Attempt {attempt_dl+1}).")
#                 last_exception_download = ValueError("Downloaded empty content.") # Consider this retryable
#                 if attempt_dl == API_RETRY_ATTEMPTS: # If last attempt and still empty
#                      return None, "Downloaded empty content after all retries."
#                 continue # Retry if not last attempt
            
#             logging.info(f"[{log_prefix}] Downloaded {len(file_content)} bytes (Attempt {attempt_dl+1}).")
#             return file_content, None

#         except requests.exceptions.HTTPError as e:
#             last_exception_download = e
#             err_details_dl = str(e)
#             if e.response is not None:
#                  err_details_dl += f" | Status: {e.response.status_code} | Response: {e.response.text}"
#             logging.error(f"[{log_prefix}] Download HTTP Error (Attempt {attempt_dl+1}): {err_details_dl}", exc_info=False)
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429 and e.response.status_code != 408:
#                  return None, f"Client error downloading content: {err_details_dl}"
#         except requests.exceptions.Timeout as e:
#             last_exception_download = e; logging.warning(f"[{log_prefix}] Download Timeout attempt {attempt_dl+1}: {e}")
#         except requests.exceptions.ConnectionError as e:
#             last_exception_download = e; logging.warning(f"[{log_prefix}] Download Connection error attempt {attempt_dl+1}: {e}")
#         except requests.exceptions.RequestException as e:
#             last_exception_download = e; logging.error(f"[{log_prefix}] Download RequestException (Attempt {attempt_dl+1}): {e}", exc_info=True)
#             return None, f"Request error downloading content: {str(e)}"
#         except Exception as e:
#              logging.error(f"[{log_prefix}] Unexpected error download content attempt {attempt_dl+1}: {e}", exc_info=True)
#              return None, f"Unexpected error downloading content: {e}"

#         if attempt_dl == API_RETRY_ATTEMPTS: # All retries for download content exhausted
#             final_error_msg_download = f"Failed to download content after {API_RETRY_ATTEMPTS + 1} attempts."
#             if last_exception_download: final_error_msg_download += f" Last error: {str(last_exception_download)}"
#             logging.error(f"[{log_prefix}] {final_error_msg_download}", exc_info=last_exception_download if last_exception_download else False)
#             return None, final_error_msg_download
            
#     return None, "Unknown error during content download after loop (should not be reached)."


# logging.info("Telegram API functions defined with Session and Retries.")