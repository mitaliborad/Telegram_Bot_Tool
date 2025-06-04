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
    format_bytes,
    MAX_DOWNLOAD_WORKERS
)
from requests.adapters import HTTPAdapter

# --- Type Aliases ---
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] 

# --- Module Level Requests Session ---
session = requests.Session()
logging.info("Initialized requests.Session for Telegram API calls.")
adapter_pool_maxsize = max(10, MAX_DOWNLOAD_WORKERS + 5) 
general_adapter = HTTPAdapter(pool_maxsize=adapter_pool_maxsize, pool_connections=10)
session.mount('https://', general_adapter)
session.mount('http://', general_adapter)

logging.info(f"Initialized requests.Session for Telegram API calls. Adapter configured with pool_maxsize={adapter_pool_maxsize}.")

# --- Telegram API Interaction ---
# def send_file_to_telegram(
#     file_handle: IO[bytes],
#     filename: str,
#     target_chat_id: Union[str, int]
# ) -> ApiResult:
#     """
#     Sends a file (from buffer or bytes) to a specific Telegram chat ID.
#     Implements retries for transient network errors.

#     Args:
#         file_object: A file-like object opened in binary mode or raw bytes.
#         filename: The desired filename for the uploaded document.
#         target_chat_id: The target chat ID (string or integer).

#     Returns:
#         A tuple: (success: bool, message: str, response_json: Optional[dict])
#     """
#     api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
#     files_payload = {'document': (filename, file_handle)}
#     data_payload = {'chat_id': str(target_chat_id)} 
#     log_prefix = f"ChatID {target_chat_id}, File '{filename}'"
#     logging.info(f"[{log_prefix}] Attempting send.")
#     last_exception: Optional[Exception] = None
#     response: Optional[requests.Response] = None

#     for attempt in range(API_RETRY_ATTEMPTS + 1):
#         response = None 
#         try:
#             if attempt > 0:
#                 logging.info(f"[{log_prefix}] Retrying send (Attempt {attempt + 1}/{API_RETRY_ATTEMPTS + 1})...")
#                 time.sleep(API_RETRY_DELAY)
#                 try:
#                     file_handle.seek(0)
#                     logging.debug(f"[{log_prefix}] Reset file handle position for retry {attempt+1}.")
                    
                    
#                 except Exception as seek_err:
#                     logging.error(f"[{log_prefix}] Failed reset file pos for retry: {seek_err}")
#                     return False, f"Error resetting file stream: {seek_err}", None
#             response = session.post(
#                 api_url,
#                 data=data_payload,
#                 files=files_payload,
#                 timeout=(
#                     TELEGRAM_API_TIMEOUTS['connect'],
#                     TELEGRAM_API_TIMEOUTS.get('send_document', TELEGRAM_API_TIMEOUTS['read'])
#                 )
#             )
#             response.raise_for_status() 
#             response_json = response.json() 

#             if response_json.get('ok'):
#                 logging.info(f"[{log_prefix}] API success (Attempt {attempt+1}).")
#                 return True, f"File '{filename}' sent successfully!", response_json
#             else:
#                 error_desc = response_json.get('description', 'Unknown Telegram error')
#                 logging.error(f"[{log_prefix}] API Error (Attempt {attempt+1}): {error_desc} (Resp: {response.text})")
#                 return False, f"Telegram API Error: {error_desc}", None 
#         except requests.exceptions.HTTPError as e: # From raise_for_status()
#             last_exception = e
#             error_details = str(e)
#             if e.response is not None:
#                 error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
#             logging.error(f"[{log_prefix}] HTTP Error (Attempt {attempt+1}): {error_details}", exc_info=False) # exc_info can be True for more details
#             # Non-retryable client errors (4xx except 408, 429)
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
#                 return False, f"Client-side HTTP error: {error_details}", None
        
#         except requests.exceptions.Timeout as e:
#             last_exception = e; logging.warning(f"[{log_prefix}] Timeout attempt {attempt+1}: {e}")
#         except requests.exceptions.ConnectionError as e:
#             last_exception = e; logging.warning(f"[{log_prefix}] Connection error attempt {attempt+1}: {e}")
#         except requests.exceptions.RequestException as e: 
#             last_exception = e
#             error_details = str(e)
#             current_response = e.response if e.response is not None else response
#             if current_response is not None:
#                 error_details += f" | Status: {current_response.status_code} | Response: {current_response.text}"
#                 logging.error(f"[{log_prefix}] Network/Request Error (Attempt {attempt+1}): {error_details}", exc_info=True)
#                 return False, f"Network/Request Error: {error_details}", None
#             else: 
#                 logging.warning(f"[{log_prefix}] Network/Request Error no response (Attempt {attempt+1}): {error_details}", exc_info=True)

#         except json.JSONDecodeError as e:
#              status = response.status_code if response else 'N/A'
#              body = response.text if response else 'N/A'
#              logging.error(f"[{log_prefix}] Invalid JSON response. Status: {status}, Body: {body}", exc_info=True)
#              return False, "Error: Received invalid JSON response from Telegram.", None 

#         except Exception as e: 
#              logging.error(f"[{log_prefix}] Unexpected error send attempt {attempt+1}: {e}", exc_info=True)
#              return False, f"An unexpected error occurred: {e}", None 

#         # --- Retry logic for Timeout/ConnectionError/RequestException without response ---
#         if attempt < API_RETRY_ATTEMPTS : # If not the last attempt
#              if last_exception: # And an exception occurred that didn't lead to an early return
#                  #logging.info(f"[{log_prefix}] Retrying in {API_RETRY_DELAY}s...") # Moved delay to start of loop
#                  # time.sleep(API_RETRY_DELAY) # Moved to start of loop
#                  last_exception = None 
#                  continue # Go to next attempt
#              # If no exception but loop continues (e.g., 'ok':false was caught and returned), this won't be hit.
#         elif last_exception: # Last attempt and there was an exception
#              logging.error(f"[{log_prefix}] Send failed after {attempt+1} attempts.", exc_info=last_exception)
#              return False, f"Failed after multiple attempts: {str(last_exception)}", None
#     # Fallback if loop finishes unexpectedly (should not happen)
#     logging.error(f"[{log_prefix}] Send file logic exited loop unexpectedly.")
#     return False, "Unknown error during file sending.", None

def send_file_to_telegram(
    file_handle: IO[bytes],
    filename: str,
    target_chat_id: Union[str, int]
) -> ApiResult:
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
    files_payload = {'document': (filename, file_handle)}
    data_payload = {'chat_id': str(target_chat_id)}
    log_prefix = f"ChatID {target_chat_id}, File '{filename}'"
    # Removed: logging.info(f"[{log_prefix}] Attempting send.") # Moved inside loop for retries
    last_exception: Optional[Exception] = None
    response: Optional[requests.Response] = None

    for attempt in range(API_RETRY_ATTEMPTS + 1): # Corrected range to include 0 up to API_RETRY_ATTEMPTS
        response = None
        logging.info(f"[{log_prefix}] Attempting send (Attempt {attempt + 1}/{API_RETRY_ATTEMPTS + 1}).")
        try:
            if attempt > 0:
                # General delay is handled here, specific Retry-After is handled in except HTTPError
                logging.info(f"[{log_prefix}] Retrying after general delay {API_RETRY_DELAY}s...")
                time.sleep(API_RETRY_DELAY)
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
                error_desc = response_json.get('description', 'Unknown Telegram error (ok=false)')
                error_code = response_json.get('error_code')
                full_error_msg = f"TG API Error (Code {error_code}): {error_desc}"
                logging.error(f"[{log_prefix}] {full_error_msg} (Attempt {attempt+1}, Resp: {response.text})")
                # Non-retryable based on 'ok: false' unless it's a known transient error code if any
                return False, full_error_msg, response_json # Return response_json for further inspection if needed

        except requests.exceptions.HTTPError as e:
            last_exception = e
            error_details = str(e)
            if e.response is not None:
                status_code = e.response.status_code
                response_text = e.response.text
                error_details += f" | Status: {status_code} | Response: {response_text[:200]}" # Log snippet
                logging.error(f"[{log_prefix}] HTTP Error (Attempt {attempt+1}): {error_details}", exc_info=False)
                
                try:
                    response_json_error = e.response.json()
                    tg_error_desc = response_json_error.get('description', 'No description in JSON error')
                    tg_error_code = response_json_error.get('error_code')
                    logging.error(f"[{log_prefix}] Parsed Telegram API error details: Code {tg_error_code}, Desc: {tg_error_desc}")
                    
                    if status_code == 429: # Too Many Requests
                        retry_after_val = response_json_error.get('parameters', {}).get('retry_after')
                        if retry_after_val and attempt < API_RETRY_ATTEMPTS:
                            wait_time = int(retry_after_val)
                            logging.warning(f"[{log_prefix}] Received 429. Retrying after {wait_time}s (Attempt {attempt+1}).")
                            time.sleep(wait_time)
                            # seek(0) is handled at the start of the loop for next attempt
                            continue # Continue to next attempt in the loop
                        else:
                            logging.error(f"[{log_prefix}] Received 429 but no retry_after or max retries reached. Failing.")
                            return False, f"Telegram rate limit hit (429): {tg_error_desc}", response_json_error
                except json.JSONDecodeError:
                    logging.warning(f"[{log_prefix}] Could not parse JSON from HTTP error response body: {response_text[:200]}")

                # For other 4xx client errors (that are not 408 for timeout or 429 handled above)
                if 400 <= status_code < 500 and status_code not in [408, 429]:
                    # Pass the more detailed error message if available from JSON parsing
                    parsed_tg_msg = response_json_error.get('description') if 'response_json_error' in locals() else None
                    final_err_msg = f"Client-side HTTP error ({status_code}): {parsed_tg_msg or response_text[:100]}"
                    return False, final_err_msg, response_json_error if 'response_json_error' in locals() else None
            else: # HTTPError without e.response (should be rare)
                 logging.error(f"[{log_prefix}] HTTP Error (Attempt {attempt+1}) with no e.response: {error_details}", exc_info=False)
        
        # ... (rest of the exception handlers: Timeout, ConnectionError, RequestException, JSONDecodeError, generic Exception remain the same)
        except requests.exceptions.Timeout as e_timeout:
            last_exception = e_timeout
            logging.warning(f"[{log_prefix}] Timeout (Attempt {attempt+1}): {e_timeout}")
        except requests.exceptions.ConnectionError as e_conn:
            last_exception = e_conn
            logging.warning(f"[{log_prefix}] Connection error (Attempt {attempt+1}): {e_conn}")
        except requests.exceptions.RequestException as e_req:
            last_exception = e_req
            err_details_req = str(e_req)
            # This part was problematic as it could return early without exhausting retries for network issues
            # Let it fall through to the retry logic if attempts remain.
            logging.error(f"[{log_prefix}] Network/Request Error (Attempt {attempt+1}): {err_details_req}", exc_info=True)

        except json.JSONDecodeError as e_json:
            # This means response.raise_for_status() didn't trigger, but response.json() failed.
            # Should be rare if API is consistent. Likely implies non-JSON success response or malformed error.
            status_code_json_err = response.status_code if response else 'N/A'
            body_json_err = response.text if response else 'N/A'
            logging.error(f"[{log_prefix}] Invalid JSON response. Status: {status_code_json_err}, Body: {body_json_err}", exc_info=True)
            return False, "Error: Received invalid JSON response from Telegram.", None

        except Exception as e_generic:
            last_exception = e_generic # Store for final error message
            logging.error(f"[{log_prefix}] Unexpected error send (Attempt {attempt+1}): {e_generic}", exc_info=True)
            # For truly unexpected errors, might be better to fail fast rather than retry,
            # but current logic will retry. If it's the last attempt, it will fail.

        # Check if we should break the loop or continue to the next retry
        if attempt >= API_RETRY_ATTEMPTS: # If this was the last allowed attempt
            break # Exit loop, will fall through to final error reporting

    # After the loop (either completed all retries or broke out early from a non-retryable error return)
    if last_exception:
        final_err_msg = f"Failed after {API_RETRY_ATTEMPTS + 1} attempts. Last error: {str(last_exception)}"
        logging.error(f"[{log_prefix}] {final_err_msg}", exc_info=last_exception) # Log with last exception details
        return False, final_err_msg, None
    
    # Fallback if loop finishes unexpectedly without returning (should not happen with correct logic)
    logging.error(f"[{log_prefix}] Send file logic exited loop unexpectedly without success or explicit failure.")
    return False, "Unknown error during file sending after all attempts.", None

# def download_telegram_file_content(file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
#     """
#     Gets download URL and downloads content for a file_id, with retries.
#     Handles specific "file is too big" error during content download.

#     Args:
#         file_id: The file_id from Telegram.

#     Returns:
#         Tuple (content_bytes, None) on success, (None, error_message) on failure.
#     """
#     log_prefix = f"FileID {file_id}"
#     logging.info(f"[{log_prefix}] Attempting download.")
#     get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
#     params = {'file_id': file_id}
#     direct_download_url: Optional[str] = None
#     last_exception_getfile: Optional[Exception] = None

#     # --- Step 1: Get file path (with retries) ---
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

#             # Check for specific 400 error in JSON *before* raise_for_status
#             if response_getfile.status_code == 400:
#                  try:
#                      err_json = response_getfile.json()
#                      description = err_json.get("description", "Unknown 400 error from Telegram")
#                      logging.error(f"[{log_prefix}] Telegram API Bad Request (getFile): {description} (Full Response: {response_getfile.text})")
#                      # Errors like "file_id doesn't correspond to a file" are not retryable here.
#                      return None, f"Telegram error (getFile): {description}"
#                  except json.JSONDecodeError:
#                      logging.error(f"[{log_prefix}] Got 400 from getFile but failed to parse JSON: {response_getfile.text}")
#                      # Treat as generic HTTPError if JSON parsing fails
#                      response_getfile.raise_for_status() # Let HTTPError handler catch it

#             response_getfile.raise_for_status() # For other non-200 codes (401, 404, 5xx etc.)
#             response_json_getfile = response_getfile.json()

#             if response_json_getfile.get('ok'):
#                 file_path = response_json_getfile.get('result', {}).get('file_path')
#                 if file_path:
#                     direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
#                     logging.info(f"[{log_prefix}] Got DL URL (Attempt {attempt_getf+1}).")
#                     last_exception_getfile = None # Clear exception on success
#                     break # Exit getFile loop
#                 else:
#                     logging.error(f"[{log_prefix}] getFile OK but no path. Resp: {response_json_getfile}")
#                     return None, "Telegram API OK but no file path received."
#             else: # Should technically be caught by raise_for_status, but defensive check
#                 error_desc = response_json_getfile.get('description', 'Unknown TG error (getFile, ok=false)')
#                 logging.error(f"[{log_prefix}] API error getFile (ok=false, Attempt {attempt_getf+1}): {error_desc}. Resp: {response_json_getfile}")
#                 return None, f"API error getting file path: {error_desc}"

#         except requests.exceptions.HTTPError as e:
#             last_exception_getfile = e
#             err_details = str(e)
#             if e.response is not None:
#                  err_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
#             logging.error(f"[{log_prefix}] getFile HTTP Error (Attempt {attempt_getf+1}): {err_details}", exc_info=False)
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
#                 # Non-retryable client errors (401, 403, 404 etc.)
#                 return None, f"Client error getting download URL: {err_details}"

#         except requests.exceptions.Timeout as e:
#             last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Timeout attempt {attempt_getf+1}: {e}")
#         except requests.exceptions.ConnectionError as e:
#             last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Connection error attempt {attempt_getf+1}: {e}")
#         except requests.exceptions.RequestException as e:
#             last_exception_getfile = e; logging.error(f"[{log_prefix}] getFile RequestException (Attempt {attempt_getf+1}): {e}", exc_info=True)
#             return None, f"Request error getting download URL: {str(e)}"
#         except json.JSONDecodeError as e:
#              status = response_getfile.status_code if response_getfile else 'N/A'
#              body = response_getfile.text if response_getfile else 'N/A'
#              logging.error(f"[{log_prefix}] Invalid JSON (getFile). Status: {status}, Body: {body}", exc_info=True)
#              return None, "Invalid response from Telegram (getFile)."
#         except Exception as e:
#              logging.error(f"[{log_prefix}] Unexpected error getFile attempt {attempt_getf+1}: {e}", exc_info=True)
#              return None, f"Unexpected error getting download URL: {e}"

#         # --- Retry logic for getFile ---
#         if attempt_getf == API_RETRY_ATTEMPTS and direct_download_url is None: # All retries done, still no URL
#             final_error_msg = f"Failed to get download URL after {API_RETRY_ATTEMPTS + 1} attempts."
#             if last_exception_getfile: final_error_msg += f" Last error: {str(last_exception_getfile)}"
#             logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_getfile if last_exception_getfile else False)
#             return None, final_error_msg

#     if not direct_download_url:
#          # This case should be covered by the retry logic's final check, but as a safeguard:
#          logging.error(f"[{log_prefix}] Exited getFile loop without URL and without final error. This should not happen.")
#          return None, "Unknown error obtaining download URL."

#     # --- Step 2: Download content (with retries) ---
#     logging.info(f"[{log_prefix}] Attempting content download from URL.")
#     last_exception_download: Optional[Exception] = None

#     for attempt_dl in range(API_RETRY_ATTEMPTS + 1):
#         response_dl: Optional[requests.Response] = None
#         try:
#             if attempt_dl > 0:
#                 logging.info(f"[{log_prefix}] Retrying content download (Attempt {attempt_dl + 1}/{API_RETRY_ATTEMPTS + 1})...")
#                 time.sleep(API_RETRY_DELAY)

#             response_dl = session.get(
#                 direct_download_url,
#                 stream=True, # Important for potentially large files, though we read all at once with .content later
#                 timeout=(
#                     TELEGRAM_API_TIMEOUTS['connect'],
#                     TELEGRAM_API_TIMEOUTS.get('download_file', TELEGRAM_API_TIMEOUTS['read'])
#                 )
#             )

#             # --- >>> Check for "file is too big" error specifically <<< ---
#             if response_dl.status_code == 400 and response_dl.request.method == 'GET': # Check method for safety
#                  # Need to read *some* content to check the error message for TG downloads
#                  try:
#                      error_text_sample = response_dl.text # Read the error body
#                      if "file is too big" in error_text_sample.lower():
#                          logging.error(f"[{log_prefix}] Telegram Error: File is too big to be downloaded (Status 400). Response: {error_text_sample[:200]}")
#                          return None, "Telegram error: File is too big to be downloaded." # Non-retryable specific error
#                      else:
#                          # It's another 400 error, let raise_for_status handle it
#                          logging.warning(f"[{log_prefix}] Received 400 during content download, but not 'file too big'. Response: {error_text_sample[:200]}")
#                          response_dl.raise_for_status() # Raise for generic HTTPError handling
#                  except Exception as read_err:
#                       logging.error(f"[{log_prefix}] Error reading response body for 400 error check: {read_err}")
#                       response_dl.raise_for_status() # Raise anyway

#             response_dl.raise_for_status() # Check for other errors (404, 5xx, etc.)

#             # Read content (potential memory issue for huge files, but required by current design)
#             file_content = response_dl.content
#             if not file_content:
#                 logging.warning(f"[{log_prefix}] Downloaded content is empty (Attempt {attempt_dl+1}). Might retry.")
#                 last_exception_download = ValueError("Downloaded empty content") # Treat as potentially transient
#                 if attempt_dl == API_RETRY_ATTEMPTS:
#                     return None, "Downloaded empty content after multiple attempts."
#                 continue # Retry

#             logging.info(f"[{log_prefix}] Downloaded {format_bytes(len(file_content))} (Attempt {attempt_dl+1}).")
#             return file_content, None # Success

#         except requests.exceptions.HTTPError as e:
#             last_exception_download = e
#             err_details = str(e)
#             if e.response is not None:
#                  err_details += f" | Status: {e.response.status_code} | Response: {e.response.text[:200]}" # Limit response log
#             logging.error(f"[{log_prefix}] Download HTTP Error (Attempt {attempt_dl+1}): {err_details}", exc_info=False)
#             if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
#                  # Specific client errors (e.g., 404 Not Found on download URL) - non-retryable
#                  return None, f"Client error downloading content: {err_details}"

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

#         # --- Retry logic for download ---
#         if attempt_dl == API_RETRY_ATTEMPTS: # All retries for download content exhausted
#             final_error_msg = f"Failed to download content after {API_RETRY_ATTEMPTS + 1} attempts."
#             if last_exception_download: final_error_msg += f" Last error: {str(last_exception_download)}"
#             logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_download if last_exception_download else False)
#             return None, final_error_msg

#     # Fallback if loop finishes unexpectedly
#     logging.error(f"[{log_prefix}] Download content loop finished unexpectedly.")
#     return None, "Unknown error during content download."

def download_telegram_file_content(file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Gets download URL and downloads content for a file_id, with retries.
    Handles specific "file is too big" error during content download.
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

            if response_getfile.status_code == 400:
                 try:
                     err_json = response_getfile.json()
                     description = err_json.get("description", "Unknown 400 error from Telegram")
                     logging.error(f"[{log_prefix}] Telegram API Bad Request (getFile): {description} (Full Response: {response_getfile.text})")
                     return None, f"Telegram error (getFile): {description}" # Non-retryable
                 except json.JSONDecodeError:
                     logging.error(f"[{log_prefix}] Got 400 from getFile but failed to parse JSON: {response_getfile.text}")
                     response_getfile.raise_for_status() 

            response_getfile.raise_for_status() 
            response_json_getfile = response_getfile.json()

            if response_json_getfile.get('ok'):
                file_path = response_json_getfile.get('result', {}).get('file_path')
                if file_path:
                    direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                    logging.info(f"[{log_prefix}] Got DL URL (Attempt {attempt_getf+1}).")
                    last_exception_getfile = None 
                    break 
                else:
                    logging.error(f"[{log_prefix}] getFile OK but no path. Resp: {response_json_getfile}")
                    return None, "Telegram API OK but no file path received." # Non-retryable
            else: 
                error_desc = response_json_getfile.get('description', 'Unknown TG error (getFile, ok=false)')
                logging.error(f"[{log_prefix}] API error getFile (ok=false, Attempt {attempt_getf+1}): {error_desc}. Resp: {response_json_getfile}")
                return None, f"API error getting file path: {error_desc}" # Non-retryable

        except requests.exceptions.HTTPError as e:
            last_exception_getfile = e
            err_details = str(e)
            if e.response is not None:
                 err_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
            logging.error(f"[{log_prefix}] getFile HTTP Error (Attempt {attempt_getf+1}): {err_details}", exc_info=False)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                return None, f"Client error getting download URL: {err_details}"

        except requests.exceptions.Timeout as e:
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Timeout attempt {attempt_getf+1}: {e}")
        except requests.exceptions.ConnectionError as e: # This is the error we are seeing
            last_exception_getfile = e; logging.warning(f"[{log_prefix}] getFile Connection error attempt {attempt_getf+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_getfile = e; logging.error(f"[{log_prefix}] getFile RequestException (Attempt {attempt_getf+1}): {e}", exc_info=True)
            # Make this potentially non-retryable if it's a clear client error
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                return None, f"Request error getting download URL (Client): {str(e)}"
            # Otherwise, it will be retried if attempts remain

        except json.JSONDecodeError as e:
             status = response_getfile.status_code if response_getfile else 'N/A'
             body = response_getfile.text if response_getfile else 'N/A'
             logging.error(f"[{log_prefix}] Invalid JSON (getFile). Status: {status}, Body: {body}", exc_info=True)
             return None, "Invalid response from Telegram (getFile)." # Non-retryable
        except Exception as e:
             last_exception_getfile = e # Capture for retry
             logging.error(f"[{log_prefix}] Unexpected error getFile attempt {attempt_getf+1}: {e}", exc_info=True)
        
        # Retry logic for getFile
        if attempt_getf == API_RETRY_ATTEMPTS: # If this was the last attempt
            if direct_download_url is None: # And we still don't have a URL
                final_error_msg = f"Failed to get download URL after {API_RETRY_ATTEMPTS + 1} attempts."
                if last_exception_getfile: final_error_msg += f" Last error: {str(last_exception_getfile)}"
                logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_getfile if last_exception_getfile else False)
                return None, final_error_msg
        # If not the last attempt, and an exception occurred, the loop continues (implicit retry)

    if not direct_download_url:
         logging.error(f"[{log_prefix}] Exited getFile loop without URL. This implies all retries failed or an unhandled case.")
         # This should ideally be caught by the retry logic's final error message.
         # If last_exception_getfile has a value, use it.
         err_msg_fallback = f"Unknown error obtaining download URL. Last known issue: {str(last_exception_getfile)}" if last_exception_getfile else "Unknown error obtaining download URL after all attempts."
         return None, err_msg_fallback

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
                stream=True, 
                timeout=(
                    TELEGRAM_API_TIMEOUTS['connect'],
                    TELEGRAM_API_TIMEOUTS.get('download_file', TELEGRAM_API_TIMEOUTS['read'])
                )
            )

            if response_dl.status_code == 400 and response_dl.request and response_dl.request.method == 'GET':
                 try:
                     error_text_sample = response_dl.text 
                     if "file is too big" in error_text_sample.lower():
                         logging.error(f"[{log_prefix}] Telegram Error: File is too big to be downloaded (Status 400). Response: {error_text_sample[:200]}")
                         return None, "Telegram error: File is too big to be downloaded." 
                     else:
                         logging.warning(f"[{log_prefix}] Received 400 during content download, but not 'file too big'. Response: {error_text_sample[:200]}")
                         response_dl.raise_for_status() 
                 except Exception as read_err:
                      logging.error(f"[{log_prefix}] Error reading response body for 400 error check: {read_err}")
                      response_dl.raise_for_status() 

            response_dl.raise_for_status() 
            file_content = response_dl.content
            if not file_content:
                logging.warning(f"[{log_prefix}] Downloaded content is empty (Attempt {attempt_dl+1}). Might retry.")
                last_exception_download = ValueError("Downloaded empty content") 
                if attempt_dl == API_RETRY_ATTEMPTS:
                    return None, "Downloaded empty content after multiple attempts."
                continue 

            logging.info(f"[{log_prefix}] Downloaded {format_bytes(len(file_content))} (Attempt {attempt_dl+1}).")
            return file_content, None 

        except requests.exceptions.HTTPError as e:
            last_exception_download = e
            err_details = str(e)
            if e.response is not None:
                 err_details += f" | Status: {e.response.status_code} | Response: {e.response.text[:200]}" 
            logging.error(f"[{log_prefix}] Download HTTP Error (Attempt {attempt_dl+1}): {err_details}", exc_info=False)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                 return None, f"Client error downloading content: {err_details}"

        except requests.exceptions.Timeout as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Timeout attempt {attempt_dl+1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_exception_download = e; logging.warning(f"[{log_prefix}] Download Connection error attempt {attempt_dl+1}: {e}")
        except requests.exceptions.RequestException as e:
            last_exception_download = e; logging.error(f"[{log_prefix}] Download RequestException (Attempt {attempt_dl+1}): {e}", exc_info=True)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code not in [408, 429]:
                return None, f"Request error downloading content (Client): {str(e)}"
        except Exception as e:
             last_exception_download = e
             logging.error(f"[{log_prefix}] Unexpected error download content attempt {attempt_dl+1}: {e}", exc_info=True)
        
        # Retry logic for download
        if attempt_dl == API_RETRY_ATTEMPTS: 
            final_error_msg = f"Failed to download content after {API_RETRY_ATTEMPTS + 1} attempts."
            if last_exception_download: final_error_msg += f" Last error: {str(last_exception_download)}"
            logging.error(f"[{log_prefix}] {final_error_msg}", exc_info=last_exception_download if last_exception_download else False)
            return None, final_error_msg

    logging.error(f"[{log_prefix}] Download content loop finished unexpectedly.")
    err_msg_fallback_dl = f"Unknown error during content download. Last known issue: {str(last_exception_download)}" if last_exception_download else "Unknown error during content download after all attempts."
    return None, err_msg_fallback_dl

# --- Logging Confirmation ---
logging.info("Telegram API functions updated with specific 'file too big' handling and format_bytes logging.")

logging.info("Telegram API functions defined with Session and Retries.")


