# --- Imports ---
import requests
import logging
import json

# --- Import config variables ---
from config import TELEGRAM_BOT_TOKEN

# --- Telegram API Interaction ---
def send_file_to_telegram(file_object, filename, target_chat_id):
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
    files_payload = {'document': (filename, file_object)}
    data_payload = {'chat_id': target_chat_id}
    logging.info(f"Sending file '{filename}' to Telegram chat ID: {target_chat_id}")

    try:
        response = requests.post(api_url, data=data_payload, files=files_payload, timeout=60)
        response.raise_for_status() # Check for HTTP errors like 404, 500
        response_json = response.json()

        if response_json.get('ok'):
            logging.info(f"Telegram API success for '{filename}'.")
            return True, f"File '{filename}' sent successfully!", response_json
        else:
            error_desc = response_json.get('description', 'Unknown Telegram error')
            logging.error(f"Telegram API Error for '{filename}': {error_desc} (Full Response: {response.text})")
            return False, f"Telegram API Error: {error_desc}", None

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out sending '{filename}' to Telegram.", exc_info=True)
        return False, "Error: The request to Telegram timed out.", None
    except requests.exceptions.RequestException as e:
        error_details = str(e)
        if e.response is not None:
            error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
        logging.error(f"Network/Request Error sending '{filename}' to Telegram: {error_details}", exc_info=True)
        return False, f"Network/Request Error: {error_details}", None
    except json.JSONDecodeError:
        logging.error(f"Telegram returned non-JSON response for '{filename}'. Status: {response.status_code}, Body: {response.text}", exc_info=True)
        return False, "Error: Received invalid response from Telegram.", None
    except Exception as e:
        logging.error(f"Unexpected error sending '{filename}' to Telegram: {e}", exc_info=True)
        return False, f"An unexpected error occurred: {e}", None

def download_telegram_file_content(file_id):
    """
    Gets the temporary download URL for a file_id and downloads its content.

    Args:
        file_id: The file_id from Telegram.

    Returns:
        Bytes object containing the file content, or None if an error occurs.
        Also returns an error message string (None on success).
    """
    logging.info(f"Attempting to get download URL for file_id: {file_id}")
    get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
    params = {'file_id': file_id}
    direct_download_url = None
    error_message = None

    try:
        response = requests.get(get_file_url, params=params, timeout=30)
        response.raise_for_status()
        response_json = response.json()

        if response_json.get('ok'):
            file_path = response_json.get('result', {}).get('file_path')
            if file_path:
                direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                logging.info(f"Obtained temporary download URL: {direct_download_url}")
            else:
                logging.error(f"getFile OK but no 'file_path' for file_id {file_id}. Response: {response_json}")
                error_message = "Telegram API OK but no file path received."
                return None, error_message
        else:
            error_desc = response_json.get('description', 'Unknown Telegram error')
            logging.error(f"Telegram API error (getFile) for file_id {file_id}: {error_desc}. Response: {response_json}")
            error_message = f"Telegram API error getting file path: {error_desc}"
            return None, error_message

    except requests.exceptions.Timeout:
        logging.error(f"Timeout calling Telegram getFile API for file_id: {file_id}", exc_info=True)
        error_message = "Timeout getting download URL from Telegram."
        return None, error_message
    except requests.exceptions.RequestException as e:
        error_details = str(e)
        if e.response is not None:
             error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
        logging.error(f"Network/Request error calling Telegram getFile API: {error_details}", exc_info=True)
        error_message = f"Network error getting download URL: {error_details}"
        return None, error_message
    except json.JSONDecodeError:
        logging.error(f"Telegram getFile response was not valid JSON. Status: {response.status_code if 'response' in locals() else 'N/A'}, Body: {response.text if 'response' in locals() else 'N/A'}", exc_info=True)
        error_message = "Invalid response from Telegram (getFile)."
        return None, error_message
    except Exception as e:
        logging.error(f"Unexpected error during getFile for file_id {file_id}: {e}", exc_info=True)
        error_message = "Unexpected error getting download URL."
        return None, error_message

    # If we have the URL, now download the content
    if direct_download_url:
        logging.info(f"Attempting to download content from: {direct_download_url}")
        try:
            # Use stream=True for potentially large files, although we read all at once here.
            # Timeout increased for potentially large downloads
            download_response = requests.get(direct_download_url, stream=True, timeout=120)
            download_response.raise_for_status()

            # Read the content into bytes
            file_content = download_response.content
            logging.info(f"Successfully downloaded {len(file_content)} bytes for file_id {file_id}.")
            return file_content, None # Success: return content, no error message

        except requests.exceptions.Timeout:
            logging.error(f"Timeout downloading file content for file_id: {file_id} from {direct_download_url}", exc_info=True)
            error_message = "Timeout downloading file content from Telegram."
            return None, error_message
        except requests.exceptions.RequestException as e:
            error_details = str(e)
            if e.response is not None:
                 error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
            logging.error(f"Network/Request error downloading file content: {error_details}", exc_info=True)
            error_message = f"Network error downloading content: {error_details}"
            return None, error_message
        except Exception as e:
            logging.error(f"Unexpected error downloading content for file_id {file_id}: {e}", exc_info=True)
            error_message = "Unexpected error downloading content."
            return None, error_message
    else:
         # Should not happen if previous checks worked, but defensive coding
         return None, error_message

logging.info("Telegram API functions defined.")