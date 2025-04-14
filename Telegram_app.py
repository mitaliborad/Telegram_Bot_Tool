# --- Imports ---
import io
import os
import requests
from flask import Flask, request, render_template, flash, redirect, url_for, make_response, jsonify
import json
from datetime import datetime, timezone
import logging
import time # Needed for unique log filename

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' # Replace with your actual Bot Token
TELEGRAM_CHAT_ID = '-4603853425'     # Replace with your actual Chat ID
METADATA_FILE = 'metadata.json'
CHUNK_SIZE = 45 * 1024 * 1024 # ~45MB chunk size for splitting large files

# --- Logging Setup ---
LOG_DIR = "Selenium-Logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    print(f"Created logging directory: {LOG_DIR}")

log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
# Use force=True if you might re-run setup in the same process (e.g., during development)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() # Also print logs to console
    ],
    force=True
)
logging.info("Logging configured. Log file: %s", log_filename)

# --- Basic Configuration Check ---
if 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN or 'YOUR_CHAT_ID' in TELEGRAM_CHAT_ID:
    logging.warning("="*60)
    logging.warning("!!! WARNING: Please replace placeholder BOT TOKEN and CHAT ID !!!")
    logging.warning("="*60)

# --- Metadata Handling ---
def load_metadata():
    logging.debug(f"Attempting to load metadata from {METADATA_FILE}")
    if not os.path.exists(METADATA_FILE):
        logging.info(f"Metadata file '{METADATA_FILE}' not found. Returning empty data.")
        return {}
    try:
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logging.info(f"Successfully loaded metadata from '{METADATA_FILE}'.")
            return data
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from '{METADATA_FILE}'. File might be corrupted. Returning empty data.", exc_info=True)
        return {}
    except IOError as e:
        logging.error(f"Could not read metadata file '{METADATA_FILE}': {e}", exc_info=True)
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred loading metadata: {e}", exc_info=True)
        return {}

def save_metadata(data):
    logging.debug(f"Attempting to save metadata to {METADATA_FILE}")
    try:
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully saved metadata to '{METADATA_FILE}'.")
        return True
    except IOError as e:
        logging.error(f"Could not write metadata file '{METADATA_FILE}': {e}", exc_info=True)
        return False
    except TypeError as e:
        logging.error(f"Data type error saving metadata (invalid data?): {e}", exc_info=True)
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred saving metadata: {e}", exc_info=True)
        return False

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.')
app.secret_key = 'a_simple_secret_key_for_now' # Important for flashing messages
logging.info("Flask application initialized.")

# --- Telegram API Interaction ---
def send_file_to_telegram(file_object, filename):
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
    files_payload = {'document': (filename, file_object)}
    data_payload = {'chat_id': TELEGRAM_CHAT_ID}
    logging.info(f"Sending file '{filename}' to Telegram chat ID: {TELEGRAM_CHAT_ID}")

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

# --- Flask Routes ---
@app.route('/')
def index():
    logging.info("Serving index page.")
    try:
        return render_template('index.html')
    except Exception as e:
        logging.error(f"Error rendering index.html: {e}", exc_info=True)
        return "Error: Could not load the upload page.", 500

@app.route('/upload', methods=['POST'])
def upload_file():
    logging.info("Received file upload request.")
    start_time = time.time()

    # Basic validation
    if 'file' not in request.files:
        flash('No file part in the request!', 'error')
        logging.warning("Upload failed: 'file' part missing in request.")
        return redirect(url_for('index'))

    username = request.form.get('username', '').strip()
    if not username:
        flash('Username is required!', 'error')
        logging.warning("Upload failed: Username missing.")
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        flash('No file selected!', 'error')
        logging.warning(f"Upload failed for user '{username}': No file selected.")
        return redirect(url_for('index'))

    original_filename = file.filename
    logging.info(f"Processing upload: User='{username}', File='{original_filename}'")

    try:
        # Determine file size
        file.seek(0, os.SEEK_END)
        total_size = file.tell()
        file.seek(0, os.SEEK_SET)
        logging.info(f"File '{original_filename}' size: {total_size} bytes.")

        if total_size == 0:
             flash('Error: Uploaded file is empty.', 'error')
             logging.warning(f"Upload failed for '{original_filename}' (user: {username}): File is empty.")
             return redirect(url_for('index'))

        # Decide workflow: single file or split
        if total_size <= CHUNK_SIZE:
            # --- Single File Upload ---
            logging.info(f"'{original_filename}' is small ({total_size} bytes). Sending as single file.")
            success, message, tg_response_json = send_file_to_telegram(file, original_filename)

            if success and tg_response_json:
                logging.info(f"Single file '{original_filename}' sent successfully via Telegram.")
                try:
                    result_data = tg_response_json.get('result', {})
                    message_id = result_data.get('message_id')
                    doc_data = result_data.get('document', {})
                    file_id = doc_data.get('file_id')
                    file_unique_id = doc_data.get('file_unique_id')

                    if not message_id or not file_unique_id:
                        logging.error(f"Upload SUCCESS for '{original_filename}' but failed to extract IDs from response: {tg_response_json}")
                        flash(f"File '{original_filename}' sent, but tracking info missing.", 'warning')
                        return redirect(url_for('index'))

                    logging.info(f"Extracted IDs for '{original_filename}': MsgID={message_id}, UniqueID={file_unique_id}")

                    metadata = load_metadata()
                    timestamp = datetime.now(timezone.utc).isoformat()
                    new_file_record = {
                        "original_filename": original_filename,
                        "sent_filename": original_filename,
                        "is_split": False,
                        "is_compressed": False, # Compression was removed
                        "total_size": total_size,
                        "telegram_message_id": message_id,
                        "telegram_file_id": file_id,
                        "telegram_file_unique_id": file_unique_id,
                        "upload_timestamp": timestamp,
                        "username": username # Store username with file record
                    }
                    user_files_list = metadata.setdefault(username, [])
                    user_files_list.append(new_file_record)

                    if save_metadata(metadata):
                        logging.info(f"Successfully saved metadata for single file '{original_filename}'.")
                        flash(f"File '{original_filename}' sent successfully!", 'success')
                    else:
                        logging.error(f"CRITICAL: File '{original_filename}' sent, but FAILED TO SAVE METADATA.")
                        flash(f"File '{original_filename}' sent, but error saving tracking info!", 'error')

                except Exception as e:
                    logging.error(f"Error processing response/saving metadata for single file '{original_filename}': {e}", exc_info=True)
                    flash(f"File '{original_filename}' sent, but internal error occurred processing result.", 'error')
            else:
                logging.error(f"Failed to send single file '{original_filename}' to Telegram. Message: {message}")
                flash(message, 'error')

            # End of single file workflow
            processing_time = time.time() - start_time
            logging.info(f"Finished processing single file '{original_filename}' in {processing_time:.2f} seconds.")
            return redirect(url_for('index'))

        else:
            # --- Split File Upload ---
            logging.info(f"'{original_filename}' is large ({total_size} bytes). Starting split upload (Chunk size: {CHUNK_SIZE} bytes).")
            chunk_number = 0
            uploaded_chunks_metadata = []
            bytes_read = 0

            while True:
                chunk_start_time = time.time()
                chunk_number += 1
                logging.info(f"Reading chunk {chunk_number} for '{original_filename}' starting at byte {bytes_read}.")
                file_chunk_data = file.read(CHUNK_SIZE)
                current_chunk_size = len(file_chunk_data)

                if not file_chunk_data:
                    logging.info(f"Finished reading all chunks for '{original_filename}'.")
                    break # End of file

                bytes_read += current_chunk_size
                logging.info(f"Read chunk {chunk_number} ({current_chunk_size} bytes) for '{original_filename}'. Total read: {bytes_read}/{total_size}")

                chunk_filename = f"{original_filename}.part_{str(chunk_number).zfill(3)}"
                chunk_file_object = io.BytesIO(file_chunk_data) # Wrap bytes chunk in a file-like object

                logging.info(f"Attempting to send chunk: '{chunk_filename}'")
                success, message, tg_response_json = send_file_to_telegram(chunk_file_object, chunk_filename)
                chunk_file_object.close() # Release memory for this chunk object

                if success and tg_response_json:
                    logging.info(f"Chunk '{chunk_filename}' sent successfully.")
                    try:
                        result_data = tg_response_json.get('result', {})
                        message_id = result_data.get('message_id')
                        doc_data = result_data.get('document', {})
                        file_id = doc_data.get('file_id')
                        file_unique_id = doc_data.get('file_unique_id')

                        if not message_id or not file_unique_id:
                           logging.error(f"Missing IDs in Telegram response for chunk '{chunk_filename}': {tg_response_json}")
                           raise ValueError("Missing message_id or file_unique_id in chunk response")

                        chunk_meta = {
                            "part_number": chunk_number,
                            "chunk_filename": chunk_filename,
                            "message_id": message_id,
                            "file_id": file_id,
                            "file_unique_id": file_unique_id
                        }
                        uploaded_chunks_metadata.append(chunk_meta)
                        chunk_send_time = time.time() - chunk_start_time
                        logging.info(f"Successfully processed chunk '{chunk_filename}' (MsgID={message_id}) in {chunk_send_time:.2f}s.")

                    except Exception as e:
                        logging.error(f"Error processing Telegram response for chunk '{chunk_filename}': {e}. Aborting split upload.", exc_info=True)
                        flash(f"Error processing response for chunk {chunk_number}. Upload incomplete. Please try again.", 'error')
                        # TODO: Optional cleanup: Delete already uploaded chunks from Telegram?
                        return redirect(url_for('index')) # Abort
                else:
                    logging.error(f"Failed to send chunk '{chunk_filename}'. Aborting split upload. Error: {message}")
                    flash(f"Error sending chunk {chunk_number} ('{chunk_filename}'): {message}. Upload incomplete. Please try again.", 'error')
                    # TODO: Optional cleanup
                    return redirect(url_for('index')) # Abort

            # --- After the loop: Check if all chunks were processed ---
            expected_chunks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
            if len(uploaded_chunks_metadata) == expected_chunks:
                logging.info(f"All {expected_chunks} chunks for '{original_filename}' uploaded successfully. Saving metadata.")
                metadata = load_metadata()
                timestamp = datetime.now(timezone.utc).isoformat()
                new_file_record = {
                    "original_filename": original_filename,
                    "is_split": True,
                    "is_compressed": False, # Compression was removed
                    "total_size": total_size,
                    "chunk_size": CHUNK_SIZE,
                    "num_chunks": expected_chunks,
                    "chunks": uploaded_chunks_metadata, # List of details for each chunk
                    "upload_timestamp": timestamp,
                    "username": username # Store username
                }
                user_files_list = metadata.setdefault(username, [])
                user_files_list.append(new_file_record)

                if save_metadata(metadata):
                    logging.info(f"Successfully saved metadata for split file '{original_filename}'.")
                    flash(f"Large file '{original_filename}' split and sent successfully!", 'success')
                else:
                    logging.error(f"CRITICAL: All chunks for '{original_filename}' sent, but FAILED TO SAVE METADATA.")
                    flash(f"File '{original_filename}' sent, but error saving tracking info!", 'error')
            else:
                # This path indicates an inconsistency, though errors in the loop should prevent reaching here.
                logging.error(f"Inconsistency after upload loop for '{original_filename}'. Expected {expected_chunks} chunks, got metadata for {len(uploaded_chunks_metadata)}. Aborting save.")
                flash(f"An internal inconsistency occurred after uploading chunks for '{original_filename}'. Please check logs.", 'error')

            # End of split file workflow
            processing_time = time.time() - start_time
            logging.info(f"Finished processing split file '{original_filename}' in {processing_time:.2f} seconds.")
            return redirect(url_for('index'))

    except Exception as e:
        logging.error(f"Unexpected error during upload processing for '{original_filename}' (user: {username}): {e}", exc_info=True)
        flash(f"An internal error occurred processing your upload: {e}", 'error')
        return redirect(url_for('index'))

@app.route('/files/<username>', methods=['GET'])
def list_user_files(username):
    logging.info(f"Request received to list files for user: '{username}'")
    metadata = load_metadata()
    user_files = metadata.get(username, [])

    if not user_files:
        logging.info(f"No files found in metadata for user: '{username}'")
    else:
        logging.info(f"Found {len(user_files)} file records for user '{username}'.")

    # Return the list (even if empty) as JSON
    return jsonify(user_files)

@app.route('/download/<username>/<filename>', methods=['GET'])
def download_user_file(username, filename):
    logging.info(f"Download request: User='{username}', File='{filename}'")

    metadata = load_metadata()
    user_files = metadata.get(username, [])
    file_info = next((f for f in user_files if f.get('original_filename') == filename), None)

    if not file_info:
        logging.warning(f"Download failed: File '{filename}' not found for user '{username}'.")
        flash(f"Error: File '{filename}' not found for user '{username}'.", 'error')
        return redirect(url_for('index'))

    # --- Crucial Check: Can we download this file type? ---
    if file_info.get('is_split', False):
        logging.warning(f"Download attempt failed for SPLIT file '{filename}' (user: '{username}'). Not implemented.")
        flash(f"Error: Downloading split files ('{filename}') is not yet supported.", 'error')
        return redirect(url_for('index'))
    # --- End Check ---

    telegram_file_id = file_info.get('telegram_file_id')
    if not telegram_file_id:
        logging.error(f"Download failed: Metadata for '{filename}' (user: '{username}') missing 'telegram_file_id'. Record: {file_info}")
        flash(f"Error: Cannot download '{filename}'. File tracking info is incomplete.", 'error')
        return redirect(url_for('index'))

    logging.info(f"Found file_id '{telegram_file_id}' for '{filename}' (user: '{username}'). Requesting download path.")

    get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
    params = {'file_id': telegram_file_id}

    try:
        response = requests.get(get_file_url, params=params, timeout=30)
        response.raise_for_status()
        response_json = response.json()

        if response_json.get('ok'):
            file_path = response_json.get('result', {}).get('file_path')
            if file_path:
                direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                logging.info(f"Obtained temporary download URL for '{filename}'. Redirecting user to: {direct_download_url}")
                return redirect(direct_download_url) # Send user's browser to Telegram download link
            else:
                logging.error(f"Download failed: Telegram responded OK for '{filename}' but no 'file_path'. Response: {response_json}")
                flash(f"Error: Could not get download link for '{filename}' (missing path).", 'error')
                return redirect(url_for('index'))
        else:
            error_desc = response_json.get('description', 'Unknown Telegram error')
            logging.error(f"Download failed: Telegram API error (getFile) for '{filename}': {error_desc}. Response: {response_json}")
            flash(f"Error: Telegram rejected the download request: {error_desc}", 'error')
            return redirect(url_for('index'))

    except requests.exceptions.Timeout:
        logging.error(f"Timeout calling Telegram getFile API for file_id: {telegram_file_id}", exc_info=True)
        flash("Error: Timed out preparing download.", 'error')
        return redirect(url_for('index'))
    except requests.exceptions.RequestException as e:
        error_details = str(e)
        if e.response is not None:
             error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
        logging.error(f"Network/Request error calling Telegram getFile API: {error_details}", exc_info=True)
        flash(f"Network Error preparing download: {error_details}", 'error')
        return redirect(url_for('index'))
    except json.JSONDecodeError:
        logging.error(f"Telegram getFile response was not valid JSON. Status: {response.status_code if 'response' in locals() else 'N/A'}, Body: {response.text if 'response' in locals() else 'N/A'}", exc_info=True)
        flash("Error: Received an invalid response from Telegram.", 'error')
        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"Unexpected error during download prep for '{filename}' (user: {username}): {e}", exc_info=True)
        flash("An unexpected internal error occurred preparing download.", 'error')
        return redirect(url_for('index'))

# --- Application Runner ---
if __name__ == '__main__':
    logging.info("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000, debug=True)