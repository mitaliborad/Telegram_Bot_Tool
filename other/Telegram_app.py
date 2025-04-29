# --- Imports ---
import io
import os
import requests
from flask import Flask, request, render_template, flash, redirect, url_for, make_response, jsonify, send_file, Response, stream_with_context
import json
from datetime import datetime, timezone
import logging
import time 
import zipfile
import tempfile
import uuid
import shutil
from dateutil import parser as dateutil_parser
import pytz
import math

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
TELEGRAM_CHAT_IDS = ['-4603853425','-1002614397019','-4641852757']
PRIMARY_TELEGRAM_CHAT_ID = TELEGRAM_CHAT_IDS[0] if TELEGRAM_CHAT_IDS else None
METADATA_FILE = 'metadata.json'
CHUNK_SIZE = 20 * 1024 * 1024 

# --- Logging Setup ---
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Ensure Temp Directory Exists ---
if not os.path.exists(UPLOADS_TEMP_DIR):
    try:
        os.makedirs(UPLOADS_TEMP_DIR)
        logging.info(f"Created temporary upload directory: {UPLOADS_TEMP_DIR}")
    except OSError as e:
        logging.error(f"Could not create temporary upload directory {UPLOADS_TEMP_DIR}: {e}", exc_info=True)

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
if 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN or 'YOUR_CHAT_ID' in TELEGRAM_CHAT_IDS:
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

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if seconds < 0: seconds = 0 # Avoid negative display
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

# --- Helper Functions ---
# ... (format_time function) ...

def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_bytes is None or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.')
app.secret_key = 'a_simple_secret_key_for_now' # Important for flashing messages
# Register the custom filter with Jinja
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Flask application initialized.")

upload_progress_data = {}
download_prep_data = {}

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

# --- Telegram API Interaction ---
# (send_file_to_telegram function is here)

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
def upload_file_legacy():

     logging.warning("Legacy /upload route hit - should be handled by /initiate-upload now.")

     return jsonify({"message": "Processing..."}), 200 

def upload_file():
    logging.info("Received file upload request.")
    route_start_time = time.time()
    #start_time = time.time()


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
    username = request.form.get('username')
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
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            
                file.seek(0)
                zip_file.writestr(original_filename, file.read())
            compressed_size = zip_buffer.tell()
            logging.info(f"Compressed '{original_filename}' to {compressed_size} bytes.")
            zip_buffer.seek(0)
            compressed_filename = f"{original_filename}.zip"

            # --- Timing Start (Single File) ---
            tg_send_start_time = time.time()
            logging.info(f"Starting Telegram send for single compressed file '{compressed_filename}'...")

            success, message, tg_response_json = send_file_to_telegram(zip_buffer, compressed_filename)

            # --- Timing End (Single File) ---
            tg_send_end_time = time.time()
            tg_send_duration = tg_send_end_time - tg_send_start_time
            logging.info(f"Finished Telegram send for '{compressed_filename}'. Duration: {tg_send_duration:.2f} seconds.")
            if compressed_size > 0 and tg_send_duration > 0:
                rate_bps = compressed_size / tg_send_duration
                logging.info(f"Average Upload Rate (Flask->Telegram): {rate_bps / 1024 / 1024:.2f} MB/s")

            zip_buffer.close()
            
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
                        "sent_filename": compressed_filename,
                        "is_split": False,
                        "is_compressed": True, # Compression was removed
                        "original_size": total_size,
                        "compressed_size": compressed_size,
                        "telegram_message_id": message_id,
                        "telegram_file_id": file_id,
                        "telegram_file_unique_id": file_unique_id,
                        "upload_timestamp": timestamp,
                        "username": username ,
                        "upload_duration_seconds": tg_send_duration
                    }
                    user_files_list = metadata.setdefault(username, [])
                    user_files_list.append(new_file_record)

                    if save_metadata(metadata):
                        logging.info(f"Successfully saved metadata for single file '{original_filename}'.")
                        flash(f"File '{original_filename}' sent successfully!", 'success')
                    else:
                        logging.error(f"CRITICAL: Compressed file '{compressed_filename}' sent, but FAILED TO SAVE METADATA.")
                        flash(f"File '{original_filename}' sent(compress), but error saving tracking info!", 'error')

                except Exception as e:
                    logging.error(f"Error processing response/saving metadata for single file '{original_filename}': {e}", exc_info=True)
                    flash(f"File '{original_filename}' sent, but internal error occurred processing result.", 'error')
            else:
                logging.error(f"Failed to send compressed file '{compressed_filename}' to Telegram. Message: {message}")
                flash(message, 'error')

            # End of single file workflow
            processing_time = time.time() - route_start_time
            logging.info(f"Finished processing single(compressed) file '{original_filename}' in {processing_time:.2f} seconds.")
            return redirect(url_for('index'))

        else:

            # --- Large File: Compress First, then Split ---
            logging.info(f"'{original_filename}' is large ({total_size} bytes). Compressing before splitting.")
            compressed_filename = f"{original_filename}.zip"
            temp_zip_filepath = None # Initialize variable for finally block
            original_file_stream = file # Keep a reference to the original stream

            try:
                # 1. Create a temporary file to store the compressed data
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip_handle:
                    temp_zip_filepath = temp_zip_handle.name # Get the actual path
                    logging.info(f"Created temporary file for compression: {temp_zip_filepath}")

                # 2. Compress the original file into the temporary file (chunk by chunk)
                logging.info(f"Compressing '{original_filename}' into temporary file...")
                compression_start_time = time.time()
                with zipfile.ZipFile(temp_zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                    original_file_stream.seek(0) # Go to the start of the uploaded file
                    # Use a buffer to avoid reading the whole original file at once
                    buffer_size = 4 * 1024 * 1024 # 4MB buffer
                    # Write the file content into the zip using the original filename
                    # We'll write it as one entry inside the zip
                    with zip_out.open(original_filename, 'w') as zip_entry:
                         # Read from the original file stream and write to the zip entry
                         while True:
                               chunk = original_file_stream.read(buffer_size) # Read from original file
                               if not chunk:
                                   break
                               zip_entry.write(chunk) # Write into the zip file

                compression_time = time.time() - compression_start_time
                compressed_total_size = os.path.getsize(temp_zip_filepath)
                logging.info(f"Finished compressing to '{temp_zip_filepath}'. Size: {compressed_total_size} bytes. Time: {compression_time:.2f}s.")

                # --- Now Split the *Compressed* Temporary File ---
                logging.info(f"Starting split upload for compressed file '{compressed_filename}' (Chunk size: {CHUNK_SIZE} bytes).")
                
                # --- ETA Initialization ---
                start_time_split_upload = None 
                bytes_successfully_sent = 0
                # --- End ETA Initialization ---

                chunk_number = 0
                uploaded_chunks_metadata = []
                bytes_read = 0
                total_tg_send_duration_split = 0

                # 3. Open the TEMPORARY COMPRESSED file for reading chunks
                with open(temp_zip_filepath, 'rb') as temp_file_to_read:
                    while True:
                        loop_chunk_start_time = time.time() 
                        chunk_number += 1
                        logging.info(f"Reading chunk {chunk_number} for COMPRESSED file '{compressed_filename}' starting at byte {bytes_read}.")

                        # 4. Read chunk from the TEMPORARY file
                        file_chunk_data = temp_file_to_read.read(CHUNK_SIZE)
                        current_chunk_size = len(file_chunk_data)

                        if not file_chunk_data:
                            logging.info(f"Finished reading all chunks for compressed file '{compressed_filename}'.")
                            break # End of compressed file

                        bytes_read += current_chunk_size
                        logging.info(f"Read chunk {chunk_number} ({current_chunk_size} bytes) for '{compressed_filename}'. Total read: {bytes_read}/{compressed_total_size}")

                        # 5. Use the COMPRESSED filename for the part name
                        chunk_filename = f"{compressed_filename}.part_{str(chunk_number).zfill(3)}"
                        chunk_file_object = io.BytesIO(file_chunk_data)

                        # --- ETA: Record start time on first chunk ---
                        if chunk_number == 1:
                            start_time_split_upload = time.time()
                        # --- End ETA Start Time ---


                        logging.info(f"Attempting to send chunk: '{chunk_filename}'")

                        # --- Timing Start (Chunk) ---
                        tg_chunk_send_start_time = time.time()
                        # --- End Timing Start ---

                        success, message, tg_response_json = send_file_to_telegram(chunk_file_object, chunk_filename)
                        
                        # --- Timing End (Chunk) ---
                        tg_chunk_send_end_time = time.time()
                        tg_chunk_duration = tg_chunk_send_end_time - tg_chunk_send_start_time
                        total_tg_send_duration_split += tg_chunk_duration # Accumulate
                        logging.info(f"Finished Telegram send for chunk '{chunk_filename}'. Duration: {tg_chunk_duration:.2f} seconds.")
                        if current_chunk_size > 0 and tg_chunk_duration > 0:
                            rate_bps = current_chunk_size / tg_chunk_duration
                            logging.info(f"  Chunk Upload Rate (Flask->Telegram): {rate_bps / 1024 / 1024:.2f} MB/s")
                        
                        chunk_file_object.close()

                        if success and tg_response_json:
                            bytes_successfully_sent += current_chunk_size

                            # --- ETA Calculation and Logging ---
                            if start_time_split_upload is not None and bytes_successfully_sent > 0:
                                elapsed_time = time.time() - start_time_split_upload
                                if elapsed_time > 0: # Avoid division by zero
                                    average_speed_bps = bytes_successfully_sent / elapsed_time
                                    if average_speed_bps > 0: # Avoid division by zero
                                        remaining_bytes = compressed_total_size - bytes_successfully_sent
                                        if remaining_bytes > 0:
                                            eta_seconds = remaining_bytes / average_speed_bps
                                            progress_percent = (bytes_successfully_sent / compressed_total_size) * 100
                                            logging.info(f"  Progress: {bytes_successfully_sent / (1024*1024):.1f}/{compressed_total_size / (1024*1024):.1f} MB ({progress_percent:.1f}%)")
                                            logging.info(f"  Average Speed: {average_speed_bps / (1024*1024):.2f} MB/s")
                                            logging.info(f"  Estimated Time Remaining (ETA): {format_time(eta_seconds)}")
                                        else:
                                            # If remaining_bytes is 0 or less, upload is effectively complete
                                            logging.info(f"  Progress: 100% - Final chunk processed.")
                                    else:
                                        logging.debug("  ETA calculation skipped: Average speed is zero.") # Less likely
                                else:
                                    logging.debug("  ETA calculation skipped: Elapsed time is zero.") # Should only happen briefly after first chunk
                            # --- End ETA Calculation and Logging ---

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

                                # 6. Store metadata for the chunk (using the compressed part name)
                                chunk_meta = {
                                    "part_number": chunk_number,
                                    "chunk_filename": chunk_filename, # Correct name stored
                                    "message_id": message_id,
                                    "file_id": file_id,
                                    "file_unique_id": file_unique_id ,
                                    "chunk_upload_duration_seconds": tg_chunk_duration
                                }
                                uploaded_chunks_metadata.append(chunk_meta)
                                loop_chunk_process_time = time.time() - loop_chunk_start_time
                                logging.info(f"Successfully processed chunk '{chunk_filename}' (MsgID={message_id}) in {tg_chunk_duration:.2f}s.")

                            except Exception as e:
                                logging.error(f"Error processing Telegram response for chunk '{chunk_filename}': {e}. Aborting split upload.", exc_info=True)
                                flash(f"Error processing response for chunk {chunk_number} of compressed file. Upload incomplete.", 'error')
                                return redirect(url_for('index')) # Abort
                        else:
                            logging.error(f"Failed to send chunk '{chunk_filename}'. Aborting split upload. Error: {message}")
                            flash(f"Error sending chunk {chunk_number} ('{chunk_filename}'): {message}. Upload incomplete.", 'error')
                            return redirect(url_for('index')) # Abort
                    # --- End of while loop for reading/sending chunks ---
                # --- End of with open(temp_zip_filepath...) ---

                # --- After the loop: Check if all chunks were processed ---
                # 7. Use COMPRESSED size for calculation
                expected_chunks = (compressed_total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
                if len(uploaded_chunks_metadata) == expected_chunks:
                    logging.info(f"All {expected_chunks} chunks for compressed file '{compressed_filename}' uploaded successfully. Saving metadata.")
                    if compressed_total_size > 0 and total_tg_send_duration_split > 0:
                         avg_rate_bps = compressed_total_size / total_tg_send_duration_split
                         logging.info(f"Overall Average Upload Rate (Flask->Telegram): {avg_rate_bps / 1024 / 1024:.2f} MB/s")
                    
                    metadata = load_metadata()
                    timestamp = datetime.now(timezone.utc).isoformat()

                    # 8. Update metadata record for compressed & split file
                    new_file_record = {
                        "original_filename": original_filename,     # Original name before zip
                        "sent_filename": compressed_filename,       # Name of the zip file (conceptually)
                        "is_split": True,
                        "is_compressed": True,                      # <<< SET TO TRUE >>>
                        "original_size": total_size,                # Size before compression
                        "compressed_total_size": compressed_total_size, # Size after compression
                        "chunk_size": CHUNK_SIZE,
                        "num_chunks": expected_chunks,
                        "chunks": uploaded_chunks_metadata,         # List includes .zip.part_xxx names
                        "upload_timestamp": timestamp,
                        "username": username,
                        "total_upload_duration_seconds": total_tg_send_duration_split
                    }
                    user_files_list = metadata.setdefault(username, [])
                    user_files_list.append(new_file_record)

                    if save_metadata(metadata):
                        logging.info(f"Successfully saved metadata for compressed split file '{original_filename}'.")
                        flash(f"Large file '{original_filename}' compressed, split, and sent successfully!", 'success')
                    else:
                        logging.error(f"CRITICAL: All chunks for compressed '{compressed_filename}' sent, but FAILED TO SAVE METADATA.")
                        flash(f"File '{original_filename}' sent (compressed & split), but error saving tracking info!", 'error')
                else:
                    # This path indicates an inconsistency
                    logging.error(f"Inconsistency after upload loop for '{compressed_filename}'. Expected {expected_chunks} chunks, got metadata for {len(uploaded_chunks_metadata)}. Aborting save.")
                    flash(f"An internal inconsistency occurred after uploading chunks for '{original_filename}' (compressed). Please check logs.", 'error')

                # End of split file workflow
                processing_time = time.time() - route_start_time            
                logging.info(f"Finished processing compressed split file '{original_filename}' in {processing_time:.2f} seconds.")
                return redirect(url_for('index'))

            # --- End of try block for large file processing ---
            finally:
                # 9. --- Crucial Cleanup: Delete the temporary file ---
                if temp_zip_filepath and os.path.exists(temp_zip_filepath):
                    try:
                        os.remove(temp_zip_filepath)
                        logging.info(f"Successfully deleted temporary compressed file: {temp_zip_filepath}")
                    except OSError as e:
                        logging.error(f"Error deleting temporary compressed file '{temp_zip_filepath}': {e}", exc_info=True)
                # Close the original file stream if it's still open (Flask might handle this, but belt-and-suspenders)
                if original_file_stream and hasattr(original_file_stream, 'close') and not original_file_stream.closed:
                     try:
                         original_file_stream.close()
                         logging.debug("Closed original file stream in finally block.")
                     except Exception as e:
                         logging.warning(f"Exception while closing original file stream in finally block: {e}")
            # --- End of finally block ---
        # --- End of else block for large files ---

    except Exception as e:
        logging.error(f"Unexpected error during upload processing for '{original_filename}' (user: {username}): {e}", exc_info=True)
        flash(f"An internal error occurred processing your upload: {e}", 'error')
        if 'temp_zip_filepath' in locals() and temp_zip_filepath and os.path.exists(temp_zip_filepath):
             try: os.remove(temp_zip_filepath)
             except Exception: pass
        if 'original_file_stream' in locals() and original_file_stream and hasattr(original_file_stream, 'close') and not original_file_stream.closed:
            try: original_file_stream.close()
            except Exception: pass
        return redirect(url_for('index'))


@app.route('/initiate-upload', methods=['POST'])
def initiate_upload():
    logging.info("Received request to initiate upload.")
    if 'file' not in request.files:
        logging.warning("Initiate upload failed: 'file' part missing.")
        return jsonify({"error": "No file part in the request"}), 400

    username = request.form.get('username', '').strip()
    if not username:
        logging.warning("Initiate upload failed: Username missing.")
        return jsonify({"error": "Username is required"}), 400

    file = request.files['file']
    if file.filename == '':
        logging.warning(f"Initiate upload failed for user '{username}': No file selected.")
        return jsonify({"error": "No file selected"}), 400

    original_filename = file.filename
    upload_id = str(uuid.uuid4())
    temp_file_path = os.path.join(UPLOADS_TEMP_DIR, f"{upload_id}_{original_filename}")

    try:
        logging.info(f"Saving temporary file for upload_id '{upload_id}' to '{temp_file_path}'")
        file.save(temp_file_path)
        logging.info(f"Temporary file saved successfully for '{original_filename}' (ID: {upload_id}).")

        # Store minimal info needed for the stream route
        # We will fetch the full file size later in the stream if needed
        upload_progress_data[upload_id] = {
            "status": "initiated",
            "original_filename": original_filename,
            "temp_file_path": temp_file_path,
            "username": username,
            "error": None,
            # Add other initial data as needed
        }

        return jsonify({"upload_id": upload_id, "filename": original_filename})

    except Exception as e:
        logging.error(f"Error saving temporary file for '{original_filename}': {e}", exc_info=True)
        # Clean up partial file if it exists
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except OSError:
                pass
        return jsonify({"error": f"Server error saving file: {e}"}), 500
    
@app.route('/stream-progress/<upload_id>')
def stream_progress(upload_id):
    logging.info(f"SSE connection request for upload_id: {upload_id}")
    if upload_id not in upload_progress_data or upload_progress_data[upload_id]['status'] == 'completed':
         # Handle cases where the ID is invalid or already done (maybe client reconnected)
         # For simplicity, just return an event saying it's unknown or complete
         def unknown_stream():
              yield f"event: error\ndata: {json.dumps({'message': 'Unknown or completed upload ID'})}\n\n"
         logging.warning(f"Upload ID '{upload_id}' not found or already completed.")
         return Response(unknown_stream(), mimetype='text/event-stream')

    # Use stream_with_context to ensure generator runs within application context
    # Pass the actual processing function (which will be a generator)
    return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')

# --- Placeholder for the core processing logic (will be implemented next) ---
# --- MODIFIED: process_upload_and_generate_updates (Step 3a Applied) ---
# --- Function with Step 3a and Step 3b applied ---
def process_upload_and_generate_updates(upload_id):
    """
    Processes the upload for the given ID, yielding SSE progress updates.
    Sends file/chunks to multiple chat IDs.
    """
    logging.info(f"[{upload_id}] Starting processing...")
    upload_data = upload_progress_data.get(upload_id)

    if not upload_data or not upload_data.get('temp_file_path'):
        logging.error(f"[{upload_id}] Critical error: Upload data or temp_file_path missing.")
        yield f"event: error\ndata: {json.dumps({'message': 'Internal server error: Upload data missing.'})}\n\n"
        # Ensure status is updated even in this early error
        if upload_id in upload_progress_data:
             upload_progress_data[upload_id]['status'] = 'error'
             upload_progress_data[upload_id]['error'] = 'Upload data missing'
        return # Stop processing

    temp_file_path = upload_data['temp_file_path']
    original_filename = upload_data['original_filename']
    username = upload_data['username']
    logging.info(f"[{upload_id}] Processing upload: User='{username}', File='{original_filename}', TempPath='{temp_file_path}'")

    upload_data['status'] = 'processing'

    # --- Variables for potential large file processing ---
    temp_compressed_zip_filepath = None # For storing the result of compression if needed
    overall_start_time = time.time()
    access_id = None # Initialize access_id

    try:
        # --- Determine file size (from the temporary file) ---
        if not os.path.exists(temp_file_path):
             raise FileNotFoundError(f"Temporary file not found: {temp_file_path}")
        total_size = os.path.getsize(temp_file_path)
        logging.info(f"[{upload_id}] Original temp file size: {total_size} bytes.")

        if total_size == 0:
            raise ValueError("Uploaded file is empty.")

        # --- Yield initial size ---
        yield f"event: start\ndata: {json.dumps({'filename': original_filename, 'totalSize': total_size})}\n\n"

        # --- Generate Access ID early ---
        # We need it for the completion event even if metadata saving fails later
        access_id = uuid.uuid4().hex[:10]
        logging.info(f"[{upload_id}] Generated Access ID: {access_id}")

        # --- Decide workflow: single file or split ---
        if total_size <= CHUNK_SIZE:
            # ==============================================================
            # === Single File Upload Workflow (Multi-Chat Modifications) ===
            # ==============================================================
            logging.info(f"[{upload_id}] '{original_filename}' is small. Compressing and sending as single file.")
            yield f"event: status\ndata: {json.dumps({'message': 'Compressing file...'})}\n\n"

            zip_buffer = io.BytesIO()
            # Read from the temporary file on disk
            with open(temp_file_path, 'rb') as f_in, \
                 zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                 zip_file.writestr(original_filename, f_in.read())

            compressed_size = zip_buffer.tell()
            # zip_buffer.seek(0) # Seek happens inside the loop now
            compressed_filename = f"{original_filename}.zip"
            logging.info(f"[{upload_id}] Compressed '{original_filename}' to {compressed_size} bytes.")

            yield f"event: progress\ndata: {json.dumps({'bytesSent': 0, 'totalBytes': compressed_size, 'percentage': 0, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
            yield f"event: status\ndata: {json.dumps({'message': f'Sending to {len(TELEGRAM_CHAT_IDS)} locations...'})}\n\n"

            # --- Send to Multiple Chat IDs (Single File) ---
            send_results = [] # To store results from each chat ID send attempt
            overall_success = False # Track if primary send was successful
            combined_message = "Upload status unknown." # Default message
            primary_tg_response_json = None # Store the response from the primary chat

            tg_send_start_time = time.time() # Start timer before the loop

            # Loop through each chat ID defined in the configuration
            for chat_id_to_send in TELEGRAM_CHAT_IDS:
                try:
                    # IMPORTANT: Reset the buffer's read position for each send attempt
                    zip_buffer.seek(0)

                    logging.info(f"[{upload_id}] Attempting send '{compressed_filename}' to chat ID: {chat_id_to_send}")
                    # Call the modified function, passing the current chat ID from the loop
                    success, message, tg_response_json = send_file_to_telegram(zip_buffer, compressed_filename, chat_id_to_send)

                    # Store the result of this attempt
                    result_entry = {
                        "chat_id": chat_id_to_send,
                        "success": success,
                        "message": message,
                        "tg_response": tg_response_json # Store the raw JSON response
                    }
                    send_results.append(result_entry)

                    # Check if this was the primary chat ID
                    if chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                        if success:
                            overall_success = True # Mark overall success if primary worked
                            primary_tg_response_json = tg_response_json # Save primary response
                        # Always store the message from the primary attempt for user feedback/raising errors
                        combined_message = message

                    # Log errors for non-primary chats, but don't stop the loop
                    if not success and chat_id_to_send != PRIMARY_TELEGRAM_CHAT_ID:
                        logging.error(f"[{upload_id}] Failed to send '{compressed_filename}' to BACKUP chat ID {chat_id_to_send}: {message}")
                    elif not success and chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                        logging.error(f"[{upload_id}] Failed to send '{compressed_filename}' to PRIMARY chat ID {chat_id_to_send}: {message}")
                        # Optional: You could 'break' here if primary failure means aborting all backups too.
                        # For now, we continue to try backups even if primary fails, but overall_success remains False.

                except Exception as loop_error:
                    # Catch potential errors within the loop (e.g., unexpected issue with seek or the send function)
                    logging.error(f"[{upload_id}] Unexpected error during send loop for chat ID {chat_id_to_send}: {loop_error}", exc_info=True)
                    send_results.append({
                        "chat_id": chat_id_to_send, "success": False, "message": str(loop_error), "tg_response": None
                    })
                    # If it's the primary chat failing here, record the error message
                    if chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                        combined_message = f"Unexpected error sending to primary: {loop_error}"
                        # overall_success remains False

            # --- Timing and Cleanup after loop ---
            tg_send_duration = time.time() - tg_send_start_time # Stop timer after the loop
            zip_buffer.close() # Close the buffer once after all sends are attempted

            # --- Process Results ---
            # Check if the primary send was successful
            if overall_success and primary_tg_response_json:
                num_successful_sends = sum(1 for r in send_results if r['success'])
                logging.info(f"[{upload_id}] Single file '{original_filename}' sent successfully to PRIMARY chat ID {PRIMARY_TELEGRAM_CHAT_ID}.")
                logging.info(f"[{upload_id}] Total successful sends (including backups): {num_successful_sends} / {len(TELEGRAM_CHAT_IDS)}")

                # --- Yield Final Progress (Based on overall time for all sends) ---
                yield f"event: progress\ndata: {json.dumps({'bytesSent': compressed_size, 'totalBytes': compressed_size, 'percentage': 100, 'speedMBps': (compressed_size / (1024*1024) / tg_send_duration) if tg_send_duration > 0 else 0, 'etaFormatted': '00:00'})}\n\n"

                # --- Save Metadata (Modified Structure) ---
                try:
                    # Extract primary IDs just for potential convenience (optional)
                    primary_result_data = primary_tg_response_json.get('result', {})
                    primary_message_id = primary_result_data.get('message_id')
                    primary_doc_data = primary_result_data.get('document', {})
                    primary_file_id = primary_doc_data.get('file_id')
                    primary_file_unique_id = primary_result_data.get('file_unique_id')

                    # Ensure primary IDs were found (should be true if overall_success is true)
                    if not primary_message_id or not primary_file_unique_id:
                        logging.error(f"[{upload_id}] Inconsistency: Primary send succeeded but failed to extract IDs from its response: {primary_tg_response_json}")
                        # Handle this potential inconsistency? For now, log and continue, metadata might be incomplete.

                    # Create the list of detailed send locations for metadata
                    all_chat_details = []
                    for res in send_results:
                        detail = {"chat_id": res["chat_id"], "success": res["success"]}
                        if res["success"] and res["tg_response"]:
                            # Extract IDs if send was successful
                            res_data = res["tg_response"].get('result', {})
                            msg_id = res_data.get('message_id')
                            doc_data = res_data.get('document', {})
                            f_id = doc_data.get('file_id')
                            f_uid = doc_data.get('file_unique_id')
                            # Only add IDs if all are present
                            if msg_id and f_id and f_uid:
                                detail["message_id"] = msg_id
                                detail["file_id"] = f_id
                                detail["file_unique_id"] = f_uid
                            else:
                                # Mark as failure if IDs are missing even if API said success
                                detail["success"] = False
                                detail["error"] = "Missing IDs in successful response"
                                logging.warning(f"[{upload_id}] Marked send to {res['chat_id']} as failed due to missing IDs in response: {res['tg_response']}")
                        elif not res["success"]:
                            # Include the error message if the send failed
                            detail["error"] = res["message"]
                        all_chat_details.append(detail)

                    # Prepare the final metadata record
                    # access_id was generated earlier
                    metadata = load_metadata()
                    timestamp = datetime.now(timezone.utc).isoformat()
                    new_file_record = {
                        "original_filename": original_filename,
                        "sent_filename": compressed_filename,
                        "is_split": False,
                        "is_compressed": True,
                        "original_size": total_size,
                        "compressed_size": compressed_size,
                        # NEW: Store the detailed list of send results
                        "send_locations": all_chat_details,
                        "upload_timestamp": timestamp,
                        "username": username,
                        "upload_duration_seconds": tg_send_duration, # Reflects time for all attempts
                        "access_id": access_id
                        # Removed old single telegram_message_id, telegram_file_id etc.
                    }

                    # Append and save metadata
                    user_files_list = metadata.setdefault(username, [])
                    user_files_list.append(new_file_record)
                    if not save_metadata(metadata):
                         logging.error(f"[{upload_id}] CRITICAL: File sent (primary success), but FAILED TO SAVE METADATA.")
                         # Consider yielding a warning event to the UI?

                    # --- Yield Completion Event (using the landing page URL) ---
                    landing_page_url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                    logging.info(f"[{upload_id}] Generated landing page URL: {landing_page_url}")
                    yield f"event: complete\ndata: {json.dumps({'message': f'File {original_filename} uploaded successfully!', 'download_url': landing_page_url, 'filename': original_filename})}\n\n"
                    upload_data['status'] = 'completed'

                except Exception as meta_error:
                    # Catch errors specifically during metadata processing/saving
                    logging.error(f"[{upload_id}] Error processing response/saving metadata after multi-send (single file): {meta_error}", exc_info=True)
                    # Upload succeeded to primary, but metadata failed. Mark status accordingly.
                    upload_data['status'] = 'completed_metadata_error'
                    # Yield a completion event anyway, but maybe with a warning? Or rely on logs.
                    landing_page_url = url_for('get_file_by_access_id', access_id=access_id, _external=True) if access_id else None # Generate URL if possible
                    if landing_page_url:
                         yield f"event: complete\ndata: {json.dumps({'message': f'File {original_filename} uploaded (metadata error)!', 'download_url': landing_page_url, 'filename': original_filename})}\n\n"
                    else: # If access_id wasn't generated for some reason
                         yield f"event: error\ndata: {json.dumps({'message': f'File uploaded but metadata and link generation failed: {meta_error}'})}\n\n"


            else: # Primary send failed (overall_success is False)
                 # Raise an error using the message from the primary attempt
                 # This will be caught by the main try...except block and yield an error event
                 logging.error(f"[{upload_id}] Upload failed because PRIMARY send to {PRIMARY_TELEGRAM_CHAT_ID} failed.")
                 raise IOError(f"Primary Telegram API Error: {combined_message}")
            # --- End of Single File Section ---

        else:
            # ==============================================================
            # === Large File Upload Workflow (Multi-Chat Modifications) ====
            # ==============================================================
            logging.info(f"[{upload_id}] '{original_filename}' is large. Compressing before splitting.")
            compressed_filename = f"{original_filename}.zip"

            yield f"event: status\ndata: {json.dumps({'message': 'Compressing large file...'})}\n\n"

            # 1. Create a *new* temporary file for the compressed data
            with tempfile.NamedTemporaryFile(prefix=f"{upload_id}_comp_", suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR) as temp_zip_handle:
                temp_compressed_zip_filepath = temp_zip_handle.name
                logging.info(f"[{upload_id}] Created temporary file for compression result: {temp_compressed_zip_filepath}")

            # 2. Compress the *original* temporary file into the *new* compressed temp file
            compression_start_time = time.time()
            buffer_size = 4 * 1024 * 1024
            with open(temp_file_path, 'rb') as f_in, \
                 zipfile.ZipFile(temp_compressed_zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                 with zip_out.open(original_filename, 'w') as zip_entry:
                      while True:
                            chunk_read = f_in.read(buffer_size) # Renamed to avoid conflict
                            if not chunk_read: break
                            zip_entry.write(chunk_read)
            compression_time = time.time() - compression_start_time
            compressed_total_size = os.path.getsize(temp_compressed_zip_filepath)
            logging.info(f"[{upload_id}] Finished compressing to '{temp_compressed_zip_filepath}'. Size: {compressed_total_size} bytes. Time: {compression_time:.2f}s.")

            yield f"event: status\ndata: {json.dumps({'message': f'Starting chunked upload to {len(TELEGRAM_CHAT_IDS)} locations...'})}\n\n"
            # Update total size for progress calculation to the compressed size
            yield f"event: start\ndata: {json.dumps({'filename': compressed_filename, 'totalSize': compressed_total_size})}\n\n"

            # --- Now Split the *Compressed* Temporary File ---
            chunk_number = 0
            uploaded_chunks_metadata = []
            bytes_read_from_compressed = 0
            total_tg_send_duration_split = 0
            start_time_split_upload = None # ETA specific start time
            bytes_successfully_sent = 0  # ETA specific counter (for primary success)

            # 3. Open the COMPRESSED temporary file for reading chunks
            with open(temp_compressed_zip_filepath, 'rb') as temp_file_to_read:
                while True: # <<< START OF OUTER WHILE LOOP (Reading Chunks)
                    chunk_number += 1
                    logging.info(f"[{upload_id}] Reading chunk {chunk_number} for COMPRESSED file starting at byte {bytes_read_from_compressed}.")

                    file_chunk_data = temp_file_to_read.read(CHUNK_SIZE)
                    current_chunk_size = len(file_chunk_data)

                    if not file_chunk_data:
                        logging.info(f"[{upload_id}] Finished reading all chunks for compressed file.")
                        break # Exit outer while loop

                    # --- Code below THIS line MUST be indented inside the 'while True:' ---
                    bytes_read_from_compressed += current_chunk_size
                    logging.info(f"[{upload_id}] Read chunk {chunk_number} ({current_chunk_size} bytes). Total read: {bytes_read_from_compressed}/{compressed_total_size}")

                    chunk_part_filename = f"{compressed_filename}.part_{str(chunk_number).zfill(3)}"
                    # Create a new BytesIO object for each chunk iteration
                    chunk_file_object = io.BytesIO(file_chunk_data)

                    # --- ETA: Record start time on first chunk ---
                    if chunk_number == 1:
                        start_time_split_upload = time.time()

                    # --- MODIFIED START: Send Chunk to Multiple Chat IDs --- # <<< INDENTED
                    chunk_send_results = [] # Results for *this specific chunk* across all chats
                    chunk_overall_success = False # Success for *this chunk* (based on primary)
                    chunk_primary_tg_response_json = None
                    chunk_combined_message = "Chunk send status unknown."

                    # Timer for sending this chunk to ALL targets
                    tg_chunk_multi_send_start_time = time.time()

                    # <<< START OF INNER FOR LOOP (Sending Chunk to Chats) >>>
                    for chat_id_to_send in TELEGRAM_CHAT_IDS: # <<< INDENTED
                        try: # <<< INDENTED
                            # IMPORTANT: Reset chunk buffer position for each send
                            chunk_file_object.seek(0)

                            logging.info(f"[{upload_id}] Attempting send chunk '{chunk_part_filename}' to chat ID: {chat_id_to_send}")
                            # Call the modified function for the current chunk and chat ID
                            success, message, tg_response_json = send_file_to_telegram(chunk_file_object, chunk_part_filename, chat_id_to_send)

                            # Store result for this chat
                            result_entry = {
                                "chat_id": chat_id_to_send,
                                "success": success,
                                "message": message,
                                "tg_response": tg_response_json
                            }
                            chunk_send_results.append(result_entry)

                            # Check primary status for this chunk
                            if chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                                if success:
                                    chunk_overall_success = True
                                    chunk_primary_tg_response_json = tg_response_json
                                chunk_combined_message = message # Store primary message

                            # Log errors for backups
                            if not success and chat_id_to_send != PRIMARY_TELEGRAM_CHAT_ID:
                                logging.error(f"[{upload_id}] Failed send chunk '{chunk_part_filename}' to BACKUP {chat_id_to_send}: {message}")
                            elif not success and chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                                logging.error(f"[{upload_id}] Failed send chunk '{chunk_part_filename}' to PRIMARY {chat_id_to_send}: {message}")
                                # Decide: break loop if primary chunk fails? Let's continue trying backups for now.

                        except Exception as chunk_loop_error: # <<< INDENTED
                            logging.error(f"[{upload_id}] Unexpected error during chunk send loop for chat ID {chat_id_to_send}: {chunk_loop_error}", exc_info=True)
                            chunk_send_results.append({
                                "chat_id": chat_id_to_send, "success": False, "message": str(chunk_loop_error), "tg_response": None
                            })
                            if chat_id_to_send == PRIMARY_TELEGRAM_CHAT_ID:
                                chunk_combined_message = f"Unexpected error sending chunk to primary: {chunk_loop_error}"
                                # chunk_overall_success remains False
                    # <<< END OF INNER FOR LOOP >>> # <<< INDENTED

                    # --- Timing and Cleanup after chunk loop --- # <<< INDENTED
                    tg_chunk_multi_send_end_time = time.time()
                    # Duration for sending this chunk to *all* targets
                    tg_chunk_duration = tg_chunk_multi_send_end_time - tg_chunk_multi_send_start_time
                    # Accumulate total time spent sending chunks
                    total_tg_send_duration_split += tg_chunk_duration
                    # Close the chunk buffer *after* the inner loop for this chunk
                    chunk_file_object.close()

                    # --- Process Chunk Send Results --- # <<< INDENTED
                    # Check if the primary send *for this chunk* succeeded
                    if chunk_overall_success and chunk_primary_tg_response_json: # <<< INDENTED
                        # Increment total bytes sent ONLY if primary succeeded (for progress calculation)
                        bytes_successfully_sent += current_chunk_size
                        logging.info(f"[{upload_id}] Chunk '{chunk_part_filename}' sent successfully to PRIMARY chat.")
                        num_chunk_successful_sends = sum(1 for r in chunk_send_results if r['success'])
                        logging.debug(f"[{upload_id}] Chunk {chunk_number} successful sends: {num_chunk_successful_sends} / {len(TELEGRAM_CHAT_IDS)}")

                        # --- Calculate & Yield Progress/ETA (remains based on primary success timeline) --- # <<< INDENTED
                        progress_data = {"bytesSent": bytes_successfully_sent, "totalBytes": compressed_total_size, "percentage": 0, "speedMBps": 0, "etaFormatted": "--:--", "etaSeconds": -1}
                        if start_time_split_upload is not None and bytes_successfully_sent > 0:
                            elapsed_time = time.time() - start_time_split_upload
                            if elapsed_time > 0.1: # Avoid initial instability
                                average_speed_bps = bytes_successfully_sent / elapsed_time
                                if average_speed_bps > 0:
                                    remaining_bytes = compressed_total_size - bytes_successfully_sent
                                    progress_data["percentage"] = min((bytes_successfully_sent / compressed_total_size) * 100, 100)
                                    progress_data["speedMBps"] = average_speed_bps / (1024*1024)
                                    if remaining_bytes > 0:
                                        eta_seconds = remaining_bytes / average_speed_bps
                                        progress_data["etaSeconds"] = eta_seconds
                                        progress_data["etaFormatted"] = format_time(eta_seconds)
                                    else: # Done
                                        progress_data["percentage"] = 100
                                        progress_data["etaSeconds"] = 0
                                        progress_data["etaFormatted"] = "00:00"

                        yield f"event: progress\ndata: {json.dumps(progress_data)}\n\n"
                        # Update status to show progress across locations
                        yield f"event: status\ndata: {json.dumps({'message': f'Sent chunk {chunk_number} ({num_chunk_successful_sends}/{len(TELEGRAM_CHAT_IDS)} OK)'})}\n\n"


                        # --- Store Chunk Metadata (Modified Structure) --- # <<< INDENTED
                        try: # <<< INDENTED
                            # Create the list of detailed send locations for *this chunk*
                            chunk_all_chat_details = []
                            for res in chunk_send_results:
                                detail = {"chat_id": res["chat_id"], "success": res["success"]}
                                if res["success"] and res["tg_response"]:
                                    res_data = res["tg_response"].get('result', {})
                                    msg_id = res_data.get('message_id')
                                    doc_data = res_data.get('document', {})
                                    f_id = doc_data.get('file_id')
                                    f_uid = doc_data.get('file_unique_id')
                                    if msg_id and f_id and f_uid:
                                        detail["message_id"] = msg_id
                                        detail["file_id"] = f_id
                                        detail["file_unique_id"] = f_uid
                                    else:
                                        detail["success"] = False
                                        detail["error"] = "Missing IDs in successful response"
                                        logging.warning(f"[{upload_id}] Marked chunk {chunk_number} send to {res['chat_id']} as failed due to missing IDs: {res['tg_response']}")
                                elif not res["success"]:
                                    detail["error"] = res["message"]
                                chunk_all_chat_details.append(detail)

                            # Prepare the final metadata for *this chunk*
                            chunk_meta = {
                                "part_number": chunk_number,
                                "chunk_filename": chunk_part_filename,
                                # NEW: Store the list of location details for this chunk
                                "send_locations": chunk_all_chat_details,
                                "chunk_upload_duration_seconds": tg_chunk_duration # Time for all sends of this chunk
                            }
                            # IMPORTANT: This append needs to happen inside the successful primary check
                            uploaded_chunks_metadata.append(chunk_meta)
                            logging.info(f"[{upload_id}] Successfully processed metadata for chunk {chunk_number} across all attempts.")

                        except Exception as chunk_meta_error: # <<< INDENTED
                            # Abort on metadata processing error for a chunk
                            logging.error(f"[{upload_id}] Error processing metadata for chunk '{chunk_part_filename}': {chunk_meta_error}. Aborting.", exc_info=True)
                            raise ValueError(f"Error processing metadata for chunk {chunk_number}. Upload incomplete.") from chunk_meta_error

                    else: # Primary send for *this chunk* failed # <<< INDENTED
                        logging.error(f"[{upload_id}] Failed to send chunk '{chunk_part_filename}' to PRIMARY chat. Aborting entire upload. Error: {chunk_combined_message}")
                        # Raise an error to stop the whole upload process
                        raise IOError(f"Primary send failed for chunk {chunk_number} ('{chunk_part_filename}'): {chunk_combined_message}. Upload incomplete.")
                    # --- MODIFIED END (Chunk Multi-Send Logic) --- # <<< INDENTED

                # <<< END OF OUTER WHILE LOOP >>>

            # --- After the loop: Check consistency and Save Metadata ---
            # (This part should now be correctly placed *after* the 'while True:' loop finishes)
            expected_chunks = (compressed_total_size + CHUNK_SIZE - 1) // CHUNK_SIZE if CHUNK_SIZE > 0 else 1
            if len(uploaded_chunks_metadata) == expected_chunks:
                logging.info(f"[{upload_id}] All {expected_chunks} chunks sent successfully to primary. Saving metadata.") # Log clarified

                # --- Save Metadata (Structure already updated for chunks) ---
                metadata = load_metadata()
                timestamp = datetime.now(timezone.utc).isoformat()
                # The 'uploaded_chunks_metadata' list now contains the detailed structure
                new_file_record = {
                    "original_filename": original_filename, "sent_filename": compressed_filename,
                    "is_split": True, "is_compressed": True, "original_size": total_size,
                    "compressed_total_size": compressed_total_size, "chunk_size": CHUNK_SIZE,
                    "num_chunks": expected_chunks,
                    # This list now holds the detailed 'send_locations' per chunk
                    "chunks": uploaded_chunks_metadata,
                    "upload_timestamp": timestamp, "username": username,
                    "total_upload_duration_seconds": total_tg_send_duration_split,
                    "access_id": access_id # Generated earlier
                }
                # Check that old single ID fields are removed from this top level

                user_files_list = metadata.setdefault(username, [])
                user_files_list.append(new_file_record)
                if not save_metadata(metadata):
                     logging.error(f"[{upload_id}] CRITICAL: Chunks sent (primary success), but FAILED TO SAVE METADATA.")
                     # Yield warning?

                # --- Yield Completion ---
                landing_page_url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                logging.info(f"[{upload_id}] Generated landing page URL: {landing_page_url}")
                yield f"event: complete\ndata: {json.dumps({'message': f'Large file {original_filename} uploaded successfully!', 'download_url': landing_page_url, 'filename': original_filename})}\n\n"
                upload_data['status'] = 'completed'

            else: # Inconsistency
                 # This is where the error you saw was raised
                 logging.error(f"[{upload_id}] Inconsistency after upload. Expected {expected_chunks} chunks, got metadata for {len(uploaded_chunks_metadata)}. Aborting save.")
                 # Ensure this error message is passed to the frontend
                 raise SystemError(f"Internal inconsistency uploading chunks. Expected {expected_chunks}, got {len(uploaded_chunks_metadata)}.")

    except Exception as e:
        # --- Handle any error during processing ---
        error_message = f"Upload failed: {str(e) or type(e).__name__}" # Use str(e) for better messages
        logging.error(f"[{upload_id}] {error_message}", exc_info=True)
        # Yield an error event to the client
        yield f"event: error\ndata: {json.dumps({'message': error_message})}\n\n"
        # Update status in global dict
        if upload_id in upload_progress_data:
            upload_data['status'] = 'error'
            upload_data['error'] = error_message

    finally:
        # --- Cleanup ---
        logging.info(f"[{upload_id}] Entering final cleanup.")
        # Delete the ORIGINAL temporary file received from the client
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logging.info(f"[{upload_id}] Successfully deleted original temporary file: {temp_file_path}")
            except OSError as e_del1:
                logging.error(f"[{upload_id}] Error deleting original temporary file '{temp_file_path}': {e_del1}", exc_info=True)

        # Delete the COMPRESSED temporary file if it was created
        if temp_compressed_zip_filepath and os.path.exists(temp_compressed_zip_filepath):
             try:
                 os.remove(temp_compressed_zip_filepath)
                 logging.info(f"[{upload_id}] Successfully deleted temporary compressed file: {temp_compressed_zip_filepath}")
             except OSError as e_del2:
                 logging.error(f"[{upload_id}] Error deleting temporary compressed file '{temp_compressed_zip_filepath}': {e_del2}", exc_info=True)

        # Log final status
        final_status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
        logging.info(f"[{upload_id}] Processing finished with status: {final_status}")
        
# --- END OF MODIFIED process_upload_and_generate_updates ---
# === DOWNLOAD PREPARATION STREAMING ===

@app.route('/prepare-download/<username>/<filename>')
def prepare_download_stream(username, filename):
    """SSE endpoint to stream download preparation status."""
    prep_id = str(uuid.uuid4()) # Unique ID for this preparation request
    logging.info(f"[{prep_id}] SSE connection request for download prep: User='{username}', File='{filename}'")

    # Store initial info before starting generator
    # We create the entry here so the generator function can update it
    download_prep_data[prep_id] = {
        "status": "initiated",
        "username": username,
        "original_filename": filename,
        "error": None,
        "final_temp_file_path": None, # Will be set by the generator when ready
        "final_file_size": 0
    }

    

    # Return streaming response calling the generator
    # Return streaming response calling the generator
    return Response(stream_with_context(
        # _placeholder_generator(prep_id) # Using placeholder for now <-- COMMENTED OUT
        # !!! In the next step, we will replace the line above with:
        _prepare_download_and_generate_updates(prep_id, username, filename) # <-- ACTIVE
    ), mimetype='text/event-stream')    

# === SERVE PREPARED DOWNLOAD ===

@app.route('/serve-temp-file/<temp_id>/<filename>')
def serve_temp_file(temp_id, filename):
    """Serves the temporarily prepared file for download and cleans it up."""
    logging.info(f"Request to serve temporary download file for ID: {temp_id}, Filename: {filename}")

    prep_info = download_prep_data.get(temp_id)

    # --- Validation ---
    if not prep_info:
        logging.error(f"Serve request failed: Invalid temp_id '{temp_id}'.")
        return make_response(f"Error: Invalid or expired download link (ID: {temp_id}). Please try preparing the download again.", 404)

    if prep_info.get('status') != 'ready':
        error_msg = prep_info.get('error', 'File not ready or preparation failed.')
        logging.error(f"Serve request failed: File for temp_id '{temp_id}' not ready. Status: {prep_info.get('status')}. Error: {error_msg}")
        return make_response(f"Error: File not ready or preparation failed ({error_msg}). Please try preparing the download again.", 400)

    temp_file_path = prep_info.get('final_temp_file_path')
    file_size = prep_info.get('final_file_size')
    # Use the original filename stored during prep for the download name
    download_filename = prep_info.get('original_filename', filename) # Fallback to URL filename

    if not temp_file_path or not os.path.exists(temp_file_path):
        logging.error(f"Serve request failed: Temporary file path missing or file does not exist for ID '{temp_id}'. Path: {temp_file_path}")
        # Mark as error in case something went wrong after 'ready'
        prep_info['status'] = 'error'
        prep_info['error'] = 'Prepared file missing'
        return make_response("Error: Prepared file is missing on the server. Please try preparing the download again.", 500)

    # --- Generator for Streaming & Cleanup ---
    def generate_and_cleanup(file_path_to_stream, prep_id_to_clear):
        logging.debug(f"[{prep_id_to_clear}] Starting file streaming from: {file_path_to_stream}")
        try:
            with open(file_path_to_stream, 'rb') as f:
                while True:
                    chunk = f.read(65536) # Stream in 64KB chunks
                    if not chunk:
                        break
                    yield chunk
            logging.info(f"[{prep_id_to_clear}] Finished streaming file: {file_path_to_stream}")
        except Exception as e:
             logging.error(f"[{prep_id_to_clear}] Error during file streaming from {file_path_to_stream}: {e}", exc_info=True)
             # Don't raise here, allow finally to run
        finally:
            logging.info(f"[{prep_id_to_clear}] Cleaning up final temp file post-streaming: {file_path_to_stream}")
            # --- Cleanup the specific temp file ---
            if file_path_to_stream and os.path.exists(file_path_to_stream):
                try:
                    os.remove(file_path_to_stream)
                    logging.info(f"[{prep_id_to_clear}] Successfully deleted final temp file: {file_path_to_stream}")
                except OSError as e:
                    logging.error(f"[{prep_id_to_clear}] Error deleting final temp file {file_path_to_stream}: {e}")
            # --- Remove entry from global dict ---
            if prep_id_to_clear in download_prep_data:
                logging.debug(f"[{prep_id_to_clear}] Removing download prep data entry.")
                try:
                    del download_prep_data[prep_id_to_clear]
                except KeyError:
                     logging.warning(f"[{prep_id_to_clear}] Tried to remove prep data entry, but it was already gone.")


    # --- Create and Return Streaming Response ---
    logging.info(f"[{temp_id}] Preparing streaming response for '{download_filename}' from '{temp_file_path}'")
    response = Response(stream_with_context(generate_and_cleanup(temp_file_path, temp_id)), mimetype='application/octet-stream')
    # Set headers for browser download
    response.headers.set('Content-Disposition', 'attachment', filename=download_filename)
    if file_size: # Set Content-Length if known
        response.headers.set('Content-Length', str(file_size))
    return response

# (Keep imports and other functions above as they are)

# (Keep imports and other functions above as they are)

# --- Function with Step 4 & 5 Applied (Download Prep - /prepare-download Route) ---
def _prepare_download_and_generate_updates(prep_id, username, filename):
    """
    Generator function: Prepares file for download, yields status updates
    matching the upload flow style, and estimated progress. Handles new metadata.
    """
    logging.info(f"[{prep_id}] Starting download preparation generator for user '{username}', file '{filename}'...")
    prep_data = download_prep_data.get(prep_id)
    if not prep_data:
        logging.error(f"[{prep_id}] Critical: Prep data missing at generator start.")
        yield f"event: error\ndata: {json.dumps({'message': 'Internal Server Error: Prep data lost.'})}\n\n"
        return

    if prep_data.get('status') != 'initiated':
        logging.warning(f"[{prep_id}] Prep already running/finished (Status: {prep_data.get('status')}). Aborting.")
        return

    prep_data['status'] = 'preparing'
    temp_decompressed_path_local = None
    temp_reassembled_zip_path_local = None
    temp_final_file_path_local = None
    zip_file_handle = None
    start_time_part_fetch = None
    bytes_fetched_from_tg = 0
    total_bytes_to_fetch = 0 # Used for split file compressed size
    final_expected_size = 0 # Used for original file size
    original_filename_local = filename # Use local var to avoid modifying dict directly

    try:
        # --- Initializing Phase ---
        yield f"event: filename\ndata: {json.dumps({'filename': filename})}\n\n"
        yield f"event: status\ndata: {json.dumps({'message': 'Initializing...'})}\n\n"
        yield f"event: progress\ndata: {json.dumps({'percentage': 0, 'bytesProcessed': 0, 'totalBytes': 0, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
        time.sleep(0.2)

        metadata = load_metadata()
        user_files = metadata.get(username, [])
        file_info = next((f for f in user_files if f.get('original_filename') == filename), None)
        if not file_info: raise FileNotFoundError(f"File '{filename}' not found for user '{username}'.")

        is_split = file_info.get('is_split', False)
        is_compressed = file_info.get('is_compressed', False)
        original_filename_local = file_info.get('original_filename', filename) # Update local var
        final_expected_size = file_info.get('original_size', 0)
        prep_data['original_filename'] = original_filename_local # Update dict once
        if original_filename_local != filename:
            yield f"event: filename\ndata: {json.dumps({'filename': original_filename_local})}\n\n"
        yield f"event: totalSizeUpdate\ndata: {json.dumps({'totalSize': final_expected_size})}\n\n"

        # --- Preparing Phase ---
        yield f"event: status\ndata: {json.dumps({'message': 'Preparing file...'})}\n\n"

        if not is_split:
            # --- Single File Prep ---
            logging.info(f"[{prep_id}] Prep non-split '{original_filename_local}'")
            percentage = 10
            bytes_processed = int(final_expected_size * (percentage / 100)) if final_expected_size else 0
            yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"

            # --- Get file_id from send_locations (Single File) ---
            send_locations = file_info.get('send_locations', [])
            primary_location = next((loc for loc in send_locations if loc.get('chat_id') == PRIMARY_TELEGRAM_CHAT_ID), None)

            telegram_file_id = None
            if primary_location and primary_location.get('success') and primary_location.get('file_id'):
                telegram_file_id = primary_location.get('file_id')
                logging.info(f"[{prep_id}] Using primary file_id: {telegram_file_id} from chat {PRIMARY_TELEGRAM_CHAT_ID}")
            else:
                logging.warning(f"[{prep_id}] Primary location ({PRIMARY_TELEGRAM_CHAT_ID}) missing, failed, or file_id absent. Looking for fallback.")
                fallback_location = next((loc for loc in send_locations if loc.get('success') and loc.get('file_id')), None)
                if fallback_location:
                    telegram_file_id = fallback_location.get('file_id')
                    fallback_chat_id = fallback_location.get('chat_id')
                    logging.info(f"[{prep_id}] Using fallback file_id: {telegram_file_id} from chat {fallback_chat_id}")

            if not telegram_file_id:
                logging.error(f"[{prep_id}] Could not find any usable file_id in send_locations: {send_locations}")
                raise ValueError("Missing usable 'file_id' in any successful send location.")

            # --- Download Content ---
            start_dl_time = time.time()
            file_content_bytes, error_msg = download_telegram_file_content(telegram_file_id)
            dl_duration = time.time() - start_dl_time
            dl_speed = (len(file_content_bytes) / (1024*1024) / dl_duration) if dl_duration > 0 and file_content_bytes else 0

            if error_msg: raise ValueError(f"TG download failed: {error_msg}")
            if not file_content_bytes: raise ValueError("TG downloaded empty content.")

            percentage = 50
            bytes_processed = int(final_expected_size * (percentage / 100)) if final_expected_size else int(len(file_content_bytes)*0.5)
            yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size or len(file_content_bytes), 'speedMBps': dl_speed, 'etaFormatted': '--:--'})}\n\n"

            if is_compressed:
                logging.info(f"[{prep_id}] Decompressing single file...")
                try:
                    zip_buffer = io.BytesIO(file_content_bytes)
                    zip_file_handle = zipfile.ZipFile(zip_buffer, 'r')
                    file_list_in_zip = zip_file_handle.namelist()
                    if not file_list_in_zip: raise ValueError("Zip empty.")
                    inner_filename_to_extract = original_filename_local
                    if inner_filename_to_extract not in file_list_in_zip:
                        if len(file_list_in_zip) == 1:
                            inner_filename_to_extract = file_list_in_zip[0]
                            logging.warning(f"[{prep_id}] Original filename '{original_filename_local}' not found in zip, using only entry: '{inner_filename_to_extract}'")
                        else:
                            raise ValueError(f"Cannot find '{original_filename_local}' in zip and multiple entries exist: {file_list_in_zip}")

                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_decomp_{prep_id}_") as tf:
                        temp_final_file_path_local = tf.name # Assign final path here
                        with zip_file_handle.open(inner_filename_to_extract, 'r') as inner_file_stream:
                            shutil.copyfileobj(inner_file_stream, tf)
                    temp_decompressed_path_local = temp_final_file_path_local # Track for potential cleanup if different
                finally:
                    if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
            else: # Non-split, Non-compressed
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_nocomp_{prep_id}_") as tf:
                    temp_final_file_path_local = tf.name
                    tf.write(file_content_bytes)
                temp_decompressed_path_local = temp_final_file_path_local # Track for potential cleanup

            percentage = 95
            bytes_processed = int(final_expected_size * 0.95) if final_expected_size else 0
            yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"


        else: # is_split is True
            # --- Split File Prep ---
            logging.info(f"[{prep_id}] Prep SPLIT download for '{original_filename_local}'")
            chunks_metadata = file_info.get('chunks', [])
            if not chunks_metadata: raise ValueError("Missing 'chunks' list in metadata.")
            chunks_metadata.sort(key=lambda c: c.get('part_number', 0))
            num_chunks_total = len(chunks_metadata)
            total_bytes_to_fetch = file_info.get('compressed_total_size', 0) # Fetch compressed size
            if total_bytes_to_fetch <= 0:
                logging.warning(f"[{prep_id}] Compressed size unknown or zero. Progress reporting during fetch might be less accurate.")
                total_bytes_to_fetch = 0 # Treat as unknown if invalid

            yield f"event: progress\ndata: {json.dumps({'percentage': 0, 'bytesProcessed': 0, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"

            with tempfile.NamedTemporaryFile(suffix=".zip.tmp", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_zip_path_local = tf_reassemble.name
                start_time_part_fetch = time.time()
                bytes_fetched_from_tg = 0
                fetch_percentage_allocation = 80.0

                for i, chunk_info in enumerate(chunks_metadata):
                    part_num = chunk_info.get('part_number')
                    if part_num is None: raise ValueError(f"Chunk metadata missing 'part_number' at index {i}")

                    # --- Get chunk_file_id from send_locations --- # <<< CORRECTED LOGIC >>>
                    chunk_locations = chunk_info.get('send_locations', [])
                    primary_chunk_loc = next((loc for loc in chunk_locations if loc.get('chat_id') == PRIMARY_TELEGRAM_CHAT_ID), None)

                    chunk_file_id = None
                    if primary_chunk_loc and primary_chunk_loc.get('success') and primary_chunk_loc.get('file_id'):
                        chunk_file_id = primary_chunk_loc.get('file_id')
                        logging.debug(f"[{prep_id}] Chunk {part_num}: Using primary file_id {chunk_file_id}")
                    else:
                        logging.warning(f"[{prep_id}] Chunk {part_num}: Primary location ({PRIMARY_TELEGRAM_CHAT_ID}) missing/failed/no_id. Trying fallback.")
                        fallback_chunk_loc = next((loc for loc in chunk_locations if loc.get('success') and loc.get('file_id')), None)
                        if fallback_chunk_loc:
                            chunk_file_id = fallback_chunk_loc.get('file_id')
                            fallback_chat_id = fallback_chunk_loc.get('chat_id')
                            logging.debug(f"[{prep_id}] Chunk {part_num}: Using fallback file_id {chunk_file_id} from chat {fallback_chat_id}")

                    if not chunk_file_id:
                        logging.error(f"[{prep_id}] Could not find any usable file_id for chunk {part_num} in locations: {chunk_locations}")
                        raise ValueError(f"Missing usable 'file_id' for chunk {part_num}.")
                    # --- END OF Get chunk_file_id ---

                    # --- Download the chunk content ---
                    logging.debug(f"[{prep_id}] Downloading chunk {part_num}/{num_chunks_total} using file_id {chunk_file_id}...")
                    chunk_content_bytes, error_msg = download_telegram_file_content(chunk_file_id)
                    if error_msg: raise ValueError(f"Error downloading chunk {part_num}: {error_msg}")
                    if not chunk_content_bytes: raise ValueError(f"Downloaded chunk {part_num} is empty.")

                    tf_reassemble.write(chunk_content_bytes)
                    bytes_fetched_from_tg += len(chunk_content_bytes)

                    # --- Calculate and Yield Progress ---
                    percentage_complete_fetch = ((i + 1) / num_chunks_total) * 100.0
                    overall_percentage = percentage_complete_fetch * (fetch_percentage_allocation / 100.0)
                    bytes_processed = int(final_expected_size * (overall_percentage / 100.0)) if final_expected_size else 0

                    current_speed_mbps = 0; eta_formatted = "--:--"
                    elapsed_time_fetch = time.time() - start_time_part_fetch
                    if elapsed_time_fetch > 0.1 and bytes_fetched_from_tg > 0:
                        average_speed_bps = bytes_fetched_from_tg / elapsed_time_fetch
                        current_speed_mbps = average_speed_bps / (1024*1024)
                        if total_bytes_to_fetch > 0 and average_speed_bps > 0:
                            remaining_bytes = total_bytes_to_fetch - bytes_fetched_from_tg
                            if remaining_bytes > 0: eta_formatted = format_time(remaining_bytes / average_speed_bps)
                            else: eta_formatted = "00:00"
                        elif average_speed_bps > 0:
                            chunks_remaining = num_chunks_total - (i + 1)
                            if chunks_remaining > 0:
                                avg_chunk_size_estimate = bytes_fetched_from_tg / (i + 1)
                                time_per_chunk_estimate = avg_chunk_size_estimate / average_speed_bps if average_speed_bps else 0
                                eta_seconds_chunk = chunks_remaining * time_per_chunk_estimate if time_per_chunk_estimate > 0 else -1
                                if eta_seconds_chunk >= 0: eta_formatted = format_time(eta_seconds_chunk)
                            else: eta_formatted = "00:00"

                    yield f"event: progress\ndata: {json.dumps({'percentage': overall_percentage, 'bytesProcessed': bytes_fetched_from_tg, 'totalBytes': total_bytes_to_fetch if total_bytes_to_fetch else 0, 'speedMBps': current_speed_mbps, 'etaFormatted': eta_formatted})}\n\n"

            logging.info(f"[{prep_id}] Finished reassembling {num_chunks_total} chunks. Total bytes fetched: {bytes_fetched_from_tg}.")
            yield f"event: progress\ndata: {json.dumps({'percentage': fetch_percentage_allocation, 'bytesProcessed': bytes_fetched_from_tg, 'totalBytes': total_bytes_to_fetch, 'speedMBps': current_speed_mbps, 'etaFormatted': '00:00'})}\n\n"

            if is_compressed:
                logging.info(f"[{prep_id}] Decompressing reassembled file...")
                yield f"event: status\ndata: {json.dumps({'message': 'Decompressing...'})}\n\n"
                decomp_start_percent = fetch_percentage_allocation
                decomp_end_percent = 98.0
                try:
                    zip_file_handle = zipfile.ZipFile(temp_reassembled_zip_path_local, 'r')
                    file_list_in_zip = zip_file_handle.namelist()
                    if not file_list_in_zip: raise ValueError("Reassembled zip is empty.")
                    inner_filename_to_extract = original_filename_local
                    if inner_filename_to_extract not in file_list_in_zip:
                        if len(file_list_in_zip) == 1:
                            inner_filename_to_extract = file_list_in_zip[0]
                            logging.warning(f"[{prep_id}] Original filename '{original_filename_local}' not found in zip, using only entry: '{inner_filename_to_extract}'")
                        else:
                            raise ValueError(f"Cannot find '{original_filename_local}' in zip and multiple entries exist: {file_list_in_zip}")

                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf_final:
                        temp_final_file_path_local = tf_final.name
                        with zip_file_handle.open(inner_filename_to_extract, 'r') as inner_file_stream:
                            mid_decomp_percent = decomp_start_percent + (decomp_end_percent - decomp_start_percent) / 2
                            bytes_processed = int(final_expected_size * (mid_decomp_percent/100)) if final_expected_size else 0
                            yield f"event: progress\ndata: {json.dumps({'percentage': mid_decomp_percent, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size})}\n\n"
                            shutil.copyfileobj(inner_file_stream, tf_final)
                    temp_decompressed_path_local = temp_final_file_path_local # Track intermediate for cleanup
                finally:
                    if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
                bytes_processed = int(final_expected_size * (decomp_end_percent/100)) if final_expected_size else 0
                yield f"event: progress\ndata: {json.dumps({'percentage': decomp_end_percent, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size})}\n\n"

            else: # Split, but not compressed
                logging.info(f"[{prep_id}] Split file not compressed. Using reassembled directly.")
                temp_final_file_path_local = temp_reassembled_zip_path_local
                temp_reassembled_zip_path_local = None # Prevent deletion later
                bytes_processed = bytes_fetched_from_tg
                yield f"event: progress\ndata: {json.dumps({'percentage': 98.0, 'bytesProcessed': bytes_processed, 'totalBytes': bytes_processed})}\n\n"


        # --- Preparation Complete ---
        if not temp_final_file_path_local or not os.path.exists(temp_final_file_path_local):
            raise RuntimeError("Failed to produce final temp file.")

        final_actual_size = os.path.getsize(temp_final_file_path_local)
        logging.info(f"[{prep_id}] Final prepared file: '{temp_final_file_path_local}', Size: {final_actual_size}")

        prep_data['final_temp_file_path'] = temp_final_file_path_local
        prep_data['final_file_size'] = final_actual_size
        prep_data['status'] = 'ready'

        yield f"event: status\ndata: {json.dumps({'message': f'File ready!'})}\n\n"
        yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_actual_size, 'speedMBps': 0, 'etaFormatted': '00:00'})}\n\n"

        yield f"event: ready\ndata: {json.dumps({'temp_file_id': prep_id, 'final_filename': original_filename_local})}\n\n"
        logging.info(f"[{prep_id}] Preparation complete. Sent 'ready' event.")

    except Exception as e:
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
        logging.error(f"[{prep_id}] {error_message}", exc_info=True)
        yield f"event: error\ndata: {json.dumps({'message': error_message})}\n\n"
        # Ensure prep_data exists before trying to update it
        if prep_id in download_prep_data:
            download_prep_data[prep_id]['status'] = 'error'
            download_prep_data[prep_id]['error'] = error_message
    finally:
        # --- Cleanup ---
        logging.info(f"[{prep_id}] Generator finished or errored. Cleaning up intermediate files.")
        # Cleanup intermediate reassembled file (if applicable and different)
        if temp_reassembled_zip_path_local and temp_reassembled_zip_path_local != temp_final_file_path_local and os.path.exists(temp_reassembled_zip_path_local):
            logging.info(f"[{prep_id}] Cleaning up intermediate reassembled file: {temp_reassembled_zip_path_local}")
            try: os.remove(temp_reassembled_zip_path_local)
            except OSError as e: logging.error(f"[{prep_id}] Error deleting intermediate file {temp_reassembled_zip_path_local}: {e}")
        # Cleanup the decompressed file ONLY if it's different from the final file AND exists
        if temp_decompressed_path_local and temp_decompressed_path_local != temp_final_file_path_local and os.path.exists(temp_decompressed_path_local):
            logging.info(f"[{prep_id}] Cleaning up intermediate decompressed file: {temp_decompressed_path_local}")
            try: os.remove(temp_decompressed_path_local)
            except OSError as e: logging.error(f"[{prep_id}] Error deleting intermediate decompressed file {temp_decompressed_path_local}: {e}")

        logging.info(f"[{prep_id}] Generator task ended.")

# --- END OF _prepare_download_and_generate_updates ---
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

@app.route('/stream-download/<access_id>')
def stream_download_by_access_id(access_id): # Renamed function
    """
    SSE endpoint to stream download preparation status initiated by access_id.
    Replaces the old direct download logic for this URL pattern.
    """
    prep_id = str(uuid.uuid4()) # Unique ID for *this* preparation task
    logging.info(f"[{prep_id}] SSE connection request for download prep using access_id: {access_id}")

    # --- Basic Metadata Lookup (Just to ensure ID is valid before streaming) ---
    metadata = load_metadata()
    found_file_info = None
    for username, files in metadata.items():
        for file_info in files:
            if file_info.get('access_id') == access_id:
                found_file_info = file_info
                break
        if found_file_info: break

    if not found_file_info:
        logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' for SSE download stream.")
        # Return an immediate error event in the stream
        def error_stream():
            yield f"event: error\ndata: {json.dumps({'message': 'Invalid or expired download link.'})}\n\n"
        return Response(error_stream(), mimetype='text/event-stream')

    # Store initial info for the generator to use/update
    # Use the SAME download_prep_data dictionary as the list->prepare function
    download_prep_data[prep_id] = {
        "status": "initiated",
        "access_id": access_id, # Store access_id for the generator
        "original_filename": found_file_info.get('original_filename', 'file'), # Store for generator
        "error": None,
        "final_temp_file_path": None,
        "final_file_size": 0
    }
    logging.debug(f"[{prep_id}] Stored initial prep data. Starting SSE generator.")

    # Return streaming response calling the new generator function
    return Response(stream_with_context(
        _prepare_and_stream_download_generator(prep_id) # Call the generator
    ), mimetype='text/event-stream')


# Create the new generator function that performs the preparation and yields SSE updates
# --- Function with Step 4 & 5 Applied (Download Prep - /stream-download Route) ---
def _prepare_and_stream_download_generator(prep_id):
    """
    Generator: Fetches file info using access_id, prepares file (TG download,
    reassemble, decompress), yields SSE progress/status updates, and finally
    yields a 'ready' event with the temp_file_id for the actual download. Handles new metadata.
    """
    logging.info(f"[{prep_id}] Download preparation generator started.")
    prep_data = download_prep_data.get(prep_id)
    if not prep_data:
        logging.error(f"[{prep_id}] Critical: Prep data missing at generator start.")
        yield f"event: error\ndata: {json.dumps({'message': 'Internal Server Error: Prep data lost.'})}\n\n"
        return

    if prep_data.get('status') != 'initiated':
        logging.warning(f"[{prep_id}] Generator started but status is not 'initiated' (Status: {prep_data.get('status')}). Aborting duplicate run.")
        return

    access_id = prep_data['access_id']
    prep_data['status'] = 'preparing'
    # --- Initialize local variables ---
    temp_decompressed_path_local = None
    temp_reassembled_zip_path_local = None
    temp_final_file_path_local = None
    zip_file_handle = None
    original_filename_from_meta = "file" # Default
    final_expected_size = 0 # Will hold original size

    try:
        # --- Phase 1: Metadata Lookup & Initial Events ---
        yield f"event: status\ndata: {json.dumps({'message': 'Looking up file info...'})}\n\n"
        time.sleep(0.1) # Small delay

        metadata = load_metadata()
        found_file_info = None
        found_username = None
        for username, files in metadata.items():
            for file_info_item in files: # Renamed inner variable
                if file_info_item.get('access_id') == access_id:
                    found_file_info = file_info_item # Assign the correct item
                    found_username = username
                    break
            if found_file_info: break

        if not found_file_info:
            raise FileNotFoundError(f"Access ID '{access_id}' not found in metadata.")

        is_split = found_file_info.get('is_split', False)
        is_compressed = found_file_info.get('is_compressed', False)
        original_filename_from_meta = found_file_info.get('original_filename', f'download_{access_id}.dat')
        # Use original_size from metadata as the target for progress display
        final_expected_size = found_file_info.get('original_size', 0)
        # Update the filename in prep_data if different from initial guess
        prep_data['original_filename'] = original_filename_from_meta

        logging.info(f"[{prep_id}] Found metadata for '{original_filename_from_meta}' (User: {found_username}, Size: {final_expected_size})")

        # Send 'start' event with final expected size
        yield f"event: start\ndata: {json.dumps({'filename': original_filename_from_meta, 'totalSize': final_expected_size})}\n\n"
        # Send initial progress (0%) - Use final_expected_size as the reference total
        yield f"event: progress\ndata: {json.dumps({'percentage': 0, 'bytesProcessed': 0, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
        yield f"event: status\ndata: {json.dumps({'message': 'Preparing file...'})}\n\n"
        time.sleep(0.2)

        # --- Phase 2: File Preparation ---
        calculated_speed_mbps = 0 # Reset speed

        if not is_split:
            # --- Prepare Single File ---
            logging.info(f"[{prep_id}] Preparing non-split file '{original_filename_from_meta}'.")
            yield f"event: status\ndata: {json.dumps({'message': 'Downloading from source...'})}\n\n"

            # --- Get file_id from send_locations (Single File) ---
            send_locations = found_file_info.get('send_locations', []) # Use found_file_info here
            primary_location = next((loc for loc in send_locations if loc.get('chat_id') == PRIMARY_TELEGRAM_CHAT_ID), None)

            telegram_file_id = None
            if primary_location and primary_location.get('success') and primary_location.get('file_id'):
                telegram_file_id = primary_location.get('file_id')
                logging.info(f"[{prep_id}] Using primary file_id: {telegram_file_id} from chat {PRIMARY_TELEGRAM_CHAT_ID}")
            else:
                logging.warning(f"[{prep_id}] Primary location ({PRIMARY_TELEGRAM_CHAT_ID}) missing, failed, or file_id absent. Looking for fallback.")
                fallback_location = next((loc for loc in send_locations if loc.get('success') and loc.get('file_id')), None)
                if fallback_location:
                    telegram_file_id = fallback_location.get('file_id')
                    fallback_chat_id = fallback_location.get('chat_id')
                    logging.info(f"[{prep_id}] Using fallback file_id: {telegram_file_id} from chat {fallback_chat_id}")

            if not telegram_file_id:
                logging.error(f"[{prep_id}] Could not find any usable file_id in send_locations: {send_locations}")
                raise ValueError("Missing usable 'file_id' in any successful send location.")

            # --- Download Content ---
            dl_start_time = time.time()
            yield f"event: progress\ndata: {json.dumps({'percentage': 5, 'bytesProcessed': int(final_expected_size*0.05) if final_expected_size else 0, 'totalBytes': final_expected_size})}\n\n"

            file_content_bytes, error_msg = download_telegram_file_content(telegram_file_id) # This blocks

            dl_duration = time.time() - dl_start_time
            actual_bytes_downloaded = len(file_content_bytes) if file_content_bytes else 0
            calculated_speed_mbps = (actual_bytes_downloaded / (1024*1024) / dl_duration) if dl_duration > 0 and actual_bytes_downloaded > 0 else 0
            logging.info(f"[{prep_id}] Source download took {dl_duration:.2f}s, Speed: {calculated_speed_mbps:.2f} MB/s")

            if error_msg: raise ValueError(f"Telegram download failed: {error_msg}")
            if not file_content_bytes: raise ValueError("Telegram download returned empty content.")

            yield f"event: progress\ndata: {json.dumps({'percentage': 50, 'bytesProcessed': int(final_expected_size*0.5) if final_expected_size else int(actual_bytes_downloaded*0.5), 'totalBytes': final_expected_size or actual_bytes_downloaded, 'speedMBps': calculated_speed_mbps, 'etaFormatted': '00:00'})}\n\n"


            if is_compressed:
                yield f"event: status\ndata: {json.dumps({'message': 'Decompressing...'})}\n\n"
                logging.info(f"[{prep_id}] Decompressing single downloaded file...")
                decompress_start = time.time()
                zip_buffer = io.BytesIO(file_content_bytes)
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf:
                    temp_final_file_path_local = tf.name
                    try:
                        zip_file_handle = zipfile.ZipFile(zip_buffer, 'r')
                        inner_filename_to_extract = original_filename_from_meta
                        file_list_in_zip = zip_file_handle.namelist()
                        if not file_list_in_zip: raise ValueError("Downloaded zip is empty.")
                        if original_filename_from_meta not in file_list_in_zip:
                            if len(file_list_in_zip) == 1:
                                inner_filename_to_extract = file_list_in_zip[0]
                                logging.warning(f"[{prep_id}] Original filename '{original_filename_from_meta}' not found in zip, using only entry: '{inner_filename_to_extract}'")
                            else:
                                raise ValueError(f"Cannot find '{original_filename_from_meta}' in zip and multiple entries exist: {file_list_in_zip}")

                        with zip_file_handle.open(inner_filename_to_extract, 'r') as inner_file_stream:
                            yield f"event: progress\ndata: {json.dumps({'percentage': 75, 'bytesProcessed': int(final_expected_size*0.75) if final_expected_size else 0, 'totalBytes': final_expected_size})}\n\n"
                            shutil.copyfileobj(inner_file_stream, tf)
                    finally:
                        if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
                decompress_duration = time.time() - decompress_start
                logging.info(f"[{prep_id}] Decompression finished in {decompress_duration:.2f}s.")
                yield f"event: progress\ndata: {json.dumps({'percentage': 95, 'bytesProcessed': int(final_expected_size*0.95) if final_expected_size else 0, 'totalBytes': final_expected_size})}\n\n"

            else: # Non-split, Non-compressed
                yield f"event: status\ndata: {json.dumps({'message': 'Saving temporary file...'})}\n\n"
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf:
                    temp_final_file_path_local = tf.name
                    tf.write(file_content_bytes)
                yield f"event: progress\ndata: {json.dumps({'percentage': 95, 'bytesProcessed': int(actual_bytes_downloaded*0.95), 'totalBytes': actual_bytes_downloaded})}\n\n"

        else: # is_split is True
            # --- Prepare Split File ---
            logging.info(f"[{prep_id}] Preparing split file '{original_filename_from_meta}'.")
            yield f"event: status\ndata: {json.dumps({'message': 'Downloading & Reassembling...'})}\n\n"

            chunks_metadata = found_file_info.get('chunks', [])
            if not chunks_metadata: raise ValueError("Missing 'chunks' metadata.")
            chunks_metadata.sort(key=lambda c: c.get('part_number', 0))
            num_chunks_total = len(chunks_metadata)

            total_bytes_to_fetch = found_file_info.get('compressed_total_size', 0)
            logging.info(f"[{prep_id}] Expecting {num_chunks_total} chunks, compressed size ~{format_bytes(total_bytes_to_fetch)}.")
            if total_bytes_to_fetch <= 0:
                logging.warning(f"[{prep_id}] Compressed size unknown or zero. Progress reporting during fetch might be less accurate.")
                total_bytes_to_fetch = 0

            start_time_part_fetch = time.time()
            bytes_fetched_so_far = 0
            fetch_percentage_allocation = 80.0

            with tempfile.NamedTemporaryFile(suffix=".zip.tmp", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_zip_path_local = tf_reassemble.name
                logging.debug(f"[{prep_id}] Reassembling chunks into: {temp_reassembled_zip_path_local}")

                for i, chunk_info in enumerate(chunks_metadata):
                    part_num = chunk_info.get('part_number')
                    if part_num is None: raise ValueError(f"Chunk metadata missing 'part_number' at index {i}")

                    # --- Get chunk_file_id from send_locations --- # <<< CORRECTED LOGIC >>>
                    chunk_locations = chunk_info.get('send_locations', [])
                    primary_chunk_loc = next((loc for loc in chunk_locations if loc.get('chat_id') == PRIMARY_TELEGRAM_CHAT_ID), None)

                    chunk_file_id = None
                    if primary_chunk_loc and primary_chunk_loc.get('success') and primary_chunk_loc.get('file_id'):
                        chunk_file_id = primary_chunk_loc.get('file_id')
                        logging.debug(f"[{prep_id}] Chunk {part_num}: Using primary file_id {chunk_file_id}")
                    else:
                        logging.warning(f"[{prep_id}] Chunk {part_num}: Primary location ({PRIMARY_TELEGRAM_CHAT_ID}) missing/failed/no_id. Trying fallback.")
                        fallback_chunk_loc = next((loc for loc in chunk_locations if loc.get('success') and loc.get('file_id')), None)
                        if fallback_chunk_loc:
                            chunk_file_id = fallback_chunk_loc.get('file_id')
                            fallback_chat_id = fallback_chunk_loc.get('chat_id')
                            logging.debug(f"[{prep_id}] Chunk {part_num}: Using fallback file_id {chunk_file_id} from chat {fallback_chat_id}")

                    if not chunk_file_id:
                        logging.error(f"[{prep_id}] Could not find any usable file_id for chunk {part_num} in locations: {chunk_locations}")
                        raise ValueError(f"Missing usable 'file_id' for chunk {part_num}.")
                    # --- END OF Get chunk_file_id ---

                    # --- Download the chunk content ---
                    logging.debug(f"[{prep_id}] Downloading chunk {part_num}/{num_chunks_total} using file_id {chunk_file_id}...")
                    chunk_dl_start = time.time()
                    chunk_content_bytes, error_msg = download_telegram_file_content(chunk_file_id)
                    chunk_dl_duration = time.time() - chunk_dl_start

                    if error_msg: raise ValueError(f"Error downloading chunk {part_num}: {error_msg}")
                    if not chunk_content_bytes: raise ValueError(f"Downloaded chunk {part_num} is empty.")

                    tf_reassemble.write(chunk_content_bytes)
                    bytes_fetched_so_far += len(chunk_content_bytes)

                    # --- Calculate and Yield Progress ---
                    percentage_complete_fetch = ((i + 1) / num_chunks_total) * 100.0
                    overall_percentage = percentage_complete_fetch * (fetch_percentage_allocation / 100.0)

                    current_speed_mbps = 0; eta_formatted = "--:--"
                    elapsed_time_fetch = time.time() - start_time_part_fetch
                    if elapsed_time_fetch > 0.1 and bytes_fetched_so_far > 0:
                        average_speed_bps = bytes_fetched_so_far / elapsed_time_fetch
                        current_speed_mbps = average_speed_bps / (1024*1024)
                        if total_bytes_to_fetch > 0 and average_speed_bps > 0:
                            remaining_bytes_to_fetch = total_bytes_to_fetch - bytes_fetched_so_far
                            if remaining_bytes_to_fetch > 0: eta_formatted = format_time(remaining_bytes_to_fetch / average_speed_bps)
                            else: eta_formatted = "00:00"
                        elif average_speed_bps > 0:
                            chunks_remaining = num_chunks_total - (i + 1)
                            if chunks_remaining > 0:
                                avg_chunk_size_estimate = bytes_fetched_so_far / (i + 1)
                                time_per_chunk_estimate = avg_chunk_size_estimate / average_speed_bps if average_speed_bps else 0
                                eta_seconds_chunk = chunks_remaining * time_per_chunk_estimate if time_per_chunk_estimate > 0 else -1
                                if eta_seconds_chunk >= 0: eta_formatted = format_time(eta_seconds_chunk)
                            else: eta_formatted = "00:00"

                    yield f"event: progress\ndata: {json.dumps({'percentage': overall_percentage, 'bytesProcessed': bytes_fetched_so_far, 'totalBytes': total_bytes_to_fetch if total_bytes_to_fetch else 0, 'speedMBps': current_speed_mbps, 'etaFormatted': eta_formatted})}\n\n"
                    logging.debug(f"[{prep_id}] Fetched chunk {part_num} ({len(chunk_content_bytes)} bytes) in {chunk_dl_duration:.2f}s")

            # --- Reassembly Finished ---
            calculated_speed_mbps = current_speed_mbps # Store last calculated speed
            logging.info(f"[{prep_id}] Finished reassembling {num_chunks_total} chunks. Total bytes fetched: {bytes_fetched_so_far}.")
            yield f"event: progress\ndata: {json.dumps({'percentage': fetch_percentage_allocation, 'bytesProcessed': bytes_fetched_so_far, 'totalBytes': total_bytes_to_fetch, 'speedMBps': calculated_speed_mbps, 'etaFormatted': '00:00'})}\n\n"


            if is_compressed:
                # Now switch status and progress basis to final size/decompression
                yield f"event: status\ndata: {json.dumps({'message': 'Decompressing...'})}\n\n"
                logging.info(f"[{prep_id}] Decompressing reassembled file: {temp_reassembled_zip_path_local}")
                decompress_start = time.time()
                decomp_start_percent = fetch_percentage_allocation
                decomp_end_percent = 98.0
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf_final:
                    temp_final_file_path_local = tf_final.name
                    try:
                        zip_file_handle = zipfile.ZipFile(temp_reassembled_zip_path_local, 'r')
                        inner_filename_to_extract = original_filename_from_meta
                        file_list_in_zip = zip_file_handle.namelist()
                        if not file_list_in_zip: raise ValueError("Reassembled zip is empty.")
                        if original_filename_from_meta not in file_list_in_zip:
                            if len(file_list_in_zip) == 1:
                                inner_filename_to_extract = file_list_in_zip[0]
                                logging.warning(f"[{prep_id}] Original filename '{original_filename_from_meta}' not found in zip, using only entry: '{inner_filename_to_extract}'")
                            else:
                                raise ValueError(f"Cannot find '{original_filename_from_meta}' in zip and multiple entries exist: {file_list_in_zip}")

                        with zip_file_handle.open(inner_filename_to_extract, 'r') as inner_file_stream:
                            mid_decomp_percent = decomp_start_percent + (decomp_end_percent - decomp_start_percent) / 2
                            yield f"event: progress\ndata: {json.dumps({'percentage': mid_decomp_percent, 'bytesProcessed': int(final_expected_size * (mid_decomp_percent/100)) if final_expected_size else 0, 'totalBytes': final_expected_size})}\n\n"
                            shutil.copyfileobj(inner_file_stream, tf_final)
                    finally:
                        if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
                decompress_duration = time.time() - decompress_start
                logging.info(f"[{prep_id}] Decompression finished in {decompress_duration:.2f}s.")
                yield f"event: progress\ndata: {json.dumps({'percentage': decomp_end_percent, 'bytesProcessed': int(final_expected_size* (decomp_end_percent/100)) if final_expected_size else 0, 'totalBytes': final_expected_size})}\n\n"

            else: # Split, but not compressed
                logging.info(f"[{prep_id}] Split file not compressed. Using reassembled directly.")
                temp_final_file_path_local = temp_reassembled_zip_path_local
                temp_reassembled_zip_path_local = None # Don't delete the intermediate
                bytes_processed = bytes_fetched_so_far
                yield f"event: progress\ndata: {json.dumps({'percentage': 98.0, 'bytesProcessed': bytes_processed, 'totalBytes': bytes_processed})}\n\n"


        # --- Phase 3: Preparation Complete ---
        if not temp_final_file_path_local or not os.path.exists(temp_final_file_path_local):
            raise RuntimeError("Failed to produce final temp file after preparation.")

        final_actual_size = os.path.getsize(temp_final_file_path_local)
        logging.info(f"[{prep_id}] Final prepared file: '{temp_final_file_path_local}', Size: {final_actual_size}")

        prep_data['final_temp_file_path'] = temp_final_file_path_local
        prep_data['final_file_size'] = final_actual_size
        prep_data['status'] = 'ready' # Mark as ready

        # Yield final progress (using actual final size) and 'ready' event
        yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_actual_size, 'speedMBps': 0, 'etaFormatted': '00:00'})}\n\n"
        yield f"event: status\ndata: {json.dumps({'message': 'File ready!'})}\n\n"
        time.sleep(0.1) # Brief pause before ready event

        yield f"event: ready\ndata: {json.dumps({'temp_file_id': prep_id, 'final_filename': original_filename_from_meta})}\n\n"
        logging.info(f"[{prep_id}] Preparation complete. Sent 'ready' event.")

    except Exception as e:
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
        logging.error(f"[{prep_id}] {error_message}", exc_info=True)
        yield f"event: error\ndata: {json.dumps({'message': error_message})}\n\n"
        if prep_id in download_prep_data: # Update status if entry still exists
            download_prep_data[prep_id]['status'] = 'error'
            download_prep_data[prep_id]['error'] = error_message
    finally:
        # --- Cleanup Intermediate Files (Only if they aren't the final file) ---
        logging.info(f"[{prep_id}] Generator finished or errored. Cleaning up intermediate files.")
        # Delete the reassembled zip file ONLY if it exists AND it's different from the final path
        if temp_reassembled_zip_path_local and \
           temp_reassembled_zip_path_local != temp_final_file_path_local and \
           os.path.exists(temp_reassembled_zip_path_local):
            try:
                os.remove(temp_reassembled_zip_path_local)
                logging.info(f"[{prep_id}] Cleaned up intermediate reassembled file: {temp_reassembled_zip_path_local}")
            except OSError as e:
                logging.error(f"[{prep_id}] Error deleting intermediate reassembled file '{temp_reassembled_zip_path_local}': {e}")
        # Cleanup the decompressed file ONLY if it's different from the final file AND exists
        if temp_decompressed_path_local and \
           temp_decompressed_path_local != temp_final_file_path_local and \
           os.path.exists(temp_decompressed_path_local):
             logging.info(f"[{prep_id}] Cleaning up intermediate decompressed file: {temp_decompressed_path_local}")
             try: os.remove(temp_decompressed_path_local)
             except OSError as e: logging.error(f"[{prep_id}] Error deleting intermediate decompressed file '{temp_decompressed_path_local}': {e}")

        logging.info(f"[{prep_id}] Generator task ended.")

# --- END OF _prepare_and_stream_download_generator ---
    

@app.route('/get/<access_id>')
def get_file_by_access_id(access_id):
    """
    Looks up a file by its access_id and renders the download page.
    (This function remains unchanged)
    """
    # (Existing code for this route is correct)
    logging.info(f"Received request for access_id: {access_id}")
    metadata = load_metadata()
    # ... (lookup logic) ...
    found_file_info = None
    found_username = None
    for username, files in metadata.items():
        for file_info in files:
            if file_info.get('access_id') == access_id:
                found_file_info = file_info
                found_username = username
                break
        if found_file_info: break

    if not found_file_info:
        logging.warning(f"Access ID '{access_id}' not found in metadata.")
        return make_response(render_template('404_error.html', message=f"File link '{access_id}' not found or expired."), 404)
    # ... (extract details) ...
    original_filename = found_file_info.get('original_filename', 'Unknown Filename')
    file_size_bytes = found_file_info.get('original_size')
    # ... (handle missing size) ...
    if file_size_bytes is None: file_size_bytes = 0
    upload_timestamp_iso = found_file_info.get('upload_timestamp')
    # ... (format date) ...
    upload_datetime_str = "Unknown date"
    if upload_timestamp_iso:
        try:
            upload_dt_utc = dateutil_parser.isoparse(upload_timestamp_iso)
            upload_datetime_str = upload_dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e:
            logging.warning(f"Could not parse timestamp '{upload_timestamp_iso}': {e}")


    logging.info(f"Found file '{original_filename}'. Rendering download page.")
    # Render the download page template
    return render_template('download_page.html',
                           filename=original_filename,
                           filesize=file_size_bytes,
                           upload_date=upload_datetime_str,
                           username=found_username, # Pass username if needed by template
                           access_id=access_id
                           )

# --- NEW: Route to Delete File Record ---
@app.route('/delete-file/<username>/<filename>', methods=['DELETE'])
def delete_file_record(username, filename):
    """Deletes a file record from the metadata for a specific user."""
    logging.info(f"Received DELETE request for user='{username}', file='{filename}'")

    metadata = load_metadata()

    if username not in metadata:
        logging.warning(f"Delete request failed: User '{username}' not found in metadata.")
        return jsonify({"error": f"User '{username}' not found."}), 404

    user_files = metadata[username]
    initial_length = len(user_files)

    # Find and remove the file record
    # We create a new list excluding the item to delete to avoid modification during iteration issues
    updated_user_files = [
        file_record for file_record in user_files
        if file_record.get('original_filename') != filename
    ]

    if len(updated_user_files) == initial_length:
        # No file was removed, meaning it wasn't found
        logging.warning(f"Delete request failed: File '{filename}' not found for user '{username}'.")
        return jsonify({"error": f"File '{filename}' not found for user '{username}'."}), 404
    else:
        # File was found and removed (implicitly by not being included in the new list)
        metadata[username] = updated_user_files
        logging.info(f"Found and removed record for '{filename}' for user '{username}'.")

        # Handle case where user might now have no files left
        if not metadata[username]:
            logging.info(f"User '{username}' has no files left. Removing user entry from metadata.")
            del metadata[username] # Optional: clean up empty user lists

        # Attempt to save the updated metadata
        if save_metadata(metadata):
            logging.info(f"Successfully saved updated metadata after deleting '{filename}'.")
            return jsonify({"message": f"File record '{filename}' deleted successfully."}), 200
        else:
            # This is a critical error - the file is removed in memory but not saved!
            # In a more robust system, you might want to try reloading the original metadata
            # or implementing a locking mechanism. For now, we log and return an error.
            logging.error(f"CRITICAL: Failed to save metadata after removing record for '{filename}'. State might be inconsistent.")
            # Return 500 Internal Server Error
            return jsonify({"error": "Failed to update metadata file on server after deletion."}), 500

# --- End of NEW Delete Route ---

# --- Application Runner ---
if __name__ == '__main__':
    # Ensure Log Directory Exists
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
            logging.info(f"Created log directory: {LOG_DIR}")
        except OSError as e:
            logging.error(f"Could not create log directory {LOG_DIR}: {e}", exc_info=True)
            # Decide if you want to exit or continue without file logging
            # For now, it will continue but file logging might fail if dir creation failed.

    if not os.path.exists(UPLOADS_TEMP_DIR):
        try:
            os.makedirs(UPLOADS_TEMP_DIR)
            logging.info(f"Created temporary upload directory: {UPLOADS_TEMP_DIR}")
        except OSError as e:
            logging.error(f"Could not create temporary upload directory {UPLOADS_TEMP_DIR}: {e}", exc_info=True)

    logging.info("Starting Flask development server...")
    # Use host='0.0.0.0' to make it accessible on your network
    # debug=True is useful for development, but should be False in production
    app.run(host='0.0.0.0', port=5000, debug=True) # Consider debug=False for production