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

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
TELEGRAM_CHAT_ID = '-4603853425'     
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

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.')
app.secret_key = 'a_simple_secret_key_for_now' # Important for flashing messages
logging.info("Flask application initialized.")

upload_progress_data = {}
download_prep_data = {}

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
def process_upload_and_generate_updates(upload_id):
    """
    Processes the upload for the given ID, yielding SSE progress updates.
    This function contains the core logic adapted from the original upload_file route.
    """
    logging.info(f"[{upload_id}] Starting processing...")
    upload_data = upload_progress_data.get(upload_id)

    if not upload_data or not upload_data.get('temp_file_path'):
        logging.error(f"[{upload_id}] Critical error: Upload data or temp_file_path missing.")
        yield f"event: error\ndata: {json.dumps({'message': 'Internal server error: Upload data missing.'})}\n\n"
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

    try:
        # --- Determine file size (from the temporary file) ---
        if not os.path.exists(temp_file_path):
             raise FileNotFoundError(f"Temporary file not found: {temp_file_path}")
        total_size = os.path.getsize(temp_file_path)
        logging.info(f"[{upload_id}] Original temp file size: {total_size} bytes.")

        if total_size == 0:
            raise ValueError("Uploaded file is empty.")

        # --- Yield initial size ---
        # The frontend needs the total size to calculate percentages correctly
        yield f"event: start\ndata: {json.dumps({'filename': original_filename, 'totalSize': total_size})}\n\n"


        # --- Decide workflow: single file or split ---
        if total_size <= CHUNK_SIZE:
            # --- Single File Upload Workflow (Adapted) ---
            logging.info(f"[{upload_id}] '{original_filename}' is small. Compressing and sending as single file.")
            yield f"event: status\ndata: {json.dumps({'message': 'Compressing file...'})}\n\n"

            zip_buffer = io.BytesIO()
            # Read from the temporary file on disk
            with open(temp_file_path, 'rb') as f_in, \
                 zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                 zip_file.writestr(original_filename, f_in.read())

            compressed_size = zip_buffer.tell()
            zip_buffer.seek(0)
            compressed_filename = f"{original_filename}.zip"
            logging.info(f"[{upload_id}] Compressed '{original_filename}' to {compressed_size} bytes.")

            # --- Yield progress before sending (can represent compression as part of progress) ---
            # For small files, we can just show 0% then jump to 100% on completion,
            # or show an intermediate step like 50% for "sending".
            yield f"event: progress\ndata: {json.dumps({'bytesSent': 0, 'totalBytes': compressed_size, 'percentage': 0, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
            yield f"event: status\ndata: {json.dumps({'message': 'Sending compressed file...'})}\n\n"

            tg_send_start_time = time.time()
            success, message, tg_response_json = send_file_to_telegram(zip_buffer, compressed_filename)
            tg_send_duration = time.time() - tg_send_start_time
            zip_buffer.close()

            if success and tg_response_json:
                logging.info(f"[{upload_id}] Single file '{original_filename}' sent successfully.")
                # --- Yield Final Progress ---
                yield f"event: progress\ndata: {json.dumps({'bytesSent': compressed_size, 'totalBytes': compressed_size, 'percentage': 100, 'speedMBps': (compressed_size / (1024*1024) / tg_send_duration) if tg_send_duration > 0 else 0, 'etaFormatted': '00:00'})}\n\n"

                # --- Save Metadata ---
                try:
                    result_data = tg_response_json.get('result', {})
                    message_id = result_data.get('message_id')
                    doc_data = result_data.get('document', {})
                    file_id = doc_data.get('file_id')
                    file_unique_id = doc_data.get('file_unique_id')

                    if not message_id or not file_unique_id:
                        raise ValueError("Missing message_id or file_unique_id in Telegram response")
                    
                    access_id = uuid.uuid4().hex[:10]

                    metadata = load_metadata()
                    timestamp = datetime.now(timezone.utc).isoformat()
                    new_file_record = {
                        "original_filename": original_filename, "sent_filename": compressed_filename,
                        "is_split": False, "is_compressed": True, "original_size": total_size,
                        "compressed_size": compressed_size, "telegram_message_id": message_id,
                        "telegram_file_id": file_id, "telegram_file_unique_id": file_unique_id,
                        "upload_timestamp": timestamp, "username": username,
                        "upload_duration_seconds": tg_send_duration,
                        "access_id": access_id
                    }
                    
                    user_files_list = metadata.setdefault(username, [])
                    user_files_list.append(new_file_record)
                    if not save_metadata(metadata):
                         logging.error(f"[{upload_id}] CRITICAL: File sent, but FAILED TO SAVE METADATA.")
                         # Yield a warning/error event even on success? Maybe just log.

                    # --- Yield Completion Event (Modified) ---
                    yield f"event: complete\ndata: {json.dumps({'message': f'File {original_filename} uploaded successfully!', 'access_id': access_id, 'filename': original_filename})}\n\n" # <<< Step 1: Send access_id
                    upload_data['status'] = 'completed'
                    

                except Exception as e:
                    logging.error(f"[{upload_id}] Error processing response/saving metadata for single file: {e}", exc_info=True)
                    # Don't yield error here, as the upload itself succeeded. Log is sufficient.

                # --- Yield Completion Event ---
                upload_data['status'] = 'completed_metadata_error'
                #yield f"event: complete\ndata: {json.dumps({'message': f'File {original_filename} uploaded successfully!'})}\n\n"
                #upload_data['status'] = 'completed'

            else: # Send failed
                 raise IOError(f"Telegram API Error: {message}")

        else:
            # --- Large File: Compress First, then Split Workflow (Adapted) ---
            logging.info(f"[{upload_id}] '{original_filename}' is large. Compressing before splitting.")
            compressed_filename = f"{original_filename}.zip"

            yield f"event: status\n data: {json.dumps({'message': 'Compressing large file...'})}\n\n"

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
                            chunk = f_in.read(buffer_size)  
                            if not chunk: break
                            zip_entry.write(chunk)
            compression_time = time.time() - compression_start_time
            compressed_total_size = os.path.getsize(temp_compressed_zip_filepath)
            logging.info(f"[{upload_id}] Finished compressing to '{temp_compressed_zip_filepath}'. Size: {compressed_total_size} bytes. Time: {compression_time:.2f}s.")

            yield f"event: status\ndata: {json.dumps({'message': 'Starting chunked upload...'})}\n\n"
            # Update total size for progress calculation to the compressed size
            yield f"event: start\ndata: {json.dumps({'filename': compressed_filename, 'totalSize': compressed_total_size})}\n\n"

            # --- Now Split the *Compressed* Temporary File ---
            chunk_number = 0
            uploaded_chunks_metadata = []
            bytes_read_from_compressed = 0
            total_tg_send_duration_split = 0
            start_time_split_upload = None # ETA specific start time
            bytes_successfully_sent = 0  # ETA specific counter

            # 3. Open the COMPRESSED temporary file for reading chunks
            with open(temp_compressed_zip_filepath, 'rb') as temp_file_to_read:
                while True:
                    # loop_chunk_start_time = time.time() # Not strictly needed for SSE
                    chunk_number += 1
                    logging.info(f"[{upload_id}] Reading chunk {chunk_number} for COMPRESSED file starting at byte {bytes_read_from_compressed}.")

                    file_chunk_data = temp_file_to_read.read(CHUNK_SIZE)
                    current_chunk_size = len(file_chunk_data)

                    if not file_chunk_data:
                        logging.info(f"[{upload_id}] Finished reading all chunks for compressed file.")
                        break

                    bytes_read_from_compressed += current_chunk_size
                    logging.info(f"[{upload_id}] Read chunk {chunk_number} ({current_chunk_size} bytes). Total read: {bytes_read_from_compressed}/{compressed_total_size}")

                    chunk_part_filename = f"{compressed_filename}.part_{str(chunk_number).zfill(3)}"
                    chunk_file_object = io.BytesIO(file_chunk_data)

                    # --- ETA: Record start time on first chunk ---
                    if chunk_number == 1:
                        start_time_split_upload = time.time()

                    logging.info(f"[{upload_id}] Attempting to send chunk: '{chunk_part_filename}'")
                    tg_chunk_send_start_time = time.time()

                    success, message, tg_response_json = send_file_to_telegram(chunk_file_object, chunk_part_filename)
                    chunk_file_object.close()

                    tg_chunk_send_end_time = time.time()
                    tg_chunk_duration = tg_chunk_send_end_time - tg_chunk_send_start_time
                    total_tg_send_duration_split += tg_chunk_duration

                    if success and tg_response_json:
                        bytes_successfully_sent += current_chunk_size
                        logging.info(f"[{upload_id}] Chunk '{chunk_part_filename}' sent successfully.")

                        # --- Calculate & Yield Progress/ETA ---
                        progress_data = {"bytesSent": bytes_successfully_sent, "totalBytes": compressed_total_size, "percentage": 0, "speedMBps": 0, "etaFormatted": "--:--", "etaSeconds": -1}
                        if start_time_split_upload is not None and bytes_successfully_sent > 0:
                             elapsed_time = time.time() - start_time_split_upload
                             if elapsed_time > 0:
                                 average_speed_bps = bytes_successfully_sent / elapsed_time
                                 if average_speed_bps > 0:
                                     remaining_bytes = compressed_total_size - bytes_successfully_sent
                                     progress_data["percentage"] = (bytes_successfully_sent / compressed_total_size) * 100
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
                        yield f"event: status\ndata: {json.dumps({'message': f'Sent chunk {chunk_number}'})}\n\n"


                        # --- Store Chunk Metadata ---
                        try:
                            result_data = tg_response_json.get('result', {})
                            message_id = result_data.get('message_id')
                            doc_data = result_data.get('document', {})
                            file_id = doc_data.get('file_id')
                            file_unique_id = doc_data.get('file_unique_id')

                            if not message_id or not file_unique_id:
                                raise ValueError("Missing message_id or file_unique_id in chunk response")

                            chunk_meta = {
                                "part_number": chunk_number, "chunk_filename": chunk_part_filename,
                                "message_id": message_id, "file_id": file_id,
                                "file_unique_id": file_unique_id,
                                "chunk_upload_duration_seconds": tg_chunk_duration
                            }
                            uploaded_chunks_metadata.append(chunk_meta)
                            logging.info(f"[{upload_id}] Successfully processed chunk '{chunk_part_filename}' (MsgID={message_id}) in {tg_chunk_duration:.2f}s.")

                        except Exception as e:
                             # Abort on metadata processing error for a chunk
                             logging.error(f"[{upload_id}] Error processing Telegram response for chunk '{chunk_part_filename}': {e}. Aborting.", exc_info=True)
                             raise ValueError(f"Error processing response for chunk {chunk_number}. Upload incomplete.") from e

                    else: # Send chunk failed
                         logging.error(f"[{upload_id}] Failed to send chunk '{chunk_part_filename}'. Aborting. Error: {message}")
                         raise IOError(f"Error sending chunk {chunk_number} ('{chunk_part_filename}'): {message}. Upload incomplete.")
            # --- End of while loop / with open(compressed_file) ---

            # --- After the loop: Check consistency and Save Metadata ---
            expected_chunks = (compressed_total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
            if len(uploaded_chunks_metadata) == expected_chunks:
                logging.info(f"[{upload_id}] All {expected_chunks} chunks uploaded successfully. Saving metadata.")

                # <<< Step 1 START: Generate and Add access_id >>>
                access_id = uuid.uuid4().hex[:10] # Generate unique ID
                logging.info(f"[{upload_id}] Generated Access ID: {access_id}")
                # <<< Step 1 END: Generate and Add access_id >>>
                
                # --- Save Metadata ---
                metadata = load_metadata()
                timestamp = datetime.now(timezone.utc).isoformat()
                new_file_record = {
                    "original_filename": original_filename, "sent_filename": compressed_filename,
                    "is_split": True, "is_compressed": True, "original_size": total_size,
                    "compressed_total_size": compressed_total_size, "chunk_size": CHUNK_SIZE,
                    "num_chunks": expected_chunks, "chunks": uploaded_chunks_metadata,
                    "upload_timestamp": timestamp, "username": username,
                    "total_upload_duration_seconds": total_tg_send_duration_split,
                    "access_id": access_id
                }
                user_files_list = metadata.setdefault(username, [])
                user_files_list.append(new_file_record)
                if not save_metadata(metadata):
                     logging.error(f"[{upload_id}] CRITICAL: Chunks sent, but FAILED TO SAVE METADATA.")
                     # Yield warning?

                # --- Yield Completion ---
                # --- Yield Completion (Modified for Split File) ---
                yield f"event: complete\ndata: {json.dumps({'message': f'Large file {original_filename} uploaded successfully!', 'access_id': access_id, 'filename': original_filename})}\n\n" # <<< Step 1: Send access_id
                upload_data['status'] = 'completed'

            else: # Inconsistency
                 logging.error(f"[{upload_id}] Inconsistency after upload. Expected {expected_chunks} chunks, got metadata for {len(uploaded_chunks_metadata)}. Aborting save.")
                 raise SystemError("Internal inconsistency uploading chunks.") # Raise error to trigger error event

    except Exception as e:
        # --- Handle any error during processing ---
        error_message = f"Upload failed: {e}"
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
            except OSError as e:
                logging.error(f"[{upload_id}] Error deleting original temporary file '{temp_file_path}': {e}", exc_info=True)

        # Delete the COMPRESSED temporary file if it was created
        if temp_compressed_zip_filepath and os.path.exists(temp_compressed_zip_filepath):
             try:
                 os.remove(temp_compressed_zip_filepath)
                 logging.info(f"[{upload_id}] Successfully deleted temporary compressed file: {temp_compressed_zip_filepath}")
             except OSError as e:
                 logging.error(f"[{upload_id}] Error deleting temporary compressed file '{temp_compressed_zip_filepath}': {e}", exc_info=True)

        # Optionally remove the entry from the global dict if completed or errored?
        # Or keep it for potential inspection? Let's keep it for now.
        final_status = upload_progress_data.get(upload_id, {}).get('status', 'unknown (entry removed)')
        logging.info(f"[{upload_id}] Processing finished with status: {upload_data.get('status', 'unknown')}")

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

def _prepare_download_and_generate_updates(prep_id, username, filename):
    """
    Generator function: Prepares file for download, yields status updates
    matching the upload flow style, and estimated progress.
    """
    logging.info(f"[{prep_id}] Starting download preparation generator...")
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
    total_bytes_to_fetch = 0
    final_expected_size = 0
    original_filename = filename # Default

    try:
        # --- Initializing Phase ---
        yield f"event: filename\ndata: {json.dumps({'filename': filename})}\n\n"
        yield f"event: status\ndata: {json.dumps({'message': 'Initializing...'})}\n\n" # UPDATED MESSAGE
        yield f"event: progress\ndata: {json.dumps({'percentage': 0, 'bytesProcessed': 0, 'totalBytes': 0, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
        time.sleep(0.2) # Slightly longer pause for "Initializing"

        metadata = load_metadata()
        user_files = metadata.get(username, [])
        file_info = next((f for f in user_files if f.get('original_filename') == filename), None)
        if not file_info: raise FileNotFoundError(f"File '{filename}' not found.")

        is_split = file_info.get('is_split', False)
        is_compressed = file_info.get('is_compressed', False)
        original_filename = file_info.get('original_filename')
        final_expected_size = file_info.get('original_size', 0)
        prep_data['original_filename'] = original_filename
        if original_filename != filename:
             yield f"event: filename\ndata: {json.dumps({'filename': original_filename})}\n\n"
        yield f"event: totalSizeUpdate\ndata: {json.dumps({'totalSize': final_expected_size})}\n\n"

        # --- Preparing Phase ---
        yield f"event: status\ndata: {json.dumps({'message': 'Preparing file...'})}\n\n" # UPDATED MESSAGE

        if not is_split:
            # --- Single File Prep ---
            logging.info(f"[{prep_id}] Prep non-split '{original_filename}'")
            percentage = 10 # Represents starting the download part of prep
            bytes_processed = int(final_expected_size * (percentage / 100))
            yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"
            # yield f"event: status\ndata: {json.dumps({'message': 'Downloading from source...'})}\n\n" # Subsumed into "Preparing"

            telegram_file_id = file_info.get('telegram_file_id')
            if not telegram_file_id: raise ValueError("Missing 'telegram_file_id'")
            sent_filename = file_info.get('sent_filename')

            start_dl_time = time.time()
            file_content_bytes, error_msg = download_telegram_file_content(telegram_file_id)
            dl_duration = time.time() - start_dl_time
            dl_speed = (len(file_content_bytes) / (1024*1024) / dl_duration) if dl_duration > 0 and file_content_bytes else 0

            if error_msg: raise ValueError(f"TG download failed: {error_msg}")
            if not file_content_bytes: raise ValueError("TG downloaded empty content.")

            percentage = 50 # Download complete, before potential decompression
            bytes_processed = int(final_expected_size * (percentage / 100))
            yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': dl_speed, 'etaFormatted': '--:--'})}\n\n"

            if is_compressed:
                # yield f"event: status\ndata: {json.dumps({'message': 'Decompressing...'})}\n\n" # Subsumed
                logging.info(f"[{prep_id}] Decompressing single file...")
                try:
                    # ... (decompression logic as before) ...
                    zip_buffer = io.BytesIO(file_content_bytes)
                    zip_file_handle = zipfile.ZipFile(zip_buffer, 'r')
                    file_list_in_zip = zip_file_handle.namelist()
                    if not file_list_in_zip: raise ValueError("Zip empty.")
                    inner_filename = original_filename
                    if original_filename not in file_list_in_zip:
                         if len(file_list_in_zip) == 1: inner_filename = file_list_in_zip[0]
                         else: raise ValueError(f"Cannot find '{original_filename}' in zip.")
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_decomp_{prep_id}_") as tf:
                        temp_decompressed_path_local = tf.name
                        with zip_file_handle.open(inner_filename, 'r') as inner_file_stream:
                            shutil.copyfileobj(inner_file_stream, tf)
                    temp_final_file_path_local = temp_decompressed_path_local
                finally:
                    if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
            else: # Non-split, Non-compressed
                # yield f"event: status\ndata: {json.dumps({'message': 'Writing temporary file...'})}\n\n" # Subsumed
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_nocomp_{prep_id}_") as tf:
                    temp_decompressed_path_local = tf.name
                    tf.write(file_content_bytes)
                temp_final_file_path_local = temp_decompressed_path_local
            
            percentage = 100 # Final prep stage done
            bytes_processed = final_expected_size
            yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '00:00'})}\n\n"


        else: # is_split is True
            # --- Split File Prep ---
            logging.info(f"[{prep_id}] Prep SPLIT download for '{original_filename}'")
            chunks_metadata = file_info.get('chunks', [])
            if not chunks_metadata: raise ValueError("Missing 'chunks' list.")
            chunks_metadata.sort(key=lambda c: c.get('part_number', 0))
            num_chunks_total = len(chunks_metadata)
            total_bytes_to_fetch = file_info.get('compressed_total_size', 0)
            if total_bytes_to_fetch == 0: logging.warning(f"[{prep_id}] Compressed size unknown.")

            # Still use "Preparing file..." as the main status during fetch
            # yield f"event: status\ndata: {json.dumps({'message': 'Reassembling file parts...'})}\n\n"
            yield f"event: progress\ndata: {json.dumps({'percentage': 0, 'bytesProcessed': 0, 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n"

            with tempfile.NamedTemporaryFile(suffix=".zip.tmp", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_zip_path_local = tf_reassemble.name
                start_time_part_fetch = time.time()
                bytes_fetched_from_tg = 0

                for i, chunk_info in enumerate(chunks_metadata):
                    part_num = chunk_info.get('part_number')
                    chunk_file_id = chunk_info.get('file_id')
                    if not chunk_file_id: raise ValueError(f"Tracking info missing part {part_num}.")

                    # Maybe don't yield status for every part? Or make it less prominent?
                    # Let's keep it in logs but not send to UI for now to simplify status text
                    logging.debug(f"[{prep_id}] Downloading chunk {part_num}/{num_chunks_total}...")

                    chunk_content_bytes, error_msg = download_telegram_file_content(chunk_file_id)
                    if error_msg: raise ValueError(f"Error downloading part {part_num}: {error_msg}")
                    if not chunk_content_bytes: raise ValueError(f"Part {part_num} empty.")
                    tf_reassemble.write(chunk_content_bytes)

                    bytes_fetched_from_tg += len(chunk_content_bytes)
                    percentage = ((i + 1) / num_chunks_total) * 100
                    bytes_processed = int(final_expected_size * (percentage / 100)) # Estimate final bytes based on parts %

                    # Calculate Fetch Speed/ETA
                    current_speed_mbps = 0; eta_formatted = "--:--"
                    elapsed_time = time.time() - start_time_part_fetch
                    if elapsed_time > 0 and bytes_fetched_from_tg > 0:
                         average_speed_bps = bytes_fetched_from_tg / elapsed_time
                         current_speed_mbps = average_speed_bps / (1024*1024)
                         if total_bytes_to_fetch > 0 and average_speed_bps > 0:
                              remaining_bytes = total_bytes_to_fetch - bytes_fetched_from_tg
                              if remaining_bytes > 0: eta_formatted = format_time(remaining_bytes / average_speed_bps)
                              else: eta_formatted = "00:00"
                    
                    yield f"event: progress\ndata: {json.dumps({'percentage': percentage, 'bytesProcessed': bytes_processed, 'totalBytes': final_expected_size, 'speedMBps': current_speed_mbps, 'etaFormatted': eta_formatted})}\n\n"

            logging.info(f"[{prep_id}] Finished reassembling. Total bytes: {bytes_fetched_from_tg}.")
            # Ensure progress shows 100% fetch complete
            yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': int(final_expected_size * 0.9), 'totalBytes': final_expected_size, 'speedMBps': current_speed_mbps, 'etaFormatted': '00:00'})}\n\n"


            if is_compressed:
                # Keep "Preparing file..." status during decompression
                # yield f"event: status\ndata: {json.dumps({'message': 'Decompressing...'})}\n\n"
                yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': int(final_expected_size * 0.95), 'totalBytes': final_expected_size, 'speedMBps': 0, 'etaFormatted': '--:--'})}\n\n" # Show 95%
                logging.info(f"[{prep_id}] Decompressing reassembled file...")
                try:
                    # ... (decompression logic as before) ...
                    zip_file_handle = zipfile.ZipFile(temp_reassembled_zip_path_local, 'r')
                    file_list_in_zip = zip_file_handle.namelist()
                    if not file_list_in_zip: raise ValueError("Zip empty.")
                    inner_filename = original_filename
                    if original_filename not in file_list_in_zip:
                         if len(file_list_in_zip) == 1: inner_filename = file_list_in_zip[0]
                         else: raise ValueError(f"Cannot find '{original_filename}' in zip.")
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf_final:
                        temp_decompressed_path_local = tf_final.name
                        with zip_file_handle.open(inner_filename, 'r') as inner_file_stream:
                            shutil.copyfileobj(inner_file_stream, tf_final)
                    temp_final_file_path_local = temp_decompressed_path_local
                finally:
                     if zip_file_handle: zip_file_handle.close(); zip_file_handle = None
            else:
                 logging.info(f"[{prep_id}] Split file not compressed. Using reassembled.")
                 temp_final_file_path_local = temp_reassembled_zip_path_local
                 temp_reassembled_zip_path_local = None

        # --- Preparation Complete ---
        if not temp_final_file_path_local or not os.path.exists(temp_final_file_path_local):
            raise RuntimeError("Failed to produce final temp file.")

        final_actual_size = os.path.getsize(temp_final_file_path_local)
        logging.info(f"[{prep_id}] Final prepared file: '{temp_final_file_path_local}', Size: {final_actual_size}")

        prep_data['final_temp_file_path'] = temp_final_file_path_local
        prep_data['final_file_size'] = final_actual_size
        prep_data['status'] = 'ready'

        # Yield final "Ready" status and 100% progress
        yield f"event: status\ndata: {json.dumps({'message': f'File {original_filename} ready for download.'})}\n\n" # UPDATED MESSAGE
        yield f"event: progress\ndata: {json.dumps({'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_actual_size, 'speedMBps': 0, 'etaFormatted': '00:00'})}\n\n"

        yield f"event: ready\ndata: {json.dumps({'temp_file_id': prep_id, 'final_filename': original_filename})}\n\n"
        logging.info(f"[{prep_id}] Preparation complete. Sent 'ready' event.")

        # Cleanup intermediate reassembled file (if applicable and different)
        if temp_reassembled_zip_path_local and temp_reassembled_zip_path_local != temp_final_file_path_local and os.path.exists(temp_reassembled_zip_path_local):
            logging.info(f"[{prep_id}] Cleaning up intermediate file: {temp_reassembled_zip_path_local}")
            try: os.remove(temp_reassembled_zip_path_local)
            except OSError as e: logging.error(f"[{prep_id}] Error deleting intermediate file: {e}")

    except Exception as e:
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
        logging.error(f"[{prep_id}] {error_message}", exc_info=True)
        yield f"event: error\ndata: {json.dumps({'message': error_message})}\n\n"
        prep_data['status'] = 'error'; prep_data['error'] = error_message
        # Cleanup ALL potential temp files on error
        logging.info(f"[{prep_id}] Cleaning up temp files due to prep error.")
        paths_to_clean = [p for p in [temp_decompressed_path_local, temp_reassembled_zip_path_local, temp_final_file_path_local] if p]
        for path_to_delete in list(dict.fromkeys(paths_to_clean)):
            if path_to_delete and os.path.exists(path_to_delete):
                try: os.remove(path_to_delete); logging.info(f"[{prep_id}] Cleaned (error): {path_to_delete}")
                except OSError as err: logging.error(f"[{prep_id}] Error deleting temp file {path_to_delete} on error: {err}")

    logging.info(f"[{prep_id}] Generator finished with status: {prep_data.get('status')}")

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


@app.route('/get/<access_id>')
def get_file_by_access_id(access_id):
    """
    Looks up a file by its access_id and renders a download page.
    """
    logging.info(f"Received request for access_id: {access_id}")
    metadata = load_metadata()
    found_file_info = None
    found_username = None

    # Iterate through all users and their files to find the access_id
    for username, files in metadata.items():
        for file_info in files:
            if file_info.get('access_id') == access_id:
                found_file_info = file_info
                found_username = username
                break # Stop inner loop once found
        if found_file_info:
            break # Stop outer loop once found

    if not found_file_info:
        logging.warning(f"Access ID '{access_id}' not found in metadata.")
        # You could render a nicer 404 template here
        return make_response(render_template('404_error.html', message=f"File link '{access_id}' not found or expired."), 404)
        #return make_response("File not found or link expired.", 404)

    # Extract details for the download page
    original_filename = found_file_info.get('original_filename', 'Unknown Filename')
    # Show original size if available, otherwise maybe compressed size as fallback?
    file_size_bytes = found_file_info.get('original_size')
    if not file_size_bytes: # Fallback if original_size wasn't stored correctly
         file_size_bytes = found_file_info.get('compressed_total_size', 0)

    upload_timestamp_iso = found_file_info.get('upload_timestamp')
    upload_datetime_str = "Unknown date" # Default

    # Optional: Parse and format the timestamp nicely
    if upload_timestamp_iso:
        try:
            # Parse the ISO string (aware of timezone offset from isoformat())
            upload_dt_utc = dateutil_parser.isoparse(upload_timestamp_iso)
            # Convert to local timezone (or keep as UTC if preferred)
            # Example: Convert to US Eastern time
            # local_tz = pytz.timezone('America/New_York')
            # upload_dt_local = upload_dt_utc.astimezone(local_tz)
            # upload_datetime_str = upload_dt_local.strftime('%Y-%m-%d %H:%M:%S %Z') # Format with timezone

            # Simpler: Format as UTC or just date/time
            upload_datetime_str = upload_dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

        except Exception as e:
            logging.warning(f"Could not parse timestamp '{upload_timestamp_iso}': {e}")
            # Keep default "Unknown date"

    logging.info(f"Found file '{original_filename}' for user '{found_username}' with access_id '{access_id}'. Rendering download page.")

    # Render the NEW download page template, passing necessary info
    return render_template('download_page.html',
                           filename=original_filename,
                           filesize=file_size_bytes, # Pass raw bytes
                           upload_date=upload_datetime_str,
                           username=found_username, # Needed for the download button action
                           access_id=access_id      # May not be strictly needed by template, but good practice
                           )

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