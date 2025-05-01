# --- START OF FILE routes.py ---

"""Flask routes and core logic for the Telegram File Storage tool."""
import io
import os
import uuid
import time
import json
import zipfile
import tempfile
import shutil
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List, Generator, Union
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from flask import ( Flask, request, render_template, flash, redirect, url_for,
    make_response, jsonify, send_file, Response, stream_with_context )
from dateutil import parser as dateutil_parser
import threading
# --- Import necessary components ---
# Assume database.py is correctly set up for metadata operations
import database # Assuming database.py handles the actual DB interactions
from database import (
    save_file_metadata,
    find_metadata_by_username,
    find_metadata_by_access_id,
    delete_metadata_by_filename
)
# Assume config.py holds configurations and utility functions
from app_setup import app, upload_progress_data, download_prep_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, MAX_DOWNLOAD_WORKERS,
    format_bytes, format_time # Make sure format_time is available if used here
)
# Assume telegram_api.py handles the Telegram interactions
from telegram_api import send_file_to_telegram, download_telegram_file_content

# --- Type Aliases ---
Metadata = Dict[str, List[Dict[str, Any]]]
UploadProgressData = Dict[str, Any]; DownloadPrepData = Dict[str, Any]
SseEvent = str; ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
ChunkDataResult = Tuple[int, Optional[bytes], Optional[str]]

# --- Constants ---
DEFAULT_CHUNK_READ_SIZE = 4 * 1024 * 1024; STREAM_CHUNK_SIZE = 65536

# --- Flask Routes ---
@app.route('/')
def index() -> str:
    logging.info("Serving index page.")
    try: return render_template('index.html')
    except Exception as e: logging.error(f"Error rendering index.html: {e}", exc_info=True); return make_response("Error loading page.", 500)

@app.route('/initiate-upload', methods=['POST'])
def initiate_upload() -> Response:
    """
    Handles the initial file upload from the client.
    Saves the file temporarily and returns an upload_id to track progress.
    This endpoint should return *quickly* after saving the file.
    """
    logging.info("Request initiate upload.")
    if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
    username = request.form.get('username','').strip()
    if not username: return jsonify({"error": "Username required"}), 400
    file = request.files['file']
    if not file or file.filename == '': return jsonify({"error": "No file selected"}), 400

    original_filename = file.filename
    # Generate a unique ID for this upload process
    upload_id = str(uuid.uuid4())
    # Construct the temporary path for the uploaded file
    temp_file_path = os.path.join(UPLOADS_TEMP_DIR, f"{upload_id}_{original_filename}")
    logging.info(f"[{upload_id}] Preparing to save upload to temp path: {temp_file_path}")

    try:
        os.makedirs(UPLOADS_TEMP_DIR, exist_ok=True) 
        file.save(temp_file_path) 
        logging.info(f"[{upload_id}] Successfully saved temporary file: '{original_filename}' ({os.path.getsize(temp_file_path)} bytes).")

        upload_progress_data[upload_id] = {
            "status": "initiated", 
            "original_filename": original_filename,
            "temp_file_path": temp_file_path,
            "username": username,
            "error": None,
            "start_time": time.time() 
        }
        logging.debug(f"[{upload_id}] Initial progress data stored. Status: initiated.")

        return jsonify({"upload_id": upload_id, "filename": original_filename})

    except Exception as e:
        logging.error(f"Error saving temporary file '{original_filename}' (ID:{upload_id}): {e}", exc_info=True)

        _safe_remove_file(temp_file_path, upload_id, "partial temp on save error")

        if upload_id in upload_progress_data:
            del upload_progress_data[upload_id]
        return jsonify({"error": f"Server error saving file: {e}"}), 500

@app.route('/stream-progress/<upload_id>')
def stream_progress(upload_id: str) -> Response:
    """
    Server-Sent Events (SSE) endpoint to stream upload progress updates.
    Triggers the background processing (compression, Telegram upload).
    """
    logging.info(f"SSE connection request received for upload_id: {upload_id}")

    # Check the status associated with the upload_id
    upload_info = upload_progress_data.get(upload_id)
    status = upload_info.get('status', 'unknown') if upload_info else 'unknown'

    if not upload_info or status != 'initiated':
        error_message = f"Invalid or expired upload session ID: {upload_id}. Status: {status}."
        logging.warning(error_message)
        def stream_error_and_close():
            yield _yield_sse_event('error', {'message': error_message})
        return Response(stream_with_context(stream_error_and_close()), mimetype='text/event-stream')

    # If status is 'initiated', start the background processing generator
    logging.info(f"[{upload_id}] Valid SSE request. Starting background processing generator...")
    return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')

# --- Helper Functions ---
def _yield_sse_event(event_type: str, data: Dict[str, Any]) -> SseEvent:
    """Formats data as a Server-Sent Event string."""
    json_data = json.dumps(data); return f"event: {event_type}\ndata: {json_data}\n\n"

def _send_single_file_task(file_bytes: bytes, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    """Task function to send a single file (in memory) to Telegram."""
    try:
        buffer = io.BytesIO(file_bytes)
        logging.info(f"[{upload_id}] T> Sending single file '{filename}' ({len(file_bytes)} bytes) to chat {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"[{upload_id}] T> Sent single file '{filename}' to chat {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Error sending single file to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    """Task function to send a file chunk (in memory) to Telegram."""
    try:
        buffer = io.BytesIO(chunk_data); logging.info(f"[{upload_id}] T> Sending chunk {chunk_num} ('{filename}', {len(chunk_data)} bytes) to chat {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id); buffer.close()
        logging.info(f"[{upload_id}] T> Sent chunk {chunk_num} to chat {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Error sending chunk {chunk_num} to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)

def _download_chunk_task(file_id: str, part_num: int, prep_id: str) -> ChunkDataResult:
    """Task function to download a file chunk from Telegram."""
    logging.info(f"[{prep_id}] T> Starting download chunk {part_num} (Telegram file_id: {file_id})")
    try:
        content, err_msg = download_telegram_file_content(file_id)
        if err_msg: logging.error(f"[{prep_id}] T> API Error downloading chunk {part_num}: {err_msg}"); return part_num, None, err_msg
        elif not content: logging.error(f"[{prep_id}] T> Error downloading chunk {part_num}: Received empty content."); return part_num, None, "Empty chunk content received."
        else: logging.info(f"[{prep_id}] T> Successfully downloaded chunk {part_num} ({len(content)} bytes)."); return part_num, content, None
    except Exception as e: logging.error(f"[{prep_id}] T> Unexpected thread error downloading chunk {part_num}: {e}", exc_info=True); return part_num, None, f"Thread error: {e}"

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parses results from Telegram send tasks to extract necessary details."""
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Union[str, int, bool, None]] = {"chat_id": res.get("chat_id", "N/A"), "success": res.get("success", False)}
        if detail["success"] and res.get("tg_response"):
            res_data = res["tg_response"].get('result', {})
            msg_id = res_data.get('message_id'); doc_data = res_data.get('document', {})
            f_id = doc_data.get('file_id'); f_uid = doc_data.get('file_unique_id'); f_size = doc_data.get('file_size')
            if msg_id and f_id and f_uid:
                detail["message_id"] = msg_id; detail["file_id"] = f_id; detail["file_unique_id"] = f_uid
                if f_size is not None: detail["file_size"] = f_size
                logging.debug(f"[{log_prefix}] Parsed TG success: Chat={detail['chat_id']}, MsgID={msg_id}, FileID={f_id}")
            else:
                detail["success"] = False; detail["error"] = "Missing key IDs in successful Telegram response"
                logging.warning(f"[{log_prefix}] Missing IDs in successful TG response: {res['tg_response']}")
        elif not detail["success"]:
            detail["error"] = res.get("message", "Unknown failure reason")
            logging.warning(f"[{log_prefix}] Parsed TG failure: Chat={detail['chat_id']}, Error: {detail['error']}")
        all_chat_details.append(detail)
    return all_chat_details

def _calculate_progress(start_time: float, bytes_done: int, total_bytes: int) -> Dict[str, Any]:
    """Calculates progress percentage, speed, and ETA. (Used internally if needed, not sent via SSE anymore)"""
    progress = {"bytesSent": bytes_done, "totalBytes": total_bytes, "percentage": 0, "speedMBps": 0, "etaFormatted": "--:--", "etaSeconds": -1}
    if total_bytes <= 0: return progress 
    progress["percentage"] = min(max((bytes_done / total_bytes) * 100, 0), 100) 
    elapsed = time.time() - start_time
    if elapsed > 0.1 and bytes_done > 0: 
        speed_bps = bytes_done / elapsed; progress["speedMBps"] = speed_bps / (1024 * 1024)
        remaining_bytes = total_bytes - bytes_done
        if remaining_bytes > 0 and speed_bps > 0:
            eta_sec = remaining_bytes / speed_bps; progress["etaSeconds"] = eta_sec; progress["etaFormatted"] = format_time(eta_sec)
        else: 
             progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    elif bytes_done == total_bytes: 
        progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    return progress

def _find_best_telegram_file_id(locations: List[Dict[str, Any]], primary_chat_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Finds the best available Telegram file_id, preferring the primary chat."""
    primary_found: Optional[Tuple[str, str]] = None
    fallback_found: Optional[Tuple[str, str]] = None
    primary_cid_str = str(primary_chat_id) if primary_chat_id else None

    for loc in locations:
        if loc.get('success') and loc.get('file_id'):
            chat_id = str(loc.get('chat_id'))
            file_id = loc.get('file_id')
            if primary_cid_str and chat_id == primary_cid_str:
                primary_found = (file_id, chat_id)
                break 
            elif not fallback_found: 
                fallback_found = (file_id, chat_id)

    if primary_found:
        logging.debug(f"Found primary file_id '{primary_found[0]}' in chat {primary_found[1]}")
        return primary_found
    elif fallback_found:
        logging.debug(f"Using fallback file_id '{fallback_found[0]}' from chat {fallback_found[1]}")
        return fallback_found
    else:
        logging.warning("No successful Telegram location found to get a file_id.")
        return None, None

def _find_filename_in_zip(zf: zipfile.ZipFile, expected_filename: str, log_prefix: str) -> str:
    """Finds the correct filename inside a zip, handling minor variations."""
    namelist = zf.namelist()
    if not namelist:
        raise ValueError("Zip file is empty.")
    if expected_filename in namelist:
        return expected_filename
    if len(namelist) == 1:
        actual = namelist[0]
        logging.warning(f"[{log_prefix}] Expected filename '{expected_filename}' not found in zip. Using the only entry: '{actual}'")
        return actual
    # Try matching without extension if original had one
    base_expected, _ = os.path.splitext(expected_filename)
    for name in namelist:
        if name == base_expected:
            logging.warning(f"[{log_prefix}] Expected filename '{expected_filename}' not found. Using entry matching base name: '{name}'")
            return name
    # If still not found, raise error
    raise ValueError(f"Cannot find expected file '{expected_filename}' or a suitable alternative within the zip archive. Contents: {namelist}")

def _calculate_download_fetch_progress(start_time: float, bytes_fetched: int, total_bytes_expected_fetch: int, chunks_done_count: int, total_chunks_count: int, base_progress_percentage: float, final_decompressed_size: int) -> Dict[str, Any]:
    """Calculates progress during the chunk fetching phase of download prep."""
    prog = {
        'percentage': base_progress_percentage, # Base percentage completion (e.g., 0-80% allocated for fetching)
        'bytesProcessed': bytes_fetched,
        'totalBytes': total_bytes_expected_fetch if total_bytes_expected_fetch > 0 else 0,
        'speedMBps': 0,
        'etaFormatted': '--:--',
        'displayTotalBytes': final_decompressed_size # Added field for UI total
    }
    elapsed = time.time() - start_time
    if elapsed > 0.1 and bytes_fetched > 0:
        speed_bps = bytes_fetched / elapsed
        prog['speedMBps'] = speed_bps / (1024 * 1024)
        # --- ETA Calculation ---
        eta_seconds = -1
        # 1. Based on bytes if total compressed size is known
        if total_bytes_expected_fetch > 0 and speed_bps > 0:
            remaining_bytes = total_bytes_expected_fetch - bytes_fetched
            if remaining_bytes > 0:
                eta_seconds = remaining_bytes / speed_bps
        # 2. Based on chunks if total bytes unknown but speed/chunks known
        elif speed_bps > 0 and chunks_done_count > 0 and total_chunks_count > 0:
             time_per_chunk = elapsed / chunks_done_count
             remaining_chunks = total_chunks_count - chunks_done_count
             if remaining_chunks > 0:
                 eta_seconds = remaining_chunks * time_per_chunk

        # Format ETA if calculated
        if eta_seconds >= 0:
            prog['etaFormatted'] = format_time(eta_seconds)
        else:
            prog['etaFormatted'] = "--:--" # Keep default if cannot estimate

    return prog

def _safe_remove_file(file_path: Optional[str], log_prefix: str, description: str):
    """Safely removes a file, logging success or failure."""
    if not file_path or not isinstance(file_path, str):
         logging.warning(f"[{log_prefix}] Attempted to remove invalid path for {description}: {file_path}")
         return
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f"[{log_prefix}] Successfully cleaned up {description} file: {file_path}")
        except OSError as e:
            logging.error(f"[{log_prefix}] Error deleting {description} file '{file_path}': {e}", exc_info=True)
    else:
        logging.debug(f"[{log_prefix}] Cleanup skipped, {description} file not found: {file_path}")

def _schedule_cleanup(temp_download_prep_id: str, path_to_clean: Optional[str]):
    """ Safely cleans up a temporary download file and its associated state data.
        Intended to be run in a separate thread after a delay. """
    log_prefix = f"CleanupTask-{temp_download_prep_id}"
    logging.info(f"[{log_prefix}] Scheduled cleanup task executing for path: {path_to_clean}")

    # 1. Clean up the temporary file on disk
    if path_to_clean:
        _safe_remove_file(path_to_clean, log_prefix, "final downloaded temp")

    # 2. Clean up the state data from the global dictionary
    if temp_download_prep_id in download_prep_data:
        logging.debug(f"[{log_prefix}] Removing download preparation state data.")
        try:
            del download_prep_data[temp_download_prep_id]
            logging.info(f"[{log_prefix}] Successfully removed download preparation state data.")
        except KeyError:
            # This might happen if another cleanup or error handler already removed it
            logging.warning(f"[{log_prefix}] Download preparation state data was already removed before scheduled cleanup task ran.")
    else:
        logging.warning(f"[{log_prefix}] Download preparation state data not found during scheduled cleanup.")


# --- Upload Core Logic ---
def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
    """
    Generator function that performs the background processing (compress, send)
    and yields SSE events (status, start, complete, error) to the client.
    Progress events have been removed as per new requirements.
    This is triggered *after* the initial file save in /initiate-upload.
    """
    # --- (Initial checks and setup remain the same) ---
    logging.info(f"[{upload_id}] Starting background processing generator...")
    upload_data = upload_progress_data.get(upload_id)
    if not upload_data:
        logging.error(f"[{upload_id}] CRITICAL: Upload data not found in global state.")
        yield _yield_sse_event('error', {'message': 'Internal server error: Upload session data lost.'})
        return
    if upload_data.get('status') != 'initiated':
        logging.warning(f"[{upload_id}] Processing generator started, but status is '{upload_data.get('status')}'. Aborting redundant run.")
        return
    if not upload_data.get('temp_file_path') or not os.path.exists(upload_data['temp_file_path']):
        logging.error(f"[{upload_id}] CRITICAL: Temporary file path missing or file does not exist ('{upload_data.get('temp_file_path')}').")
        yield _yield_sse_event('error', {'message': 'Internal server error: Temporary file lost.'})
        if upload_id in upload_progress_data: upload_progress_data[upload_id]['status'] = 'error'
        return

    upload_data['status'] = 'processing' # Mark as processing NOW
    temp_file_path = upload_data['temp_file_path']
    original_filename = upload_data['original_filename']
    username = upload_data['username']
    logging.info(f"[{upload_id}] Background processing started for: User='{username}', File='{original_filename}'")

    temp_compressed_zip_filepath: Optional[str] = None
    access_id: Optional[str] = None
    total_original_size = 0
    executor: Optional[ThreadPoolExecutor] = None
    if len(TELEGRAM_CHAT_IDS) > 1:
        executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
        logging.info(f"[{upload_id}] Initialized Upload ThreadPoolExecutor (max_workers={MAX_UPLOAD_WORKERS})")

    try:
        total_original_size = os.path.getsize(temp_file_path)
        if total_original_size == 0: raise ValueError("Uploaded file is empty (0 bytes).")

        # Send 'start' event - provides context, but size isn't for progress bar
        yield _yield_sse_event('start', {'filename': original_filename, 'totalSize': total_original_size})
        yield _yield_sse_event('status', {'message': 'Generating Link: Preparing...'})

        access_id = uuid.uuid4().hex[:10]
        upload_data['access_id'] = access_id
        logging.info(f"[{upload_id}] Generated Access ID: {access_id}")

        # --- Single File Workflow ---
        if total_original_size <= CHUNK_SIZE:
            logging.info(f"[{upload_id}] Single file workflow.")
            yield _yield_sse_event('status', {'message': 'Generating Link'})

            zip_buffer = io.BytesIO()
            with open(temp_file_path, 'rb') as f_in, zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf: zf.writestr(original_filename, f_in.read())
            comp_size = zip_buffer.tell(); comp_filename = f"{os.path.splitext(original_filename)[0]}.zip"; logging.info(f"[{upload_id}] Compressed Size: {comp_size}")
            file_bytes_content = zip_buffer.getvalue(); zip_buffer.close()

            # *** STEP 1 CHANGE: REMOVE progress event yield ***

            yield _yield_sse_event('status', {'message': 'Generating Link..... '})

            start_send = time.time(); futures: Dict[Future, str] = {}; results: Dict[str, ApiResult] = {}
            # ... (sending logic) ...
            if executor:
                 for chat_id in TELEGRAM_CHAT_IDS: cid = str(chat_id); fut = executor.submit(_send_single_file_task, file_bytes_content, comp_filename, cid, upload_id); futures[fut] = cid
            else:
                 cid = str(TELEGRAM_CHAT_IDS[0]); _, res = _send_single_file_task(file_bytes_content, comp_filename, cid, upload_id); results[cid] = res

            # ... (wait for results) ...
            if executor:
                 primary_cid = str(PRIMARY_TELEGRAM_CHAT_ID); primary_fut = None
                 for fut, cid in futures.items():
                     if cid == primary_cid: primary_fut = fut; break
                 if primary_fut:
                     cid_res, res = primary_fut.result(); results[cid_res] = res; logging.info(f"[{upload_id}] Primary send completed. Success: {res[0]}")
                     if not res[0]: raise IOError(f"Primary Telegram send failed: {res[1]}")
                 else: logging.warning(f"[{upload_id}] Primary chat {primary_cid} not found in target list.")
                 for fut in as_completed(futures):
                     cid_res, res = fut.result();
                     if cid_res not in results: results[cid_res] = res

            send_duration = time.time() - start_send; logging.info(f"[{upload_id}] Sends done in {send_duration:.2f}s.")
            send_report = [{"chat_id": cid, "success": r[0], "message": r[1], "tg_response": r[2]} for cid, r in results.items()]
            primary_result = results.get(str(PRIMARY_TELEGRAM_CHAT_ID)) if PRIMARY_TELEGRAM_CHAT_ID else results.get(str(TELEGRAM_CHAT_IDS[0]))
            was_successful = primary_result is not None and primary_result[0]

            if was_successful:
                num_ok = sum(1 for r in send_report if r['success']); logging.info(f"[{upload_id}] Success: {num_ok}/{len(TELEGRAM_CHAT_IDS)}")

                # *** STEP 1 CHANGE: REMOVE progress event yield ***

                yield _yield_sse_event('status', {'message': 'Generating Link...'})

                # ... (Save metadata) ...
                ts = datetime.now(timezone.utc).isoformat(); parsed_details = _parse_send_results(upload_id, send_report)
                metadata_record = { "original_filename": original_filename, "sent_filename": comp_filename, "is_split": False, "is_compressed": True, "original_size": total_original_size, "compressed_size": comp_size, "send_locations": parsed_details, "upload_timestamp": ts, "username": username, "upload_duration_seconds": round(send_duration, 2), "access_id": access_id }
                save_success, save_msg = save_file_metadata(metadata_record)
                if not save_success: logging.error(f"[{upload_id}] CRITICAL: Metadata save failed: {save_msg}"); yield _yield_sse_event('status', {'message': 'Warning: Link generated, but saving record failed.'})
                else: logging.info(f"[{upload_id}] Metadata saved. Msg: {save_msg}")

                # ... (Generate URL and yield 'complete') ...
                download_url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                yield _yield_sse_event('complete', {'message': f'Link generated for {original_filename}!', 'download_url': download_url, 'filename': original_filename})
                upload_data['status'] = 'completed'
            else:
                 fail_msg = primary_result[1] if primary_result else "Primary chat result missing."
                 raise IOError(f"Primary Telegram send failed: {fail_msg}")

        # --- Large File Workflow ---
        else:
            logging.info(f"[{upload_id}] Large file workflow.")
            comp_filename = f"{os.path.splitext(original_filename)[0]}.zip"
            yield _yield_sse_event('status', {'message': 'Generating Link...'})

            with tempfile.NamedTemporaryFile(prefix=f"{upload_id}_comp_", suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR) as tf: temp_compressed_zip_filepath = tf.name
            start_comp = time.time()
            try:
                with open(temp_file_path, 'rb') as f_in, zipfile.ZipFile(temp_compressed_zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                    with zf.open(original_filename, 'w') as entry: shutil.copyfileobj(f_in, entry, length=DEFAULT_CHUNK_READ_SIZE)
            except Exception as e: _safe_remove_file(temp_compressed_zip_filepath, upload_id, "partial large compressed"); temp_compressed_zip_filepath = None; raise IOError(f"Compression failed: {e}") from e

            comp_duration = time.time() - start_comp; comp_size = os.path.getsize(temp_compressed_zip_filepath); logging.info(f"[{upload_id}] Compressed Size: {comp_size} bytes in {comp_duration:.2f}s.")

            yield _yield_sse_event('status', {'message': f'Generating Link....'})
            
            yield _yield_sse_event('start', {'filename': comp_filename, 'totalSize': comp_size})

            chunk_num = 0; chunks_meta = []; total_send_dur = 0.0
            expected_chunks = expected_chunks_calc(comp_size)

            try:
                with open(temp_compressed_zip_filepath, 'rb') as f_comp:
                    while True:
                        chunk_num += 1
                        chunk_data = f_comp.read(CHUNK_SIZE); chunk_size = len(chunk_data)
                        if not chunk_data: break # EOF

                        chunk_part_name = f"{comp_filename}.part_{str(chunk_num).zfill(3)}"
                        logging.info(f"[{upload_id}] Sending chunk {chunk_num}/{expected_chunks} ('{chunk_part_name}', {chunk_size} bytes)")

                        start_chunk_send = time.time(); chunk_futures: Dict[Future, str] = {}; chunk_results: Dict[str, ApiResult] = {}
                        
                        if executor:
                             primary_cid = str(PRIMARY_TELEGRAM_CHAT_ID); primary_chunk_fut = None
                             for chat_id in TELEGRAM_CHAT_IDS: cid=str(chat_id); fut=executor.submit(_send_chunk_task, chunk_data, chunk_part_name, cid, upload_id, chunk_num); chunk_futures[fut]=cid; 
                             if cid == primary_cid: primary_chunk_fut = fut
                        else:
                             cid=str(TELEGRAM_CHAT_IDS[0]); _, res= _send_chunk_task(chunk_data, chunk_part_name, cid, upload_id, chunk_num); chunk_results[cid]=res
                             if cid==str(PRIMARY_TELEGRAM_CHAT_ID) and not res[0]: raise IOError(f"Primary fail chunk {chunk_num}: {res[1]}")

                        if executor and primary_chunk_fut:
                             cid_res, res = primary_chunk_fut.result(); chunk_results[cid_res]=res
                             if not res[0]: raise IOError(f"Primary fail chunk {chunk_num}: {res[1]}")
                        elif executor and not primary_chunk_fut and primary_cid in map(str, TELEGRAM_CHAT_IDS): raise SystemError(f"Primary fut missing chunk {chunk_num}.")

                        # *** STEP 1 CHANGE: REMOVE progress event yield ***
                        if executor:
                            for fut in as_completed(chunk_futures):
                                cid_res, res = fut.result();
                                if cid_res not in chunk_results: chunk_results[cid_res] = res

                        chunk_dur = time.time() - start_chunk_send; total_send_dur += chunk_dur
                        chunk_send_report = [{"chat_id": cid, "success": r[0], "message": r[1], "tg_response": r[2]} for cid, r in chunk_results.items()]
                        num_ok = sum(1 for r in chunk_send_report if r['success'])

                        yield _yield_sse_event('status', {'message': f'Generating Link....'})

                        # ... (Store chunk metadata) ...
                        try: parsed_details = _parse_send_results(f"{upload_id}-c{chunk_num}", chunk_send_report); chunk_meta_entry = {"part_number": chunk_num, "chunk_filename": chunk_part_name, "send_locations": parsed_details, "chunk_upload_duration_seconds": round(chunk_dur, 2)}; chunks_meta.append(chunk_meta_entry)
                        except Exception as e: raise ValueError(f"Error processing metadata chunk {chunk_num}.") from e

                # ... (Final checks after loop) ...
                actual_chunks = len(chunks_meta)
                if actual_chunks == expected_chunks:
                    logging.info(f"[{upload_id}] All {expected_chunks} chunks processed. Saving meta.")
                    yield _yield_sse_event('status', {'message': 'Generating Link....'})

                    # ... (Save chunked metadata) ...
                    ts = datetime.now(timezone.utc).isoformat()
                    metadata_record = { "original_filename": original_filename, "sent_filename": comp_filename, "is_split": True, "is_compressed": True, "original_size": total_original_size, "compressed_total_size": comp_size, "chunk_size": CHUNK_SIZE, "num_chunks": expected_chunks, "chunks": chunks_meta, "upload_timestamp": ts, "username": username, "total_upload_duration_seconds": round(total_send_dur, 2), "access_id": access_id }
                    save_success, save_msg = save_file_metadata(metadata_record)
                    if not save_success: logging.error(f"[{upload_id}] CRITICAL: Chunked metadata save failed: {save_msg}"); yield _yield_sse_event('status', {'message': 'Warning: Link generated, but saving record failed.'})
                    else: logging.info(f"[{upload_id}] Chunked metadata saved. Msg: {save_msg}")

                    # ... (Generate URL and yield 'complete') ...
                    download_url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                    yield _yield_sse_event('complete', {'message': f'Link generated for {original_filename}!', 'download_url': download_url, 'filename': original_filename})
                    upload_data['status'] = 'completed'
                else:
                    raise SystemError(f"Chunk count mismatch. Expected:{expected_chunks}, Got:{actual_chunks}.")
            finally:
                if temp_compressed_zip_filepath:
                    _safe_remove_file(temp_compressed_zip_filepath, upload_id, "temporary compressed zip")

    # --- (Error handling) ---
    except (IOError, ValueError, SystemError, FileNotFoundError) as e:
        error_message = f"Processing failed: {str(e) or type(e).__name__}"; logging.error(f"[{upload_id}] {error_message}", exc_info=False)
        yield _yield_sse_event('error', {'message': error_message});
        if upload_id in upload_progress_data: upload_progress_data[upload_id]['status'] = 'error'; upload_progress_data[upload_id]['error'] = error_message
    except Exception as e:
        error_message = f"An unexpected error occurred during processing."; logging.error(f"[{upload_id}] {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message});
        if upload_id in upload_progress_data: upload_progress_data[upload_id]['status'] = 'error'; upload_progress_data[upload_id]['error'] = f"Unexpected: {type(e).__name__}"
    # --- (Finally block for cleanup) ---
    finally:
        logging.info(f"[{upload_id}] Reached end of processing generator.")
        if executor: executor.shutdown(wait=False); logging.info(f"[{upload_id}] Upload ThreadPoolExecutor shutdown signaled.")
        original_temp_path_to_clean = upload_data.get('temp_file_path') if upload_data else None
        if original_temp_path_to_clean: _safe_remove_file(original_temp_path_to_clean, upload_id, "original uploaded temp")
        else: logging.warning(f"[{upload_id}] Skipping final cleanup of original temp file: path not available.")
        final_status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
        logging.info(f"[{upload_id}] Background processing generator finished. Final Status: {final_status}")


# --- Utility function for chunk calculation ---
def expected_chunks_calc(total_compressed_size: int) -> int:
    """Calculates the expected number of chunks."""
    if CHUNK_SIZE <= 0: return 1 if total_compressed_size > 0 else 0
    return (total_compressed_size + CHUNK_SIZE - 1) // CHUNK_SIZE

# --- Download Preparation Route (SSE) using Username/Filename ---
@app.route('/prepare-download/<username>/<path:filename>') # Use path converter for filename
def prepare_download_stream(username: str, filename: str) -> Response:
    prep_id = str(uuid.uuid4())
    logging.info(f"[{prep_id}] SSE download prep request received: User='{username}', Requested File='{filename}'")
    download_prep_data[prep_id] = { "prep_id": prep_id, "status": "initiated", "username": username, "requested_filename": filename, "original_filename": None, "access_id": None, "error": None, "final_temp_file_path": None, "final_file_size": 0, "start_time": time.time() }
    logging.debug(f"[{prep_id}] Stored initial download prep data. Status: initiated.")
    return Response(stream_with_context( _prepare_download_and_generate_updates(prep_id) ), mimetype='text/event-stream')

# --- Download Preparation Route (SSE) using Access ID ---
@app.route('/stream-download/<access_id>')
def stream_download_by_access_id(access_id: str) -> Response:
    prep_id = str(uuid.uuid4())
    logging.info(f"[{prep_id}] SSE connection request for download prep via access_id: {access_id}")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not file_info:
        error_message_for_user = error_msg if error_msg else f"Invalid or expired download link (ID: {access_id})."
        logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' for SSE stream. Reason: {error_message_for_user}")
        def error_stream_and_close(): yield _yield_sse_event('error', {'message': error_message_for_user})
        return Response(stream_with_context(error_stream_and_close()), mimetype='text/event-stream')
    username = file_info.get('username')
    original_filename = file_info.get('original_filename')
    if not username or not original_filename:
         error_message_for_user = "File record is incomplete (missing username or filename)."
         logging.error(f"[{prep_id}] Metadata found for access_id '{access_id}' but incomplete: User='{username}', Filename='{original_filename}'")
         def error_stream_and_close(): yield _yield_sse_event('error', {'message': error_message_for_user})
         return Response(stream_with_context(error_stream_and_close()), mimetype='text/event-stream')
    download_prep_data[prep_id] = { "prep_id": prep_id, "status": "initiated", "username": username, "requested_filename": original_filename, "original_filename": original_filename, "access_id": access_id, "error": None, "final_temp_file_path": None, "final_file_size": 0, "start_time": time.time() }
    logging.debug(f"[{prep_id}] Stored initial prep data from access_id lookup. Status: initiated. Calling generator...")
    return Response(stream_with_context( _prepare_download_and_generate_updates(prep_id) ), mimetype='text/event-stream')

# --- Download Preparation Core Logic (Generator) ---
def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    logging.info(f"[{prep_id}] Download preparation generator started.")
    prep_data = download_prep_data.get(prep_id)
    if not prep_data: logging.error(f"[{prep_id}] CRITICAL: Prep data missing."); yield _yield_sse_event('error', {'message': 'Internal Error: data lost.'}); return
    if prep_data.get('status') != 'initiated': logging.warning(f"[{prep_id}] Gen started but status not 'initiated'. Aborting."); return

    prep_data['status'] = 'preparing'
    temp_reassembled_zip_path: Optional[str] = None; temp_decompressed_path: Optional[str] = None
    temp_final_file_path: Optional[str] = None
    file_info: Optional[Dict[str, Any]] = None; username = prep_data.get('username')
    requested_filename = prep_data.get('requested_filename'); access_id = prep_data.get('access_id')
    original_filename_from_meta = "unknown"; final_expected_original_size = 0; total_bytes_to_fetch = 0
    download_executor: Optional[ThreadPoolExecutor] = None

    try:
        # --- Phase 1: Metadata Lookup ---
        yield _yield_sse_event('status', {'message': 'Looking up file information...'}); time.sleep(0.1)
        lookup_error_msg = ""
        if access_id: 
            logging.debug(f"[{prep_id}] Looking up metadata using access_id: {access_id}")
            file_info, lookup_error_msg = find_metadata_by_access_id(access_id)
            if not file_info and not lookup_error_msg: lookup_error_msg = f"Access ID '{access_id}' not found or link expired."
            elif file_info:
                username_from_lookup = file_info.get('username'); original_filename_from_lookup = file_info.get('original_filename')
                if not username: prep_data['username'] = username = username_from_lookup
                if not requested_filename: prep_data['requested_filename'] = requested_filename = original_filename_from_lookup
                if not username_from_lookup or not original_filename_from_lookup: lookup_error_msg = "File record found but is incomplete."; file_info = None
        elif username and requested_filename: 
            logging.debug(f"[{prep_id}] Looking up metadata using username/filename: User='{username}', File='{requested_filename}'")
            all_user_files, lookup_error_msg = find_metadata_by_username(username)
            if not lookup_error_msg and all_user_files is not None:
                file_info = next((f for f in all_user_files if f.get('original_filename') == requested_filename), None)
                if not file_info: lookup_error_msg = f"File '{requested_filename}' not found for user '{username}'."
                else: found_access_id = file_info.get('access_id');      
                if found_access_id: prep_data['access_id'] = access_id = found_access_id
            elif not lookup_error_msg and all_user_files is None: lookup_error_msg = "Internal error: Failed to retrieve user file list."
        else: lookup_error_msg = "Insufficient information to look up file (missing access_id or username/filename)."

        if lookup_error_msg or not file_info:
            final_error_message = lookup_error_msg or "File metadata not found."; logging.error(f"[{prep_id}] Metadata lookup failed: {final_error_message}"); raise FileNotFoundError(final_error_message)

        # --- Metadata Found ---
        logging.info(f"[{prep_id}] Successfully found metadata for download preparation.")
        original_filename_from_meta = file_info.get('original_filename','Unknown Filename')
        prep_data['original_filename'] = original_filename_from_meta
        final_expected_original_size = file_info.get('original_size', 0)
        is_split = file_info.get('is_split', False); is_compressed = file_info.get('is_compressed', True)
        logging.info(f"[{prep_id}] Meta Details: OriginalName='{original_filename_from_meta}', Size={final_expected_original_size}, Split={is_split}, Comp={is_compressed}")
        yield _yield_sse_event('filename', {'filename': original_filename_from_meta}); yield _yield_sse_event('start', {'filename': original_filename_from_meta, 'totalSize': final_expected_original_size})
        yield _yield_sse_event('progress', {'percentage': 0}); yield _yield_sse_event('status', {'message': 'Preparing file download...'}); time.sleep(0.2)

        # --- Phase 2: File Preparation ---
        if not is_split: 
            logging.info(f"[{prep_id}] Preparing non-split file."); yield _yield_sse_event('status', {'message': 'Downloading file from storage...'}); yield _yield_sse_event('progress', {'percentage': 5})
            locations = file_info.get('send_locations', []); assert locations, "Metadata error: 'send_locations' missing."
            total_bytes_to_fetch = file_info.get('compressed_size', 0) if is_compressed else final_expected_original_size
            tg_file_id, source_chat_id = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID); assert tg_file_id, "No usable Telegram source file found."
            logging.info(f"[{prep_id}] Selected TG file_id '{tg_file_id}' from chat {source_chat_id}.")
            start_dl = time.time(); content, download_error = download_telegram_file_content(tg_file_id); download_duration = time.time() - start_dl; downloaded_bytes = len(content) if content else 0
            speed_mbps = (downloaded_bytes / (1024 * 1024) / download_duration) if download_duration > 0 and downloaded_bytes > 0 else 0; logging.info(f"[{prep_id}] TG download ({downloaded_bytes}b) in {download_duration:.2f}s @ {speed_mbps:.2f} MB/s")
            assert not download_error, f"Failed TG download: {download_error}"; assert content, "Downloaded TG content empty."
            yield _yield_sse_event('progress', {'percentage': 50, 'bytesProcessed': downloaded_bytes, 'totalBytes': total_bytes_to_fetch, 'speedMBps': speed_mbps, 'etaFormatted': '00:00', 'displayTotalBytes': final_expected_original_size})
            if is_compressed: 
                yield _yield_sse_event('status', {'message': 'Decompressing file...'})
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf:
                    temp_final_file_path = tf.name; zip_file = None
                    try: 
                        zip_buffer=io.BytesIO(content); zip_file=zipfile.ZipFile(zip_buffer,'r'); inner_filename=_find_filename_in_zip(zip_file, original_filename_from_meta, prep_id); logging.info(f"[{prep_id}] Decompressing '{inner_filename}' into {temp_final_file_path}")
                        with zip_file.open(inner_filename,'r') as zfs: yield _yield_sse_event('progress',{'percentage':75,'displayTotalBytes':final_expected_original_size}); shutil.copyfileobj(zfs, tf, length=DEFAULT_CHUNK_READ_SIZE)
                    finally: 
                        if zip_file: zip_file.close()
                temp_decompressed_path = temp_final_file_path; logging.info(f"[{prep_id}] Decompression complete."); yield _yield_sse_event('progress', {'percentage': 95, 'displayTotalBytes': final_expected_original_size})
            else: 
                 yield _yield_sse_event('status', {'message': 'Saving temporary file...'})
                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf: temp_final_file_path = tf.name; tf.write(content)
                 logging.info(f"[{prep_id}] Saved non-compressed file: {temp_final_file_path}"); yield _yield_sse_event('progress', {'percentage': 95, 'displayTotalBytes': final_expected_original_size})
        else: 
             logging.info(f"[{prep_id}] Preparing split file."); yield _yield_sse_event('status', {'message': 'Locating file chunks...'})
             chunks_meta = file_info.get('chunks'); assert chunks_meta and isinstance(chunks_meta, list), "Metadata error: 'chunks' invalid."
             try: chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
             except (TypeError, ValueError): raise ValueError("Metadata error: Invalid 'part_number'.")
             num_chunks = len(chunks_meta); assert num_chunks > 0, "Metadata error: 'chunks' empty."
             total_bytes_to_fetch = file_info.get('compressed_total_size', 0); logging.info(f"[{prep_id}] Expect {num_chunks} chunks. Total fetch: ~{format_bytes(total_bytes_to_fetch)}.")
             download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}'); logging.info(f"[{prep_id}] Download Executor (max={MAX_DOWNLOAD_WORKERS})")
             futures: List[Future] = []; results: Dict[int, bytes] = {}; first_download_error: Optional[str] = None; logging.info(f"[{prep_id}] Submitting {num_chunks} chunk downloads..."); yield _yield_sse_event('status', {'message': 'Downloading...'})
             fetch_progress_allocation = 75.0; yield _yield_sse_event('progress', {'percentage': 5})
             for i, chunk_info in enumerate(chunks_meta): 
                 part_num = chunk_info.get('part_number'); assert part_num is not None, f"Chunk {i} missing part#."
                 locations = chunk_info.get('send_locations', []); assert locations, f"Chunk {part_num} missing locs."
                 tg_file_id, source_chat_id = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID); assert tg_file_id, f"No source chunk {part_num}."
                 logging.debug(f"[{prep_id}] Submit task chunk {part_num} (FileID:{tg_file_id})"); futures.append(download_executor.submit(_download_chunk_task, tg_file_id, part_num, prep_id))
             logging.info(f"[{prep_id}] All {len(futures)} chunk download tasks submitted.")
             start_fetch_phase = time.time(); fetched_bytes_count = 0; downloaded_chunks_count = 0
             for future in as_completed(futures): 
                try: 
                  part_num_result, content_result, error_result = future.result()
                  if error_result: 
                      logging.error(f"[{prep_id}] Fail dl chunk {part_num_result}: {error_result}"); first_download_error = first_download_error or f"Chunk {part_num_result}: {error_result}"
                  elif content_result: 
                      downloaded_chunks_count += 1; chunk_len = len(content_result); fetched_bytes_count += chunk_len; results[part_num_result] = content_result; logging.debug(f"[{prep_id}] Dl chunk {part_num_result} OK ({chunk_len}b). Total:{downloaded_chunks_count}/{num_chunks}")
                      download_completion_percent = (downloaded_chunks_count / num_chunks) * 100.0; current_overall_progress = 5.0 + (download_completion_percent * (fetch_progress_allocation / 100.0)); progress_update = _calculate_download_fetch_progress(start_fetch_phase, fetched_bytes_count, total_bytes_to_fetch, downloaded_chunks_count, num_chunks, current_overall_progress, final_expected_original_size); yield _yield_sse_event('progress', progress_update)
                  else: 
                      logging.error(f"[{prep_id}] Task chunk {part_num_result} invalid state."); first_download_error = first_download_error or f"Chunk {part_num_result}: Task error."
                except Exception as e: 
                    logging.error(f"[{prep_id}] Err processing future: {e}", exc_info=True); first_download_error = first_download_error or f"Err result processing: {str(e)}"
             assert not first_download_error, f"Download failed: {first_download_error}"; assert downloaded_chunks_count == num_chunks, f"Chunk dl mismatch. Exp:{num_chunks}, Got:{downloaded_chunks_count}."
             logging.info(f"[{prep_id}] All {num_chunks} chunks dl OK. Total fetched: {fetched_bytes_count}b."); yield _yield_sse_event('status', {'message': 'Reassembling file parts...'})
             reassembled_suffix = ".zip.reassembled" if is_compressed else os.path.splitext(original_filename_from_meta)[1] or ".reassembled"
             with tempfile.NamedTemporaryFile(suffix=reassembled_suffix, delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf: # Reassemble
                 temp_reassembled_zip_path = tf.name; logging.debug(f"[{prep_id}] Reassembling into: {temp_reassembled_zip_path}")
                 for part_num_write in range(1, num_chunks + 1): chunk_content = results.get(part_num_write); assert chunk_content, f"Missing content chunk {part_num_write}."; tf.write(chunk_content)
             results.clear(); logging.info(f"[{prep_id}] Finished reassembly."); yield _yield_sse_event('progress', {'percentage': 80.0 + (fetch_progress_allocation / 100.0 * 0.1), 'bytesProcessed': fetched_bytes_count, 'displayTotalBytes': final_expected_original_size})
             if is_compressed: 
                 yield _yield_sse_event('status', {'message': 'downloading.....'})
                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_", suffix=os.path.splitext(original_filename_from_meta)[1] or ".tmp") as tf:
                    temp_final_file_path = tf.name; zip_file = None
                    try: 
                        zip_file=zipfile.ZipFile(temp_reassembled_zip_path,'r'); inner_filename=_find_filename_in_zip(zip_file, original_filename_from_meta, prep_id); logging.info(f"[{prep_id}] Decompressing '{inner_filename}' into {temp_final_file_path}")
                        with zip_file.open(inner_filename,'r') as zfs: yield _yield_sse_event('progress',{'percentage':90,'displayTotalBytes':final_expected_original_size}); shutil.copyfileobj(zfs, tf, length=DEFAULT_CHUNK_READ_SIZE)
                    finally: 
                        if zip_file: zip_file.close()
                 temp_decompressed_path = temp_final_file_path; logging.info(f"[{prep_id}] Decompression complete."); yield _yield_sse_event('progress', {'percentage': 98, 'displayTotalBytes': final_expected_original_size})
             else: 
                 temp_final_file_path = temp_reassembled_zip_path; temp_reassembled_zip_path = None; logging.info(f"[{prep_id}] Using reassembled non-compressed file: {temp_final_file_path}"); yield _yield_sse_event('progress', {'percentage': 98, 'displayTotalBytes': final_expected_original_size})

        # --- Phase 3: Final Validation and Completion ---
        assert temp_final_file_path and os.path.exists(temp_final_file_path), f"[{prep_id}] Failed final file."
        final_actual_size = os.path.getsize(temp_final_file_path); logging.info(f"[{prep_id}] Final file ready: '{temp_final_file_path}', Size: {final_actual_size}.")
        if final_expected_original_size > 0 and final_actual_size != final_expected_original_size: logging.warning(f"[{prep_id}] Size mismatch! Exp:{final_expected_original_size}, Act:{final_actual_size}")
        prep_data['final_temp_file_path'] = temp_final_file_path; prep_data['final_file_size'] = final_actual_size; prep_data['status'] = 'ready'
        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_actual_size, 'displayTotalBytes': final_actual_size}); yield _yield_sse_event('status', {'message': 'File is ready for download!'}); time.sleep(0.1)
        yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_from_meta}); logging.info(f"[{prep_id}] Prep complete. Sent 'ready'.")

    except (FileNotFoundError, ValueError, SystemError, RuntimeError, IOError, AssertionError) as e: 
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"; logging.error(f"[{prep_id}] {error_message}", exc_info=False)
        yield _yield_sse_event('error', {'message': error_message});
        if prep_id in download_prep_data: download_prep_data[prep_id]['status'] = 'error'; download_prep_data[prep_id]['error'] = error_message
    except Exception as e: 
        error_message = f"An unexpected error occurred during download preparation."; logging.error(f"[{prep_id}] {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message});
        if prep_id in download_prep_data: download_prep_data[prep_id]['status'] = 'error'; download_prep_data[prep_id]['error'] = f"Unexpected: {type(e).__name__}"
    finally:
        logging.info(f"[{prep_id}] Reached end of download preparation generator.")
        if download_executor: download_executor.shutdown(wait=False); logging.info(f"[{prep_id}] Download Executor shutdown signaled.")
        if temp_reassembled_zip_path and temp_reassembled_zip_path != temp_final_file_path: _safe_remove_file(temp_reassembled_zip_path, prep_id, "intermediate reassembled")
        final_status = download_prep_data.get(prep_id, {}).get('status', 'unknown'); logging.info(f"[{prep_id}] Download prep generator finished. Status: {final_status}")


# --- File Serving Route ---
@app.route('/serve-temp-file/<temp_id>/<path:filename>') 
def serve_temp_file(temp_id: str, filename: str) -> Response:
    logging.info(f"File serve request received: TempID='{temp_id}', Filename='{filename}'")
    prep_info = download_prep_data.get(temp_id)
    if not prep_info: logging.warning(f"Serve failed: Prep data not found for TempID '{temp_id}'."); return make_response(f"Error: Invalid or expired link (ID: {temp_id}).", 404)
    current_status = prep_info.get('status');
    if current_status != 'ready': error_msg = prep_info.get('error', f'File not ready (Status: {current_status})'); logging.error(f"Serve failed: '{temp_id}' status '{current_status}'. Err:{error_msg}"); status_code = 400 if current_status == 'error' else 409; return make_response(f"Error: {error_msg}", status_code)
    temp_path = prep_info.get('final_temp_file_path'); size = prep_info.get('final_file_size'); download_name = prep_info.get('original_filename', filename)
    if not temp_path or not os.path.exists(temp_path): logging.error(f"Serve failed: Prepared file missing '{temp_id}'. Path:{temp_path}"); prep_info['status']='error'; prep_info['error']='File missing post-prep'; _schedule_cleanup(temp_id, temp_path); logging.info(f"[{temp_id}] Scheduled immediate cleanup (missing file)."); return make_response("Server Error: Prepared file missing.", 500)

    def generate_stream_with_cleanup(file_path: str, prep_task_id: str):
        cleanup_delay_seconds = 120; logging.info(f"[{prep_task_id}] Scheduling cleanup in {cleanup_delay_seconds}s for: {file_path}"); timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[prep_task_id, file_path]); timer.daemon = True; timer.start()
        logging.debug(f"[{prep_task_id}] Starting stream from: {file_path}")
        try:
            with open(file_path, 'rb') as f:
                while True: 
                    chunk = f.read(STREAM_CHUNK_SIZE); yield chunk;      
                    if not chunk: 
                        logging.info(f"[{prep_task_id}] Finished streaming."); 
                        break
        except Exception as e: logging.error(f"[{prep_task_id}] Error during streaming '{file_path}': {e}", exc_info=True)

    logging.info(f"[{temp_id}] Preparing streaming response for '{download_name}'.")
    response = Response(stream_with_context(generate_stream_with_cleanup(temp_path, temp_id)), mimetype='application/octet-stream')
    try: encoded_dl_name = download_name.encode('utf-8', 'replace').decode('latin-1', 'replace')
    except Exception as enc_e: encoded_dl_name = f"download_{temp_id}.dat"; logging.warning(f"[{temp_id}] Filename encode failed '{download_name}', fallback: {encoded_dl_name}. Err: {enc_e}")
    response.headers.set('Content-Disposition', 'attachment', filename=encoded_dl_name)
    if size is not None and size >= 0: response.headers.set('Content-Length', str(size))
    return response

# --- File Listing Route ---
@app.route('/files/<username>', methods=['GET'])
def list_user_files(username: str) -> Response:
    logging.info(f"List files request received for username: '{username}'")
    if not username: return jsonify({"error": "Username required"}), 400
    logging.info(f"Fetching metadata for user '{username}' from DB...")
    user_files, error_msg = find_metadata_by_username(username)
    if error_msg: logging.error(f"DB Error listing files for '{username}': {error_msg}"); return jsonify({"error": "Server error retrieving file list."}), 500
    user_files = user_files if user_files is not None else []
    logging.info(f"Found {len(user_files)} records for '{username}'.")
    serializable_files = []
    for file_record in user_files:
        if '_id' in file_record and hasattr(file_record['_id'], 'binary'): file_record['_id'] = str(file_record['_id'])
        serializable_files.append(file_record)
    return jsonify(serializable_files)

# --- Download Page Route (Accessed via Link) ---
@app.route('/get/<access_id>')
def get_file_by_access_id(access_id: str) -> Union[str, Response]:
    logging.info(f"Download page request received for access_id: {access_id}")
    if not access_id: return make_response(render_template('404_error.html', message="No download ID provided."), 400)
    logging.info(f"Looking up metadata for access_id '{access_id}' in DB...")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not file_info:
        error_message_for_user = error_msg if error_msg else f"Link '{access_id}' not found or expired."
        logging.warning(f"Metadata lookup failed for access_id '{access_id}': {error_message_for_user}")
        status_code = 404 if "not found" in error_message_for_user.lower() else 500
        return make_response(render_template('404_error.html', message=error_message_for_user), status_code)
    username = file_info.get('username'); original_filename = file_info.get('original_filename'); original_size = file_info.get('original_size'); upload_timestamp_iso = file_info.get('upload_timestamp')
    if not username or not original_filename: logging.error(f"Metadata record incomplete for access_id '{access_id}'."); message = "File record incomplete."; return make_response(render_template('404_error.html', message=message), 500)
    upload_date_str = "Unknown date" 
    if upload_timestamp_iso:
        try: dt_object = dateutil_parser.isoparse(upload_timestamp_iso); dt_object = dt_object.replace(tzinfo=timezone.utc) if dt_object.tzinfo is None else dt_object.astimezone(timezone.utc); upload_date_str = dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e: logging.warning(f"Could not parse timestamp '{upload_timestamp_iso}' for {access_id}: {e}"); upload_date_str = "Invalid date"
    logging.info(f"Rendering download page for '{original_filename}' (AccessID: {access_id}).")
    return render_template('download_page.html', filename=original_filename, filesize=original_size if original_size is not None else 0, upload_date=upload_date_str, username=username, access_id=access_id)

# --- File Deletion Route ---
@app.route('/delete-file/<username>/<path:filename>', methods=['DELETE']) 
def delete_file_record(username: str, filename: str) -> Response:
    logging.info(f"DELETE request received: User='{username}', File='{filename}'")
    if not username or not filename: return jsonify({"error": "Username and filename required."}), 400
    logging.info(f"Attempting DB delete: User='{username}', Filename='{filename}'")
    deleted_count, error_msg = delete_metadata_by_filename(username, filename)
    if error_msg: logging.error(f"DB Error deleting '{username}/{filename}': {error_msg}"); return jsonify({"error": "Server error during deletion."}), 500
    if deleted_count == 0: logging.warning(f"No record found to delete for '{username}/{filename}'."); return jsonify({"error": f"File '{filename}' not found for user '{username}'."}), 404
    else: logging.info(f"Successfully deleted {deleted_count} record(s) for '{username}/{filename}'."); return jsonify({"message": f"Record for '{filename}' deleted successfully."}), 200

logging.info("Flask routes defined.")

