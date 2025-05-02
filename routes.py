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
from werkzeug.security import generate_password_hash
from datetime import datetime, timezone
import re
from dateutil import parser as dateutil_parser
import threading
import database
from database import (
    save_file_metadata,
    find_metadata_by_username,
    find_metadata_by_access_id,
    delete_metadata_by_filename,
    find_user_by_email,
    save_user
)
from config import format_time
# --- Import necessary components ---
from app_setup import app, upload_progress_data, download_prep_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, MAX_DOWNLOAD_WORKERS,
    format_bytes
)
# from utils import load_metadata, save_metadata, format_time, format_bytes
from telegram_api import send_file_to_telegram, download_telegram_file_content

# --- Type Aliases ---
Metadata = Dict[str, List[Dict[str, Any]]]
UploadProgressData = Dict[str, Any]; DownloadPrepData = Dict[str, Any]
SseEvent = str; ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
ChunkDataResult = Tuple[int, Optional[bytes], Optional[str]]

# --- Constants ---
DEFAULT_CHUNK_READ_SIZE = 4 * 1024 * 1024; STREAM_CHUNK_SIZE = 65536

@app.route('/register', methods=['GET'])
def show_register_page():
    """Displays the registration page."""
    logging.info("Serving registration page.")
    try:
        # Assumes register.html is in the 'templates' folder (or root if template_folder='.')
        return render_template('register.html')
    except Exception as e:
        logging.error(f"Error rendering register.html: {e}", exc_info=True)
        return make_response("Error loading page.", 500)
    
# --- Flask Routes ---
@app.route('/')
def index() -> str:
    logging.info("Serving index page.")
    try: return render_template('index.html')
    except Exception as e: logging.error(f"Error rendering index.html: {e}", exc_info=True); return make_response("Error loading page.", 500)

@app.route('/initiate-upload', methods=['POST'])
def initiate_upload() -> Response:
    logging.info("Request initiate upload.")
    if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
    username = request.form.get('username','').strip()
    if not username: return jsonify({"error": "Username required"}), 400
    file = request.files['file']
    if not file or file.filename == '': return jsonify({"error": "No file selected"}), 400
    original_filename = file.filename; upload_id = str(uuid.uuid4())
    temp_file_path = os.path.join(UPLOADS_TEMP_DIR, f"{upload_id}_{original_filename}")
    logging.info(f"[{upload_id}] Temp storage: {temp_file_path}")
    try:
        os.makedirs(UPLOADS_TEMP_DIR, exist_ok=True); file.save(temp_file_path)
        logging.info(f"[{upload_id}] Temp saved: '{original_filename}'.")
        upload_progress_data[upload_id] = { "status": "initiated", "original_filename": original_filename, "temp_file_path": temp_file_path, "username": username, "error": None, "start_time": time.time() }
        logging.debug(f"[{upload_id}] Initial progress data stored.")
        return jsonify({"upload_id": upload_id, "filename": original_filename})
    except Exception as e:
        logging.error(f"Err saving temp '{original_filename}' (ID:{upload_id}): {e}", exc_info=True)
        if os.path.exists(temp_file_path): _safe_remove_file(temp_file_path, upload_id, "partial temp")
        if upload_id in upload_progress_data: del upload_progress_data[upload_id]
        return jsonify({"error": f"Server error saving file: {e}"}), 500

@app.route('/stream-progress/<upload_id>')
def stream_progress(upload_id: str) -> Response:
    logging.info(f"SSE connect request for upload_id: {upload_id}")
    status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
    if upload_id not in upload_progress_data or status in ['completed', 'error', 'completed_metadata_error']:
        logging.warning(f"Upload ID '{upload_id}' unknown or finalized (Status:{status}).")
        def stream_gen(): yield _yield_sse_event('error', {'message': f'Upload ID {upload_id} unknown/finalized.'})
        return Response(stream_with_context(stream_gen()), mimetype='text/event-stream')
    return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')

# # Example simplification in server.py (TEMPORARY)
# @app.route('/stream-progress/<upload_id>')
# def stream_progress(upload_id):
#     print(f"SSE stream requested for ID: {upload_id}") # Log request
#     def event_stream():
#                 # Just send a connected message and keep connection open (for testing)
#                 yield f"event: status\ndata: {json.dumps({'message': 'SSE Connected'})}\n\n"
# # Keep connection open without doing real work
#                 while True:
#                     time.sleep(30) # Keep alive ping essentially
#                     yield ": keepalive\n\n" # Send a comment to keep connection alive
#                     try:
#                         return Response(event_stream(), mimetype='text/event-stream')
#                     except Exception as e:
#                         print(f"Error during SSE stream for {upload_id}: {e}")
# # Return an error response if the stream setup fails
#                     return jsonify({"message": f"Failed to start stream: {e}"}), 500


# --- Helper Functions ---
def _yield_sse_event(event_type: str, data: Dict[str, Any]) -> SseEvent:
    json_data = json.dumps(data); return f"event: {event_type}\ndata: {json_data}\n\n"

def _send_single_file_task(file_bytes: bytes, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    try:
        buffer = io.BytesIO(file_bytes)
        logging.info(f"[{upload_id}] T> Sending '{filename}' ({len(file_bytes)}b) to {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"[{upload_id}] T> Sent '{filename}' to {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Err send single file to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)


def _schedule_cleanup(temp_id: str, path: Optional[str]):
    """Safely cleans up temporary download file and state data."""
    log_prefix = f"Cleanup-{temp_id}"
    logging.info(f"[{log_prefix}] Scheduled cleanup executing for path: {path}")
    if path:
        _safe_remove_file(path, log_prefix, "final dl") # Reuse existing safe remove

    if temp_id in download_prep_data:
        logging.debug(f"[{log_prefix}] Removing prep data.")
        try:
            del download_prep_data[temp_id]
            logging.info(f"[{log_prefix}] Prep data removed.")
        except KeyError:
            logging.warning(f"[{log_prefix}] Prep data already removed before cleanup task.")
    else:
        logging.warning(f"[{log_prefix}] Prep data not found during scheduled cleanup.")

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    try:
        buffer = io.BytesIO(chunk_data); logging.info(f"[{upload_id}] T> Sending chunk {chunk_num} ('{filename}') to {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id); buffer.close()
        logging.info(f"[{upload_id}] T> Sent chunk {chunk_num} to {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Err send chunk {chunk_num} to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)

def _download_chunk_task(file_id: str, part_num: int, prep_id: str) -> ChunkDataResult:
    logging.info(f"[{prep_id}] T> Starting download chunk {part_num} (id: {file_id})")
    try:
        content, err_msg = download_telegram_file_content(file_id)
        if err_msg: logging.error(f"[{prep_id}] T> API Err dl chunk {part_num}: {err_msg}"); return part_num, None, err_msg
        elif not content: logging.error(f"[{prep_id}] T> Err dl chunk {part_num}: Empty content."); return part_num, None, "Empty chunk content."
        else: logging.info(f"[{prep_id}] T> OK dl chunk {part_num} ({len(content)} bytes)."); return part_num, content, None
    except Exception as e: logging.error(f"[{prep_id}] T> Unexp err dl chunk {part_num}: {e}", exc_info=True); return part_num, None, f"Thread error: {e}"

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Union[str, int, bool, None]] = {"chat_id": res["chat_id"], "success": res["success"]}
        if res["success"] and res["tg_response"]:
            res_data = res["tg_response"].get('result', {})
            msg_id = res_data.get('message_id'); doc_data = res_data.get('document', {})
            f_id = doc_data.get('file_id'); f_uid = doc_data.get('file_unique_id'); f_size = doc_data.get('file_size')
            if msg_id and f_id and f_uid:
                detail["message_id"] = msg_id; detail["file_id"] = f_id; detail["file_unique_id"] = f_uid
                if f_size is not None: detail["file_size"] = f_size
            else: detail["success"] = False; detail["error"] = "Missing IDs in TG response"; logging.warning(f"[{log_prefix}] Missing IDs: {res['tg_response']}")
        elif not res["success"]: detail["error"] = res["message"]
        all_chat_details.append(detail)
    return all_chat_details

def _calculate_progress(start_time: float, bytes_done: int, total_bytes: int) -> Dict[str, Any]:
    progress = {"bytesSent": bytes_done, "totalBytes": total_bytes, "percentage": 0, "speedMBps": 0, "etaFormatted": "--:--", "etaSeconds": -1}
    if total_bytes <= 0: return progress
    progress["percentage"] = min((bytes_done / total_bytes) * 100, 100)
    elapsed = time.time() - start_time
    if elapsed > 0.1 and bytes_done > 0:
        speed_bps = bytes_done / elapsed; progress["speedMBps"] = speed_bps / (1024 * 1024)
        remaining = total_bytes - bytes_done
        if remaining > 0 and speed_bps > 0:
            eta_sec = remaining / speed_bps; progress["etaSeconds"] = eta_sec; progress["etaFormatted"] = format_time(eta_sec)
        else: progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    elif bytes_done == total_bytes: progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    return progress

def _find_best_telegram_file_id(locations: List[Dict[str, Any]], primary_chat_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    pf, ff = None, None; pcid_str = str(primary_chat_id) if primary_chat_id else None
    for loc in locations:
        if loc.get('success') and loc.get('file_id'):
            cid = str(loc.get('chat_id')); fid = loc.get('file_id')
            if pcid_str and cid == pcid_str: pf = (fid, cid); break
            elif not ff: ff = (fid, cid)
    if pf: return pf;
    elif ff: return ff;
    else: return None, None
def _find_filename_in_zip(zf: zipfile.ZipFile, expected: str, prefix: str) -> str:
    names = zf.namelist();
    if not names: raise ValueError("Zip empty.")
    if expected in names: return expected
    if len(names) == 1: actual = names[0]; logging.warning(f"[{prefix}] Expected '{expected}' not in zip. Using only entry: '{actual}'"); return actual
    base, _ = os.path.splitext(expected);
    for name in names:
        if name == base: logging.warning(f"[{prefix}] Expected '{expected}' not found. Using match: '{name}'"); return name
    raise ValueError(f"Cannot find '{expected}' in zip ({names})")

def _calculate_download_fetch_progress(start: float, fetched: int, total_fetch: int, done_count: int, total_count: int, base_perc: float, final_size: int) -> Dict[str, Any]:
    prog = {'percentage': base_perc, 'bytesProcessed': fetched, 'totalBytes': total_fetch if total_fetch > 0 else 0, 'speedMBps': 0, 'etaFormatted': '--:--', 'displayTotalBytes': final_size }
    elapsed = time.time() - start
    if elapsed > 0.1 and fetched > 0:
        speed_bps = fetched / elapsed; prog['speedMBps'] = speed_bps / (1024*1024)
        if total_fetch > 0 and speed_bps > 0:
            remaining_bytes = total_fetch - fetched;
            if remaining_bytes > 0: prog['etaFormatted'] = format_time(remaining_bytes / speed_bps)
            else: prog['etaFormatted'] = "00:00"
        elif speed_bps > 0 and done_count > 0:
            remaining_chunks = total_count - done_count;
            if remaining_chunks > 0:
                time_per_chunk = elapsed / done_count;
                eta_seconds = remaining_chunks * time_per_chunk; prog['etaFormatted'] = format_time(eta_seconds)
            else: prog['etaFormatted'] = "00:00"
    return prog

def _safe_remove_file(path: str, prefix: str, desc: str):
    if not path or not isinstance(path, str):
         logging.warning(f"[{prefix}] Attempted remove invalid path for {desc}: {path}")
         return
    if os.path.exists(path):
        try: os.remove(path); logging.info(f"[{prefix}] Cleaned up {desc}: {path}")
        except OSError as e: logging.error(f"[{prefix}] Error deleting {desc} '{path}': {e}", exc_info=True)
    else: logging.debug(f"[{prefix}] Cleanup skipped, {desc} file not found: {path}")

# --- Upload Core Logic ---
def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
    logging.info(f"[{upload_id}] Starting processing generator...")
    upload_data = upload_progress_data.get(upload_id)
    if not upload_data or not upload_data.get('temp_file_path') or not os.path.exists(upload_data['temp_file_path']):
        logging.error(f"[{upload_id}] Critical: Upload data/temp file missing."); yield _yield_sse_event('error', {'message': 'Internal error: data missing.'}); return

    temp_file_path = upload_data['temp_file_path']; original_filename = upload_data['original_filename']
    username = upload_data['username']; logging.info(f"[{upload_id}] Processing: User='{username}', File='{original_filename}'")
    upload_data['status'] = 'processing'
    temp_compressed_zip_filepath: Optional[str] = None; overall_start_time = upload_data.get('start_time', time.time())
    access_id: Optional[str] = None; total_size = 0
    executor: Optional[ThreadPoolExecutor] = None
    if len(TELEGRAM_CHAT_IDS) > 1:
        executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
        logging.info(f"[{upload_id}] Initialized Upload Executor (max={MAX_UPLOAD_WORKERS})")

    try:
        total_size = os.path.getsize(temp_file_path);
        if total_size == 0: raise ValueError("Uploaded file empty.")
        yield _yield_sse_event('start', {'filename': original_filename, 'totalSize': total_size})
        access_id = uuid.uuid4().hex[:10]; upload_data['access_id'] = access_id; logging.info(f"[{upload_id}] Access ID: {access_id}")

        if total_size <= CHUNK_SIZE:
            logging.info(f"[{upload_id}] Single file workflow."); yield _yield_sse_event('status', {'message': 'Compressing...'})
            zip_buffer = io.BytesIO();
            with open(temp_file_path, 'rb') as f_in, zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf: zf.writestr(original_filename, f_in.read())
            comp_size = zip_buffer.tell(); comp_filename = f"{os.path.splitext(original_filename)[0]}.zip"; logging.info(f"[{upload_id}] Compressed size: {comp_size}")
            file_bytes_content = zip_buffer.getvalue()
            zip_buffer.close()
            yield _yield_sse_event('progress', {'bytesSent': 0, 'totalBytes': comp_size}); yield _yield_sse_event('status', {'message': f'Sending...'})
            start_send = time.time(); futures: Dict[Future, str] = {}; results: Dict[str, ApiResult] = {}
            if executor:
                for chat_id in TELEGRAM_CHAT_IDS: cid = str(chat_id); fut = executor.submit(_send_single_file_task, file_bytes_content, comp_filename, cid, upload_id); futures[fut] = cid
                logging.info(f"[{upload_id}] Submitted {len(futures)} single-file tasks.")
            else: cid = str(TELEGRAM_CHAT_IDS[0]); _, res = _send_single_file_task(file_bytes_content, comp_filename, cid, upload_id); results[cid] = res
            primary_fut: Optional[Future] = None
            if executor:
                primary_cid = str(PRIMARY_TELEGRAM_CHAT_ID);
                for fut, cid in futures.items():
                    if cid == primary_cid: primary_fut = fut; break
                if primary_fut:
                     logging.info(f"[{upload_id}] Wait primary ({primary_cid})..."); cid_res, res = primary_fut.result(); results[cid_res] = res; logging.info(f"[{upload_id}] Primary done. OK:{res[0]}");
                     if not res[0]: raise IOError(f"Primary send fail: {res[1]}")
                else: logging.warning(f"[{upload_id}] Primary fut {primary_cid} not found.")
                logging.info(f"[{upload_id}] Wait backups...");
                for fut in as_completed(futures):
                    cid_res, res = fut.result();
                    if cid_res not in results: results[cid_res] = res; logging.debug(f"[{upload_id}] Done backup {cid_res}. OK:{res[0]}")
            duration = time.time() - start_send; logging.info(f"[{upload_id}] Single sends done in {duration:.2f}s.")
            send_res = [{"chat_id": cid, "success": r[0], "message": r[1], "tg_response": r[2]} for cid, r in results.items()]
            primary_res = results.get(str(PRIMARY_TELEGRAM_CHAT_ID)); success = primary_res is not None and primary_res[0]
            if success:
                num_ok = sum(1 for r in send_res if r['success']); logging.info(f"[{upload_id}] Success: {num_ok}/{len(TELEGRAM_CHAT_IDS)}")
                speed = (comp_size/(1024*1024)/duration) if duration > 0 else 0
                yield _yield_sse_event('progress', {'bytesSent': comp_size, 'totalBytes': comp_size, 'percentage': 100, 'speedMBps': speed, 'etaFormatted': '00:00'})
                # --- New block using database function ---
            ts = datetime.now(timezone.utc).isoformat()
            details = _parse_send_results(upload_id, send_res)
            record = {
                "original_filename": original_filename,
                "sent_filename": comp_filename,
                "is_split": False,
                "is_compressed": True,
                "original_size": total_size,
                "compressed_size": comp_size,
                "send_locations": details,
                "upload_timestamp": ts,
                "username": username,
                "upload_duration_seconds": round(duration, 2),
                "access_id": access_id # Make sure access_id is in the record
            }
            logging.info(f"[{upload_id}] Attempting to save metadata to DB...")
            save_success, save_msg = save_file_metadata(record) # Call the DB function
            if not save_success:
                # Log the error critically, yield a warning status to the user
                logging.error(f"[{upload_id}] CRITICAL: Metadata save to DB failed: {save_msg}")
                yield _yield_sse_event('status', {'message': 'Warning: Upload OK, but metadata save failed.'})
                # Consider setting upload_data['status'] = 'completed_metadata_error'
            else:
                logging.info(f"[{upload_id}] Metadata successfully saved to DB. Msg: {save_msg}")
            # --- End of new block ---

                # The rest (url = url_for..., yield _yield_sse_event('complete'...) stays the same
                url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                yield _yield_sse_event('complete', {'message': f'File {original_filename} uploaded!', 'download_url': url, 'filename': original_filename})
                upload_data['status'] = 'completed' # Set status even if meta save failed with warning
           
        else:
            logging.info(f"[{upload_id}] Large file workflow."); comp_filename = f"{os.path.splitext(original_filename)[0]}.zip"
            yield _yield_sse_event('status', {'message': 'Compressing large file...'})
            with tempfile.NamedTemporaryFile(prefix=f"{upload_id}_comp_", suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR) as tf: temp_compressed_zip_filepath = tf.name
            start_comp = time.time()
            try:
                with open(temp_file_path, 'rb') as f_in, zipfile.ZipFile(temp_compressed_zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                    with zf.open(original_filename, 'w') as entry: shutil.copyfileobj(f_in, entry, length=DEFAULT_CHUNK_READ_SIZE)
            except Exception as e:
                 logging.error(f"[{upload_id}] Err large compress: {e}", exc_info=True);
                 if temp_compressed_zip_filepath and os.path.exists(temp_compressed_zip_filepath): _safe_remove_file(temp_compressed_zip_filepath, upload_id, "partial large comp");
                 temp_compressed_zip_filepath = None;
                 raise e # Re-raise the original exception
            comp_duration = time.time() - start_comp; comp_size = os.path.getsize(temp_compressed_zip_filepath); logging.info(f"[{upload_id}] Compressed to {comp_size} bytes in {comp_duration:.2f}s.")
            yield _yield_sse_event('status', {'message': f'Starting chunk upload ({format_bytes(comp_size)})...'})
            yield _yield_sse_event('start', {'filename': comp_filename, 'totalSize': comp_size})
            chunk_num = 0; chunks_meta = []; read_bytes = 0; total_send_dur = 0.0; start_split = time.time(); sent_bytes = 0
            try:
                with open(temp_compressed_zip_filepath, 'rb') as f_comp:
                    while True:
                        chunk_num += 1; logging.debug(f"[{upload_id}] Reading chunk {chunk_num}...")
                        chunk_data = f_comp.read(CHUNK_SIZE); chunk_size = len(chunk_data)
                        if not chunk_data: logging.info(f"[{upload_id}] EOF reached."); break
                        read_bytes += chunk_size; chunk_part_name = f"{comp_filename}.part_{str(chunk_num).zfill(3)}"; logging.info(f"[{upload_id}] Read chunk {chunk_num} ({chunk_size} bytes). Name: '{chunk_part_name}'")
                        start_chunk_send = time.time(); chunk_futures: Dict[Future, str] = {}; chunk_results: Dict[str, ApiResult] = {}; primary_chunk_fut: Optional[Future] = None
                        if executor:
                            primary_cid = str(PRIMARY_TELEGRAM_CHAT_ID)
                            for chat_id in TELEGRAM_CHAT_IDS:
                                cid = str(chat_id); fut = executor.submit(_send_chunk_task, chunk_data, chunk_part_name, cid, upload_id, chunk_num); chunk_futures[fut] = cid;
                                if cid == primary_cid: primary_chunk_fut = fut
                            logging.debug(f"[{upload_id}] Submitted {len(chunk_futures)} tasks chunk {chunk_num}.")
                        else:
                            cid = str(TELEGRAM_CHAT_IDS[0]); _, res = _send_chunk_task(chunk_data, chunk_part_name, cid, upload_id, chunk_num); chunk_results[cid] = res;
                            if cid == str(PRIMARY_TELEGRAM_CHAT_ID) and not res[0]: raise IOError(f"Primary fail chunk {chunk_num}: {res[1]}")
                        if executor and primary_chunk_fut:
                            logging.debug(f"[{upload_id}] Wait primary chunk {chunk_num}..."); cid_res, res = primary_chunk_fut.result(); chunk_results[cid_res] = res; logging.debug(f"[{upload_id}] Primary chunk {chunk_num} done. OK:{res[0]}");
                            if not res[0]: raise IOError(f"Primary fail chunk {chunk_num}: {res[1]}")
                        elif executor and not primary_chunk_fut: raise SystemError(f"Primary fut not found chunk {chunk_num}.")
                        sent_bytes += chunk_size; progress = _calculate_progress(start_split, sent_bytes, comp_size); yield _yield_sse_event('progress', progress)
                        if executor:
                            logging.debug(f"[{upload_id}] Wait backups chunk {chunk_num}...");
                            for fut in as_completed(chunk_futures):
                                cid_res, res = fut.result();
                                if cid_res not in chunk_results: chunk_results[cid_res] = res; logging.debug(f"[{upload_id}] Done backup chunk {chunk_num}, chat: {cid_res}. OK:{res[0]}")
                        chunk_dur = time.time() - start_chunk_send; total_send_dur += chunk_dur
                        chunk_send_res = [{"chat_id": cid, "success": r[0], "message": r[1], "tg_response": r[2]} for cid, r in chunk_results.items()]
                        num_ok = sum(1 for r in chunk_send_res if r['success']); yield _yield_sse_event('status', {'message': f'Sent chunk {chunk_num} ({num_ok}/{len(TELEGRAM_CHAT_IDS)} OK)'})
                        try:
                            details = _parse_send_results(f"{upload_id}-c{chunk_num}", chunk_send_res);
                            meta_entry = {"part_number": chunk_num, "chunk_filename": chunk_part_name, "send_locations": details, "chunk_upload_duration_seconds": round(chunk_dur, 2)};
                            chunks_meta.append(meta_entry); logging.debug(f"[{upload_id}] Stored meta chunk {chunk_num}.")
                        except Exception as e: raise ValueError(f"Err meta chunk {chunk_num}.") from e
                logging.info(f"[{upload_id}] Finished chunk loop.")
                expected = (comp_size + CHUNK_SIZE - 1)//CHUNK_SIZE if CHUNK_SIZE > 0 else (1 if comp_size > 0 else 0)
                actual = len(chunks_meta)
                if actual == expected:
                    logging.info(f"[{upload_id}] All {expected} chunks OK. Saving meta.")
                    # --- New block using database function for chunks ---
                    ts = datetime.now(timezone.utc).isoformat()
                    record = {
                        "original_filename": original_filename,
                        "sent_filename": comp_filename,
                        "is_split": True,
                        "is_compressed": True,
                        "original_size": total_size,
                        "compressed_total_size": comp_size,
                        "chunk_size": CHUNK_SIZE,
                        "num_chunks": expected,
                        "chunks": chunks_meta, # Ensure chunks_meta is correctly populated
                        "upload_timestamp": ts,
                        "username": username,
                        "total_upload_duration_seconds": round(total_send_dur, 2),
                        "access_id": access_id # Make sure access_id is in the record
                    }
                    logging.info(f"[{upload_id}] Attempting to save chunked metadata to DB...")
                    save_success, save_msg = save_file_metadata(record)
                    if not save_success:
                        logging.error(f"[{upload_id}] CRITICAL: Chunked metadata save to DB failed: {save_msg}")
                        yield _yield_sse_event('status', {'message': 'Warning: Upload OK, but metadata save failed.'})
                        # upload_data['status'] = 'completed_metadata_error' # Optional distinct status
                    else:
                        logging.info(f"[{upload_id}] Chunked metadata successfully saved to DB. Msg: {save_msg}")
                    # --- End of new block ---

                    # The rest (url = url_for..., yield _yield_sse_event('complete'...) stays the same
                    url = url_for('get_file_by_access_id', access_id=access_id, _external=True)
                    yield _yield_sse_event('complete', {'message': f'Large file {original_filename} uploaded!', 'download_url': url, 'filename': original_filename})
                    upload_data['status'] = 'completed' # Set status even if meta save failed with warning
                else: raise SystemError(f"Chunk count mismatch. Exp:{expected}, Got:{actual}.")
            finally:
                if temp_compressed_zip_filepath and os.path.exists(temp_compressed_zip_filepath): _safe_remove_file(temp_compressed_zip_filepath, upload_id, "large compressed temp")

    except Exception as e:
        error_message = f"Upload failed: {str(e) or type(e).__name__}"; logging.error(f"[{upload_id}] {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message});
        if upload_id in upload_progress_data: upload_data['status'] = 'error'; upload_data['error'] = error_message
    finally:
        logging.info(f"[{upload_id}] Upload generator cleanup.")
        if executor: executor.shutdown(wait=False); logging.info(f"[{upload_id}] Upload executor shutdown.")
        if temp_file_path and os.path.exists(temp_file_path): _safe_remove_file(temp_file_path, upload_id, "original temp")
        final_status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
        logging.info(f"[{upload_id}] Upload generator finished. Status: {final_status}")

# Download Preparation Route ---
@app.route('/prepare-download/<username>/<path:filename>') # <-- Use path converter
def prepare_download_stream(username: str, filename: str) -> Response:
    """
    SSE endpoint. Initiates download preparation for a given user/filename
    and streams status updates. Allows slashes in filename.
    """
    prep_id = str(uuid.uuid4())

    logging.info(f"[{prep_id}] SSE download prep request: User='{username}', File='{filename}'")
    download_prep_data[prep_id] = {
        "prep_id": prep_id, "status": "initiated", "username": username,
        "requested_filename": filename, "original_filename": filename,
        "access_id": None, "error": None, "final_temp_file_path": None,
        "final_file_size": 0, "start_time": time.time()
    }
    logging.debug(f"[{prep_id}] Stored initial download prep data.")

    return Response(stream_with_context(
        _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')

@app.route('/stream-download/<access_id>')
def stream_download_by_access_id(access_id: str) -> Response:
    """
    SSE endpoint to stream download preparation status initiated by access_id.
    """
    prep_id = str(uuid.uuid4()) # Unique ID for *this* preparation task
    logging.info(f"[{prep_id}] SSE connection request for dl prep via access_id: {access_id}")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    username = file_info.get('username')
    # --- Basic Metadata Lookup (to ensure ID is valid before streaming) ---

    if not file_info or not username:
         # Handle case where access_id is not found
         logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' for SSE stream.")
         def error_stream(): yield _yield_sse_event('error', {'message':'Invalid or expired download link.'})
         return Response(stream_with_context(error_stream()), mimetype='text/event-stream')

    # Store initial info needed by the generator, using the new prep_id
    download_prep_data[prep_id] = {
         "prep_id": prep_id,
         "status": "initiated",
         "username": username, # Found from metadata
         "requested_filename": file_info.get('original_filename', 'unknown'), # Use filename from metadata
         "original_filename": file_info.get('original_filename', 'unknown'), # Store original name
         "access_id": access_id, # Store the access_id that initiated this
         "error": None,
         "final_temp_file_path": None,
         "final_file_size": 0,
         "start_time": time.time()
    }
    logging.debug(f"[{prep_id}] Stored initial prep data for access_id lookup. Calling generator.")

    # Call the main generator function using the prep_id
    # This generator will handle the actual download/prep and yield SSE events
    return Response(stream_with_context(
         _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')
# --- End of new route function ---

def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    """Generator handling download preparation and yielding SSE updates."""
    logging.info(f"[{prep_id}] Download prep generator started.")
    prep_data = download_prep_data.get(prep_id)
    if not prep_data: logging.error(f"[{prep_id}] Critical: Prep data missing."); yield _yield_sse_event('error', {'message': 'Internal Error: data lost.'}); return
    if prep_data.get('status') != 'initiated': logging.warning(f"[{prep_id}] Gen started but status not 'initiated'. Aborting."); return

    prep_data['status'] = 'preparing'
    # Initialize variables
    temp_reassembled_zip_path: Optional[str] = None; temp_decompressed_path: Optional[str] = None
    temp_final_file_path: Optional[str] = None # This will hold the path to be served
    file_info: Optional[Dict[str, Any]] = None; username = prep_data.get('username')
    requested_filename = prep_data.get('requested_filename'); access_id = prep_data.get('access_id')
    original_filename_from_meta = "unknown"; final_expected_size = 0; total_bytes_to_fetch = 0
    download_executor: Optional[ThreadPoolExecutor] = None

    try:
        # --- Phase 1: Metadata Lookup ---
        yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
        # metadata = load_metadata(); # <<< REMOVED

        lookup_error_msg = "" # Variable to store potential errors

        # Prioritize access_id if available (e.g., from /stream-download route)
        if access_id:
            logging.debug(f"[{prep_id}] Finding metadata by access_id: {access_id}")
            file_info, lookup_error_msg = find_metadata_by_access_id(access_id)
            if not file_info and not lookup_error_msg: # Explicitly check if not found by DB function
                lookup_error_msg = f"Access ID '{access_id}' not found."
            elif file_info:
                username_from_lookup = file_info.get('username')
                if not username: # If username wasn't passed initially
                    if username_from_lookup:
                        username = username_from_lookup
                        prep_data['username'] = username # Update prep_data for logging/context
                    else:
                        lookup_error_msg = "Record found but missing username field."
                        file_info = None # Treat as error if username missing
            # If there was a DB error, lookup_error_msg will be set

        # Fallback to username/filename if access_id wasn't provided or failed,
        # but username and requested_filename are available.
        elif username and requested_filename:
            logging.debug(f"[{prep_id}] Finding metadata by username/filename: User='{username}', File='{requested_filename}'")
            # Find ALL files for the user first
            all_user_files, lookup_error_msg = find_metadata_by_username(username)

            if not lookup_error_msg and all_user_files is not None: # Check if DB call was successful
                # Now filter the specific file from the results
                file_info = next((f for f in all_user_files if f.get('original_filename') == requested_filename), None)
                if not file_info:
                    # Only set error if DB call was okay but file wasn't in the list
                    lookup_error_msg = f"File '{requested_filename}' not found for user '{username}'."
            elif not lookup_error_msg and all_user_files is None: # Should not happen normally
                lookup_error_msg = "Internal error: Failed to retrieve user file list."
            # If find_metadata_by_username had an error, lookup_error_msg is already set

        else:
            # This case means we don't have enough info to look up the file
            lookup_error_msg = "Insufficient information (access_id or username/filename) provided for download preparation."

        # --- Handle lookup results ---
        if lookup_error_msg or not file_info:
            # Use the specific error message we determined above
            final_error_message = lookup_error_msg or "File metadata not found."
            logging.error(f"[{prep_id}] Metadata lookup failed: {final_error_message}")
            # Raise the specific error to be caught by the outer try/except
            raise FileNotFoundError(final_error_message)

        # If we reach here, file_info is valid and contains the metadata
        logging.info(f"[{prep_id}] Successfully found metadata for download preparation.")
        # ... rest of Phase 1 continues as before, extracting details from file_info ...
        original_filename_from_meta = file_info.get('original_filename','?');
        final_expected_size = file_info.get('original_size', 0)
        is_split = file_info.get('is_split', False)
        is_compressed = file_info.get('is_compressed', True)
        prep_data['original_filename'] = original_filename_from_meta
        logging.info(f"[{prep_id}] Meta: '{original_filename_from_meta}', User:{username}, Size:{final_expected_size}, Split:{is_split}, Comp:{is_compressed}")
        yield _yield_sse_event('filename', {'filename': original_filename_from_meta}); yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size})
        yield _yield_sse_event('progress', {'percentage': 0}); yield _yield_sse_event('status', {'message': 'Preparing file...'}); time.sleep(0.2)
        # --- Phase 2: File Preparation ---
        if not is_split:
            # --- Single File Prep ---
            logging.info(f"[{prep_id}] Preparing non-split file."); yield _yield_sse_event('status', {'message': 'Downloading...'})
            yield _yield_sse_event('progress', {'percentage': 5}); locations = file_info.get('send_locations', [])
            if not locations: raise ValueError("Locations missing."); total_bytes_to_fetch = file_info.get('compressed_size', 0)
            if not total_bytes_to_fetch and is_compressed: logging.warning(f"[{prep_id}] Comp size missing single.")
            tg_file_id, chat_id = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID);
            if not tg_file_id: raise ValueError("No usable TG source."); logging.info(f"[{prep_id}] Using file_id {tg_file_id} from {chat_id}.")
            start = time.time(); content, err = download_telegram_file_content(tg_file_id); duration = time.time() - start; dl_bytes = len(content) if content else 0
            speed = (dl_bytes/(1024*1024)/duration) if duration > 0 and dl_bytes > 0 else 0; logging.info(f"[{prep_id}] TG dl ({dl_bytes} bytes) in {duration:.2f}s. Avg Speed: {speed:.2f} MB/s")
            if err: raise ValueError(f"TG dl fail: {err}");
            if not content: raise ValueError("TG dl empty.")
            yield _yield_sse_event('progress', {'percentage': 50, 'speedMBps': speed, 'etaFormatted': '00:00'})
            if is_compressed:
                yield _yield_sse_event('status', {'message': 'Decompressing...'})
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name # Assign final path
                    zf = None
                    try:
                        buf = io.BytesIO(content); zf = zipfile.ZipFile(buf, 'r'); inner = _find_filename_in_zip(zf, original_filename_from_meta, prep_id)
                        with zf.open(inner, 'r') as i:
                            yield _yield_sse_event('progress', {'percentage': 75}); shutil.copyfileobj(i, tf, length=DEFAULT_CHUNK_READ_SIZE)
                    finally:
                        if zf: zf.close()
                temp_decompressed_path = temp_final_file_path # Track intermediate step (same as final here)
                yield _yield_sse_event('progress', {'percentage': 95})
            else:
                 yield _yield_sse_event('status', {'message': 'Saving temp...'})
                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                     temp_final_file_path = tf.name; tf.write(content) # Assign final path
                 yield _yield_sse_event('progress', {'percentage': 95})
        else:
             # --- Split File Prep ---
             logging.info(f"[{prep_id}] Preparing SPLIT file."); yield _yield_sse_event('status', {'message': 'Downloading chunks concurrently...'})
             chunks_meta = file_info.get('chunks');
             if not chunks_meta or not isinstance(chunks_meta, list): raise ValueError("Invalid 'chunks'.");
             try: chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
             except (TypeError, ValueError): raise ValueError("Invalid 'part_number'.");
             num_chunks = len(chunks_meta)
             if num_chunks == 0: raise ValueError("Chunks list empty.")
             total_bytes_to_fetch = file_info.get('compressed_total_size', 0); logging.info(f"[{prep_id}] Expecting {num_chunks} chunks. Total comp: ~{format_bytes(total_bytes_to_fetch)}.")
             start_fetch = time.time(); fetched_bytes = 0; fetch_alloc = 80.0; dl_count = 0
             download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
             logging.info(f"[{prep_id}] Initialized Download Executor (max={MAX_DOWNLOAD_WORKERS})")
             futures: List[Future] = []; results: Dict[int, bytes] = {}; first_err: Optional[str] = None
             logging.info(f"[{prep_id}] Submitting {num_chunks} chunk dl tasks...")
             for i, chunk_info in enumerate(chunks_meta):
                 pnum = chunk_info.get('part_number');
                 if pnum is None: raise ValueError(f"Chunk {i} missing part#.")
                 locs = chunk_info.get('send_locations', []);
                 if not locs: raise ValueError(f"Chunk {pnum} missing locs.")
                 fid, cid = _find_best_telegram_file_id(locs, PRIMARY_TELEGRAM_CHAT_ID);
                 if not fid: raise ValueError(f"No source chunk {pnum}.")
                 futures.append(download_executor.submit(_download_chunk_task, fid, pnum, prep_id))
             logging.info(f"[{prep_id}] All {len(futures)} chunk dl tasks submitted.")
             yield _yield_sse_event('status', {'message': f'Downloading your file'})
             prog: Dict[str, Any] = {}
             for fut in as_completed(futures):
                 try:
                     pnum_res, content, err = fut.result()
                     if err: 
                         logging.error(f"[{prep_id}] Fail dl chunk {pnum_res}: {err}");
                         if not first_err: first_err = f"Chunk {pnum_res}: {err}"
                     elif content:
                         dl_count += 1; chunk_len = len(content); fetched_bytes += chunk_len; results[pnum_res] = content; logging.debug(f"[{prep_id}] Dl chunk {pnum_res}. Count:{dl_count}/{num_chunks} ({chunk_len}b)")
                         perc_comp = (dl_count / num_chunks) * 100.0; overall_perc = perc_comp * (fetch_alloc / 100.0)
                         prog = _calculate_download_fetch_progress(start_fetch, fetched_bytes, total_bytes_to_fetch, dl_count, num_chunks, overall_perc, final_expected_size);
                         yield _yield_sse_event('progress', prog)
                     else: 
                         logging.error(f"[{prep_id}] Task chunk {pnum_res} invalid state.");
                         if not first_err: first_err = f"Chunk {pnum_res}: Internal task err."
                 except Exception as e: 
                     logging.error(f"[{prep_id}] Err processing dl future: {e}", exc_info=True);
                     if not first_err: first_err = f"Err processing result: {str(e)}"
             if first_err: raise ValueError(f"Download fail: {first_err}")
             if dl_count != num_chunks: raise SystemError(f"Chunk dl count mismatch. Exp:{num_chunks}, Got:{dl_count}.")
             logging.info(f"[{prep_id}] All {num_chunks} chunks dl OK. Total fetched: {fetched_bytes} bytes.")
             yield _yield_sse_event('status', {'message': 'Reassembling file...'})
             # Create reassembly temp file
             with tempfile.NamedTemporaryFile(suffix=".zip.reassembled", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf:
                 temp_reassembled_zip_path = tf.name; logging.debug(f"[{prep_id}] Reassembling into: {temp_reassembled_zip_path}")
                 for pnum_write in range(1, num_chunks + 1):
                     chunk = results.get(pnum_write);
                     if not chunk: raise SystemError(f"Missing content chunk {pnum_write} for reassembly.")
                     tf.write(chunk); logging.debug(f"[{prep_id}] Wrote chunk {pnum_write} to reassembly.")
             results.clear(); logging.info(f"[{prep_id}] Finished reassembly.")
             yield _yield_sse_event('progress', {'percentage': fetch_alloc, 'bytesProcessed': fetched_bytes, 'speedMBps': prog.get('speedMBps',0), 'etaFormatted':'00:00'})
             # Decompress or use directly
             if is_compressed:
                 yield _yield_sse_event('status', {'message': 'Decompressing...'})
                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name # Assign final path
                    zf = None
                    try:
                        zf = zipfile.ZipFile(temp_reassembled_zip_path, 'r')
                        inner = _find_filename_in_zip(zf, original_filename_from_meta, prep_id)
                        with zf.open(inner, 'r') as i:
                            yield _yield_sse_event('progress', {'percentage': 90});
                            shutil.copyfileobj(i, tf, length=DEFAULT_CHUNK_READ_SIZE)
                    finally:
                         if zf: zf.close()
                 temp_decompressed_path = temp_final_file_path # Track intermediate step (same as final here)
                 yield _yield_sse_event('progress', {'percentage': 98})
             else:
                 temp_final_file_path = temp_reassembled_zip_path # Assign final path
                 temp_reassembled_zip_path = None # Prevent deletion in finally block
                 logging.info(f"[{prep_id}] Using reassembled directly as final file."); yield _yield_sse_event('progress', {'percentage': 98})

        # --- Phase 3: Complete ---
        if not temp_final_file_path or not os.path.exists(temp_final_file_path): raise RuntimeError(f"[{prep_id}] Failed final file.")
        final_size = os.path.getsize(temp_final_file_path); logging.info(f"[{prep_id}] Final file ready: '{temp_final_file_path}', Size: {final_size}.")
        if final_expected_size > 0 and final_size != final_expected_size: logging.warning(f"[{prep_id}] Size mismatch! Exp:{final_expected_size}, Act:{final_size}")
        prep_data['final_temp_file_path'] = temp_final_file_path; prep_data['final_file_size'] = final_size; prep_data['status'] = 'ready'
        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_size, 'totalBytes': final_size}); yield _yield_sse_event('status', {'message': 'File ready!'}); time.sleep(0.1)
        yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_from_meta}); logging.info(f"[{prep_id}] Prep complete. Sent 'ready'.")
    except Exception as e:
        error_message = f"Download prep failed: {str(e) or type(e).__name__}"; logging.error(f"[{prep_id}] {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message});
        if prep_id in download_prep_data: download_prep_data[prep_id]['status'] = 'error'; download_prep_data[prep_id]['error'] = error_message
    finally:
        logging.info(f"[{prep_id}] Download prep generator cleanup.")
        if download_executor: download_executor.shutdown(wait=False); logging.info(f"[{prep_id}] Download executor shutdown.")
        # --- MODIFIED CLEANUP ---
        if temp_reassembled_zip_path and temp_reassembled_zip_path != temp_final_file_path:
            _safe_remove_file(temp_reassembled_zip_path, prep_id, "intermediate reassembled")
        logging.info(f"[{prep_id}] Download prep generator task ended.")

# --- File Serving, Listing, Deletion Routes ---
@app.route('/serve-temp-file/<temp_id>/<path:filename>')
def serve_temp_file(temp_id: str, filename: str) -> Response:
    """Serves the prepared temporary file and schedules its cleanup."""
    logging.info(f"Request serve temp file ID: {temp_id}, Filename: {filename}")
    prep_info = download_prep_data.get(temp_id)

    if not prep_info:
        logging.warning(f"Serve fail: Prep data not found for ID '{temp_id}'.") # More specific log
        return make_response(f"Error: Invalid or expired link (ID: {temp_id}).", 404)

    # Check status more carefully
    current_status = prep_info.get('status')
    if current_status != 'ready':
        err = prep_info.get('error', f'File not ready (Status: {current_status})')
        logging.error(f"Serve fail: '{temp_id}' status is '{current_status}'. Err:{err}")
        # Don't delete here, let scheduled cleanup handle it eventually if needed
        return make_response(f"Error: {err}", 400 if current_status == 'error' else 409) # 409 Conflict might be suitable

    temp_path = prep_info.get('final_temp_file_path')
    size = prep_info.get('final_file_size')
    dl_name = prep_info.get('original_filename', filename)

    if not temp_path or not os.path.exists(temp_path):
        logging.error(f"Serve fail: Prepared file missing ID '{temp_id}'. Expected Path:{temp_path}")
        # Mark as error, maybe schedule cleanup? Or let timeout handle it.
        prep_info['status'] = 'error'
        prep_info['error'] = 'File missing after preparation'
        # Schedule cleanup immediately in this error case
        cleanup_delay_seconds = 5 # Short delay for immediate error cleanup
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[temp_id, temp_path])
        timer.daemon = True # Allow program to exit even if timer is waiting
        timer.start()
        logging.info(f"[{temp_id}] Scheduled immediate cleanup due to missing file.")
        return make_response("Error: Prepared file data missing or corrupted.", 500)

    # --- Generator modification ---
    def generate_stream(path: str, pid: str):
        # Schedule cleanup BEFORE starting the stream.
        # If streaming fails, cleanup still happens eventually.
        cleanup_delay_seconds = 120 # Delay in seconds (e.g., 2 minutes)
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[pid, path])
        timer.daemon = True # Allow program to exit even if timer is waiting
        timer.start()
        logging.info(f"[{pid}] Scheduled cleanup in {cleanup_delay_seconds}s for path: {path}")

        logging.debug(f"[{pid}] Starting stream from: {path}")
        try:
            with open(path, 'rb') as f:
                while True:
                    chunk = f.read(STREAM_CHUNK_SIZE)
                    if not chunk:
                        logging.info(f"[{pid}] Finished streaming file.")
                        break
                    yield chunk
        except Exception as e:
            logging.error(f"[{pid}] Error during streaming {path}: {e}", exc_info=True)
            # Don't re-raise here, let the request finish potentially partially
        # NO finally block here for cleanup - it's scheduled now

    logging.info(f"[{temp_id}] Preparing streaming response for '{dl_name}'.")
    response = Response(stream_with_context(generate_stream(temp_path, temp_id)), mimetype='application/octet-stream')

    try:
        # Try UTF-8 first, then Latin-1 as fallback for filename encoding
        enc_name = filename.encode('utf-8').decode('latin-1', 'ignore')
    except Exception:
        enc_name = f"download_{temp_id}.dat"
        logging.warning(f"[{temp_id}] Could not encode filename '{filename}', using fallback: {enc_name}")

    response.headers.set('Content-Disposition', 'attachment', filename=enc_name)
    if size is not None and size >= 0:
        response.headers.set('Content-Length', str(size))

    return response

@app.route('/files/<username>', methods=['GET'])
def list_user_files(username: str) -> Response:
    logging.info(f"List files request for: '{username}'")
    logging.info(f"Fetching files for user '{username}' from DB...")
    user_files, error_msg = find_metadata_by_username(username)

    if error_msg:
        logging.error(f"DB Error listing files for '{username}': {error_msg}")
        return jsonify({"error": "Server error retrieving file list."}), 500

    if user_files is None:
         user_files = []

    logging.info(f"Found {len(user_files)} records for '{username}'.")

    # --- NEW: Convert ObjectId to string before returning ---
    serializable_files = []
    for file_record in user_files:
        # Convert the '_id' field if it exists and is an ObjectId
        if '_id' in file_record and hasattr(file_record['_id'], 'binary'): # Check it's likely an ObjectId
             file_record['_id'] = str(file_record['_id']) # Convert ObjectId to string

        # --- Optional: Convert datetime objects too if needed ---
        # You might also have datetime objects from 'upload_timestamp'
        # If jsonify has issues with those later, add conversion here:
        # if 'upload_timestamp' in file_record and isinstance(file_record['upload_timestamp'], datetime):
        #     file_record['upload_timestamp'] = file_record['upload_timestamp'].isoformat()

        serializable_files.append(file_record)
    # --- End of NEW block ---

    # Return the modified list
    return jsonify(serializable_files) # Return the list with converted IDs

@app.route('/get/<access_id>')
def get_file_by_access_id(access_id: str) -> Union[str, Response]:
    logging.info(f"Request dl page via access_id: {access_id}")
    logging.info(f"Looking up access_id '{access_id}' in DB...")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not file_info:
        error_message_for_user = error_msg if error_msg else f"Link '{access_id}' not found or expired."
        logging.warning(f"Failed lookup for access_id '{access_id}': {error_message_for_user}")
    # Distinguish "not found" from server errors for status code
        status_code = 404 if "not found" in error_message_for_user.lower() else 500
        return make_response(render_template('404_error.html', message=error_message_for_user), status_code)
    
    username = file_info.get('username')
    if not username:    
     # This shouldn't happen if records are saved correctly, but handle defensively
        logging.error(f"Record found for access_id '{access_id}' but missing username field.")
        message = "File record found but is incomplete (missing user info)."
        return make_response(render_template('404_error.html', message=message), 500)

    orig_name = file_info.get('original_filename', 'Unknown'); size = file_info.get('original_size')
    ts_iso = file_info.get('upload_timestamp'); date_str = "Unknown date"
    if ts_iso:
        try:
            dt = dateutil_parser.isoparse(ts_iso);
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            else: dt = dt.astimezone(timezone.utc)
            date_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e: logging.warning(f"Could not parse ts '{ts_iso}': {e}")
    logging.info(f"Rendering dl page for '{orig_name}' (id: {access_id}).")
    return render_template('download_page.html', filename=orig_name, filesize=size if size is not None else 0, upload_date=date_str, username=username, access_id=access_id)

@app.route('/delete-file/<username>/<path:filename>', methods=['DELETE'])
def delete_file_record(username: str, filename: str) -> Response:
    logging.info(f"DELETE request user='{username}', file='{filename}'")
    logging.info(f"Attempting delete from DB: User='{username}', File='{filename}'")
    # Using original_filename for deletion as per previous logic.
    # Consider changing frontend/backend to use access_id for guaranteed uniqueness if needed.
    deleted_count, error_msg = delete_metadata_by_filename(username, filename) # Call DB function

    if error_msg:
        logging.error(f"DB Error deleting file record for '{username}/{filename}': {error_msg}")
        # Provide a generic server error message to the user
        return jsonify({"error": "Server error during deletion. Please try again later."}), 500

    if deleted_count == 0:
        logging.warning(f"No file record found to delete for '{username}/{filename}'.")
        return jsonify({"error": f"File '{filename}' not found for user '{username}'."}), 404
    else:
        # It's possible multiple records were deleted if names weren't unique
        logging.info(f"Successfully deleted {deleted_count} record(s) for '{username}/{filename}' from DB.")
        return jsonify({"message": f"Record for '{filename}' deleted successfully."}), 200
logging.info("Flask routes defined using configurable workers and linter fixes.")


# routes.py (Add this new route function)

# --- User Registration Route ---
@app.route('/register', methods=['POST'])
def register_user():
    """Handles user registration requests."""
    logging.info("Received registration request.")

    # --- 1. Get Data from Request Form ---
    # Use .get() with default '' to avoid KeyError if field is missing
    first_name = request.form.get('first_name', '').strip()
    last_name = request.form.get('last_name', '').strip()
    email = request.form.get('email', '').strip()
    password = request.form.get('password', '') # Don't strip password yet
    repeat_password = request.form.get('repeat_password', '')
    # You might want to check the agreement checkboxes too, depending on requirements
    # agree_terms = request.form.get('agree_terms') == 'on' # Example

    # --- 2. Basic Validation ---
    if not all([first_name, last_name, email, password, repeat_password]):
        logging.warning("Registration failed: Missing required fields.")
        return make_response(jsonify({"error": "Please fill in all required fields."}), 400)

    if password != repeat_password:
        logging.warning("Registration failed: Passwords do not match.")
        return make_response(jsonify({"error": "Passwords do not match."}), 400)

    # Basic email format check (not exhaustive)
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
         logging.warning(f"Registration failed: Invalid email format '{email}'.")
         return make_response(jsonify({"error": "Invalid email format."}), 400)

    # --- 3. Check if User Already Exists ---
    existing_user, db_error = find_user_by_email(email)
    if db_error:
        logging.error(f"Database error checking email '{email}': {db_error}")
        return make_response(jsonify({"error": "Server error during registration. Please try again."}), 500)
    if existing_user:
        logging.warning(f"Registration failed: Email '{email}' already exists.")
        return make_response(jsonify({"error": "An account with this email address already exists."}), 409) # 409 Conflict

    # --- 4. Hash the Password ---
    # Use a strong hashing method like sha256 or bcrypt (default method is good)
    try:
        # Let werkzeug handle salt generation etc.
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
    except Exception as e:
        logging.error(f"Password hashing failed: {e}", exc_info=True)
        return make_response(jsonify({"error": "Server error during registration processing."}), 500)

    # --- 5. Prepare User Document for Database ---
    new_user_data = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email, # Will be lowercased in save_user function
        "password_hash": hashed_password,
        "created_at": datetime.now(timezone.utc) # Store registration timestamp
        # Add other fields as needed, e.g., agree_terms
    }

    # --- 6. Save User to Database ---
    save_success, save_msg = save_user(new_user_data)

    if not save_success:
        logging.error(f"Failed to save new user '{email}': {save_msg}")
        # Check if the error was specifically about the email existing (just in case check failed earlier)
        if "already exists" in save_msg.lower():
             return make_response(jsonify({"error": "An account with this email address already exists."}), 409)
        else:
             return make_response(jsonify({"error": "Server error saving registration. Please try again."}), 500)

    # --- 7. Success Response ---
    logging.info(f"User '{email}' registered successfully.")
    return make_response(jsonify({"message": "Registration successful!"}), 201) # 201 Created