import logging
import mimetypes
import os
import uuid
import time
import json
import shutil
import io
from typing import Dict, Any, Tuple, Optional, List, Generator
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from extensions import login_manager, upload_progress_data, download_prep_data
from flask import (
    Blueprint, request, make_response, jsonify, Response, stream_with_context, url_for
)
from flask import Response, stream_with_context
from database import find_metadata_by_access_id
from .utils import _yield_sse_event, _calculate_progress, _safe_remove_directory, _safe_remove_file
import tempfile
# This now imports the generator version of the function
from google_drive_api import download_from_gdrive, delete_from_gdrive, upload_to_gdrive_with_progress
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId
from werkzeug.utils import secure_filename
from database import User, find_user_by_id, save_file_metadata, get_metadata_collection
from extensions import upload_progress_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes
)
from telegram_api import send_file_to_telegram
from flask import stream_with_context
from google_drive_api import delete_from_gdrive, upload_to_gdrive_with_progress, download_from_gdrive_to_file
from routes.utils import _yield_sse_event, _calculate_progress, _safe_remove_directory

background_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix='BgTgTransfer')
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
SseEvent = str

upload_bp = Blueprint('upload', __name__)

# --- All helper functions and other routes remain unchanged ---
def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Any] = {"chat_id": res["chat_id"], "success": res["success"]}
        if res["success"] and res["tg_response"]:
            res_data = res["tg_response"].get('result', {})
            msg_id = res_data.get('message_id')
            doc_data = res_data.get('document', {})
            f_id = doc_data.get('file_id')
            f_uid = doc_data.get('file_unique_id')
            f_size = doc_data.get('file_size')
            if msg_id and f_id and f_uid:
                detail.update({"message_id": msg_id, "file_id": f_id, "file_unique_id": f_uid})
                if f_size is not None: detail["file_size"] = f_size
            else:
                detail.update({"success": False, "error": "Missing critical IDs in Telegram response"})
                logging.warning(f"[{log_prefix}] Missing IDs in TG response: {res['tg_response']}")
        elif not res["success"]:
            detail["error"] = res["message"]
        all_chat_details.append(detail)
    return all_chat_details

def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    log_prefix = f"[{upload_id}] Task for '{filename}' to {chat_id}"
    try:
        with open(file_path, 'rb') as f_handle:
            file_size = os.path.getsize(file_path)
            logging.info(f"{log_prefix} Sending single file ({format_bytes(file_size)}) from path: {file_path} to Telegram.")
            success, message, tg_response = send_file_to_telegram(f_handle, filename, chat_id)
        logging.info(f"{log_prefix} Single file Telegram send result: Success={success}")
        return str(chat_id), (success, message, tg_response)
    except FileNotFoundError:
        logging.error(f"{log_prefix} Local temp file for Telegram not found at path: {file_path}")
        return str(chat_id), (False, f"Local temp file not found: {filename}", None)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error sending single file to Telegram: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing single file for Telegram: {e}", None)

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    log_prefix = f"[{upload_id}] TG Chunk {chunk_num} ('{filename}') to {chat_id}"
    try:
        buffer = io.BytesIO(chunk_data)
        logging.info(f"{log_prefix} Sending TG chunk ({format_bytes(len(chunk_data))})")
        success, message, tg_response = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"{log_prefix} Send TG chunk result: Success={success}")
        return str(chat_id), (success, message, tg_response)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error sending TG chunk: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing TG chunk: {e}", None)

@upload_bp.route('/progress-stream/<batch_id>', methods=['GET'])
def stream_upload_progress(batch_id: str):
    def generate_events():
        last_event_data = None
        log_prefix = f"[ProgressStream-{batch_id}]"
        logging.info(f"{log_prefix} SSE connection opened.")
        try:
            while True:
                current_event_data = upload_progress_data.get(batch_id)
                if current_event_data and current_event_data != last_event_data:
                    event_type = current_event_data.get("type", "status")
                    yield _yield_sse_event(event_type, current_event_data)
                    last_event_data = current_event_data
                    if current_event_data.get("type") in ["complete", "error", "finalized"]:
                        logging.info(f"{log_prefix} Received final event type '{current_event_data.get('type')}'. Closing stream.")
                        break
                time.sleep(0.2)
        except GeneratorExit:
            logging.info(f"{log_prefix} Client disconnected. Closing stream.")
        finally:
            if batch_id in upload_progress_data:
                time.sleep(1)
                del upload_progress_data[batch_id]
                logging.info(f"{log_prefix} Cleaned up progress data key.")
    return Response(stream_with_context(generate_events()), mimetype='text/event-stream')

@upload_bp.route('/initiate-batch', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True)
def initiate_batch_upload():
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
        
    batch_id = str(uuid.uuid4())
    log_prefix = f"[BatchInitiate-{batch_id}]"
    
    upload_progress_data[batch_id] = {"type": "status", "message": "Batch initiated..."}
    
    current_user_jwt_identity = get_jwt_identity()
    user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
    if current_user_jwt_identity:
        user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
        if user_doc:
            user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

    data = request.get_json()
    batch_display_name = data.get('batch_display_name', 'Unnamed Batch')
    total_original_size = data.get('total_original_size', 0)
    is_batch = data.get('is_batch', True)

    db_record_payload = {
        "access_id": batch_id,
        "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'],
        "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "storage_location": "gdrive",
        "status_overall": "batch_initiated",
        "is_batch": is_batch,
        "batch_display_name": batch_display_name,
        "files_in_batch": [],
        "total_original_size": total_original_size,
    }

    save_success, save_msg = save_file_metadata(db_record_payload)
    if not save_success:
        logging.error(f"{log_prefix} DB placeholder save failed: {save_msg}.")
        upload_progress_data[batch_id] = {"type": "error", "message": f"DB save failed: {save_msg}"}
        return jsonify({"error": f"Failed to initiate batch record: {save_msg}"}), 500

    logging.info(f"{log_prefix} Batch placeholder created for user '{user_info['username']}'.")
    return jsonify({"message": "Batch initiated successfully.", "batch_id": batch_id}), 201

# ==============================================================================
# === THE MAIN FIX IS HERE =======================================================
# ==============================================================================
# @upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
# @jwt_required(optional=True)
# def stream_file_to_batch():
#     if request.method == 'OPTIONS':
#         return make_response(("OK", 200))
    
#     batch_id = request.args.get('batch_id')
#     if not batch_id:
#         return jsonify({"error": "Request is missing the 'batch_id' query parameter."}), 400

#     log_prefix = f"[StreamForBatch-{batch_id}]"
    
#     filename = secure_filename(request.args.get('filename', ''))
#     file_size = int(request.headers.get('Content-Length', 0))
#     if not filename:
#         return jsonify({"error": "Filename parameter missing."}), 400
    
#     in_memory_stream = io.BytesIO(request.stream.read())
#     gdrive_file_id, upload_error = None, None

#     try:
#         # 1. Create the generator instance. This doesn't start the upload yet.
#         upload_generator = upload_to_gdrive_with_progress(
#             source=in_memory_stream, 
#             filename_in_gdrive=filename, 
#             operation_id_for_log=batch_id
#         )
        
#         # 2. Use a while loop to consume the generator. This is the only way to
#         #    get both the yielded progress values AND the final return value.
#         while True:
#             try:
#                 # Get the next yielded item (a progress dictionary). This drives the upload.
#                 progress_event = next(upload_generator)
                
#                 # Add filename so the UI knows which file is currently uploading
#                 progress_event['filename'] = filename
                
#                 # Publish the progress event to the shared dictionary for the SSE stream
#                 upload_progress_data[batch_id] = progress_event

#             except StopIteration as e:
#                 # This 'except' block is triggered when the generator finishes.
#                 # The 'return' value from the generator is stored in the exception's 'value' attribute.
#                 gdrive_file_id, upload_error = e.value
#                 logging.info(f"{log_prefix} GDrive upload finished for '{filename}'. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
#                 break # Exit the while loop

#         # 3. After the loop, check the results.
#         if upload_error: 
#             raise Exception(upload_error)
#         if not gdrive_file_id: 
#             raise Exception("GDrive upload complete but no file ID was returned.")
    
#         # 4. Save metadata to the database.
#         file_details = {
#             "original_filename": filename, 
#             "gdrive_file_id": gdrive_file_id, 
#             "original_size": file_size, 
#             "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', 
#             "telegram_send_status": "pending"
#         }
#         coll, db_error = get_metadata_collection()
#         if db_error: raise Exception(db_error)
        
#         result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
#         if result.matched_count == 0:
#             delete_from_gdrive(gdrive_file_id) # Clean up the orphaned GDrive file
#             raise Exception(f"Batch ID '{batch_id}' not found in database.")
            
#     except Exception as e:
#         if gdrive_file_id: 
#             delete_from_gdrive(gdrive_file_id) # Clean up GDrive file if DB save fails
#         logging.error(f"{log_prefix} Streaming to GDrive failed for file '{filename}': {e}", exc_info=True)
#         # Report the error back via the SSE stream for the frontend
#         upload_progress_data[batch_id] = {"type": "error", "message": str(e)}
#         return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500

#     # Report completion of this specific file to the SSE stream
#     upload_progress_data[batch_id] = {"type": "status", "message": f"Completed: {filename}"}
#     return jsonify({"message": f"File '{filename}' streamed successfully."}), 200

# @upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
# @jwt_required(optional=True)
# def stream_file_to_batch():
#     if request.method == 'OPTIONS':
#         return make_response(("OK", 200))
    
#     batch_id = request.args.get('batch_id')
#     if not batch_id:
#         return jsonify({"error": "Request is missing the 'batch_id' query parameter."}), 400

#     log_prefix = f"[StreamForBatch-{batch_id}]"
    
#     filename = secure_filename(request.args.get('filename', ''))
#     file_size = int(request.headers.get('Content-Length', 0))
#     if not filename:
#         return jsonify({"error": "Filename parameter missing."}), 400

#     # --- START OF THE FIX ---
#     # Instead of reading into memory, we create a temporary file on disk.
#     temp_file_for_gdrive = None
#     gdrive_file_id = None
    
#     try:
#         # Create a temporary file to write the stream to.
#         # UPLOADS_TEMP_DIR must exist.
#         with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"gdrive_up_{batch_id}_") as tf:
#             temp_file_for_gdrive = tf.name
#             # Read the request stream in chunks and write to the temp file
#             shutil.copyfileobj(request.stream, tf)
        
#         logging.info(f"{log_prefix} Finished streaming upload to temporary file: {temp_file_for_gdrive}")

#         # Now, use the temporary file path as the source for the GDrive upload.
#         # The google_drive_api function already knows how to handle a file path.
#         upload_generator = upload_to_gdrive_with_progress(
#             source=temp_file_for_gdrive, 
#             filename_in_gdrive=filename, 
#             operation_id_for_log=batch_id
#         )
        
#         # This part of the logic remains the same.
#         while True:
#             try:
#                 progress_event = next(upload_generator)
#                 progress_event['filename'] = filename
#                 upload_progress_data[batch_id] = progress_event
#             except StopIteration as e:
#                 gdrive_file_id, upload_error = e.value
#                 logging.info(f"{log_prefix} GDrive upload finished for '{filename}'. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
#                 break

#         if upload_error: 
#             raise Exception(upload_error)
#         if not gdrive_file_id: 
#             raise Exception("GDrive upload complete but no file ID was returned.")
    
#         file_details = {
#             "original_filename": filename, 
#             "gdrive_file_id": gdrive_file_id, 
#             "original_size": file_size, 
#             "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', 
#             "telegram_send_status": "pending"
#         }
#         coll, db_error = get_metadata_collection()
#         if db_error: raise Exception(db_error)
        
#         result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
#         if result.matched_count == 0:
#             delete_from_gdrive(gdrive_file_id) # Clean up the orphaned GDrive file
#             raise Exception(f"Batch ID '{batch_id}' not found in database.")
            
#     except Exception as e:
#         if gdrive_file_id: 
#             delete_from_gdrive(gdrive_file_id)
#         logging.error(f"{log_prefix} Streaming to disk/GDrive failed for file '{filename}': {e}", exc_info=True)
#         upload_progress_data[batch_id] = {"type": "error", "message": str(e)}
#         return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500
#     finally:
#         # --- IMPORTANT CLEANUP ---
#         # Always delete the temporary file from the server's disk.
#         if temp_file_for_gdrive:
#             _safe_remove_file(temp_file_for_gdrive, log_prefix, "temp gdrive upload file")
#     # --- END OF THE FIX ---

#     upload_progress_data[batch_id] = {"type": "status", "message": f"Completed: {filename}"}
#     return jsonify({"message": f"File '{filename}' streamed successfully."}), 200

@upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True)
def stream_file_to_batch():
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
    
    batch_id = request.args.get('batch_id')
    if not batch_id:
        return jsonify({"error": "Request is missing the 'batch_id' query parameter."}), 400

    log_prefix = f"[StreamForBatch-{batch_id}]"
    
    filename = secure_filename(request.args.get('filename', ''))
    file_size = int(request.headers.get('Content-Length', 0))
    if not filename:
        return jsonify({"error": "Filename parameter missing."}), 400
    
    in_memory_stream = io.BytesIO(request.stream.read())
    gdrive_file_id, upload_error = None, None

    try:
        # 1. Create the generator instance. This doesn't start the upload yet.
        upload_generator = upload_to_gdrive_with_progress(
            source=in_memory_stream, 
            filename_in_gdrive=filename, 
            operation_id_for_log=batch_id
        )
        
        # 2. Use a while loop to consume the generator. This is the only way to
        #    get both the yielded progress values AND the final return value.
        while True:
            try:
                # Get the next yielded item (a progress dictionary). This drives the upload.
                progress_event = next(upload_generator)
                
                # Add filename so the UI knows which file is currently uploading
                progress_event['filename'] = filename
                
                # Publish the progress event to the shared dictionary for the SSE stream
                upload_progress_data[batch_id] = progress_event

            except StopIteration as e:
                # This 'except' block is triggered when the generator finishes.
                # The 'return' value from the generator is stored in the exception's 'value' attribute.
                gdrive_file_id, upload_error = e.value
                logging.info(f"{log_prefix} GDrive upload finished for '{filename}'. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
                break # Exit the while loop

        # 3. After the loop, check the results.
        if upload_error: 
            raise Exception(upload_error)
        if not gdrive_file_id: 
            raise Exception("GDrive upload complete but no file ID was returned.")
    
        # 4. Save metadata to the database.
        file_details = {
            "original_filename": filename, 
            "gdrive_file_id": gdrive_file_id, 
            "original_size": file_size, 
            "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', 
            "telegram_send_status": "pending"
        }
        coll, db_error = get_metadata_collection()
        if db_error: raise Exception(db_error)
        
        result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
        if result.matched_count == 0:
            delete_from_gdrive(gdrive_file_id) # Clean up the orphaned GDrive file
            raise Exception(f"Batch ID '{batch_id}' not found in database.")
            
    except Exception as e:
        if gdrive_file_id: 
            delete_from_gdrive(gdrive_file_id) # Clean up GDrive file if DB save fails
        logging.error(f"{log_prefix} Streaming to GDrive failed for file '{filename}': {e}", exc_info=True)
        # Report the error back via the SSE stream for the frontend
        upload_progress_data[batch_id] = {"type": "error", "message": str(e)}
        return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500

    # Report completion of this specific file to the SSE stream
    upload_progress_data[batch_id] = {"type": "status", "message": f"Completed: {filename}"}
    return jsonify({"message": f"File '{filename}' streamed successfully."}), 200


# ==============================================================================
# --- The rest of the file is unchanged ---
# ==============================================================================
@upload_bp.route('/finalize-batch/<batch_id>', methods=['POST'])
@jwt_required(optional=True)
def finalize_batch_upload(batch_id: str):
    log_prefix = f"[BatchFinalize-{batch_id}]"
    
    upload_progress_data[batch_id] = {"type": "finalized", "message": "Finalizing transfer..."}
    
    coll, db_error = get_metadata_collection()
    if db_error: return jsonify({"error": "DB connection error"}), 500
    
    update_result = coll.update_one({"access_id": batch_id}, {"$set": {"status_overall": "gdrive_complete_pending_telegram"}})
    if update_result.matched_count == 0:
        upload_progress_data[batch_id] = {"type": "error", "message": "Batch record not found."}
        return jsonify({"error": "Batch record not found."}), 404

    logging.info(f"{log_prefix} Batch finalized. Submitting background task.")
    background_executor.submit(run_gdrive_to_telegram_transfer, batch_id)

    frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
    download_url = f"{frontend_base_url}/batch-view/{batch_id}"

    return jsonify({"message": "Batch finalized.", "access_id": batch_id, "download_url": download_url}), 200

# def run_gdrive_to_telegram_transfer(access_id: str):
#     log_prefix = f"[BG-TG-{access_id}]"
#     logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
#     tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
#     db_record = None
    
#     try:
#         db_record, db_error = find_metadata_by_access_id(access_id)
#         if db_error or not db_record:
#             logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
#             return

#         if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
#             logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Aborting.")
#             return

#         db_record["status_overall"] = "telegram_processing_background"
#         save_file_metadata(db_record) 

#         files_to_process = db_record.get("files_in_batch", [])
#         processed_files_for_db = []
#         batch_tg_success = True
        
#         for file_detail in files_to_process:
#             original_filename = file_detail.get("original_filename")
#             gdrive_file_id = file_detail.get("gdrive_file_id")

#             if not original_filename or not gdrive_file_id:
#                 logging.error(f"{log_prefix} Skipping file due to missing data: {file_detail}")
#                 file_detail["telegram_send_status"] = "error_missing_data"
#                 processed_files_for_db.append(file_detail)
#                 batch_tg_success = False
#                 continue

#             current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
#             updated_file_meta = file_detail.copy()
#             local_temp_file_for_tg: Optional[str] = None

#             try:
#                 logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram transfer.")
#                 gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
#                 if dl_err or not gdrive_stream:
#                     raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

#                 file_suffix_tg = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix_tg) as temp_f:
#                     local_temp_file_for_tg = temp_f.name
#                     shutil.copyfileobj(gdrive_stream, temp_f)
#                 gdrive_stream.close()
#                 current_file_size = os.path.getsize(local_temp_file_for_tg)
                
#                 # --- THIS IS THE FIXED LOGIC ---
#                 if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                     # Logic for large files (chunking)
#                     logging.info(f"{current_file_log_prefix} File is large ({format_bytes(current_file_size)}), proceeding with chunked upload to Telegram.")
#                     chunk_futures = {}
#                     chunk_results = []
#                     chunk_num = 1
                    
#                     with open(local_temp_file_for_tg, 'rb') as f_handle:
#                         while True:
#                             chunk_data = f_handle.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                             if not chunk_data:
#                                 break
                            
#                             chunk_filename = f"{original_filename}.{str(chunk_num).zfill(3)}"
#                             for chat_id in TELEGRAM_CHAT_IDS:
#                                 future = tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, chunk_num)
#                                 chunk_futures[future] = {"chat_id": str(chat_id), "chunk_num": chunk_num}
#                             chunk_num += 1

#                     for future in as_completed(chunk_futures):
#                         info = chunk_futures[future]
#                         try:
#                             _, api_result = future.result()
#                             chunk_results.append({"chat_id": info["chat_id"], "chunk_num": info["chunk_num"], "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
#                         except Exception as e_fut:
#                             chunk_results.append({"chat_id": info["chat_id"], "chunk_num": info["chunk_num"], "success": False, "message": str(e_fut), "tg_response": None})
                    
#                     total_chunks = chunk_num - 1
#                     primary_chat_results = [res for res in chunk_results if res["chat_id"] == str(PRIMARY_TELEGRAM_CHAT_ID)]
#                     successful_primary_chunks = sum(1 for res in primary_chat_results if res["success"])

#                     if successful_primary_chunks == total_chunks and total_chunks > 0:
#                         parsed_locations = _parse_send_results(f"{current_file_log_prefix}-ParseChunks", chunk_results)
#                         updated_file_meta["telegram_send_status"] = "success_chunked"
#                         updated_file_meta["telegram_send_locations"] = parsed_locations
#                         updated_file_meta["total_chunks"] = total_chunks
#                     else:
#                         raise Exception(f"Primary Telegram chunked upload failed. {successful_primary_chunks}/{total_chunks} chunks succeeded.")
#                 else:
#                     # Logic for small files (single upload)
#                     single_futures = {tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id) for chat_id in TELEGRAM_CHAT_IDS}
#                     single_results = []
#                     for future in as_completed(single_futures):
#                         chat_id_res = single_futures[future]
#                         try:
#                             _, api_result = future.result()
#                             single_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
#                         except Exception as e_fut:
#                             single_results.append({"chat_id": chat_id_res, "success": False, "message": str(e_fut), "tg_response": None})
                    
#                     parsed_locations = _parse_send_results(f"{current_file_log_prefix}-Parse", single_results)
#                     primary_res = next((res for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                    
#                     if primary_res and primary_res.get("success"):
#                         updated_file_meta["telegram_send_status"] = "success_single"
#                         updated_file_meta["telegram_send_locations"] = parsed_locations
#                     else:
#                         raise Exception("Primary Telegram upload failed.")

#                 # This logic is now common for both small and large files
#                 if updated_file_meta.get("telegram_send_status", "").startswith("success"):
#                     logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
#                     del_success, del_err = delete_from_gdrive(gdrive_file_id)
#                     if not del_success:
#                         logging.warning(f"{current_file_log_prefix} FAILED GDrive delete for ID {gdrive_file_id}: {del_err}")
#                         updated_file_meta["gdrive_cleanup_error"] = del_err
#                 else:
#                     batch_tg_success = False
#                     logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")

#             except Exception as e_file_processing:
#                 logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
#                 updated_file_meta["telegram_send_status"] = "error_processing_bg"
#                 updated_file_meta["reason_telegram"] = str(e_file_processing)
#                 batch_tg_success = False
#             finally:
#                 if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
#                     _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
#             processed_files_for_db.append(updated_file_meta)

#         db_record["files_in_batch"] = processed_files_for_db
#         if batch_tg_success:
#             db_record["storage_location"] = "telegram"
#             db_record["status_overall"] = "telegram_complete"
#         else:
#             db_record["storage_location"] = "mixed_gdrive_telegram_error"
#             db_record["status_overall"] = "telegram_processing_errors"
#             error_reasons = [f"File '{f.get('original_filename', 'N/A')}': {f.get('reason_telegram', 'Unknown TG error')}" for f in processed_files_for_db if not f.get("telegram_send_status", "").startswith("success")]
#             db_record["last_error"] = "; ".join(error_reasons)
        
#         save_file_metadata(db_record)
#         logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

#     except Exception as e_bg:
#         logging.error(f"{log_prefix} Unhandled exception in background transfer: {e_bg}", exc_info=True)
#         if db_record: 
#             db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
#             db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
#             save_file_metadata(db_record)
#     finally:
#         if tg_send_executor: tg_send_executor.shutdown(wait=True)
#         logging.info(f"{log_prefix} Background transfer finished and all cleanup is complete.")

def run_gdrive_to_telegram_transfer(access_id: str):
    log_prefix = f"[BG-TG-{access_id}]"
    logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
    tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
    db_record = None
    
    try:
        db_record, db_error = find_metadata_by_access_id(access_id)
        if db_error or not db_record:
            logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
            return

        if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
            logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Aborting.")
            return

        db_record["status_overall"] = "telegram_processing_background"
        save_file_metadata(db_record) 

        files_to_process = db_record.get("files_in_batch", [])
        processed_files_for_db = []
        batch_tg_success = True
        
        for file_detail in files_to_process:
            original_filename = file_detail.get("original_filename")
            gdrive_file_id = file_detail.get("gdrive_file_id")

            if not original_filename or not gdrive_file_id:
                logging.error(f"{log_prefix} Skipping file due to missing data: {file_detail}")
                file_detail["telegram_send_status"] = "error_missing_data"
                processed_files_for_db.append(file_detail)
                batch_tg_success = False
                continue

            current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
            updated_file_meta = file_detail.copy()
            local_temp_file_for_tg: Optional[str] = None

            try:
                logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram transfer.")
                gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
                if dl_err or not gdrive_stream:
                    raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

                logging.info(f"{current_file_log_prefix} Preparing to stream from GDrive (ID: {gdrive_file_id}) directly to a temp file.")
                file_suffix_tg = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix_tg) as temp_f:
                    local_temp_file_for_tg = temp_f.name
                    shutil.copyfileobj(gdrive_stream, temp_f)
                gdrive_stream.close()
                current_file_size = os.path.getsize(local_temp_file_for_tg)
                
                download_success, dl_err = download_from_gdrive_to_file(gdrive_file_id, local_temp_file_for_tg)

                if not download_success:
                    _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "failed gdrive download target")
                    raise Exception(f"GDrive download to file failed for TG: {dl_err}")
                
                # --- THIS IS THE FIXED LOGIC ---
                if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                    # Logic for large files (chunking)
                    logging.info(f"{current_file_log_prefix} File is large ({format_bytes(current_file_size)}), proceeding with chunked upload to Telegram.")
                    chunk_futures = {}
                    chunk_results = []
                    chunk_num = 1
                    
                    with open(local_temp_file_for_tg, 'rb') as f_handle:
                        while True:
                            chunk_data = f_handle.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                            if not chunk_data:
                                break
                            
                            chunk_filename = f"{original_filename}.{str(chunk_num).zfill(3)}"
                            for chat_id in TELEGRAM_CHAT_IDS:
                                future = tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, chunk_num)
                                chunk_futures[future] = {"chat_id": str(chat_id), "chunk_num": chunk_num}
                            chunk_num += 1

                    for future in as_completed(chunk_futures):
                        info = chunk_futures[future]
                        try:
                            _, api_result = future.result()
                            chunk_results.append({"chat_id": info["chat_id"], "chunk_num": info["chunk_num"], "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                        except Exception as e_fut:
                            chunk_results.append({"chat_id": info["chat_id"], "chunk_num": info["chunk_num"], "success": False, "message": str(e_fut), "tg_response": None})
                    
                    total_chunks = chunk_num - 1
                    primary_chat_results = [res for res in chunk_results if res["chat_id"] == str(PRIMARY_TELEGRAM_CHAT_ID)]
                    successful_primary_chunks = sum(1 for res in primary_chat_results if res["success"])

                    if successful_primary_chunks == total_chunks and total_chunks > 0:
                        parsed_locations = _parse_send_results(f"{current_file_log_prefix}-ParseChunks", chunk_results)
                        updated_file_meta["telegram_send_status"] = "success_chunked"
                        updated_file_meta["telegram_send_locations"] = parsed_locations
                        updated_file_meta["total_chunks"] = total_chunks
                    else:
                        raise Exception(f"Primary Telegram chunked upload failed. {successful_primary_chunks}/{total_chunks} chunks succeeded.")
                else:
                    # Logic for small files (single upload)
                    single_futures = {tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id) for chat_id in TELEGRAM_CHAT_IDS}
                    single_results = []
                    for future in as_completed(single_futures):
                        chat_id_res = single_futures[future]
                        try:
                            _, api_result = future.result()
                            single_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                        except Exception as e_fut:
                            single_results.append({"chat_id": chat_id_res, "success": False, "message": str(e_fut), "tg_response": None})
                    
                    parsed_locations = _parse_send_results(f"{current_file_log_prefix}-Parse", single_results)
                    primary_res = next((res for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                    
                    if primary_res and primary_res.get("success"):
                        updated_file_meta["telegram_send_status"] = "success_single"
                        updated_file_meta["telegram_send_locations"] = parsed_locations
                    else:
                        raise Exception("Primary Telegram upload failed.")

                # This logic is now common for both small and large files
                if updated_file_meta.get("telegram_send_status", "").startswith("success"):
                    logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
                    del_success, del_err = delete_from_gdrive(gdrive_file_id)
                    if not del_success:
                        logging.warning(f"{current_file_log_prefix} FAILED GDrive delete for ID {gdrive_file_id}: {del_err}")
                        updated_file_meta["gdrive_cleanup_error"] = del_err
                else:
                    batch_tg_success = False
                    logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")

            except Exception as e_file_processing:
                logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
                updated_file_meta["telegram_send_status"] = "error_processing_bg"
                updated_file_meta["reason_telegram"] = str(e_file_processing)
                batch_tg_success = False
            finally:
                if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
                    _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
            processed_files_for_db.append(updated_file_meta)

        db_record["files_in_batch"] = processed_files_for_db
        if batch_tg_success:
            db_record["storage_location"] = "telegram"
            db_record["status_overall"] = "telegram_complete"
        else:
            db_record["storage_location"] = "mixed_gdrive_telegram_error"
            db_record["status_overall"] = "telegram_processing_errors"
            error_reasons = [f"File '{f.get('original_filename', 'N/A')}': {f.get('reason_telegram', 'Unknown TG error')}" for f in processed_files_for_db if not f.get("telegram_send_status", "").startswith("success")]
            db_record["last_error"] = "; ".join(error_reasons)
        
        save_file_metadata(db_record)
        logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

    except Exception as e_bg:
        logging.error(f"{log_prefix} Unhandled exception in background transfer: {e_bg}", exc_info=True)
        if db_record: 
            db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
            db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
            save_file_metadata(db_record)
    finally:
        if tg_send_executor: tg_send_executor.shutdown(wait=True)
        logging.info(f"{log_prefix} Background transfer finished and all cleanup is complete.")

        
@upload_bp.route('/stream-legacy', methods=['POST', 'OPTIONS']) # Renamed to avoid confusion
@jwt_required(optional=True) 
def stream_upload_to_gdrive():
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
    
    operation_id = str(uuid.uuid4())
    log_prefix = f"[StreamUpload-{operation_id}]"

    def generate_events():
        gdrive_file_id = None
        try:
            current_user_jwt_identity = get_jwt_identity()
            user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
            if current_user_jwt_identity:
                user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
                if user_doc:
                    user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

            filename = secure_filename(request.args.get('X-Filename', ''))
            file_size = int(request.args.get('X-Filesize', 0))
            if not filename:
                yield _yield_sse_event("error", {"message": "Filename parameter missing."})
                return

            logging.info(f"{log_prefix} Streaming file '{filename}' (Size: {format_bytes(file_size)}) to GDrive.")
            
            upload_progress_data[operation_id] = {}

            in_memory_stream = io.BytesIO(request.stream.read())
            
            for progress_event in upload_to_gdrive_with_progress(
                source=in_memory_stream,
                filename_in_gdrive=filename,
                operation_id_for_log=operation_id
            ):
                if progress_event.get("type") == "error":
                    raise Exception(progress_event.get("message", "Unknown GDrive upload error."))
                
                yield _yield_sse_event(progress_event.get("type", "status"), progress_event)
            
            final_progress_data = upload_progress_data.get(operation_id, {})
            gdrive_file_id = final_progress_data.get("gdrive_file_id_temp_result")
            if not gdrive_file_id:
                raise Exception("GDrive upload complete but no file ID returned.")
            
            file_details_for_db = {"original_filename": filename, "gdrive_file_id": gdrive_file_id, "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', "telegram_send_status": "pending"}
            db_record_payload = {"access_id": operation_id, "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "storage_location": "gdrive", "status_overall": "gdrive_complete_pending_telegram", "is_batch": False, "batch_display_name": filename, "files_in_batch": [file_details_for_db], "total_original_size": file_size}
            save_success, save_msg = save_file_metadata(db_record_payload)
            if not save_success:
                delete_from_gdrive(gdrive_file_id)
                raise Exception(f"Failed to save file record: {save_msg}")
            
            background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
            
            frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
            download_url = f"{frontend_base_url}/batch-view/{operation_id}" if frontend_base_url else url_for('download_prefixed.stream_download_by_access_id', access_id=operation_id, _external=True)

            yield _yield_sse_event("complete", {
                "message": "File uploaded successfully.",
                "access_id": operation_id,
                "download_url": download_url,
                "gdrive_file_id": gdrive_file_id
            })
            logging.info(f"{log_prefix} Successfully completed stream for {filename}.")

        except Exception as e:
            logging.error(f"{log_prefix} Error in streaming generator: {e}", exc_info=True)
            if gdrive_file_id:
                delete_from_gdrive(gdrive_file_id)
            yield _yield_sse_event("error", {"message": str(e)})
        finally:
            if operation_id in upload_progress_data:
                del upload_progress_data[operation_id]

    return Response(stream_with_context(generate_events()), mimetype='text/event-stream')