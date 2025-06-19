# import logging
# import mimetypes
# import os
# import uuid
# import time
# import json
# import shutil
# import io
# import tempfile
# from typing import Dict, Any, Tuple, Optional, List, Generator
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from flask import (
#     Blueprint, request, make_response, jsonify, Response, stream_with_context, url_for
# )
# from flask_jwt_extended import jwt_required, get_jwt_identity
# from werkzeug.utils import secure_filename
# from bson import ObjectId

# from database import User, find_user_by_id, save_file_metadata, get_metadata_collection, find_metadata_by_access_id
# from extensions import upload_progress_data
# from config import (
#     TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID,
#     UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
#     format_bytes
# )
# from telegram_api import send_file_to_telegram
# from google_drive_api import (
#     delete_from_gdrive,
#     upload_to_gdrive_with_progress,
#     download_from_gdrive_to_file
# )
# from .utils import _yield_sse_event, _safe_remove_file

# # --- Blueprint and Global Setup ---

# # This executor is ONLY for the secondary, GDrive-to-Telegram transfer.
# telegram_transfer_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix='BgTgTransfer')

# ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
# upload_bp = Blueprint('upload', __name__)


# # --- Helper Functions (No changes) ---

# def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     all_chat_details = []
#     for res in send_results:
#         detail: Dict[str, Any] = {"chat_id": res["chat_id"], "success": res["success"]}
#         if res["success"] and res["tg_response"]:
#             res_data = res["tg_response"].get('result', {})
#             msg_id, f_id, f_uid = res_data.get('message_id'), res_data.get('document', {}).get('file_id'), res_data.get('document', {}).get('file_unique_id')
#             if msg_id and f_id and f_uid:
#                 detail.update({"message_id": msg_id, "file_id": f_id, "file_unique_id": f_uid})
#                 if res_data.get('document', {}).get('file_size') is not None:
#                     detail["file_size"] = res_data['document']['file_size']
#             else:
#                 detail.update({"success": False, "error": "Missing critical IDs in Telegram response"})
#         elif not res["success"]:
#             detail["error"] = res["message"]
#         all_chat_details.append(detail)
#     return all_chat_details

# def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
#     with open(file_path, 'rb') as f_handle:
#         return str(chat_id), send_file_to_telegram(f_handle, filename, chat_id)

# def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
#     with io.BytesIO(chunk_data) as buffer:
#         return str(chat_id), send_file_to_telegram(buffer, filename, chat_id)


# # --- API Routes ---

# @upload_bp.route('/progress-stream/<batch_id>', methods=['GET'])
# def stream_upload_progress(batch_id: str):
#     """Handles the Server-Sent Events stream with a heartbeat to prevent timeouts."""
#     def generate_events(batch_id: str):
#         last_event_data = None
#         log_prefix = f"[ProgressStream-{batch_id}]"
#         logging.info(f"{log_prefix} SSE connection opened.")
        
#         last_heartbeat_time = time.time()
#         heartbeat_interval = 15  # seconds

#         try:
#             while True:
#                 current_event_data = upload_progress_data.get(batch_id)
#                 if current_event_data and current_event_data != last_event_data:
#                     event_type = current_event_data.get("type", "status")
#                     yield _yield_sse_event(event_type, current_event_data)
#                     last_event_data = current_event_data
#                     last_heartbeat_time = time.time()
#                     if event_type in ["complete", "error", "finalized"]:
#                         logging.info(f"{log_prefix} Final event '{event_type}' received. Closing stream.")
#                         break
                
#                 if time.time() - last_heartbeat_time > heartbeat_interval:
#                     yield ": heartbeat\n\n"
#                     last_heartbeat_time = time.time()
#                     logging.debug(f"{log_prefix} Sent SSE heartbeat.")

#                 time.sleep(0.2)
#         except GeneratorExit:
#             logging.info(f"{log_prefix} Client disconnected.")
#         finally:
#             if batch_id in upload_progress_data:
#                 time.sleep(1)
#                 try:
#                     del upload_progress_data[batch_id]
#                     logging.info(f"{log_prefix} Cleaned up progress data key.")
#                 except KeyError:
#                     pass
                    
#     return Response(stream_with_context(generate_events(batch_id)), mimetype='text/event-stream')

# @upload_bp.route('/initiate-batch', methods=['POST', 'OPTIONS'])
# @jwt_required(optional=True)
# def initiate_batch_upload():
#     """Creates a placeholder database record for a new batch upload."""
#     if request.method == 'OPTIONS':
#         return make_response(("OK", 200))
        
#     batch_id = str(uuid.uuid4())
#     log_prefix = f"[BatchInitiate-{batch_id}]"
    
#     current_user_jwt_identity = get_jwt_identity()
#     user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
#     if current_user_jwt_identity:
#         user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
#         if user_doc:
#             user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

#     data = request.get_json()
#     db_record_payload = {
#         "access_id": batch_id, "username": user_info['username'], "user_email": user_info['user_email'],
#         "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "storage_location": "gdrive", "status_overall": "batch_initiated", "is_batch": data.get('is_batch', True),
#         "batch_display_name": data.get('batch_display_name', 'Unnamed Batch'), "files_in_batch": [], "total_original_size": data.get('total_original_size', 0),
#     }

#     save_success, save_msg = save_file_metadata(db_record_payload)
#     if not save_success:
#         logging.error(f"{log_prefix} DB placeholder save failed: {save_msg}.")
#         return jsonify({"error": f"Failed to initiate batch record: {save_msg}"}), 500

#     logging.info(f"{log_prefix} Batch placeholder created for user '{user_info['username']}'.")
#     return jsonify({"message": "Batch initiated successfully.", "batch_id": batch_id}), 201

# @upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
# @jwt_required(optional=True)
# def stream_file_to_batch():
#     """
#     Receives a file, saves it to disk, and then **synchronously** uploads to GDrive.
#     This is a long-running request.
#     """
#     if request.method == 'OPTIONS':
#         return make_response(("OK", 200))
    
#     batch_id = request.args.get('batch_id')
#     if not batch_id: return jsonify({"error": "Missing 'batch_id' query parameter."}), 400

#     filename = secure_filename(request.args.get('filename', ''))
#     if not filename: return jsonify({"error": "Missing 'filename' query parameter."}), 400

#     log_prefix = f"[StreamForBatch-{batch_id}]"
    
#     temp_file_for_gdrive = None
#     gdrive_file_id = None
    
#     try:
#         # Save to disk to create a seekable file source for Google Drive's resumable upload.
#         # This is where 'No space left on device' can occur, which is an environmental limit.
#         with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"gdrive_up_{batch_id}_") as tf:
#             temp_file_for_gdrive = tf.name
#             shutil.copyfileobj(request.stream, tf)
        
#         file_size = os.path.getsize(temp_file_for_gdrive)
#         logging.info(f"{log_prefix} File '{filename}' saved to temp path '{temp_file_for_gdrive}'. Starting GDrive upload.")

#         # This is now a blocking, synchronous operation.
#         # We pass the file path, which is a seekable source.
#         upload_generator = upload_to_gdrive_with_progress(
#             source=temp_file_for_gdrive, 
#             filename_in_gdrive=filename, 
#             operation_id_for_log=batch_id
#         )
        
#         while True:
#             try:
#                 progress_event = next(upload_generator)
#                 progress_event['filename'] = filename
#                 upload_progress_data[batch_id] = progress_event
#             except StopIteration as e:
#                 gdrive_file_id, upload_error = e.value
#                 logging.info(f"{log_prefix} GDrive upload finished. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
#                 break

#         if upload_error or not gdrive_file_id:
#             raise Exception(f"GDrive upload failed: {upload_error or 'No file ID returned'}")
    
#         # Update the database
#         file_details = {
#             "original_filename": filename, "gdrive_file_id": gdrive_file_id,
#             "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream',
#             "telegram_send_status": "pending"
#         }
#         coll, db_error = get_metadata_collection()
#         if db_error: raise Exception(db_error)
        
#         result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
#         if result.matched_count == 0:
#             delete_from_gdrive(gdrive_file_id)
#             raise Exception(f"Batch ID '{batch_id}' not found in database.")
            
#     except Exception as e:
#         if gdrive_file_id: delete_from_gdrive(gdrive_file_id)
#         logging.error(f"{log_prefix} GDrive upload process failed for '{filename}': {e}", exc_info=True)
#         upload_progress_data[batch_id] = {"type": "error", "message": str(e)}
#         return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500
#     finally:
#         # Crucially, clean up the temp file from the disk.
#         _safe_remove_file(temp_file_for_gdrive, log_prefix, "temp gdrive upload file")

#     upload_progress_data[batch_id] = {"type": "status", "message": f"Completed: {filename}"}
#     return jsonify({"message": f"File '{filename}' uploaded successfully to GDrive."}), 200

# @upload_bp.route('/finalize-batch/<batch_id>', methods=['POST'])
# @jwt_required(optional=True)
# def finalize_batch_upload(batch_id: str):
#     """Finalizes the batch and starts the GDrive->Telegram transfer in the background."""
#     log_prefix = f"[BatchFinalize-{batch_id}]"
    
#     upload_progress_data[batch_id] = {"type": "finalized", "message": "Transfer to secure storage initiated..."}
#     coll, db_error = get_metadata_collection()
#     if db_error: return jsonify({"error": "DB connection error"}), 500
    
#     update_result = coll.update_one({"access_id": batch_id}, {"$set": {"status_overall": "gdrive_complete_pending_telegram"}})
#     if update_result.matched_count == 0:
#         return jsonify({"error": "Batch record not found."}), 404

#     logging.info(f"{log_prefix} Batch finalized. Submitting background transfer task.")
#     telegram_transfer_executor.submit(run_gdrive_to_telegram_transfer, batch_id)

#     frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
#     download_url = f"{frontend_base_url}/batch-view/{batch_id}"

#     return jsonify({
#         "message": "Batch finalized and transfer to secure storage has begun.",
#         "access_id": batch_id, "download_url": download_url
#     }), 202

# def run_gdrive_to_telegram_transfer(access_id: str):
#     """Background task to download files from GDrive and upload them to Telegram."""
#     log_prefix = f"[BG-TG-{access_id}]"
#     logging.info(f"{log_prefix} Background GDrive-to-Telegram process started.")
    
#     db_record, db_error = find_metadata_by_access_id(access_id)
#     if db_error or not db_record:
#         logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
#         return

#     try:
#         if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
#             return

#         db_record["status_overall"] = "telegram_processing_background"
#         save_file_metadata(db_record)

#         files_to_process = db_record.get("files_in_batch", [])
#         processed_files_for_db = []
#         batch_tg_success = True
        
#         with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'TgSend_{access_id[:4]}') as tg_send_executor:
#             for file_detail in files_to_process:
#                 original_filename = file_detail.get("original_filename")
#                 gdrive_file_id = file_detail.get("gdrive_file_id")
#                 updated_file_meta = file_detail.copy()
#                 local_temp_file_for_tg: Optional[str] = None
                
#                 try:
#                     if not original_filename or not gdrive_file_id:
#                         raise ValueError("File detail missing data.")

#                     current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
                    
#                     with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=os.path.splitext(original_filename)[1]) as temp_f:
#                         local_temp_file_for_tg = temp_f.name
                    
#                     download_success, dl_err = download_from_gdrive_to_file(gdrive_file_id, local_temp_file_for_tg)
#                     if not download_success: raise Exception(f"GDrive download failed: {dl_err}")

#                     current_file_size = os.path.getsize(local_temp_file_for_tg)
                    
#                     if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                         logging.info(f"{current_file_log_prefix} File is large, proceeding with chunked upload.")
#                         updated_file_meta['is_split_for_telegram'] = True
#                         updated_file_meta['telegram_chunks'] = []
#                         total_chunked_size = 0
#                         chunk_num = 1
#                         all_chunks_successful_to_primary = True

#                         with open(local_temp_file_for_tg, 'rb') as f_handle:
#                             while True:
#                                 chunk_data = f_handle.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                                 if not chunk_data:
#                                     break
                                
#                                 chunk_filename = f"{original_filename}.{str(chunk_num).zfill(3)}"
#                                 chunk_size = len(chunk_data)
#                                 total_chunked_size += chunk_size
#                                 logging.info(f"{current_file_log_prefix} Sending chunk {chunk_num} ({format_bytes(chunk_size)})")

#                                 # Submit tasks for THIS chunk to all chats and wait for them to complete
#                                 chunk_send_futures = {
#                                     tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, chunk_num): str(chat_id)
#                                     for chat_id in TELEGRAM_CHAT_IDS
#                                 }
                                
#                                 chunk_send_results = []
#                                 for future in as_completed(chunk_send_futures):
#                                     chat_id_res = chunk_send_futures[future]
#                                     _, api_result = future.result()
#                                     chunk_send_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                                
#                                 parsed_chunk_locations = _parse_send_results(f"{current_file_log_prefix} C{chunk_num}", chunk_send_results)
                                
#                                 primary_chat_success_for_chunk = any(
#                                     res.get("success") for res in parsed_chunk_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)
#                                 )

#                                 if not primary_chat_success_for_chunk:
#                                     all_chunks_successful_to_primary = False
#                                     logging.error(f"{current_file_log_prefix} Failed to upload chunk {chunk_num} to primary chat ID. Aborting sends for this file.")
#                                     break

#                                 updated_file_meta['telegram_chunks'].append({
#                                     "part_number": chunk_num, "chunk_size": chunk_size,
#                                     "chunk_filename": chunk_filename, "send_locations": parsed_chunk_locations
#                                 })
#                                 chunk_num += 1
                        
#                         if all_chunks_successful_to_primary:
#                             updated_file_meta["telegram_send_status"] = "success_chunked"
#                             updated_file_meta["telegram_total_chunked_size"] = total_chunked_size
#                         else:
#                             raise Exception(f"Chunked upload failed at chunk {chunk_num}. File transfer aborted.")
#                     else:
#                         single_futures = {tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id) for chat_id in TELEGRAM_CHAT_IDS}
#                         single_results = []
#                         for future in as_completed(single_futures):
#                              _, api_result = future.result()
#                              single_results.append({"chat_id": single_futures[future], "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                        
#                         parsed_locations = _parse_send_results(current_file_log_prefix, single_results)
#                         if any(res.get("success") for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)):
#                             updated_file_meta["telegram_send_status"] = "success_single"
#                             updated_file_meta["telegram_send_locations"] = parsed_locations
#                         else:
#                             raise Exception("Primary Telegram upload failed.")

#                     if updated_file_meta.get("telegram_send_status", "").startswith("success"):
#                         logging.info(f"{current_file_log_prefix} Sent to Telegram. Deleting from GDrive.")
#                         delete_from_gdrive(gdrive_file_id)
#                     else:
#                         batch_tg_success = False
#                         logging.warning(f"{current_file_log_prefix} Failed Telegram send.")

#                 except Exception as e:
#                     logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e}", exc_info=True)
#                     updated_file_meta["telegram_send_status"] = "error_processing_bg"
#                     updated_file_meta["reason_telegram"] = str(e)
#                     batch_tg_success = False
#                 finally:
#                     _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
                
#                 processed_files_for_db.append(updated_file_meta)

#         db_record["files_in_batch"] = processed_files_for_db
#         db_record["storage_location"] = "telegram" if batch_tg_success else "mixed_gdrive_telegram_error"
#         db_record["status_overall"] = "telegram_complete" if batch_tg_success else "telegram_processing_errors"
#         save_file_metadata(db_record)
#         logging.info(f"{log_prefix} Final DB record updated.")

#     except Exception as e_bg:
#         logging.error(f"{log_prefix} Unhandled exception in background transfer: {e_bg}", exc_info=True)
#         if db_record: 
#             db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
#             db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
#             save_file_metadata(db_record)
#     finally:
#         logging.info(f"{log_prefix} Background transfer finished.")
        
# @upload_bp.route('/stream-legacy', methods=['POST', 'OPTIONS']) # Renamed to avoid confusion
# @jwt_required(optional=True) 
# def stream_upload_to_gdrive():
#     if request.method == 'OPTIONS':
#         return make_response(("OK", 200))
    
#     operation_id = str(uuid.uuid4())
#     log_prefix = f"[StreamUpload-{operation_id}]"

#     def generate_events():
#         gdrive_file_id = None
#         try:
#             current_user_jwt_identity = get_jwt_identity()
#             user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
#             if current_user_jwt_identity:
#                 user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
#                 if user_doc:
#                     user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

#             filename = secure_filename(request.args.get('X-Filename', ''))
#             file_size = int(request.args.get('X-Filesize', 0))
#             if not filename:
#                 yield _yield_sse_event("error", {"message": "Filename parameter missing."})
#                 return

#             logging.info(f"{log_prefix} Streaming file '{filename}' (Size: {format_bytes(file_size)}) to GDrive.")
            
#             upload_progress_data[operation_id] = {}

#             in_memory_stream = io.BytesIO(request.stream.read())
            
#             for progress_event in upload_to_gdrive_with_progress(
#                 source=in_memory_stream,
#                 filename_in_gdrive=filename,
#                 operation_id_for_log=operation_id
#             ):
#                 if progress_event.get("type") == "error":
#                     raise Exception(progress_event.get("message", "Unknown GDrive upload error."))
                
#                 yield _yield_sse_event(progress_event.get("type", "status"), progress_event)
            
#             final_progress_data = upload_progress_data.get(operation_id, {})
#             gdrive_file_id = final_progress_data.get("gdrive_file_id_temp_result")
#             if not gdrive_file_id:
#                 raise Exception("GDrive upload complete but no file ID returned.")
            
#             file_details_for_db = {"original_filename": filename, "gdrive_file_id": gdrive_file_id, "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', "telegram_send_status": "pending"}
#             db_record_payload = {"access_id": operation_id, "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "storage_location": "gdrive", "status_overall": "gdrive_complete_pending_telegram", "is_batch": False, "batch_display_name": filename, "files_in_batch": [file_details_for_db], "total_original_size": file_size}
#             save_success, save_msg = save_file_metadata(db_record_payload)
#             if not save_success:
#                 delete_from_gdrive(gdrive_file_id)
#                 raise Exception(f"Failed to save file record: {save_msg}")
            
#             # This route uses a different executor name, which is fine, but for consistency `telegram_transfer_executor` could be used.
#             # Assuming `background_executor` exists and is appropriate.
#             # telegram_transfer_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
            
#             frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
#             download_url = f"{frontend_base_url}/batch-view/{operation_id}" if frontend_base_url else url_for('download_prefixed.stream_download_by_access_id', access_id=operation_id, _external=True)

#             yield _yield_sse_event("complete", {
#                 "message": "File uploaded successfully.",
#                 "access_id": operation_id,
#                 "download_url": download_url,
#                 "gdrive_file_id": gdrive_file_id
#             })
#             logging.info(f"{log_prefix} Successfully completed stream for {filename}.")

#         except Exception as e:
#             logging.error(f"{log_prefix} Error in streaming generator: {e}", exc_info=True)
#             if gdrive_file_id:
#                 delete_from_gdrive(gdrive_file_id)
#             yield _yield_sse_event("error", {"message": str(e)})
#         finally:
#             if operation_id in upload_progress_data:
#                 del upload_progress_data[operation_id]

#     return Response(stream_with_context(generate_events()), mimetype='text/event-stream')


import logging
import mimetypes
import os
import uuid
import time
import json
import shutil
import io
import tempfile
from typing import Dict, Any, Tuple, Optional, List, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import (
    Blueprint, request, make_response, jsonify, Response, stream_with_context, url_for
)
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.utils import secure_filename
from bson import ObjectId

from database import User, find_user_by_id, save_file_metadata, get_metadata_collection, find_metadata_by_access_id
from extensions import upload_progress_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes
)
from telegram_api import send_file_to_telegram
from google_drive_api import (
    delete_from_gdrive,
    upload_to_gdrive_with_progress,
    download_from_gdrive_to_file
)
from .utils import _yield_sse_event, _safe_remove_file

# --- Blueprint and Global Setup ---

# This executor is ONLY for the secondary, GDrive-to-Telegram transfer.
telegram_transfer_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix='BgTgTransfer')

ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
upload_bp = Blueprint('upload', __name__)


# --- Helper Functions (No changes) ---

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Any] = {"chat_id": res["chat_id"], "success": res["success"]}
        if res["success"] and res["tg_response"]:
            res_data = res["tg_response"].get('result', {})
            msg_id, f_id, f_uid = res_data.get('message_id'), res_data.get('document', {}).get('file_id'), res_data.get('document', {}).get('file_unique_id')
            if msg_id and f_id and f_uid:
                detail.update({"message_id": msg_id, "file_id": f_id, "file_unique_id": f_uid})
                if res_data.get('document', {}).get('file_size') is not None:
                    detail["file_size"] = res_data['document']['file_size']
            else:
                detail.update({"success": False, "error": "Missing critical IDs in Telegram response"})
        elif not res["success"]:
            detail["error"] = res["message"]
        all_chat_details.append(detail)
    return all_chat_details

def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    with open(file_path, 'rb') as f_handle:
        return str(chat_id), send_file_to_telegram(f_handle, filename, chat_id)

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    with io.BytesIO(chunk_data) as buffer:
        return str(chat_id), send_file_to_telegram(buffer, filename, chat_id)


# --- API Routes ---

@upload_bp.route('/progress-stream/<batch_id>', methods=['GET'])
def stream_upload_progress(batch_id: str):
    """Handles the Server-Sent Events stream with a heartbeat to prevent timeouts."""
    def generate_events(batch_id: str):
        last_event_data = None
        log_prefix = f"[ProgressStream-{batch_id}]"
        logging.info(f"{log_prefix} SSE connection opened.")
        
        last_heartbeat_time = time.time()
        heartbeat_interval = 15  # seconds

        try:
            while True:
                current_event_data = upload_progress_data.get(batch_id)
                if current_event_data and current_event_data != last_event_data:
                    event_type = current_event_data.get("type", "status")
                    yield _yield_sse_event(event_type, current_event_data)
                    last_event_data = current_event_data
                    last_heartbeat_time = time.time()
                    if event_type in ["complete", "error", "finalized"]:
                        logging.info(f"{log_prefix} Final event '{event_type}' received. Closing stream.")
                        break
                
                if time.time() - last_heartbeat_time > heartbeat_interval:
                    yield ": heartbeat\n\n"
                    last_heartbeat_time = time.time()
                    logging.debug(f"{log_prefix} Sent SSE heartbeat.")

                time.sleep(0.2)
        except GeneratorExit:
            logging.info(f"{log_prefix} Client disconnected.")
        finally:
            if batch_id in upload_progress_data:
                time.sleep(1)
                try:
                    del upload_progress_data[batch_id]
                    logging.info(f"{log_prefix} Cleaned up progress data key.")
                except KeyError:
                    pass
                    
    return Response(stream_with_context(generate_events(batch_id)), mimetype='text/event-stream')

@upload_bp.route('/initiate-batch', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True)
def initiate_batch_upload():
    """Creates a placeholder database record for a new batch upload."""
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
        
    batch_id = str(uuid.uuid4())
    log_prefix = f"[BatchInitiate-{batch_id}]"
    
    current_user_jwt_identity = get_jwt_identity()
    user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
    if current_user_jwt_identity:
        user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
        if user_doc:
            user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

    data = request.get_json()
    db_record_payload = {
        "access_id": batch_id, "username": user_info['username'], "user_email": user_info['user_email'],
        "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "storage_location": "gdrive", "status_overall": "batch_initiated", "is_batch": data.get('is_batch', True),
        "batch_display_name": data.get('batch_display_name', 'Unnamed Batch'), "files_in_batch": [], "total_original_size": data.get('total_original_size', 0),
    }

    save_success, save_msg = save_file_metadata(db_record_payload)
    if not save_success:
        logging.error(f"{log_prefix} DB placeholder save failed: {save_msg}.")
        return jsonify({"error": f"Failed to initiate batch record: {save_msg}"}), 500

    logging.info(f"{log_prefix} Batch placeholder created for user '{user_info['username']}'.")
    return jsonify({"message": "Batch initiated successfully.", "batch_id": batch_id}), 201

@upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True)
def stream_file_to_batch():
    """
    Receives a file, holds it in an in-memory buffer, and then uploads to GDrive.
    This avoids disk I/O errors and provides immediate progress feedback.
    """
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
    
    batch_id = request.args.get('batch_id')
    if not batch_id: return jsonify({"error": "Missing 'batch_id' query parameter."}), 400

    filename = secure_filename(request.args.get('filename', ''))
    if not filename: return jsonify({"error": "Missing 'filename' query parameter."}), 400

    log_prefix = f"[StreamForBatch-{batch_id}]"
    gdrive_file_id = None
    
    try:
        # Buffer the entire request stream into memory (io.BytesIO).
        # This is fast but limited by server RAM.
        logging.info(f"{log_prefix} Buffering file '{filename}' into memory...")
        in_memory_stream = io.BytesIO(request.stream.read())
        file_size = in_memory_stream.tell()
        in_memory_stream.seek(0) # Rewind the stream to the beginning for reading.
        logging.info(f"{log_prefix} File buffered. Size: {format_bytes(file_size)}. Starting GDrive upload.")

        # Pass the seekable in-memory stream to the upload function.
        upload_generator = upload_to_gdrive_with_progress(
            source=in_memory_stream, 
            filename_in_gdrive=filename, 
            operation_id_for_log=batch_id
        )
        
        while True:
            try:
                progress_event = next(upload_generator)
                progress_event['filename'] = filename
                upload_progress_data[batch_id] = progress_event
            except StopIteration as e:
                gdrive_file_id, upload_error = e.value
                logging.info(f"{log_prefix} GDrive upload finished. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
                break

        if upload_error or not gdrive_file_id:
            raise Exception(f"GDrive upload failed: {upload_error or 'No file ID returned'}")
    
        # Update the database
        file_details = {
            "original_filename": filename, "gdrive_file_id": gdrive_file_id,
            "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream',
            "telegram_send_status": "pending"
        }
        coll, db_error = get_metadata_collection()
        if db_error: raise Exception(db_error)
        
        result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
        if result.matched_count == 0:
            delete_from_gdrive(gdrive_file_id)
            raise Exception(f"Batch ID '{batch_id}' not found in database.")
            
    except Exception as e:
        if gdrive_file_id: delete_from_gdrive(gdrive_file_id)
        logging.error(f"{log_prefix} GDrive upload process failed for '{filename}': {e}", exc_info=True)
        upload_progress_data[batch_id] = {"type": "error", "message": str(e)}
        return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500
    finally:
        # No temp file to clean up, in-memory stream is handled by garbage collection.
        pass

    upload_progress_data[batch_id] = {"type": "status", "message": f"Completed: {filename}"}
    return jsonify({"message": f"File '{filename}' uploaded successfully to GDrive."}), 200

@upload_bp.route('/finalize-batch/<batch_id>', methods=['POST'])
@jwt_required(optional=True)
def finalize_batch_upload(batch_id: str):
    """Finalizes the batch and starts the GDrive->Telegram transfer in the background."""
    log_prefix = f"[BatchFinalize-{batch_id}]"
    
    upload_progress_data[batch_id] = {"type": "finalized", "message": "Transfer to secure storage initiated..."}
    coll, db_error = get_metadata_collection()
    if db_error: return jsonify({"error": "DB connection error"}), 500
    
    update_result = coll.update_one({"access_id": batch_id}, {"$set": {"status_overall": "gdrive_complete_pending_telegram"}})
    if update_result.matched_count == 0:
        return jsonify({"error": "Batch record not found."}), 404

    logging.info(f"{log_prefix} Batch finalized. Submitting background transfer task.")
    telegram_transfer_executor.submit(run_gdrive_to_telegram_transfer, batch_id)

    frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
    download_url = f"{frontend_base_url}/batch-view/{batch_id}"

    return jsonify({
        "message": "Batch finalized and transfer to secure storage has begun.",
        "access_id": batch_id, "download_url": download_url
    }), 202

def run_gdrive_to_telegram_transfer(access_id: str):
    """Background task to download files from GDrive and upload them to Telegram."""
    log_prefix = f"[BG-TG-{access_id}]"
    logging.info(f"{log_prefix} Background GDrive-to-Telegram process started.")
    
    db_record, db_error = find_metadata_by_access_id(access_id)
    if db_error or not db_record:
        logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
        return

    try:
        if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
            return

        db_record["status_overall"] = "telegram_processing_background"
        save_file_metadata(db_record)

        files_to_process = db_record.get("files_in_batch", [])
        processed_files_for_db = []
        batch_tg_success = True
        
        with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'TgSend_{access_id[:4]}') as tg_send_executor:
            for file_detail in files_to_process:
                original_filename = file_detail.get("original_filename")
                gdrive_file_id = file_detail.get("gdrive_file_id")
                updated_file_meta = file_detail.copy()
                local_temp_file_for_tg: Optional[str] = None
                
                try:
                    if not original_filename or not gdrive_file_id:
                        raise ValueError("File detail missing data.")

                    current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
                    
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=os.path.splitext(original_filename)[1]) as temp_f:
                        local_temp_file_for_tg = temp_f.name
                    
                    download_success, dl_err = download_from_gdrive_to_file(gdrive_file_id, local_temp_file_for_tg)
                    if not download_success: raise Exception(f"GDrive download failed: {dl_err}")

                    current_file_size = os.path.getsize(local_temp_file_for_tg)
                    
                    if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                        logging.info(f"{current_file_log_prefix} File is large, proceeding with chunked upload.")
                        updated_file_meta['is_split_for_telegram'] = True
                        updated_file_meta['telegram_chunks'] = []
                        total_chunked_size = 0
                        chunk_num = 1
                        all_chunks_successful_to_primary = True

                        with open(local_temp_file_for_tg, 'rb') as f_handle:
                            while True:
                                chunk_data = f_handle.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                                if not chunk_data:
                                    break
                                
                                chunk_filename = f"{original_filename}.{str(chunk_num).zfill(3)}"
                                chunk_size = len(chunk_data)
                                total_chunked_size += chunk_size
                                logging.info(f"{current_file_log_prefix} Sending chunk {chunk_num} ({format_bytes(chunk_size)})")

                                # Submit tasks for THIS chunk to all chats and wait for them to complete
                                chunk_send_futures = {
                                    tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, chunk_num): str(chat_id)
                                    for chat_id in TELEGRAM_CHAT_IDS
                                }
                                
                                chunk_send_results = []
                                for future in as_completed(chunk_send_futures):
                                    chat_id_res = chunk_send_futures[future]
                                    _, api_result = future.result()
                                    chunk_send_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                                
                                parsed_chunk_locations = _parse_send_results(f"{current_file_log_prefix} C{chunk_num}", chunk_send_results)
                                
                                primary_chat_success_for_chunk = any(
                                    res.get("success") for res in parsed_chunk_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)
                                )

                                if not primary_chat_success_for_chunk:
                                    all_chunks_successful_to_primary = False
                                    logging.error(f"{current_file_log_prefix} Failed to upload chunk {chunk_num} to primary chat ID. Aborting sends for this file.")
                                    break

                                updated_file_meta['telegram_chunks'].append({
                                    "part_number": chunk_num, "chunk_size": chunk_size,
                                    "chunk_filename": chunk_filename, "send_locations": parsed_chunk_locations
                                })
                                chunk_num += 1
                        
                        if all_chunks_successful_to_primary:
                            updated_file_meta["telegram_send_status"] = "success_chunked"
                            updated_file_meta["telegram_total_chunked_size"] = total_chunked_size
                        else:
                            raise Exception(f"Chunked upload failed at chunk {chunk_num}. File transfer aborted.")
                    else:
                        single_futures = {tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id) for chat_id in TELEGRAM_CHAT_IDS}
                        single_results = []
                        for future in as_completed(single_futures):
                             _, api_result = future.result()
                             single_results.append({"chat_id": single_futures[future], "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                        
                        parsed_locations = _parse_send_results(current_file_log_prefix, single_results)
                        if any(res.get("success") for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)):
                            updated_file_meta["telegram_send_status"] = "success_single"
                            updated_file_meta["telegram_send_locations"] = parsed_locations
                        else:
                            raise Exception("Primary Telegram upload failed.")

                    if updated_file_meta.get("telegram_send_status", "").startswith("success"):
                        logging.info(f"{current_file_log_prefix} Sent to Telegram. Deleting from GDrive.")
                        delete_from_gdrive(gdrive_file_id)
                    else:
                        batch_tg_success = False
                        logging.warning(f"{current_file_log_prefix} Failed Telegram send.")

                except Exception as e:
                    logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e}", exc_info=True)
                    updated_file_meta["telegram_send_status"] = "error_processing_bg"
                    updated_file_meta["reason_telegram"] = str(e)
                    batch_tg_success = False
                finally:
                    _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
                
                processed_files_for_db.append(updated_file_meta)

        db_record["files_in_batch"] = processed_files_for_db
        db_record["storage_location"] = "telegram" if batch_tg_success else "mixed_gdrive_telegram_error"
        db_record["status_overall"] = "telegram_complete" if batch_tg_success else "telegram_processing_errors"
        save_file_metadata(db_record)
        logging.info(f"{log_prefix} Final DB record updated.")

    except Exception as e_bg:
        logging.error(f"{log_prefix} Unhandled exception in background transfer: {e_bg}", exc_info=True)
        if db_record: 
            db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
            db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
            save_file_metadata(db_record)
    finally:
        logging.info(f"{log_prefix} Background transfer finished.")
        
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
            
            # This route uses a different executor name, which is fine, but for consistency `telegram_transfer_executor` could be used.
            # Assuming `background_executor` exists and is appropriate.
            # telegram_transfer_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
            
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