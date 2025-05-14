import logging
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
    Blueprint, request, make_response, jsonify, Response, stream_with_context
)
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId

import database
from database import User, save_file_metadata # Add other db functions if needed
from extensions import upload_progress_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes # format_bytes is used by _send_single_file_task & _send_chunk_task
)
from telegram_api import send_file_to_telegram
from routes.utils import _yield_sse_event, _calculate_progress, _safe_remove_directory
from extensions import upload_progress_data
# Type Aliases
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] # success, message, response_json
SseEvent = str

upload_bp = Blueprint('upload', __name__)

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parses results from Telegram send operations for metadata storage."""
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
                detail["message_id"] = msg_id
                detail["file_id"] = f_id
                detail["file_unique_id"] = f_uid
                if f_size is not None: detail["file_size"] = f_size
            else:
                detail["success"] = False
                detail["error"] = "Missing critical IDs in Telegram response"
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
            logging.info(f"{log_prefix} Sending single file ({format_bytes(file_size)}) from path: {file_path}")
            result = send_file_to_telegram(f_handle, filename, chat_id)
        logging.info(f"{log_prefix} Single file send result: Success={result[0]}")
        return str(chat_id), result
    except FileNotFoundError:
         logging.error(f"{log_prefix} Single file not found at path: {file_path}")
         return str(chat_id), (False, f"File not found: {filename}", None)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error opening/sending single file: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing single file: {e}", None)

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    log_prefix = f"[{upload_id}] Chunk {chunk_num} ('{filename}') to {chat_id}"
    try:
        buffer = io.BytesIO(chunk_data)
        logging.info(f"{log_prefix} Sending chunk ({format_bytes(len(chunk_data))})")
        result = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"{log_prefix} Send chunk result: Success={result[0]}")
        return str(chat_id), result
    except Exception as e: 
        logging.error(f"{log_prefix} Unexpected error sending chunk: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing chunk: {e}", None)


@upload_bp.route('/initiate-upload', methods=['POST'])
@jwt_required(optional=True)
def initiate_upload() -> Response:
    upload_id = str(uuid.uuid4())
    log_prefix = f"[{upload_id}]"
    logging.info(f"{log_prefix} Request to initiate upload.")
    
    current_user_jwt_identity = get_jwt_identity()
    display_username: Optional[str] = None
    user_email: Optional[str] = None
    is_anonymous: bool = False
    anonymous_id: Optional[str] = None
    
    if current_user_jwt_identity:
        is_anonymous = False
        try:
            user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
            if error or not user_doc:
                logging.error(f"{log_prefix} User not found for JWT identity '{current_user_jwt_identity}'. Error: {error}")
                return jsonify({"error": "Invalid user token or user not found"}), 401 
            # Use _ensure_username_in_user_doc if it's available and User class needs it
            # For now, direct access, assuming User class handles missing username or it's guaranteed
            user_object_from_jwt = User(user_doc) # This might fail if username is missing and User requires it
            display_username = user_object_from_jwt.username
            user_email = user_object_from_jwt.email
            logging.info(f"{log_prefix} User identified via JWT: Username='{display_username}'")
        except ValueError as ve: 
            logging.error(f"{log_prefix} Failed to instantiate User for JWT identity '{current_user_jwt_identity}':{ve}")
            return jsonify({"error": "User data inconsistency"}), 500
        except Exception as e: 
            logging.error(f"{log_prefix} Error processing JWT identity '{current_user_jwt_identity}': {e}", exc_info=True)
            return jsonify({"error": "Server error processing authentication"}), 500
    else:
        is_anonymous = True
        anonymous_id = request.form.get('anonymous_upload_id')
        if not anonymous_id:
            return jsonify({"error": "Missing required anonymous identifier for anonymous upload."}), 400
        display_username = f"AnonymousUser-{anonymous_id[:6]}" 
        user_email = None 
        logging.info(f"{log_prefix} Anonymous upload identified by temp ID: {anonymous_id}")
    
    if display_username is None: # Should not happen if logic above is correct
        return jsonify({"error": "Internal server error processing user identity."}), 500
    
    uploaded_files = request.files.getlist('files[]')
    if not uploaded_files or all(not f.filename for f in uploaded_files):
        return jsonify({"error": "No files selected or files are invalid"}), 400

    batch_temp_dir = os.path.join(UPLOADS_TEMP_DIR, f"batch_{upload_id}")
    original_filenames_in_batch = []
    try:
        os.makedirs(batch_temp_dir, exist_ok=True)
        for file_storage_item in uploaded_files:
            if file_storage_item and file_storage_item.filename:
                original_filename = file_storage_item.filename 
                individual_temp_file_path = os.path.join(batch_temp_dir, original_filename)
                file_storage_item.save(individual_temp_file_path)
                original_filenames_in_batch.append(original_filename)
        if not original_filenames_in_batch:
            _safe_remove_directory(batch_temp_dir, log_prefix, "empty batch temp dir")
            return jsonify({"error": "No valid files were processed in the batch."}), 400

        batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]
            
        progress_entry = {
            "status": "initiated", "is_batch": True, "batch_directory_path": batch_temp_dir,
            "original_filenames_in_batch": original_filenames_in_batch, "batch_display_name": batch_display_name, 
            "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous, 
            "error": None, "start_time": time.time()
        }
        if is_anonymous and anonymous_id: progress_entry["anonymous_id"] = anonymous_id
            
        upload_progress_data[upload_id] = progress_entry
        return jsonify({"upload_id": upload_id, "filename": batch_display_name})
    except Exception as e:
        logging.error(f"{log_prefix} Error processing batch upload: {e}", exc_info=True)
        _safe_remove_directory(batch_temp_dir, log_prefix, "failed batch temp dir")
        if upload_id in upload_progress_data: del upload_progress_data[upload_id]
        return jsonify({"error": f"Server error processing batch: {str(e)}"}), 500
    
@upload_bp.route('/stream-progress/<upload_id>')
def stream_progress(upload_id: str) -> Response:
    logging.info(f"SSE connect request for upload_id: {upload_id}")
    status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
    if upload_id not in upload_progress_data or status in ['completed', 'error', 'completed_metadata_error']:
        logging.warning(f"Upload ID '{upload_id}' unknown or finalized (Status:{status}).")
        def stream_gen(): yield _yield_sse_event('error', {'message': f'Upload ID {upload_id} unknown/finalized.'})
        return Response(stream_with_context(stream_gen()), mimetype='text/event-stream')
    return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')


def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
    executor: Optional[ThreadPoolExecutor] = None
    batch_directory_path: Optional[str] = None 
    try:
        log_prefix = f"[{upload_id}]"
        logging.info(f"{log_prefix} Starting processing generator...")
        upload_data = upload_progress_data.get(upload_id)
        if not upload_data:
            yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
            return

        username = upload_data['username']
        batch_directory_path = upload_data.get("batch_directory_path")
        original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
        batch_display_name = upload_data.get("batch_display_name", "Upload")

        if not upload_data.get("is_batch") or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
            yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
            if batch_directory_path and os.path.isdir(batch_directory_path):
                _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
            return

        upload_data['status'] = 'processing_telegram'
        access_id: str = upload_data.get('access_id') or uuid.uuid4().hex[:10]
        upload_data['access_id'] = access_id

        if len(TELEGRAM_CHAT_IDS) > 0:
            executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
        else:
            yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats.'})
            return

        total_original_bytes_in_batch = 0
        files_to_process_details = []
        for filename_in_list in original_filenames_in_batch:
            file_path_in_list = os.path.join(batch_directory_path, filename_in_list)
            if os.path.exists(file_path_in_list):
                try:
                    size = os.path.getsize(file_path_in_list)
                    total_original_bytes_in_batch += size
                    files_to_process_details.append({"path": file_path_in_list, "name": filename_in_list, "size": size})
                except OSError as e: logging.warning(f"{log_prefix} Could not get size for {filename_in_list}, skipping. Error: {e}")
            else: logging.warning(f"{log_prefix} File {filename_in_list} not found in {batch_directory_path}, skipping.")

        if not files_to_process_details:
            yield _yield_sse_event('error', {'message': 'No valid files found to upload in the batch.'})
            _safe_remove_directory(batch_directory_path, log_prefix, "empty batch dir after file check")
            return

        yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
        yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

        overall_start_time = time.time()
        bytes_sent_so_far = 0
        all_files_metadata_for_db = [] 
        batch_overall_success = True

        for file_detail in files_to_process_details:
            current_file_path = file_detail["path"]
            current_filename = file_detail["name"]
            current_file_size = file_detail["size"]
            log_file_prefix_indiv = f"{log_prefix} File '{current_filename}'"

            file_meta_entry: Dict[str, Any] = {
                "original_filename": current_filename, "original_size": current_file_size,
                "is_split": False, "is_compressed": current_filename.lower().endswith('.zip'),
                "skipped": False, "failed": False, "reason": None,
                "send_locations": [], "chunks": []          
            }
            
            if current_file_size == 0:
                file_meta_entry["skipped"] = True; file_meta_entry["reason"] = "File is empty"
                all_files_metadata_for_db.append(file_meta_entry)
                current_batch_progress = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                yield _yield_sse_event('progress', current_batch_progress)
                continue 
            
            try:
                if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES: # CHUNKING
                    file_meta_entry["is_split"] = True
                    part_number = 1; bytes_processed_for_this_file_chunking = 0
                    all_chunks_sent_successfully_for_this_file = True
                    with open(current_file_path, 'rb') as f_in:
                        while True:
                            chunk_data = f_in.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                            if not chunk_data: break 
                            chunk_filename = f"{current_filename}.part{part_number}"
                            log_chunk_prefix = f"{log_file_prefix_indiv} Chunk {part_number}"
                            
                            chunk_specific_futures: Dict[Future, str] = {}
                            chunk_specific_results: Dict[str, ApiResult] = {}
                            primary_send_success_for_this_chunk = False
                            primary_send_message_for_this_chunk = "Primary chunk send failed."

                            if executor:
                                for chat_id_str_loop in TELEGRAM_CHAT_IDS:
                                    fut = executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id_str_loop), upload_id, part_number)
                                    chunk_specific_futures[fut] = str(chat_id_str_loop)
                            else: 
                                _, res_tuple_no_exec_chunk = _send_chunk_task(chunk_data, chunk_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id, part_number)
                                chunk_specific_results[str(TELEGRAM_CHAT_IDS[0])] = res_tuple_no_exec_chunk
                                primary_send_success_for_this_chunk, primary_send_message_for_this_chunk = res_tuple_no_exec_chunk[0], res_tuple_no_exec_chunk[1]
                            
                            if chunk_specific_futures: 
                                primary_chunk_fut = next((f for f, cid in chunk_specific_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                                if primary_chunk_fut:
                                    cid_res_chunk, res_chunk = primary_chunk_fut.result()
                                    chunk_specific_results[cid_res_chunk] = res_chunk
                                    primary_send_success_for_this_chunk, primary_send_message_for_this_chunk = res_chunk[0], res_chunk[1]
                                for fut_completed_chunk in as_completed(chunk_specific_futures):
                                    cid_res_c, res_c = fut_completed_chunk.result()
                                    if cid_res_c not in chunk_specific_results: chunk_specific_results[cid_res_c] = res_c
                            
                            parsed_locations_for_this_chunk = _parse_send_results(log_chunk_prefix, 
                                [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in chunk_specific_results.items()])

                            if primary_send_success_for_this_chunk:
                                file_meta_entry["chunks"].append({"part_number": part_number, "size": len(chunk_data), "send_locations": parsed_locations_for_this_chunk})
                                bytes_sent_so_far += len(chunk_data)
                                bytes_processed_for_this_file_chunking += len(chunk_data)
                                yield _yield_sse_event('progress', _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch))
                                yield _yield_sse_event('status', {'message': f'Uploaded chunk {part_number} for {current_filename}'})
                            else:
                                batch_overall_success = False; all_chunks_sent_successfully_for_this_file = False
                                file_meta_entry["failed"] = True; file_meta_entry["reason"] = f"Failed chunk {part_number}: {primary_send_message_for_this_chunk}"
                                break 
                            part_number += 1
                    if all_chunks_sent_successfully_for_this_file:
                        file_meta_entry["compressed_total_size"] = bytes_processed_for_this_file_chunking
                else: # SINGLE FILE (NON-CHUNKED)
                    single_file_futures: Dict[Future, str] = {}
                    single_file_results: Dict[str, ApiResult] = {}
                    primary_send_success_for_single_file = False
                    primary_send_message_single_file = "Primary send (single) failed."

                    if executor:
                        for chat_id_str_single in TELEGRAM_CHAT_IDS:
                            fut_single = executor.submit(_send_single_file_task, current_file_path, current_filename, str(chat_id_str_single), upload_id)
                            single_file_futures[fut_single] = str(chat_id_str_single)
                    else:
                        _, res_tuple_single_no_exec = _send_single_file_task(current_file_path, current_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id)
                        single_file_results[str(TELEGRAM_CHAT_IDS[0])] = res_tuple_single_no_exec
                        primary_send_success_for_single_file, primary_send_message_single_file = res_tuple_single_no_exec[0], res_tuple_single_no_exec[1]

                    if single_file_futures:
                        primary_fut_single = next((f for f, cid in single_file_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                        if primary_fut_single:
                            cid_res_s, res_s = primary_fut_single.result()
                            single_file_results[cid_res_s] = res_s
                            primary_send_success_for_single_file, primary_send_message_single_file = res_s[0], res_s[1]
                        for fut_completed_s in as_completed(single_file_futures):
                            cid_res_s_comp, res_s_comp = fut_completed_s.result()
                            if cid_res_s_comp not in single_file_results: single_file_results[cid_res_s_comp] = res_s_comp
                    
                    parsed_locations_single_file = _parse_send_results(log_file_prefix_indiv, 
                        [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k,r in single_file_results.items()])

                    if primary_send_success_for_single_file:
                        bytes_sent_so_far += current_file_size
                        file_meta_entry["send_locations"] = parsed_locations_single_file
                    else:
                        batch_overall_success = False; file_meta_entry["failed"] = True
                        file_meta_entry["reason"] = f"Primary send failed (single): {primary_send_message_single_file}"
                        file_meta_entry["send_locations"] = parsed_locations_single_file
            except Exception as file_processing_exception:
                file_meta_entry["failed"] = True; file_meta_entry["reason"] = f"Internal error: {str(file_processing_exception)}"
                batch_overall_success = False
            
            all_files_metadata_for_db.append(file_meta_entry)
            yield _yield_sse_event('progress', _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch))
            yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})
        
        total_batch_duration = time.time() - overall_start_time
        if not all_files_metadata_for_db:
            yield _yield_sse_event('error', {'message': 'Internal server error: Failed to record upload details.'})
            upload_data['status'] = 'error'; upload_data['error'] = "No metadata generated"
            return

        db_batch_record = {
            "access_id": access_id, "username": username,
            "is_anonymous": upload_data.get('is_anonymous', False), "anonymous_id": upload_data.get('anonymous_id'),
            "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "is_batch": True,
            "batch_display_name": batch_display_name, "files_in_batch": all_files_metadata_for_db,
            "total_original_size": total_original_bytes_in_batch,
            "total_upload_duration_seconds": round(total_batch_duration, 2),
        }
        if db_batch_record["anonymous_id"] is None: del db_batch_record["anonymous_id"]
        
        save_success, save_msg = save_file_metadata(db_batch_record)
        if not save_success:
            upload_data['status'] = 'completed_metadata_error'; upload_data['error'] = f"DB save fail: {save_msg}"
            yield _yield_sse_event('error', {'message': f"Upload processed, but failed to save details: {save_msg}"})
            return
        
        browser_url = f"{request.host_url.rstrip('/')}/browse/{access_id}" # Adjust browse route if needed
        complete_message = f'Batch upload ' + ('completed with errors.' if not batch_overall_success else 'complete!')
        complete_payload = {'message': complete_message, 'download_url': browser_url, 'filename': batch_display_name, 'batch_access_id': access_id }
        upload_data['status'] = 'completed_with_errors' if not batch_overall_success else 'completed'
        yield _yield_sse_event('complete', complete_payload)

    except Exception as e:
        error_msg_final = f"Critical upload processing error: {str(e) or type(e).__name__}"
        yield _yield_sse_event('error', {'message': error_msg_final})
        if upload_id in upload_progress_data:
            upload_progress_data[upload_id]['status'] = 'error'
            upload_progress_data[upload_id]['error'] = error_msg_final
    finally:
        if executor: executor.shutdown(wait=True)
        if batch_directory_path and os.path.exists(batch_directory_path):
             _safe_remove_directory(batch_directory_path, log_prefix, "batch temp dir in finally")
        logging.info(f"{log_prefix} Upload processing generator finished. Final Status: {upload_progress_data.get(upload_id, {}).get('status', 'unknown')}")