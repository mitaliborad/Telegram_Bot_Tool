# download_routes.py
import logging
import os
import uuid
import time
import json
import zipfile
import tempfile
import shutil
import io
import threading
from typing import Dict, Any, Tuple, Optional, List, Generator
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from extensions import download_prep_data
from flask import (
    Blueprint, request, make_response, jsonify, Response, stream_with_context, send_file, url_for
)
# No JWT needed for these download routes if they are public or use access_id

import database
from database import find_metadata_by_username, find_metadata_by_access_id
from config import (
    PRIMARY_TELEGRAM_CHAT_ID, UPLOADS_TEMP_DIR, MAX_DOWNLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes # Used by _download_chunk_task if logging its size
)
from telegram_api import download_telegram_file_content
from routes.utils import (
    _yield_sse_event, _find_best_telegram_file_id, _find_filename_in_zip, 
    _calculate_download_fetch_progress, _safe_remove_file, _safe_remove_directory, _calculate_progress
)

STREAMING_CHUNK_SIZE_TO_CLIENT = 1 * 1024 * 1024
# Type Aliases
SseEvent = str
ChunkDataResult = Tuple[int, Optional[bytes], Optional[str]] # part_num, content_bytes, error_message

download_bp = Blueprint('download', __name__)

def _schedule_cleanup(temp_id: str, path: Optional[str]):
    """Safely cleans up temporary download file and state data."""
    log_prefix = f"Cleanup-{temp_id}"
    if path: _safe_remove_file(path, log_prefix, "final dl file for scheduled cleanup") 
    if temp_id in download_prep_data:
        try: del download_prep_data[temp_id]; logging.info(f"[{log_prefix}] Prep data removed.")
        except KeyError: pass


def _download_chunk_task(file_id: str, part_num: int, prep_id: str) -> ChunkDataResult:
    logging.info(f"[{prep_id}] T> Starting download chunk {part_num} (id: {file_id})")
    try:
        content, err_msg = download_telegram_file_content(file_id)
        if err_msg: return part_num, None, err_msg
        elif not content: return part_num, None, "Empty chunk content."
        else: logging.info(f"[{prep_id}] T> OK dl chunk {part_num} ({format_bytes(len(content))})."); return part_num, content, None
    except Exception as e: return part_num, None, f"Thread error: {e}"

@download_bp.route('/prepare-download/<username>/<path:filename>') 
def prepare_download_stream(username: str, filename: str) -> Response:
    prep_id = str(uuid.uuid4())
    download_prep_data[prep_id] = {
        "prep_id": prep_id, "status": "initiated", "username": username,
        "requested_filename": filename, "original_filename": filename,
        "access_id": None, "error": None, "final_temp_file_path": None,
        "final_file_size": 0, "start_time": time.time()
    }
    return Response(stream_with_context(_prepare_download_and_generate_updates(prep_id)), mimetype='text/event-stream')

@download_bp.route('/stream-download/<access_id>')
def stream_download_by_access_id(access_id: str) -> Response:
    prep_id = str(uuid.uuid4()) 
    file_info, error_msg = find_metadata_by_access_id(access_id)
    
    if error_msg or not file_info : # Ensure file_info exists
        logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' or DB error for SSE stream. Error: {error_msg}")
        def error_stream(): yield _yield_sse_event('error', {'message': error_msg or 'Invalid or expired download link.'})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream')
    
    username_from_record = file_info.get('username')
    if not username_from_record:
        logging.error(f"[{prep_id}] Record for access_id '{access_id}' is missing username. Cannot proceed.")
        def error_stream(): yield _yield_sse_event('error', {'message': 'File record is incomplete (missing user info).'})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream')

    download_prep_data[prep_id] = {
         "prep_id": prep_id, "status": "initiated", "username": username_from_record, 
         "requested_filename": file_info.get('original_filename', file_info.get('batch_display_name', 'unknown')), 
         "original_filename": file_info.get('original_filename', file_info.get('batch_display_name', 'unknown')), 
         "access_id": access_id, "error": None, "final_temp_file_path": None,
         "final_file_size": 0, "start_time": time.time()
    }
    return Response(stream_with_context(_prepare_download_and_generate_updates(prep_id)), mimetype='text/event-stream')

def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLPrep-{prep_id}]"
    prep_data = download_prep_data.get(prep_id)
    if not prep_data:
        yield _yield_sse_event('error', {'message': 'Internal Server Error: Prep data lost.'})
        return
    prep_data['status'] = 'preparing'

    try:
        is_split_final: bool = prep_data.get('is_split', False)
        chunks_meta_final: Optional[List[Dict[str, Any]]] = prep_data.get('chunks_meta')
        telegram_file_id_final: Optional[str] = prep_data.get('telegram_file_id')
        original_filename_final: str = prep_data.get('original_filename', "download")
        is_compressed_final: bool = prep_data.get('is_compressed', False) 
        final_expected_size_final: int = prep_data.get('final_expected_size', 0)
        compressed_total_size_final: int = prep_data.get('compressed_total_size', 0)

        temp_reassembled_file_path: Optional[str] = None
        temp_final_file_path: Optional[str] = None
        download_executor: Optional[ThreadPoolExecutor] = None

        try:
            is_item_from_batch = prep_data.get("is_item_from_batch", False)
            needs_db_lookup = not is_item_from_batch
            
            if needs_db_lookup and not prep_data.get('telegram_file_id_is_direct_source'):
                db_access_id = prep_data.get('access_id')
                db_username = prep_data.get('username') 
                db_requested_filename = prep_data.get('requested_filename')
                fetched_file_info: Optional[Dict[str, Any]] = None; lookup_error_msg = ""

                if db_access_id: fetched_file_info, lookup_error_msg = find_metadata_by_access_id(db_access_id)
                elif db_username and db_requested_filename:
                    all_user_files, lookup_error_msg = find_metadata_by_username(db_username)
                    if not lookup_error_msg and all_user_files:
                        fetched_file_info = next((f for f in all_user_files if f.get('original_filename') == db_requested_filename or f.get('batch_display_name') == db_requested_filename), None)
                if lookup_error_msg or not fetched_file_info: raise FileNotFoundError(lookup_error_msg or "File metadata not found.")

                original_filename_final = fetched_file_info.get('original_filename', fetched_file_info.get('batch_display_name', db_requested_filename or 'unknown'))
                final_expected_size_final = fetched_file_info.get('original_size', 0)
                is_split_final = fetched_file_info.get('is_split', False)
                is_compressed_final = fetched_file_info.get('is_compressed', False) 
                compressed_total_size_final = fetched_file_info.get('compressed_total_size', 0)

                if is_split_final:
                    chunks_meta_final = fetched_file_info.get('chunks')
                    if not chunks_meta_final: raise RuntimeError(f"DB record split but no chunk data.")
                    telegram_file_id_final = None 
                else: 
                    locations = fetched_file_info.get('send_locations', [])
                    tg_id, _ = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
                    if not tg_id: raise ValueError(f"No TG file ID in DB for non-split file.")
                    telegram_file_id_final = tg_id; chunks_meta_final = None
            
            if prep_data.get('telegram_file_id_is_direct_source') and not is_item_from_batch and not needs_db_lookup:
                is_compressed_final = original_filename_final.lower().endswith('.zip')


            yield _yield_sse_event('filename', {'filename': original_filename_final})
            yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size_final}) 
            yield _yield_sse_event('status', {'message': 'Preparing file...'})

            if is_split_final:
                if not chunks_meta_final: raise RuntimeError(f"File is split but no chunk metadata.")
                chunks_meta_final.sort(key=lambda c: int(c.get('part_number', 0)))
                num_chunks = len(chunks_meta_final)
                total_bytes_to_fetch = compressed_total_size_final or sum(c.get('size',0) for c in chunks_meta_final)
                
                start_fetch_time = time.time(); fetched_bytes_count = 0; downloaded_chunk_count = 0
                download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
                submitted_futures: List[Future] = []
                downloaded_content_map: Dict[int, bytes] = {}
                first_download_error: Optional[str] = None
                file_too_big_errors_count = 0

                for i, chunk_info in enumerate(chunks_meta_final):
                    part_num = chunk_info.get("part_number")
                    chunk_send_locations = chunk_info.get("send_locations", [])
                    if not chunk_send_locations: logging.warning(f"{log_prefix} Chunk {part_num} no send_locations, skipping."); continue
                    chunk_tg_file_id, _ = _find_best_telegram_file_id(chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
                    if not chunk_tg_file_id: logging.warning(f"{log_prefix} Chunk {part_num} no TG file_id, skipping."); continue
                    submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_tg_file_id, part_num, prep_id))
                
                yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})
                for future in as_completed(submitted_futures):
                    try:
                        pnum_result, content_result, err_result = future.result()
                        if err_result:
                            if "file is too big" in err_result.lower(): file_too_big_errors_count += 1
                            if not first_download_error: first_download_error = f"Chunk {pnum_result}: {err_result}"
                        elif content_result:
                            downloaded_chunk_count += 1; fetched_bytes_count += len(content_result)
                            downloaded_content_map[pnum_result] = content_result
                            overall_perc = (downloaded_chunk_count / num_chunks) * 80.0 
                            yield _yield_sse_event('progress', _calculate_download_fetch_progress(start_fetch_time, fetched_bytes_count, total_bytes_to_fetch, downloaded_chunk_count, num_chunks, overall_perc, final_expected_size_final))
                        else: 
                            if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error (no content/error)."
                    except Exception as e:
                        if not first_download_error: first_download_error = f"Processing future for chunk: {str(e)}"

                if first_download_error:
                    error_to_raise = f"Download failed: {first_download_error}"
                    if file_too_big_errors_count > 0: error_to_raise = "Download failed: One or more file parts were too large."
                    raise ValueError(error_to_raise)
                if downloaded_chunk_count != num_chunks: raise SystemError(f"Chunk count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")
                
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                    temp_reassembled_file_path = tf_reassemble.name
                    logging.debug(f"{log_prefix} Created reassembly temp file: {temp_reassembled_file_path}")
                    for pnum_write in range(1, num_chunks + 1):
                        chunk_content_to_write = downloaded_content_map.get(pnum_write)
                        if chunk_content_to_write is None:
                        # This case should ideally be caught earlier if a chunk failed,
                        # but as a safeguard during reassembly:
                            logging.error(f"{log_prefix} CRITICAL: Chunk {pnum_write} missing from map during reassembly!")
                            raise SystemError(f"Reassembly error: Chunk {pnum_write} content missing from downloaded map.")
                        tf_reassemble.write(chunk_content_to_write)
                downloaded_content_map.clear() 
                
                if os.path.exists(temp_reassembled_file_path):
                    reassembled_size = os.path.getsize(temp_reassembled_file_path)
                    logging.info(f"{log_prefix} Reassembly complete. Reassembled file: {temp_reassembled_file_path}, Size: {format_bytes(reassembled_size)}")
                # You might compare reassembled_size with total_bytes_to_fetch or similar expected value
                else:
                    logging.error(f"{log_prefix} CRITICAL: Reassembled temp file {temp_reassembled_file_path} does not exist after writing!")
                    raise RuntimeError("Reassembled file disappeared after creation.")
                logging.info(f"{log_prefix} Reassembly complete. Reassembled file: {temp_reassembled_file_path}, Size: {format_bytes(os.path.getsize(temp_reassembled_file_path)) if os.path.exists(temp_reassembled_file_path) else 'N/A'}")
                    
                temp_final_file_path = temp_reassembled_file_path
                temp_reassembled_file_path = None 

                if is_compressed_final:
                    yield _yield_sse_event('status', {'message': 'Decompressing reassembled ZIP...'})
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_extracted_{prep_id}_") as tf_extracted:
                        extracted_path = tf_extracted.name
                    zf_reassembled = None
                    try:
                        # This logic for split ZIPs assumes that if a file was split and its original_filename_final indicates it was a zip,
                        # the reassembled file (temp_final_file_path) IS the zip to be served.
                        # No further extraction for split ZIPs is done here; the reassembled ZIP is considered the final file.
                        logging.info(f"{log_prefix} Reassembled file is a ZIP: {temp_final_file_path}. No further extraction for split ZIPs at this stage.")
                    finally:
                        if zf_reassembled: zf_reassembled.close()
                    yield _yield_sse_event('progress', {'percentage': 95})

            else: 
            
                if not telegram_file_id_final: raise ValueError("Non-split file but no TG file ID.")
                yield _yield_sse_event('status', {'message': 'Downloading...'})
                content_bytes, err_msg = download_telegram_file_content(telegram_file_id_final)
                if err_msg: raise ValueError(f"TG download failed: {err_msg}")
                if not content_bytes: raise ValueError("TG download returned empty content.")
                logging.info(f"{log_prefix} Non-split file downloaded. Content length: {len(content_bytes) if content_bytes else 0}")
                
                if is_compressed_final and not original_filename_final.lower().endswith('.zip'):
                    logging.info(f"{log_prefix} Decompressing non-split file: {original_filename_final}")
                    yield _yield_sse_event('status', {'message': 'Decompressing...'})
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                        temp_final_file_path = tf.name
                    zf_single = None
                    try:
                        zip_buffer = io.BytesIO(content_bytes)
                        zf_single = zipfile.ZipFile(zip_buffer, 'r')
                        inner_filename = _find_filename_in_zip(zf_single, original_filename_final, log_prefix)
                        with zf_single.open(inner_filename, 'r') as inner_fs, open(temp_final_file_path, 'wb') as tf_out:
                            shutil.copyfileobj(inner_fs, tf_out)  
                    finally:
                        if zf_single: zf_single.close()
                    logging.info(f"{log_prefix} Decompression complete for non-split file. Extracted to: {temp_final_file_path}")
                else: 
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                        temp_final_file_path = tf.name
                        tf.write(content_bytes)
                yield _yield_sse_event('progress', {'percentage': 95})
            logging.info(f"{log_prefix} Preparing to finalize. Current temp_final_file_path: {temp_final_file_path}")
            
            if not temp_final_file_path or not os.path.exists(temp_final_file_path):
                logging.error(f"{log_prefix} CRITICAL FAILURE: Final temp file path is invalid or file does not exist. Path: '{temp_final_file_path}'")
                raise RuntimeError(f"Failed to produce final temp file path. Check logs for path details.")
            
            final_actual_size = os.path.getsize(temp_final_file_path)
            logging.info(f"{log_prefix} Final file ready. Path: {temp_final_file_path}, Actual Size: {format_bytes(final_actual_size)}")
            
            prep_data['final_temp_file_path'] = temp_final_file_path
            prep_data['final_file_size'] = final_actual_size
            prep_data['status'] = 'ready'

            yield _yield_sse_event('progress', {'percentage': 100});
            yield _yield_sse_event('status', {'message': 'File ready!'});
            yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_final})
            logging.info(f"{log_prefix} All 'ready' SSE events sent.")

        except Exception as e:
            error_message = f"Download prep failed: {str(e) or type(e).__name__}"
            logging.error(f"{log_prefix} {error_message}", exc_info=True) # Add exc_info for more details
            yield _yield_sse_event('error', {'message': error_message})
            if prep_id in download_prep_data: # Check existence before modifying
                download_prep_data[prep_id]['status'] = 'error'; download_prep_data[prep_id]['error'] = error_message
    finally:
        if download_executor: download_executor.shutdown(wait=False)
        if temp_reassembled_file_path and os.path.exists(temp_reassembled_file_path): 
            _safe_remove_file(temp_reassembled_file_path, log_prefix, "intermediate reassembled file in finally")
        # Ensure prep_data is accessed safely if it might have been deleted or is None
        current_status = prep_data.get('status', 'unknown') if prep_data else 'unknown (prep_data missing)'
        logging.info(f"{log_prefix} Generator ended. Status: {current_status}")
        
def generate_stream_with_cleanup(path: str, temp_id_for_cleanup: str):
    """Generator to stream a file and ensure cleanup afterwards."""
    log_prefix_stream = f"StreamServe-{temp_id_for_cleanup}"
    
    # Retrieve original filename for logging, if prep_data still exists
    prep_data_entry = download_prep_data.get(temp_id_for_cleanup)
    original_fn = prep_data_entry.get('original_filename', 'unknown_file') if prep_data_entry else 'unknown_file (prep_data gone)'
    
    logging.info(f"[{log_prefix_stream}] Starting to stream file '{original_fn}' from path '{path}'.")
    try:
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(STREAMING_CHUNK_SIZE_TO_CLIENT) # Use defined chunk size
                if not chunk:
                    logging.info(f"[{log_prefix_stream}] Finished streaming file '{original_fn}'.")
                    break
                yield chunk
    except Exception as e:
        logging.error(f"[{log_prefix_stream}] Error during file streaming for '{original_fn}': {e}", exc_info=True)
        # Re-raise to ensure Flask handles the error and closes the connection.
        # The finally block will still be executed.
        raise
    finally:
        # This block executes regardless of how the stream ends (success, client disconnect, error).
        logging.info(f"[{log_prefix_stream}] Stream for '{original_fn}' ended or errored. Initiating cleanup for temp_id {temp_id_for_cleanup}.")
        _schedule_cleanup(temp_id_for_cleanup, path)

@download_bp.route('/serve-temp-file/<temp_id>/<path:filename>')
def serve_temp_file(temp_id: str, filename: str) -> Response:
    log_prefix_serve = f"[ServeTemp-{temp_id}]"
    prep_info = download_prep_data.get(temp_id)

    if not prep_info:
        logging.warning(f"{log_prefix_serve} No prep_info found. Invalid or expired link.")
        # _schedule_cleanup(temp_id, None) # Clean up prep_data if somehow it exists but file info is stale
        return make_response("Error: Invalid or expired download link.", 404)

    if prep_info.get('status') != 'ready':
        err_msg = prep_info.get('error', f"File not ready (Status: {prep_info.get('status')})")
        logging.warning(f"{log_prefix_serve} File not ready. Status: {prep_info.get('status')}, Error: {prep_info.get('error')}")
        # If status is 'error', the file might not exist or be corrupted.
        # _schedule_cleanup will be called eventually by _prepare_download_and_generate_updates's finally or if an error occurred there.
        # If it's an error state, it's good to ensure cleanup is triggered if not already.
        if prep_info.get('status') == 'error':
            _schedule_cleanup(temp_id, prep_info.get('final_temp_file_path'))
        return make_response(f"Error: {err_msg}", 400)

    temp_path = prep_info.get('final_temp_file_path')
    dl_name = prep_info.get('original_filename', filename)

    if not temp_path or not os.path.exists(temp_path):
        logging.error(f"{log_prefix_serve} Prepared file path '{temp_path}' missing or does not exist for serving.")
        # Path is gone, ensure prep_data is also cleaned up.
        _schedule_cleanup(temp_id, temp_path) 
        return make_response("Error: Prepared file data missing or already cleaned up.", 500)
    
    # The old timer logic is removed here. Cleanup is handled by generate_stream_with_cleanup.
    
    response = Response(
        stream_with_context(generate_stream_with_cleanup(temp_path, temp_id)),
        mimetype='application/octet-stream'
    )
    try:
        # Ensure filename is properly encoded for Content-Disposition header
        # Using 'latin-1' with 'ignore' is a common way to handle non-ASCII characters,
        # but modern browsers also support UTF-8 with `filename*=UTF-8''...`
        # For simplicity, keeping the existing encoding method.
        enc_name = dl_name.encode('utf-8').decode('latin-1', 'ignore')
    except Exception:
        logging.warning(f"{log_prefix_serve} Could not encode original filename '{dl_name}'. Using fallback.")
        enc_name = f"download_{temp_id}.dat" # Fallback filename

    response.headers.set('Content-Disposition', 'attachment', filename=enc_name)
    
    final_size = prep_info.get('final_file_size')
    if final_size is not None: # Ensure final_size is not None
        response.headers.set('Content-Length', str(final_size))
    else:
        logging.warning(f"{log_prefix_serve} final_file_size not found in prep_info for '{dl_name}'. Content-Length will not be set.")
        
    return response

@download_bp.route('/download-single/<access_id>/<path:filename>')
def download_single_file(access_id: str, filename: str):
    prep_id = str(uuid.uuid4())
    log_prefix = f"[SingleDLPrep-{prep_id}]"
    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg or not batch_info or not batch_info.get('is_batch'):
        err_user = error_msg or f"Batch '{access_id}' not found or invalid."
        def err_s(): yield _yield_sse_event('error', {'message': err_user})
        return Response(stream_with_context(err_s()), mimetype='text/event-stream', status=404)

    target_file_info = next((f for f in batch_info.get('files_in_batch', []) if f.get('original_filename') == filename and not f.get('skipped') and not f.get('failed')), None)
    if not target_file_info:
        def err_s(): yield _yield_sse_event('error', {'message': f"File '{filename}' not found/unavailable."})
        return Response(stream_with_context(err_s()), mimetype='text/event-stream', status=404)

    prep_is_split = target_file_info.get('is_split', False)
    prep_chunks_meta = target_file_info.get('chunks') if prep_is_split else None
    prep_telegram_file_id = None
    if not prep_is_split:
        locations = target_file_info.get('send_locations', [])
        prep_telegram_file_id, _ = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
        if not prep_telegram_file_id:
            def err_s(): yield _yield_sse_event('error', {'message': f"No source for '{filename}'."})
            return Response(stream_with_context(err_s()), mimetype='text/event-stream', status=500)

    download_prep_data[prep_id] = {
        "prep_id": prep_id, "status": "initiated", "access_id": access_id, 
        "username": batch_info.get('username'), "requested_filename": filename, "original_filename": filename, 
        "telegram_file_id": prep_telegram_file_id, "is_split": prep_is_split, 
        "chunks_meta": prep_chunks_meta, 
        "is_compressed": target_file_info.get('is_compressed', False), # Get from target_file_info
        "final_expected_size": target_file_info.get('original_size', 0),
        "compressed_total_size": target_file_info.get('compressed_total_size', 0),
        "is_item_from_batch": True, # Mark that info is pre-filled
        "error": None, "final_temp_file_path": None, "final_file_size": 0, "start_time": time.time()
    }
    logging.debug(f"{log_prefix} Prep data for single file from batch: {json.dumps(download_prep_data[prep_id], default=str)}")
    return Response(stream_with_context(_prepare_download_and_generate_updates(prep_id)), mimetype='text/event-stream')


@download_bp.route('/initiate-download-all/<access_id>')
def initiate_download_all(access_id: str):
    prep_id_for_zip = str(uuid.uuid4())
    batch_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not batch_info or not batch_info.get('is_batch'):
        return jsonify({"error": error_msg or "Batch not found/invalid.", "prep_id": None}), 404

    files_to_zip_meta: list[dict] = []
    total_expected_zip_content_size: int = 0
    for file_item in batch_info.get("files_in_batch", []):
        if file_item.get("skipped") or file_item.get("failed"): continue
        original_filename = file_item.get("original_filename")
        original_size = file_item.get("original_size", 0)
        tg_file_id = None
        meta_entry = {
            "original_filename": original_filename, "original_size": original_size,
            "is_split": file_item.get("is_split", False),
            "is_compressed": file_item.get("is_compressed", False) # Pass this along
        }
        if meta_entry["is_split"]:
            chunks = file_item.get("chunks", [])
            if chunks and chunks[0].get("send_locations"):
                tg_file_id, _ = _find_best_telegram_file_id(chunks[0]["send_locations"], PRIMARY_TELEGRAM_CHAT_ID)
            meta_entry["chunks_meta"] = chunks # Pass all chunk info
        else:
            tg_file_id, _ = _find_best_telegram_file_id(file_item.get("send_locations", []), PRIMARY_TELEGRAM_CHAT_ID)
        
        if original_filename and (tg_file_id or meta_entry["is_split"]): # Need tg_file_id for non-split, or just is_split for split
            meta_entry["telegram_file_id"] = tg_file_id # Will be None for split files here, filled by _generate_zip
            files_to_zip_meta.append(meta_entry)
            total_expected_zip_content_size += original_size
            
    if not files_to_zip_meta:
        return jsonify({"error": "No files available to zip.", "prep_id": None}), 404

    zip_name = batch_info.get('batch_display_name', f"download_all_{access_id}.zip")
    if not zip_name.lower().endswith(".zip"): zip_name += ".zip"

    download_prep_data[prep_id_for_zip] = {
        "prep_id": prep_id_for_zip, "status": "initiated_zip_all", 
        "access_id_original_batch": access_id, "username": batch_info.get('username'),
        "batch_display_name": zip_name, "original_filename": zip_name, 
        "files_to_zip_meta": files_to_zip_meta, 
        "total_expected_content_size": total_expected_zip_content_size, 
        "start_time": time.time()
    }
    return jsonify({
        "message": "Download All initiated.", "prep_id_for_zip": prep_id_for_zip, 
        "sse_stream_url": url_for('download.stream_download_all', prep_id_for_zip=prep_id_for_zip) 
    }), 200

@download_bp.route('/stream-download-all/<prep_id_for_zip>')
def stream_download_all(prep_id_for_zip: str):
    prep_entry = download_prep_data.get(prep_id_for_zip)
    if not prep_entry or prep_entry.get("status") != "initiated_zip_all":
        def error_stream(): yield _yield_sse_event('error', {'message': 'Invalid/expired download all session.'})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=400)
    return Response(stream_with_context(_generate_zip_and_stream_progress(prep_id_for_zip)), mimetype='text/event-stream')

def _generate_zip_and_stream_progress(prep_id_for_zip: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLAll-ZipGen-{prep_id_for_zip}]"
    prep_data = download_prep_data.get(prep_id_for_zip)
    if not prep_data: 
        yield _yield_sse_event('error', {'message': 'Internal error: Prep data lost.'})
        return
    prep_data['status'] = 'zipping_all_fetching' 

    files_to_process_meta = prep_data.get('files_to_zip_meta', [])
    batch_display_name_for_zip = prep_data.get('batch_display_name')
    total_expected_content_size = prep_data.get('total_expected_content_size', 0)
    temp_zip_file_path: Optional[str] = None
    download_all_executor: Optional[ThreadPoolExecutor] = None

    try:
        if not files_to_process_meta: raise ValueError("No files specified for zipping.")
        yield _yield_sse_event('status', {'message': f'Starting download of {len(files_to_process_meta)} files...'})
        yield _yield_sse_event('start', {'filename': batch_display_name_for_zip, 'totalSize': total_expected_content_size })
        
        # Create the temp zip file path first
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_all_zip_{prep_id_for_zip}_") as tf_zip:
            temp_zip_file_path = tf_zip.name
        
        bytes_downloaded_for_zip = 0; files_processed_count = 0
        overall_zip_gen_start_time = time.time()
        
        # Use a single ZipFile instance opened in 'w' mode
        with zipfile.ZipFile(temp_zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            download_all_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DLAllZip_{prep_id_for_zip[:4]}')
            future_to_filemeta: Dict[Future, Dict[str, Any]] = {}

            for file_meta_item in files_to_process_meta:
                if file_meta_item.get("is_split"):
                    # For split files, we need to reassemble them first, then add to zip
                    # This requires a nested _prepare_download_and_generate_updates like call or similar logic.
                    # For simplicity in this refactor, we'll download chunk-by-chunk and write to zip.
                    # This is less efficient for large split files but avoids deep nesting.
                    # A more robust solution would reassemble to a temp file, then add.
                    logging.info(f"{log_prefix} Processing split file for zip: {file_meta_item['original_filename']}")
                    
                    # Create a temporary file to reassemble the split file
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"reass_for_zip_{uuid.uuid4().hex[:6]}_") as temp_reass_file:
                        current_reassembled_path = temp_reass_file.name
                    
                    split_chunks_meta = file_meta_item.get("chunks_meta", [])
                    split_chunks_meta.sort(key=lambda c: int(c.get('part_number',0)))
                    
                    # Download and write chunks for this split file
                    for chunk_meta in split_chunks_meta:
                        part_num = chunk_meta.get("part_number")
                        chunk_locs = chunk_meta.get("send_locations", [])
                        chunk_tg_id, _ = _find_best_telegram_file_id(chunk_locs, PRIMARY_TELEGRAM_CHAT_ID)
                        if chunk_tg_id:
                            _, chunk_content, dl_err = _download_chunk_task(chunk_tg_id, part_num, prep_id_for_zip + f"-{file_meta_item['original_filename'][:5]}")
                            if dl_err or not chunk_content:
                                raise ValueError(f"Failed to download chunk {part_num} for {file_meta_item['original_filename']}: {dl_err}")
                            with open(current_reassembled_path, "ab") as f_reass: # Append binary
                                f_reass.write(chunk_content)
                            bytes_downloaded_for_zip += len(chunk_content) # Count towards progress
                    
                    # Add the reassembled file to the zip
                    zf.write(current_reassembled_path, arcname=file_meta_item["original_filename"])
                    _safe_remove_file(current_reassembled_path, log_prefix, f"temp reassembled {file_meta_item['original_filename']}")
                    files_processed_count += 1
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_for_zip, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Processed (split) {files_processed_count}/{len(files_to_process_meta)} files... ({file_meta_item["original_filename"]})'})

                else: # Non-split file
                    tg_id_non_split = file_meta_item.get("telegram_file_id")
                    if tg_id_non_split:
                        fut = download_all_executor.submit(_download_chunk_task, tg_id_non_split, 0, prep_id_for_zip) # part_num 0 for non-chunked context
                        future_to_filemeta[fut] = file_meta_item
            
            # Process non-split files submitted to executor
            for future in as_completed(future_to_filemeta):
                completed_file_meta = future_to_filemeta[future]
                original_filename_for_zip = completed_file_meta["original_filename"]
                try:
                    _, content, error_msg = future.result() 
                    if error_msg or not content:
                        raise ValueError(f"Failed to download '{original_filename_for_zip}': {error_msg or 'Empty content'}")
                    
                    # If the non-split file was marked as compressed by uploader script (and not originally a .zip)
                    if completed_file_meta.get("is_compressed") and not original_filename_for_zip.lower().endswith(".zip"):
                        logging.info(f"{log_prefix} Decompressing '{original_filename_for_zip}' before adding to master zip.")
                        zip_buffer_inner = io.BytesIO(content)
                        with zipfile.ZipFile(zip_buffer_inner, 'r') as zf_inner:
                            # Find the actual content within this intermediate zip
                            inner_content_name = _find_filename_in_zip(zf_inner, original_filename_for_zip, log_prefix + f"-innerzip-{original_filename_for_zip[:5]}")
                            with zf_inner.open(inner_content_name) as actual_content_stream:
                                zf.writestr(original_filename_for_zip, actual_content_stream.read())
                    else: # Add directly
                        zf.writestr(original_filename_for_zip, content)

                    bytes_downloaded_for_zip += len(content) 
                    files_processed_count += 1
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_for_zip, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Processed {files_processed_count}/{len(files_to_process_meta)} files... ({original_filename_for_zip})'})
                except Exception as exc: raise ValueError(f"Error processing download for '{original_filename_for_zip}': {exc}")

        final_zip_actual_size = os.path.getsize(temp_zip_file_path)
        prep_data['status'] = 'ready'; prep_data['final_temp_file_path'] = temp_zip_file_path 
        prep_data['final_file_size'] = final_zip_actual_size
        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': total_expected_content_size, 'totalBytes': total_expected_content_size, 'etaFormatted': '00:00'})
        yield _yield_sse_event('status', {'message': 'Archive ready!'})
        yield _yield_sse_event('ready', {'temp_file_id': prep_id_for_zip, 'final_filename': batch_display_name_for_zip })
    except Exception as e:
        error_message = f"Failed to generate 'Download All' zip: {str(e) or type(e).__name__}"
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id_for_zip in download_prep_data:
            download_prep_data[prep_id_for_zip]['status'] = 'error_zipping_all'
            download_prep_data[prep_id_for_zip]['error'] = error_message
    finally:
        if download_all_executor: download_all_executor.shutdown(wait=False)
        if prep_data.get('status') != 'ready' and temp_zip_file_path and os.path.exists(temp_zip_file_path):
            _safe_remove_file(temp_zip_file_path, log_prefix, "partially created download-all zip")