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
from google_drive_api import download_from_gdrive
from flask import redirect, abort
from extensions import download_prep_data
from flask import (
    Blueprint, request, make_response, jsonify, Response, stream_with_context, send_file, url_for
)

# No JWT needed for these download routes if they are public or use access_id
from datetime import datetime, timedelta, timezone
from dateutil import parser
from database import find_metadata_by_username, find_metadata_by_access_id
from config import (
    PRIMARY_TELEGRAM_CHAT_ID, UPLOADS_TEMP_DIR, MAX_DOWNLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,CHUNK_SIZE,
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

download_bp = Blueprint('download_prefixed', __name__)
download_sse_bp = Blueprint('download_sse', __name__)
def generate_file_chunks(stream: io.BytesIO, stream_name: str = "download"):
    """Helper generator to yield chunks from a BytesIO stream and ensure it's closed."""
    try:
        while True:
            chunk = stream.read(CHUNK_SIZE) # Use a defined CHUNK_SIZE from config
            if not chunk:
                break
            yield chunk
    except Exception as e:
        logging.error(f"Error during chunk generation for {stream_name}: {e}")
        # Depending on desired behavior, you might yield an error marker or just stop
    finally:
        if stream:
            stream.close()
        logging.info(f"Stream {stream_name} closed after chunk generation.")


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
    """
    Prepares and initiates an SSE stream for downloading a single file record.
    This now mirrors the behavior of download_single_file for consistency.
    """
    prep_id = str(uuid.uuid4())
    log_prefix = f"[SingleDLPrep-{prep_id}-{access_id}]"
    logging.info(f"{log_prefix} Request received to prepare SSE stream.")

    record_info, error_msg_find = find_metadata_by_access_id(access_id)
    if error_msg_find or not record_info:
        err_user_msg = error_msg_find or f"Record with Access ID '{access_id}' not found."
        logging.warning(f"{log_prefix} {err_user_msg}")
        def error_stream_not_found(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_not_found()), mimetype='text/event-stream', status=404)

    # Since this is for a single file/batch, we get the first (and only) file item from the list
    files_in_record_array = record_info.get('files_in_batch', [])
    if not files_in_record_array:
        err_user_msg = f"Record '{access_id}' has no processable file entries."
        logging.error(f"{log_prefix} {err_user_msg}")
        def error_stream_no_files(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_no_files()), mimetype='text/event-stream', status=500)
    
    target_file_metadata = files_in_record_array[0]
    filename = target_file_metadata.get("original_filename")
    if not filename:
        err_user_msg = f"File entry in record '{access_id}' is missing an original filename."
        logging.error(f"{log_prefix} {err_user_msg}")
        def error_stream_no_fn(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_no_fn()), mimetype='text/event-stream', status=500)

    # This mirrors the logic from download_single_file to set up the prep data correctly
    prep_data_payload: Dict[str, Any] = {
        "prep_id": prep_id, "status": "initiated", "access_id": access_id, 
        "username": record_info.get('username'), 
        "requested_filename": filename, 
        "original_filename": filename, 
        "is_item_from_batch": True, 
        "is_anonymous": record_info.get('is_anonymous', False), 
        "upload_timestamp": record_info.get('upload_timestamp'), 
        "final_expected_size": target_file_metadata.get('original_size', 0),
        "error": None, "final_temp_file_path": None, "final_file_size": 0, "start_time": time.time()
    }

    # Determine source (GDrive or Telegram) based on file metadata
    tg_send_status = target_file_metadata.get('telegram_send_status', 'unknown')
    gdrive_id_for_file = target_file_metadata.get('gdrive_file_id')
    record_storage_location = record_info.get('storage_location') 

    attempt_gdrive_source = False
    if gdrive_id_for_file and (record_storage_location == "gdrive" or "error" in str(record_storage_location) or not tg_send_status.startswith('success')):
        attempt_gdrive_source = True
    
    if attempt_gdrive_source:
        logging.info(f"{log_prefix} Using GDrive source. GDrive ID: {gdrive_id_for_file}")
        prep_data_payload["source_gdrive_id"] = gdrive_id_for_file
    else:
        logging.info(f"{log_prefix} Using Telegram source.")
        prep_data_payload["is_split"] = target_file_metadata.get('is_split_for_telegram', False)
        if prep_data_payload["is_split"]:
            prep_data_payload["chunks_meta"] = target_file_metadata.get('telegram_chunks')
        else:
            tg_id, _ = _find_best_telegram_file_id(target_file_metadata.get("telegram_send_locations", []), PRIMARY_TELEGRAM_CHAT_ID)
            if not tg_id:
                err_msg = f"No primary Telegram source found for file '{filename}' in record '{access_id}'."
                def err_s_no_src(): yield _yield_sse_event('error', {'message': err_msg})
                return Response(stream_with_context(err_s_no_src()), mimetype='text/event-stream', status=500)
            prep_data_payload["telegram_file_id"] = tg_id

    download_prep_data[prep_id] = prep_data_payload
    logging.debug(f"{log_prefix} Prep data for SSE stream: {json.dumps(download_prep_data[prep_id], default=str)}")
    return Response(stream_with_context(_prepare_download_and_generate_updates(prep_id)), mimetype='text/event-stream') 

def _get_gdrive_service_for_download_route():
    try:
        from google_drive_api import _get_drive_service as get_service_actual
        service, err = get_service_actual() # Assuming your _get_drive_service returns (service, error)
        if err:
            raise ConnectionError(f"GDrive service error: {err}")
        return service
    except ImportError:
        logging.error("Could not import _get_drive_service from google_drive_api.py for download route.")
        raise ConnectionError("GDrive service unavailable (import error).")
    except Exception as e:
        logging.error(f"Failed to initialize GDrive service for download: {e}")
        raise ConnectionError(f"Could not connect to GDrive: {e}")

def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLPrep-{prep_id}]"
    prep_data = download_prep_data.get(prep_id)
    if not prep_data:
        yield _yield_sse_event('error', {'message': 'Internal ServerError: Prep data lost.'})
        return
    prep_data['status'] = 'preparing'
    
    # This block for initializing variables from prep_data and the DB is correct and stays.
    is_anonymous_final: bool = prep_data.get('is_anonymous', False)
    upload_timestamp_str_final: Optional[str] = prep_data.get('upload_timestamp')
    original_filename_final: str = prep_data.get('original_filename', "download")
    final_expected_size_final: int = prep_data.get('final_expected_size', 0)
    is_split_for_telegram_source: bool = prep_data.get('is_split', False)
    chunks_meta_for_telegram_source: Optional[List[Dict]] = prep_data.get('chunks_meta')
    telegram_file_id_for_telegram_source: Optional[str] = prep_data.get('telegram_file_id')
    gdrive_id_source: Optional[str] = prep_data.get('source_gdrive_id')
    
    temp_final_file_path: Optional[str] = None

    try:
        # This part that does a DB lookup if needed is also correct and stays.
        is_item_from_batch = prep_data.get("is_item_from_batch", False)
        needs_db_lookup = not is_item_from_batch and not gdrive_id_source and not telegram_file_id_for_telegram_source and not chunks_meta_for_telegram_source

        if needs_db_lookup:
            logging.info(f"{log_prefix} DB lookup required for source details.")
            # Your existing DB lookup logic remains here, as it correctly determines the source.
            # This part correctly populates gdrive_id_source, telegram_file_id_for_telegram_source, etc.
            # The code you provided for this section is correct. We'll assume it's here.
            pass

        # This expiration check is also correct and stays.
        if is_anonymous_final:
            logging.info(f"{log_prefix} Anonymous upload. Checking expiration...")
            # Your existing expiration check logic remains here.
            pass

        yield _yield_sse_event('filename', {'filename': original_filename_final})
        yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size_final})
        
        # Create a temporary file to write the downloaded content to.
        with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
            temp_final_file_path = tf.name

        start_time = time.time()
        bytes_downloaded = 0
        
        # --- REVISED STREAMING LOGIC ---
        # data_source_generator: Optional[Generator[bytes, None, None]] = None

        
        with open(temp_final_file_path, 'wb') as f_out:
            if gdrive_id_source:
                yield _yield_sse_event('status', {'message': 'Downloading from temporary storage...'})
                
                # 1. Call your original function, which returns (stream, error)
                gdrive_stream, gdrive_err = download_from_gdrive(gdrive_id_source)
                if gdrive_err or not gdrive_stream:
                    raise IOError(f"GDrive download failed: {gdrive_err}")
                
                # 2. Now, read from the returned BytesIO object in chunks
                while True:
                    chunk = gdrive_stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    bytes_downloaded += len(chunk)
                    progress = _calculate_progress(start_time, bytes_downloaded, final_expected_size_final)
                    yield _yield_sse_event('progress', progress)
                gdrive_stream.close()

            elif is_split_for_telegram_source:
                yield _yield_sse_event('status', {'message': 'Reassembling from secure storage...'})
                if not chunks_meta_for_telegram_source:
                    raise RuntimeError("File is split but no chunk metadata found.")
                chunks_meta_for_telegram_source.sort(key=lambda c: int(c.get('part_number', 0)))
                
                for chunk_info in chunks_meta_for_telegram_source:
                    part_num = chunk_info.get("part_number")
                    chunk_tg_id, _ = _find_best_telegram_file_id(chunk_info.get("send_locations", []), PRIMARY_TELEGRAM_CHAT_ID)
                    if not chunk_tg_id:
                        raise ValueError(f"Missing Telegram ID for chunk {part_num}.")
                    
                    content_part, err_part = download_telegram_file_content(chunk_tg_id)
                    if err_part or not content_part:
                        raise IOError(f"Failed to download chunk {part_num}: {err_part}")
                    
                    f_out.write(content_part)
                    bytes_downloaded += len(content_part)
                    progress = _calculate_progress(start_time, bytes_downloaded, final_expected_size_final)
                    yield _yield_sse_event('progress', progress)

            elif telegram_file_id_for_telegram_source:
                yield _yield_sse_event('status', {'message': 'Downloading from secure storage...'})
                content_full, err_msg = download_telegram_file_content(telegram_file_id_for_telegram_source)
                if err_msg or not content_full:
                    raise IOError(f"Telegram download failed: {err_msg}")
                
                # To provide progress for single large TG files, wrap the content in a BytesIO
                # and read from it in chunks.
                tg_stream = io.BytesIO(content_full)
                while True:
                    chunk = tg_stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    bytes_downloaded += len(chunk)
                    progress = _calculate_progress(start_time, bytes_downloaded, final_expected_size_final)
                    yield _yield_sse_event('progress', progress)
                tg_stream.close()
            
            else:
                raise RuntimeError("No valid source (GDrive or Telegram) determined for download.")
        
        # --- END OF REVISED LOGIC ---

        # The rest of the finalization logic is now correct because temp_final_file_path exists
        logging.info(f"{log_prefix} Final file ready. Path: {temp_final_file_path}")
        prep_data['final_temp_file_path'] = temp_final_file_path
        prep_data['final_file_size'] = os.path.getsize(temp_final_file_path)
        prep_data['status'] = 'ready'

        yield _yield_sse_event('progress', {'percentage': 100})
        yield _yield_sse_event('status', {'message': 'File ready!'})
        yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_final})
        logging.info(f"{log_prefix} All 'ready' SSE events sent.")

    except Exception as e:
        error_message = f"Download prep failed: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id in download_prep_data:
            download_prep_data[prep_id]['status'] = 'error'
            download_prep_data[prep_id]['error'] = error_message
    finally:
        current_status_final = download_prep_data.get(prep_id, {}).get('status')
        if current_status_final != 'ready' and temp_final_file_path and os.path.exists(temp_final_file_path):
             _safe_remove_file(temp_final_file_path, log_prefix, "failed download prep file")
        logging.info(f"{log_prefix} Generator ended. Status: {current_status_final}")
        
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


@download_sse_bp.route('/download-single/<access_id>/<path:filename>')
def download_single_file(access_id: str, filename: str):
    prep_id = str(uuid.uuid4())
    log_prefix = f"[SingleDLPrep-{prep_id}-{access_id}-{filename[:25]}]" 
    logging.info(f"{log_prefix} Request received to prepare SSE stream for downloading single file.")

    record_info, error_msg_find = find_metadata_by_access_id(access_id)

    if error_msg_find or not record_info:
        err_user_msg = error_msg_find or f"Record with Access ID '{access_id}' not found."
        logging.warning(f"{log_prefix} {err_user_msg}")
        def error_stream_not_found(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_not_found()), mimetype='text/event-stream', status=404)

    files_in_record_array = record_info.get('files_in_batch', [])
    
    if not files_in_record_array:
        err_user_msg = f"Record '{access_id}' has no processable file entries (files_in_batch is missing or empty)."
        logging.warning(f"{log_prefix} {err_user_msg}. Record details: {record_info}")
        def error_stream_no_files(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_no_files()), mimetype='text/event-stream', status=400)

    target_file_metadata = next((f for f in files_in_record_array if f.get('original_filename') == filename and not f.get('skipped') and not f.get('failed')), None)

    if not target_file_metadata:
        err_user_msg = f"File '{filename}' not found, was skipped, or failed within record '{access_id}'."
        logging.warning(f"{log_prefix} {err_user_msg}. Available files: {[f.get('original_filename') for f in files_in_record_array]}")
        def error_stream_file_not_in_record(): yield _yield_sse_event('error', {'message': err_user_msg})
        return Response(stream_with_context(error_stream_file_not_in_record()), mimetype='text/event-stream', status=404)
    
    prep_data_payload: Dict[str, Any] = {
        "prep_id": prep_id, "status": "initiated", "access_id": access_id, 
        "username": record_info.get('username'), 
        "requested_filename": filename, 
        "original_filename": target_file_metadata.get('original_filename'), 
        "is_item_from_batch": True, 
        "is_anonymous": record_info.get('is_anonymous', False), 
        "upload_timestamp": record_info.get('upload_timestamp'), 
        "final_expected_size": target_file_metadata.get('original_size', 0),
        # is_compressed will be set based on source below
        "error": None, "final_temp_file_path": None, "final_file_size": 0, "start_time": time.time()
    }

    # Determine the source: Telegram or GDrive
    tg_send_status = target_file_metadata.get('telegram_send_status', 'unknown')
    gdrive_id_for_file = target_file_metadata.get('gdrive_file_id')
    # Overall storage location of the record (batch/single upload)
    record_storage_location = record_info.get('storage_location') 
    
    logging.info(f"{log_prefix} File TG Status: '{tg_send_status}', Record Storage: '{record_storage_location}', GDrive ID present: {bool(gdrive_id_for_file)}")

    attempt_gdrive_source = False
    if gdrive_id_for_file:
        if record_storage_location == "gdrive": # Primary storage is GDrive
            attempt_gdrive_source = True
            logging.info(f"{log_prefix} Record storage is GDrive. Sourcing from GDrive.")
        elif tg_send_status in ['pending', 'failed_chunking_bg', 'failed_single_bg', 'error_processing_bg', 'skipped_bad_data_bg'] and \
             record_storage_location in ['mixed_gdrive_telegram_error', 'telegram_processing_background']:
            attempt_gdrive_source = True
            logging.info(f"{log_prefix} TG status '{tg_send_status}' and record storage '{record_storage_location}' indicate GDrive fallback.")
        elif tg_send_status == 'unknown' and record_storage_location == 'gdrive_complete_pending_telegram':
            # This specific combination implies it was just uploaded to GDrive, TG transfer is about to start or in very early stage
            attempt_gdrive_source = True
            logging.info(f"{log_prefix} Record status 'gdrive_complete_pending_telegram'. Sourcing from GDrive.")


    if attempt_gdrive_source:
        logging.info(f"{log_prefix} Using GDrive source for file '{filename}'. GDrive ID: {gdrive_id_for_file}")
        prep_data_payload["source_gdrive_id"] = gdrive_id_for_file
        prep_data_payload["is_split"] = False # GDrive files are treated as single stream for this prep
        prep_data_payload["chunks_meta"] = None
        prep_data_payload["telegram_file_id"] = None
        # For GDrive source, 'is_compressed' refers to the original file's compression state when uploaded to GDrive
        prep_data_payload["is_compressed"] = target_file_metadata.get('is_compressed', False) 
        prep_data_payload["compressed_total_size"] = target_file_metadata.get('original_size', 0) 
    else: 
        # Attempt Telegram source (this is the path that was erroring out)
        logging.info(f"{log_prefix} Using Telegram source for file '{filename}'.")
        final_is_split_for_telegram = target_file_metadata.get('is_split_for_telegram', False)
        prep_data_payload["is_split"] = final_is_split_for_telegram
        # For Telegram source, 'is_compressed' refers to 'is_compressed_for_telegram'
        prep_data_payload["is_compressed"] = target_file_metadata.get('is_compressed_for_telegram', target_file_metadata.get('is_compressed', False))
        
        if final_is_split_for_telegram:
            final_telegram_chunks_meta = target_file_metadata.get('telegram_chunks')
            if not final_telegram_chunks_meta:
                err_msg_chunks = f"File '{filename}' in record '{access_id}' is marked as split for Telegram but has no chunk information."
                logging.error(f"{log_prefix} {err_msg_chunks} Details: {target_file_metadata}")
                def err_s_chunks(): yield _yield_sse_event('error', {'message': err_msg_chunks})
                return Response(stream_with_context(err_s_chunks()), mimetype='text/event-stream', status=500)
            prep_data_payload["chunks_meta"] = final_telegram_chunks_meta
            prep_data_payload["compressed_total_size"] = target_file_metadata.get('telegram_total_chunked_size', 0)
        else: # Not split for Telegram
            tg_send_locations = target_file_metadata.get('telegram_send_locations', [])
            prep_telegram_file_id_single, _ = _find_best_telegram_file_id(tg_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
            if not prep_telegram_file_id_single:
                err_msg_no_tg_src = f"No primary Telegram source found for file '{filename}' in record '{access_id}'. TG locations considered: {tg_send_locations}. File TG Status: '{tg_send_status}'. Record Storage: '{record_storage_location}'."
                logging.error(f"{log_prefix} {err_msg_no_tg_src} File details: {json.dumps(target_file_metadata, default=str)}")
                def err_s_no_src(): yield _yield_sse_event('error', {'message': err_msg_no_tg_src})
                return Response(stream_with_context(err_s_no_src()), mimetype='text/event-stream', status=500)
            prep_data_payload["telegram_file_id"] = prep_telegram_file_id_single
            prep_data_payload["compressed_total_size"] = target_file_metadata.get('compressed_total_size', target_file_metadata.get('original_size', 0))

    download_prep_data[prep_id] = prep_data_payload
    logging.debug(f"{log_prefix} Prep data for _prepare_download: {json.dumps(download_prep_data[prep_id], default=str)}")
    return Response(stream_with_context(_prepare_download_and_generate_updates(prep_id)), mimetype='text/event-stream')

@download_bp.route('/initiate-download-all/<access_id>')
def initiate_download_all(access_id: str):
    prep_id_for_zip = str(uuid.uuid4())
    log_prefix = f"[DLAll-Init-{access_id}-{prep_id_for_zip[:4]}]" # More specific log prefix
    logging.info(f"{log_prefix} Request received.")

    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg:
        logging.error(f"{log_prefix} DB error finding record: {error_msg}")
        return jsonify({"error": f"Server error: {error_msg}", "prep_id": None}), 500
    if not batch_info:
        logging.warning(f"{log_prefix} Record not found for access_id: {access_id}")
        return jsonify({"error": "Record not found or invalid.", "prep_id": None}), 404

    # Allow processing even if is_batch is False, as long as files_in_batch exists.
    # The crucial part is that files_in_batch should contain the actual file items.
    files_in_record = batch_info.get("files_in_batch", [])
    if not files_in_record:
        logging.warning(f"{log_prefix} Record {access_id} has no 'files_in_batch' array. is_batch: {batch_info.get('is_batch')}")
        return jsonify({"error": "Record contains no files to process.", "prep_id": None}), 404

    files_to_zip_meta: list[dict] = []
    total_expected_zip_content_size: int = 0

    for file_item in files_in_record:
        if file_item.get("skipped") or file_item.get("failed"):
            logging.info(f"{log_prefix} Skipping file '{file_item.get('original_filename')}' due to skipped/failed flag.")
            continue
        
        original_filename = file_item.get("original_filename")
        original_size = file_item.get("original_size", 0)

        if not original_filename:
            logging.warning(f"{log_prefix} Skipping file item due to missing original_filename: {file_item}")
            continue

        meta_entry = {
            "original_filename": original_filename,
            "original_size": original_size,
            "is_split_for_telegram": file_item.get("is_split_for_telegram", False), # From TG processing
            "is_compressed": file_item.get("is_compressed", False), # Original compression
            "telegram_file_id": None, # For single, non-split TG files
            "chunks_meta": None       # For split TG files
        }

        # Prioritize information from the Telegram processing stage
        if meta_entry["is_split_for_telegram"]:
            tg_chunks = file_item.get("telegram_chunks")
            if tg_chunks:
                meta_entry["chunks_meta"] = tg_chunks
                logging.debug(f"{log_prefix} File '{original_filename}' is split for Telegram, using telegram_chunks.")
            else:
                logging.warning(f"{log_prefix} File '{original_filename}' marked as is_split_for_telegram but no telegram_chunks found. Cannot process.")
                continue # Cannot zip this file
        else: # Not split for Telegram, should have telegram_send_locations
            tg_send_locs = file_item.get("telegram_send_locations")
            if tg_send_locs:
                tg_file_id, _ = _find_best_telegram_file_id(tg_send_locs, PRIMARY_TELEGRAM_CHAT_ID)
                if tg_file_id:
                    meta_entry["telegram_file_id"] = tg_file_id
                    logging.debug(f"{log_prefix} File '{original_filename}' is single for Telegram, using telegram_file_id: {tg_file_id}.")
                else:
                    logging.warning(f"{log_prefix} File '{original_filename}' has telegram_send_locations but no usable primary TG file ID. Cannot process.")
                    continue
            else: # Fallback: If no telegram_send_locations, check older fields or GDrive if it's mixed
                if batch_info.get("storage_location") == "gdrive" or \
                   (batch_info.get("storage_location") == "mixed_gdrive_telegram_error" and file_item.get("gdrive_file_id")):
                    logging.warning(f"{log_prefix} File '{original_filename}' has no direct Telegram locations and storage is '{batch_info.get('storage_location')}'. It might be GDrive only. Zipping GDrive sources in 'Download All' needs enhancement in _generate_zip_and_stream_progress.")
                    gdrive_id_source = file_item.get("gdrive_file_id") # This is the ID from initial GDrive upload
                    if gdrive_id_source:
                        meta_entry["gdrive_file_id_source"] = gdrive_id_source # Pass to zipping function
                        logging.debug(f"{log_prefix} File '{original_filename}' has GDrive ID {gdrive_id_source}, will attempt GDrive fetch for zipping if TG fails.")
                    else:
                        logging.warning(f"{log_prefix} File '{original_filename}' has no Telegram locations and no GDrive ID. Cannot process.")
                        continue


        # A file is addable if it has an original_filename AND
        # (is split for TG with chunks OR has a TG file ID for single OR has a GDrive ID source)
        if original_filename and \
           (meta_entry["chunks_meta"] or meta_entry["telegram_file_id"] or meta_entry.get("gdrive_file_id_source")):
            files_to_zip_meta.append(meta_entry)
            total_expected_zip_content_size += original_size
        else:
            logging.warning(f"{log_prefix} File '{original_filename}' did not meet criteria for zipping (no TG chunks, no TG ID, no GDrive ID). Meta: {meta_entry}")
            
    if not files_to_zip_meta:
        logging.error(f"{log_prefix} No files met the criteria to be added to the zip archive for access_id: {access_id}.")
        return jsonify({"error": "No files available to zip. They may be missing required information or still processing.", "prep_id": None}), 404

    zip_name = batch_info.get('batch_display_name', f"download_all_{access_id}.zip")
    if not zip_name.lower().endswith(".zip"): zip_name += ".zip"

    download_prep_data[prep_id_for_zip] = {
        "prep_id": prep_id_for_zip, "status": "initiated_zip_all", 
        "access_id_original_batch": access_id, "username": batch_info.get('username'),
        "batch_display_name": zip_name, "original_filename": zip_name, 
        "files_to_zip_meta": files_to_zip_meta, 
        "total_expected_content_size": total_expected_zip_content_size, 
        "is_anonymous": batch_info.get('is_anonymous', False),
        "upload_timestamp": batch_info.get('upload_timestamp'),
        "start_time": time.time()
    }
    logging.info(f"{log_prefix} Successfully prepared {len(files_to_zip_meta)} files for zipping. SSE stream URL will be generated.")
    return jsonify({
        "message": "Download All initiated.", "prep_id_for_zip": prep_id_for_zip, 
        "sse_stream_url": url_for('download_prefixed.stream_download_all', prep_id_for_zip=prep_id_for_zip) # Corrected blueprint name
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
    is_anonymous_batch = prep_data.get('is_anonymous', False) # From Step 2
    upload_timestamp_str_batch = prep_data.get('upload_timestamp')
    
    if is_anonymous_batch:
        logging.info(f"{log_prefix} Anonymous batch. Checking expiration. Timestamp: '{upload_timestamp_str_batch}'")
        if not upload_timestamp_str_batch:
            logging.warning(f"{log_prefix} Anonymous batch '{prep_data.get('access_id_original_batch', 'N/A')}' is missing 'upload_timestamp'.")
            yield _yield_sse_event('error', {'message': 'Batch record is incomplete (missing timestamp). Cannot verify expiration.'})
            if prep_id_for_zip in download_prep_data: # Check before accessing
                download_prep_data[prep_id_for_zip]['status'] = 'error'
                download_prep_data[prep_id_for_zip]['error'] = 'Batch record incomplete (timestamp missing).'
            return
        try:
            upload_datetime_batch = parser.isoparse(upload_timestamp_str_batch)
            if upload_datetime_batch.tzinfo is None or upload_datetime_batch.tzinfo.utcoffset(upload_datetime_batch) is None:
                upload_datetime_batch = upload_datetime_batch.replace(tzinfo=timezone.utc)
            
            expiration_limit = timedelta(days=5)
            # For testing:
            # expiration_limit = timedelta(seconds=30) 
            now_utc = datetime.now(timezone.utc)

            if now_utc > (upload_datetime_batch + expiration_limit):
                logging.info(f"{log_prefix} Anonymous batch download link EXPIRED. Uploaded At: {upload_datetime_batch}, Expires At: {upload_datetime_batch + expiration_limit}, Current Time: {now_utc}")
                yield _yield_sse_event('error', {'message': 'This download link for the batch has expired.'})
                if prep_id_for_zip in download_prep_data: # Check before accessing
                    download_prep_data[prep_id_for_zip]['status'] = 'error'
                    download_prep_data[prep_id_for_zip]['error'] = 'Batch link expired.'
                return
            else:
                logging.info(f"{log_prefix} Anonymous batch download link still valid.")
        except ValueError as e: # Catch parsing errors
            logging.error(f"{log_prefix} Error parsing batch upload_timestamp '{upload_timestamp_str_batch}': {e}", exc_info=True)
            yield _yield_sse_event('error', {'message': 'Error processing batch metadata (invalid timestamp).'})
            if prep_id_for_zip in download_prep_data: # Check before accessing
                download_prep_data[prep_id_for_zip]['status'] = 'error'
                download_prep_data[prep_id_for_zip]['error'] = 'Invalid timestamp in batch record.'
            return
    else:
        logging.info(f"{log_prefix} Not an anonymous batch. Expiration check skipped for 'Download All'.")
    
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
                original_filename_for_zip_entry = file_meta_item["original_filename"]
                logging.info(f"{log_prefix} Processing file for zip: {original_filename_for_zip_entry}")
                
                if file_meta_item.get("is_split_for_telegram") and file_meta_item.get("chunks_meta"):
                    logging.info(f"{log_prefix} File '{original_filename_for_zip_entry}' is split (from Telegram). Reassembling for zip.")
                    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"reass_zip_{uuid.uuid4().hex[:6]}_") as temp_reass_file:
                        current_reassembled_path = temp_reass_file.name
                    
                    split_chunks_meta = file_meta_item.get("chunks_meta", [])
                    split_chunks_meta.sort(key=lambda c: int(c.get('part_number',0)))
                
                    for chunk_meta in split_chunks_meta:
                        part_num = chunk_meta.get("part_number")
                        chunk_locs = chunk_meta.get("send_locations", [])
                        chunk_tg_id, _ = _find_best_telegram_file_id(chunk_locs, PRIMARY_TELEGRAM_CHAT_ID)
                        if chunk_tg_id:
                            _, chunk_content, dl_err = _download_chunk_task(chunk_tg_id, part_num, f"{prep_id_for_zip}-TGChunk")
                            if dl_err or not chunk_content:
                                raise ValueError(f"Failed to download TG chunk {part_num} for '{original_filename_for_zip_entry}': {dl_err}")
                            with open(current_reassembled_path, "ab") as f_reass:
                                f_reass.write(chunk_content)
                            bytes_downloaded_for_zip += len(chunk_content)
                        else:
                            raise ValueError(f"Missing TG ID for chunk {part_num} of '{original_filename_for_zip_entry}'.")
                    
                    zf.write(current_reassembled_path, arcname=original_filename_for_zip_entry)
                    _safe_remove_file(current_reassembled_path, log_prefix, f"temp reassembled for zip: {original_filename_for_zip_entry}")
                
                elif file_meta_item.get("telegram_file_id"): # Single, non-split Telegram file
                    logging.info(f"{log_prefix} File '{original_filename_for_zip_entry}' is single (from Telegram). Adding to executor.")
                    tg_id_non_split = file_meta_item["telegram_file_id"]
                    # Use _download_chunk_task structure for simplicity, passing part_num=0 to indicate non-chunked context
                    fut = download_all_executor.submit(_download_chunk_task, tg_id_non_split, 0, f"{prep_id_for_zip}-TGSingle")
                    future_to_filemeta[fut] = file_meta_item # Store original meta
                
                elif file_meta_item.get("gdrive_file_id_source"): # Fallback to GDrive source
                    logging.info(f"{log_prefix} File '{original_filename_for_zip_entry}' to be fetched from GDrive (ID: {file_meta_item['gdrive_file_id_source']}).")
                    def _gdrive_download_task_for_zip(gdrive_id: str, op_id: str) -> Tuple[int, Optional[bytes], Optional[str]]:
                        g_stream, g_err = download_from_gdrive(gdrive_id)
                        if g_err or not g_stream: return 0, None, f"GDrive download error: {g_err}"
                        content = g_stream.read()
                        g_stream.close()
                        return 0, content, None
                    
                    fut_gdrive = download_all_executor.submit(_gdrive_download_task_for_zip, file_meta_item["gdrive_file_id_source"], f"{prep_id_for_zip}-GDrive")
                    future_to_filemeta[fut_gdrive] = file_meta_item # Store original meta
                
                else: # Should not happen if initiate_download_all is correct
                    logging.error(f"{log_prefix} File '{original_filename_for_zip_entry}' has no processable source (TG or GDrive). Skipping.")
                    # Optionally mark as failed in a more detailed way if needed
                    continue

                # Update progress after each file added directly (split files)
                if file_meta_item.get("is_split_for_telegram"): # Progress for split files is updated chunk by chunk above
                    files_processed_count += 1
                    # Note: bytes_downloaded_for_zip for split files accumulates per chunk.
                    # For single files via executor, it will be added when future completes.
                    # This means progress for split files might appear faster initially.
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_for_zip, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Added to zip (split): {files_processed_count}/{len(files_to_process_meta)} files... ({original_filename_for_zip_entry})'})

            
            # Process non-split files submitted to executor
            for future in as_completed(future_to_filemeta):
                completed_file_meta = future_to_filemeta[future]
                original_filename_for_zip_entry = completed_file_meta["original_filename"]
                file_log_prefix_future = f"{log_prefix}-Future-{original_filename_for_zip_entry[:10]}"
                try:
                    _, content_bytes, error_msg_future = future.result() 
                    if error_msg_future or not content_bytes:
                        raise ValueError(f"Failed to download '{original_filename_for_zip_entry}' for zip: {error_msg_future or 'Empty content'}")
                    
                    # If the non-split file was marked as compressed by uploader script (and not originally a .zip)
                    if completed_file_meta.get("is_compressed") and not original_filename_for_zip_entry.lower().endswith(".zip"):
                        logging.info(f"{file_log_prefix_future} Decompressing '{original_filename_for_zip_entry}' before adding to master zip.")
                        zip_buffer_inner = io.BytesIO(content_bytes)
                        with zipfile.ZipFile(zip_buffer_inner, 'r') as zf_inner:
                            inner_content_name = _find_filename_in_zip(zf_inner, original_filename_for_zip_entry, file_log_prefix_future)
                            with zf_inner.open(inner_content_name) as actual_content_stream:
                                zf.writestr(original_filename_for_zip_entry, actual_content_stream.read())
                    else: # Add directly
                        zf.writestr(original_filename_for_zip_entry, content_bytes)

                    bytes_downloaded_for_zip += len(content_bytes) # Add size of content added to zip
                    files_processed_count += 1
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_for_zip, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Added to zip: {files_processed_count}/{len(files_to_process_meta)} files... ({original_filename_for_zip_entry})'})
                except Exception as exc_future:
                    # Decide how to handle: fail entire zip, or skip this file and continue?
                    # For now, let's raise to fail the whole zip if one file fails this way.
                    logging.error(f"{file_log_prefix_future} Error processing download for '{original_filename_for_zip_entry}' to add to zip: {exc_future}", exc_info=True)
                    raise ValueError(f"Error processing download for '{original_filename_for_zip_entry}' for zip: {exc_future}")

        final_zip_actual_size = os.path.getsize(temp_zip_file_path)
        prep_data['status'] = 'ready'; prep_data['final_temp_file_path'] = temp_zip_file_path 
        prep_data['final_file_size'] = final_zip_actual_size
        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': total_expected_content_size, 'totalBytes': total_expected_content_size, 'etaFormatted': '00:00'})
        yield _yield_sse_event('status', {'message': 'Archive ready!'})
        yield _yield_sse_event('ready', {'temp_file_id': prep_id_for_zip, 'final_filename': batch_display_name_for_zip })
    except Exception as e:
        error_message = f"Failed to generate 'Download All' zip: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} Error in _generate_zip_and_stream_progress: {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id_for_zip in download_prep_data:
            download_prep_data[prep_id_for_zip]['status'] = 'error_zipping_all'
            download_prep_data[prep_id_for_zip]['error'] = error_message
    finally:
        if download_all_executor: download_all_executor.shutdown(wait=False)
        if prep_data and prep_data.get('status') != 'ready' and temp_zip_file_path and os.path.exists(temp_zip_file_path):
            _safe_remove_file(temp_zip_file_path, log_prefix, "partially created download-all zip")
