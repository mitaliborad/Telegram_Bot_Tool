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
    log_prefix = f"[DL-Stream-{access_id}]"
    logging.info(f"{log_prefix} Request received.")

    metadata, error = find_metadata_by_access_id(access_id) # metadata is our db_record
    if error or not metadata:
        logging.error(f"{log_prefix} Metadata not found or DB error: {error}")
        abort(404, description=f"File record not found for ID: {access_id}. {error or ''}")

    storage_location = metadata.get("storage_location")
    if not metadata.get("files_in_batch"):
        logging.error(f"{log_prefix} 'files_in_batch' is missing or empty in metadata.")
        abort(500, description="File metadata is incomplete (no file entries).")
        
    file_info_in_batch = metadata.get("files_in_batch", [{}])[0] 
    original_filename = file_info_in_batch.get("original_filename", f"download_{access_id}")
    mime_type = file_info_in_batch.get("mime_type", "application/octet-stream")

    logging.info(f"{log_prefix} Filename: '{original_filename}', Storage: '{storage_location}'")

    if storage_location == "gdrive":
        gdrive_id = file_info_in_batch.get("gdrive_file_id")
        if not gdrive_id:
            logging.error(f"{log_prefix} GDrive ID missing in record for GDrive storage.")
            abort(500, "File record inconsistent: GDrive ID missing for GDrive storage.")
        
        logging.info(f"{log_prefix} Attempting to download from GDrive ID: {gdrive_id}")
        gdrive_stream, gdrive_err = download_from_gdrive(gdrive_id)
        if gdrive_err or not gdrive_stream:
            logging.error(f"{log_prefix} Failed to download from GDrive: {gdrive_err}")
            abort(500, f"Failed to retrieve file from temporary storage: {gdrive_err}")
        
        logging.info(f"{log_prefix} Successfully fetched from GDrive. Preparing to stream to client.")
        response = Response(stream_with_context(generate_file_chunks(gdrive_stream, f"GDrive-{gdrive_id}")), mimetype=mime_type)
        response.headers['Content-Disposition'] = f'attachment; filename="{original_filename}"'
        return response

    elif storage_location == "telegram":
        from routes.utils import _find_best_telegram_file_id 
        
        tg_file_id_to_download = None
        if file_info_in_batch.get("is_split_for_telegram"):
            logging.error(f"{log_prefix} Direct streaming of split Telegram files not fully supported for simple download. Client should use preparation endpoint.")
            abort(501, "Split files require download preparation. Cannot stream directly.")
        else:
            tg_file_id_to_download, _ = _find_best_telegram_file_id(file_info_in_batch.get("telegram_send_locations",[]), PRIMARY_TELEGRAM_CHAT_ID)

        if not tg_file_id_to_download:
            logging.error(f"{log_prefix} Telegram file ID missing in record for Telegram storage.")
            abort(500, "File record inconsistent: Telegram file ID missing.")

        logging.info(f"{log_prefix} Attempting to download from Telegram File ID: {tg_file_id_to_download}")
        telegram_stream, tg_dl_error = download_telegram_file_content(tg_file_id_to_download)
        if tg_dl_error or not telegram_stream:
            logging.error(f"{log_prefix} Failed to download from Telegram: {tg_dl_error}")
            abort(500, f"Failed to retrieve file from final storage: {tg_dl_error}")

        logging.info(f"{log_prefix} Successfully fetched from Telegram. Preparing to stream to client.")
        response = Response(stream_with_context(generate_file_chunks(telegram_stream, f"TG-{tg_file_id_to_download}")), mimetype=mime_type)
        response.headers['Content-Disposition'] = f'attachment; filename="{original_filename}"'
        return response
        
    # Corrected line breaking for Pylint
    elif (storage_location == "mixed_gdrive_telegram_error" or
          metadata.get("status_overall") == "telegram_processing_errors"):
        logging.warning(f"{log_prefix} Telegram transfer had issues. Attempting to serve from GDrive fallback.")
        gdrive_id = file_info_in_batch.get("gdrive_file_id")
        if not gdrive_id:
            logging.error(f"{log_prefix} GDrive ID missing for fallback.")
            error_msg_fallback_no_id = (
                "File unavailable: Telegram processing failed and GDrive copy is missing ID."
            )
            abort(500, error_msg_fallback_no_id)
        
        gdrive_stream, gdrive_err = download_from_gdrive(gdrive_id)
        if gdrive_err or not gdrive_stream:
            logging.error(f"{log_prefix} Failed to download from GDrive (fallback): {gdrive_err}")
            error_msg_fallback_fail = (
                "File unavailable: Telegram processing failed and GDrive copy "
                f"could not be retrieved: {gdrive_err}"
            )
            abort(500, error_msg_fallback_fail)
        
        response = Response(stream_with_context(generate_file_chunks(gdrive_stream, f"GDrive-Fallback-{gdrive_id}")), mimetype=mime_type)
        response.headers['Content-Disposition'] = f'attachment; filename="{original_filename}"'
        return response
    else:
        logging.error(
            f"{log_prefix} Unknown or error storage location: {storage_location} or "
            f"status: {metadata.get('status_overall')}"
        )
        abort(500, f"File is in an unknown or error state. Storage: {storage_location}")
 

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
    
    # Initialize with defaults or values from prep_data
    is_anonymous_final: bool = prep_data.get('is_anonymous', False)
    upload_timestamp_str_final: Optional[str] = prep_data.get('upload_timestamp')
    original_filename_final: str = prep_data.get('original_filename', "download")
    final_expected_size_final: int = prep_data.get('final_expected_size', 0)
    
    # Source-specific flags, will be refined after DB lookup or from prep_data
    is_split_for_telegram_source: bool = prep_data.get('is_split', False) 
    chunks_meta_for_telegram_source: Optional[List[Dict[str, Any]]] = prep_data.get('chunks_meta')
    telegram_file_id_for_telegram_source: Optional[str] = prep_data.get('telegram_file_id')
    compressed_total_size_for_telegram_source: int = prep_data.get('compressed_total_size', 0)
    is_compressed_for_telegram_source: bool = prep_data.get('is_compressed', False) 
    
    gdrive_id_source: Optional[str] = prep_data.get('source_gdrive_id')
    is_compressed_for_gdrive_source_flag: bool = prep_data.get('is_compressed', False) 

    # This will be the definitive compression flag for the chosen source
    current_is_compressed: bool = False 

    temp_reassembled_file_path: Optional[str] = None
    temp_final_file_path: Optional[str] = None
    download_executor: Optional[ThreadPoolExecutor] = None

    try:
        is_item_from_batch = prep_data.get("is_item_from_batch", False)
        # Determine if we need to look up details from DB or if prep_data is sufficient
        needs_db_lookup_for_source_details = not is_item_from_batch and not gdrive_id_source and not telegram_file_id_for_telegram_source and not chunks_meta_for_telegram_source

        if needs_db_lookup_for_source_details:
            logging.info(f"{log_prefix} DB lookup required for source details.")
            db_access_id = prep_data.get('access_id')
            db_username = prep_data.get('username')
            db_requested_filename = prep_data.get('requested_filename')
            fetched_record_info: Optional[Dict[str, Any]] = None
            fetched_file_item_info: Optional[Dict[str, Any]] = None
            lookup_error_msg = ""

            if db_access_id:
                fetched_record_info, lookup_error_msg = find_metadata_by_access_id(db_access_id)
                if fetched_record_info and db_requested_filename:
                     files_in_batch_arr = fetched_record_info.get('files_in_batch', [])
                     fetched_file_item_info = next((f for f in files_in_batch_arr if f.get('original_filename') == db_requested_filename), None)
                elif fetched_record_info and not fetched_record_info.get('is_batch'):
                    files_in_batch_arr = fetched_record_info.get('files_in_batch', [])
                    fetched_file_item_info = files_in_batch_arr[0] if files_in_batch_arr else fetched_record_info
            
            if lookup_error_msg or not fetched_record_info or not fetched_file_item_info:
                err_msg_to_raise = lookup_error_msg or "File metadata not found in DB."
                if not fetched_file_item_info and fetched_record_info: err_msg_to_raise = f"File '{db_requested_filename}' not found in record '{db_access_id}'."
                raise FileNotFoundError(err_msg_to_raise)

            original_filename_final = fetched_file_item_info.get('original_filename', db_requested_filename or 'unknown')
            final_expected_size_final = fetched_file_item_info.get('original_size', 0)
            is_anonymous_final = fetched_record_info.get('is_anonymous', False)
            upload_timestamp_str_final = fetched_record_info.get('upload_timestamp')
            
            # Determine source and compression from DB
            db_tg_status = fetched_file_item_info.get('telegram_send_status', 'unknown')
            db_record_storage_loc = fetched_record_info.get('storage_location')
            attempt_gdrive_source_from_db = (
                db_tg_status in ['pending', 'failed_chunking_bg', 'failed_single_bg', 'error_processing_bg', 'skipped_bad_data_bg'] and
                fetched_file_item_info.get('gdrive_file_id') and
                db_record_storage_loc in ['gdrive', 'mixed_gdrive_telegram_error', 'telegram_processing_background']
            )

            if attempt_gdrive_source_from_db:
                gdrive_id_source = fetched_file_item_info.get('gdrive_file_id')
                is_split_for_telegram_source = False 
                chunks_meta_for_telegram_source = None
                telegram_file_id_for_telegram_source = None
                current_is_compressed = fetched_file_item_info.get('is_compressed', False) # General compression for GDrive
                compressed_total_size_for_telegram_source = final_expected_size_final
                logging.info(f"{log_prefix} DB: Sourcing from GDrive ID {gdrive_id_source}. current_is_compressed: {current_is_compressed}")
            else: 
                is_split_for_telegram_source = fetched_file_item_info.get('is_split_for_telegram', False)
                current_is_compressed = fetched_file_item_info.get('is_compressed_for_telegram', fetched_file_item_info.get('is_compressed', False))
                if is_split_for_telegram_source:
                    chunks_meta_for_telegram_source = fetched_file_item_info.get('telegram_chunks')
                    if not chunks_meta_for_telegram_source: raise RuntimeError("DB: Split TG but no telegram_chunks.")
                    telegram_file_id_for_telegram_source = None
                    compressed_total_size_for_telegram_source = fetched_file_item_info.get('telegram_total_chunked_size', 0)
                else:
                    tg_locs = fetched_file_item_info.get('telegram_send_locations', [])
                    tg_id_db, _ = _find_best_telegram_file_id(tg_locs, PRIMARY_TELEGRAM_CHAT_ID)
                    if not tg_id_db: raise ValueError(f"DB: No primary TG file ID. Status: {db_tg_status}")
                    telegram_file_id_for_telegram_source = tg_id_db
                    chunks_meta_for_telegram_source = None
                    compressed_total_size_for_telegram_source = fetched_file_item_info.get('compressed_total_size', final_expected_size_final)
                logging.info(f"{log_prefix} DB: Sourcing from Telegram. Split: {is_split_for_telegram_source}. current_is_compressed: {current_is_compressed}")

        else: # Data is from prep_data (e.g., from download_single_file)
            if gdrive_id_source:
                current_is_compressed = is_compressed_for_gdrive_source_flag
                logging.info(f"{log_prefix} PrepData: Sourcing from GDrive. current_is_compressed: {current_is_compressed}")
            else: # Telegram source from prep_data
                current_is_compressed = is_compressed_for_telegram_source
                logging.info(f"{log_prefix} PrepData: Sourcing from Telegram. Split: {is_split_for_telegram_source}. current_is_compressed: {current_is_compressed}")
        
        # --- EXPIRATION CHECK (remains the same) ---
        # ... uses is_anonymous_final and upload_timestamp_str_final ...
        if is_anonymous_final:
            logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp string: '{upload_timestamp_str_final}'")
            if not upload_timestamp_str_final:
                logging.warning(f"{log_prefix} Anonymous upload metadata for access_id '{prep_data.get('access_id', 'N/A')}' is missing 'upload_timestamp'.")
                yield _yield_sse_event('error', {'message': 'File record is incomplete (missing timestamp). Cannot verify expiration.'})
                prep_data['status'] = 'error'; prep_data['error'] = 'File record incomplete (timestamp missing).'
                return
            try:
                upload_datetime = parser.isoparse(upload_timestamp_str_final)
                if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
                    upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
                expiration_limit = timedelta(days=5)
                now_utc = datetime.now(timezone.utc)
                if now_utc > (upload_datetime + expiration_limit):
                    logging.info(f"{log_prefix} Anonymous download link EXPIRED.")
                    yield _yield_sse_event('error', {'message': 'This download link has expired.'})
                    prep_data['status'] = 'error'; prep_data['error'] = 'Link expired.'
                    return 
                else:
                    logging.info(f"{log_prefix} Anonymous download link still valid.")
            except ValueError as e: 
                logging.error(f"{log_prefix} Error parsing upload_timestamp '{upload_timestamp_str_final}': {e}", exc_info=True)
                yield _yield_sse_event('error', {'message': 'Error processing file metadata (invalid timestamp).'})
                prep_data['status'] = 'error'; prep_data['error'] = 'Invalid timestamp in record.'
                return
        else:
            logging.info(f"{log_prefix} Not an anonymous upload. Expiration check skipped.")


        yield _yield_sse_event('filename', {'filename': original_filename_final})
        yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size_final}) 
        yield _yield_sse_event('status', {'message': 'Preparing file...'})

        content_bytes: Optional[bytes] = None

        if gdrive_id_source:
            logging.info(f"{log_prefix} Fetching content from GDrive ID: {gdrive_id_source}")
            yield _yield_sse_event('status', {'message': 'Downloading from temporary storage...'})
            gdrive_stream, gdrive_err = download_from_gdrive(gdrive_id_source)
            if gdrive_err or not gdrive_stream:
                raise IOError(f"GDrive download failed: {gdrive_err or 'No content stream'}")
            content_bytes = gdrive_stream.read()
            gdrive_stream.close()
            if not content_bytes: raise ValueError("GDrive download returned empty content.")
            logging.info(f"{log_prefix} GDrive content downloaded. Size: {format_bytes(len(content_bytes))}")
        
        elif is_split_for_telegram_source:
            if not chunks_meta_for_telegram_source: raise RuntimeError("File is split for Telegram but no chunk metadata.")
            chunks_meta_for_telegram_source.sort(key=lambda c: int(c.get('part_number', 0)))
            num_chunks = len(chunks_meta_for_telegram_source)
            total_bytes_to_fetch = compressed_total_size_for_telegram_source or sum(c.get('size',0) for c in chunks_meta_for_telegram_source)
            
            start_fetch_time = time.time(); fetched_bytes_count = 0; downloaded_chunk_count = 0
            download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
            submitted_futures: List[Future] = []
            downloaded_content_map: Dict[int, bytes] = {}
            first_download_error: Optional[str] = None
            file_too_big_errors_count = 0

            for i, chunk_info in enumerate(chunks_meta_for_telegram_source):
                part_num = chunk_info.get("part_number")
                chunk_send_locations = chunk_info.get("send_locations", [])
                if not chunk_send_locations: logging.warning(f"{log_prefix} Chunk {part_num} no send_locations, skipping."); continue
                chunk_tg_file_id, _ = _find_best_telegram_file_id(chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
                if not chunk_tg_file_id: logging.warning(f"{log_prefix} Chunk {part_num} no TG file_id, skipping."); continue
                submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_tg_file_id, part_num, prep_id))
            
            yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})
            for future in as_completed(submitted_futures):
                try:
                    pnum_result, chunk_content_res, err_result = future.result()
                    if err_result:
                        if "file is too big" in err_result.lower(): file_too_big_errors_count += 1
                        if not first_download_error: first_download_error = f"Chunk {pnum_result}: {err_result}"
                    elif chunk_content_res:
                        downloaded_chunk_count += 1; fetched_bytes_count += len(chunk_content_res)
                        downloaded_content_map[pnum_result] = chunk_content_res
                        overall_perc = (downloaded_chunk_count / num_chunks) * 80.0 
                        yield _yield_sse_event('progress', _calculate_download_fetch_progress(start_fetch_time, fetched_bytes_count, total_bytes_to_fetch, downloaded_chunk_count, num_chunks, overall_perc, final_expected_size_final))
                    else: 
                        if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error."
                except Exception as e_fut:
                    if not first_download_error: first_download_error = f"Processing future for chunk: {str(e_fut)}"

            if first_download_error:
                error_to_raise = f"Download failed: {first_download_error}"
                if file_too_big_errors_count > 0: error_to_raise = "Download failed: One or more file parts were too large."
                raise ValueError(error_to_raise)
            if downloaded_chunk_count != num_chunks: raise SystemError(f"Chunk count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")
            
            with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_file_path = tf_reassemble.name
                for pnum_write in range(1, num_chunks + 1):
                    chunk_content_to_write = downloaded_content_map.get(pnum_write)
                    if chunk_content_to_write is None: raise SystemError(f"Reassembly error: Chunk {pnum_write} missing.")
                    tf_reassemble.write(chunk_content_to_write)
            downloaded_content_map.clear()
            logging.info(f"{log_prefix} Telegram split file reassembled to: {temp_reassembled_file_path}")
        
        elif telegram_file_id_for_telegram_source:
            yield _yield_sse_event('status', {'message': 'Downloading from final storage...'})
            content_bytes, err_msg = download_telegram_file_content(telegram_file_id_for_telegram_source)
            if err_msg: raise ValueError(f"TG download failed: {err_msg}")
            if not content_bytes: raise ValueError("TG download returned empty content.")
            logging.info(f"{log_prefix} Non-split TG file downloaded. Content length: {len(content_bytes)}")
        else:
            raise RuntimeError("No valid source (GDrive or Telegram) determined for download.")

        # --- Decompression and Final Temp File Creation ---
        if temp_reassembled_file_path: # Reassembled TG split file
            temp_final_file_path = temp_reassembled_file_path # This IS the final content path
            if current_is_compressed: # If original was a ZIP and it was split for TG
                 yield _yield_sse_event('status', {'message': 'Reassembled ZIP ready.'})
                 logging.info(f"{log_prefix} Reassembled file is a ZIP (original type): {temp_final_file_path}.")
            temp_reassembled_file_path = None # Mark as moved/used
        
        elif content_bytes: # From GDrive or single TG file
            if current_is_compressed and not original_filename_final.lower().endswith('.zip'):
                logging.info(f"{log_prefix} Decompressing non-split file: {original_filename_final}")
                yield _yield_sse_event('status', {'message': 'Decompressing...'})
                # ... (decompression logic as before, writes to temp_final_file_path) ...
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_extracted_{prep_id}_") as tf_extracted:
                    temp_final_file_path = tf_extracted.name
                zf_source = None
                try:
                    zip_buffer = io.BytesIO(content_bytes)
                    zf_source = zipfile.ZipFile(zip_buffer, 'r')
                    inner_filename_to_extract = _find_filename_in_zip(zf_source, original_filename_final, log_prefix)
                    with zf_source.open(inner_filename_to_extract, 'r') as inner_fs, open(temp_final_file_path, 'wb') as tf_out:
                        shutil.copyfileobj(inner_fs, tf_out)  
                finally:
                    if zf_source: zf_source.close()
                logging.info(f"{log_prefix} Decompression complete. Extracted to: {temp_final_file_path}")
            else: 
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name
                    tf.write(content_bytes)
                logging.info(f"{log_prefix} Content written to final temp file: {temp_final_file_path}")
        else:
            raise RuntimeError("No content available (neither bytes nor reassembled path).")

        yield _yield_sse_event('progress', {'percentage': 95})
        
        # --- Finalization ---
        # ... (rest of finalization logic as before) ...
        logging.info(f"{log_prefix} Preparing to finalize. Current temp_final_file_path: {temp_final_file_path}")
        
        if not temp_final_file_path or not os.path.exists(temp_final_file_path):
            logging.error(f"{log_prefix} CRITICAL FAILURE: Final temp file path is invalid or file does not exist. Path: '{temp_final_file_path}'")
            raise RuntimeError(f"Failed to produce final temp file path. Check logs for path details.")
        
        final_actual_size = os.path.getsize(temp_final_file_path)
        logging.info(f"{log_prefix} Final file ready. Path: {temp_final_file_path}, Actual Size: {format_bytes(final_actual_size)}")
        
        prep_data['final_temp_file_path'] = temp_final_file_path
        prep_data['final_file_size'] = final_actual_size 
        if final_expected_size_final == 0 and final_actual_size > 0 : 
             prep_data['final_expected_size'] = final_actual_size
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
        if download_executor: download_executor.shutdown(wait=False)
        if temp_reassembled_file_path and os.path.exists(temp_reassembled_file_path): 
            _safe_remove_file(temp_reassembled_file_path, log_prefix, "intermediate reassembled file in finally")
        
        current_status_final = 'unknown (prep_data missing or cleaned up)' # Renamed variable
        if prep_data and prep_id in download_prep_data : 
            current_status_final = prep_data.get('status', 'unknown')
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
