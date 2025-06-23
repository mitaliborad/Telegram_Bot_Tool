# database/file_models.py
import logging
from typing import Optional, Dict, Any, List, Tuple
from bson import ObjectId
from pymongo.errors import PyMongoError, OperationFailure
import json
from telegram_api import download_telegram_file_content
from .connection import get_metadata_collection
from .user_models import get_all_users
from config import PRIMARY_TELEGRAM_CHAT_ID
import io
import mimetypes

def save_file_metadata(record: Dict[str, Any]) -> Tuple[bool, str]:
    collection, error = get_metadata_collection()
    if error or collection is None:
        return False, f"Failed to get collection: {error}"
    if "access_id" not in record:
        return False, "Record is missing 'access_id' field."
    
    access_id_for_log = record.get("access_id", "UNKNOWN_ACCESS_ID")
    log_prefix_save = f"[SaveMeta-{access_id_for_log}]"

    try:
        update_payload = record.copy()    
        has_id_before_del = '_id' in update_payload
        id_value_before_del = str(update_payload.get('_id')) if has_id_before_del else "N/A"
        logging.info(f"{log_prefix_save} Update payload *before* _id removal. Has _id: {has_id_before_del}. _id value: {id_value_before_del}")

        if '_id' in update_payload:
            del update_payload['_id']
            logging.info(f"{log_prefix_save} Removed '_id' from update_payload.")
        else:
            logging.info(f"{log_prefix_save} '_id' was not in update_payload to begin with (likely an initial insert).")

        result = collection.update_one(
            {"access_id": record["access_id"]},
            {"$set": update_payload}, 
            upsert=True
        )
        
        if result.upserted_id:
            logging.info(f"{log_prefix_save} Successfully INSERTED metadata. New DB _id: {result.upserted_id}")
            return True, f"Metadata inserted (ID: {result.upserted_id})."
        elif result.modified_count > 0:
            logging.info(f"{log_prefix_save} Successfully UPDATED metadata.")
            return True, "Metadata updated."
        elif result.matched_count > 0:
            logging.info(f"{log_prefix_save} Metadata already exists and is identical (no modification needed).")
            return True, "Metadata already up-to-date."
        else:
            logging.warning(f"{log_prefix_save} Upsert neither inserted nor modified (unexpected). Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted ID: {result.upserted_id}")
            return False, "Upsert completed unexpectedly (no change or insert failure)."

    except OperationFailure as of:
        if "would modify the immutable field '_id'" in str(of):
            logging.error(f"{log_prefix_save} OperationFailure: Attempted to modify immutable _id field. This indicates the _id was still in the $set payload somehow. Error: {of}", exc_info=True)
            return False, f"Database error: Attempt to modify immutable field _id. {of}"
        error_msg = f"Database operation failed saving metadata: {of}"
        logging.error(f"{log_prefix_save} {error_msg}", exc_info=True)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error saving metadata: {e}"
        logging.error(f"{log_prefix_save} {error_msg}", exc_info=True)
        return False, error_msg

def find_metadata_by_username(username: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Finds all active metadata records for a given username from the 'user_files' collection.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get collection: {error}"
    try:
        records_cursor = collection.find({"username": username}).sort("upload_timestamp", -1)
        records_list = list(records_cursor)
        for record in records_list:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Found {len(records_list)} active metadata records for username: {username}")
        return records_list, ""
    except OperationFailure as of: error_msg = f"Database op failed finding metadata by username: {of}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding metadata by username: {e}"; logging.exception(error_msg); return None, error_msg

def find_metadata_by_access_id(access_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Finds a single active metadata record by its unique access_id from the 'user_files' collection.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get collection: {error}"
    try:
        record = collection.find_one({"access_id": access_id})
        if record:
            if '_id' in record and isinstance(record['_id'], ObjectId): 
                record['_id'] = str(record['_id'])
            logging.info(f"Found active metadata record for access_id: {access_id}")
            return record, ""
        else:
            logging.info(f"No active metadata record found for access_id: {access_id}")
            return None, "File record not found."
    except OperationFailure as of: error_msg = f"Database op failed finding metadata by access_id: {of}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding metadata by access_id: {e}"; logging.exception(error_msg); return None, error_msg

def delete_metadata_by_filename(username: str, original_filename: str) -> Tuple[int, str]:
    """
    Deletes active metadata record(s) matching a username and original filename from 'user_files'.
    Warning: Could delete multiple if filenames are not unique per user.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return 0, f"Failed to get collection: {error}"
    try:
        result = collection.delete_many({"username": username, "original_filename": original_filename})
        deleted_count = result.deleted_count
        if deleted_count > 0:
            logging.info(f"Deleted {deleted_count} active metadata record(s) for user '{username}', filename '{original_filename}'.")
        else:
            logging.info(f"No active metadata records found to delete for user '{username}', filename '{original_filename}'.")
        return deleted_count, ""
    except OperationFailure as of: error_msg = f"Database op failed deleting metadata: {of}"; logging.exception(error_msg); return 0, error_msg
    except Exception as e: error_msg = f"Unexpected error deleting metadata: {e}"; logging.exception(error_msg); return 0, error_msg

def find_metadata_by_email(user_email: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Finds all active file metadata records for a given user email from 'user_files'.
    Assumes 'user_email' field exists in the records.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get metadata collection: {error}"
    try:
        records_cursor = collection.find({"user_email": user_email.lower()}).sort("upload_timestamp", -1)
        records_list = list(records_cursor)
        for record in records_list:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Found {len(records_list)} active metadata records for email: {user_email}")
        return records_list, ""
    except PyMongoError as e: error_msg = f"PyMongoError finding metadata by email '{user_email}': {e}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding metadata by email '{user_email}': {e}"; logging.exception(error_msg); return None, error_msg

def delete_metadata_by_access_id(access_id: str) -> Tuple[int, str]:
    """
    Deletes a single active metadata record matching the unique access_id from 'user_files'.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return 0, f"Failed to get metadata collection: {error}"
    try:
        result = collection.delete_one({"access_id": access_id})
        deleted_count = result.deleted_count
        if deleted_count == 1:
            logging.info(f"Deleted active metadata record for access_id '{access_id}'.")
        elif deleted_count == 0:
            logging.warning(f"No active metadata record found to delete for access_id '{access_id}'.")
        return deleted_count, ""
    except PyMongoError as e: error_msg = f"PyMongoError deleting metadata by access_id '{access_id}': {e}"; logging.exception(error_msg); return 0, error_msg
    except Exception as e: error_msg = f"Unexpected error deleting metadata by access_id '{access_id}': {e}"; logging.exception(error_msg); return 0, error_msg

def get_all_file_metadata(search_query: Optional[str] = None, user_type_filter: Optional[str] = None) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Retrieves all documents from the 'user_files' (active metadata) collection,
    optionally filtered by a search query.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        logging.error(f"Failed to get metadata collection for get_all_file_metadata: {error}")
        return None, f"Database error: {error}"
    query_conditions = []
    final_query = {}

    if user_type_filter and user_type_filter.strip():
        cleaned_user_type = user_type_filter.strip()
        logging.info(f"Filtering file metadata by user_type: '{cleaned_user_type}'")

        if cleaned_user_type == "Anonymous":
            query_conditions.append({"is_anonymous": True})
        elif cleaned_user_type == "Premium" or cleaned_user_type == "Free":
            role_for_usernames = "Premium User" if cleaned_user_type == "Premium" else "Free User"
            users_of_type, users_err = get_all_users(role_filter=role_for_usernames)
            
            if users_err:
                logging.error(f"Error fetching {role_for_usernames} usernames for file filter: {users_err}")
                return None, f"Error fetching user data for {cleaned_user_type} filter: {users_err}"
            
            if users_of_type:
                usernames = [user['username'] for user in users_of_type if user.get('username')]
                if usernames:
                    query_conditions.append({"username": {"$in": usernames}, "is_anonymous": {"$ne": True}}) 
                else:
                    logging.info(f"No usernames found for role '{role_for_usernames}'. No files will match this user_type.")
                    return [], ""
            else:
                logging.info(f"No users found with role '{role_for_usernames}'. No files will match this user_type.")
                return [], ""
        else:
            logging.warning(f"Unknown user_type_filter: '{cleaned_user_type}'. Ignoring this filter.")
    if search_query and search_query.strip():
        search_term = search_query.strip()
        try:
            import re 
            escaped_search_term = re.escape(search_term)
            regex_pattern = re.compile(escaped_search_term, re.IGNORECASE)
        except ImportError: 
            regex_pattern = search_term 

        search_condition = {
            "$or": [
                {"access_id": {"$regex": regex_pattern}},
                {"original_filename": {"$regex": regex_pattern}},
                {"batch_display_name": {"$regex": regex_pattern}},
                {"username": {"$regex": regex_pattern}} 
            ]
        }
        query_conditions.append(search_condition)
        logging.info(f"Adding search query to file metadata filter: '{search_term}'")

    # 3. Combine conditions
    if len(query_conditions) > 1:
        final_query = {"$and": query_conditions}
    elif len(query_conditions) == 1:
        final_query = query_conditions[0]
    
    logging.info(f"Final file metadata query to MongoDB: {final_query}")

    try:
        records_cursor = collection.find(final_query).sort([("upload_timestamp", -1)]) 
        records_list = list(records_cursor)
        for record in records_list:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Retrieved {len(records_list)} file metadata record(s) with current filters.")
        return records_list, ""
    except PyMongoError as e: error_msg = f"PyMongoError fetching file metadata: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error fetching file metadata: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg

logging.info("Active file models module (database/file_models.py) initialized.")

def _find_best_file_id(locations: list, primary_chat_id: str) -> Optional[str]:
    """
    Helper to find the file_id from the primary chat, with a fallback.
    This version correctly parses the flattened data structure saved by _parse_send_results.
    """
    primary_file_id = None
    fallback_file_id = None
    
    if not isinstance(locations, list):
        return None

    for loc in locations:
        if not isinstance(loc, dict):
            continue
        file_id = loc.get('file_id')

        if file_id:
            if str(loc.get('chat_id')) == str(primary_chat_id):
                primary_file_id = file_id
                break 
            if not fallback_file_id:
                fallback_file_id = file_id
                
    return primary_file_id or fallback_file_id


def get_file_chunks_data(batch_id: str, filename: str) -> Tuple[Optional[List[bytes]], str]:
    """
    Finds a batch record, then finds the specific file within it,
    and intelligently parses its location data to download all chunks.
    This is designed to work with the existing upload_routes.py logic.
    """
    # 1. Fetch the main batch record
    batch_record, error = find_metadata_by_access_id(batch_id)
    if error or not batch_record:
        return None, error or "Batch record not found."

    # 2. Find the specific file's details within the batch
    file_detail_to_process = None
    for f in batch_record.get("files_in_batch", []):
        if f.get("original_filename") == filename:
            file_detail_to_process = f
            break
    
    if not file_detail_to_process:
        return None, f"File '{filename}' not found within batch '{batch_id}'."

    # 3. Intelligently parse the location data to get a list of file_ids
    list_of_file_ids_to_download = []
    
    # Case A: It's a large, chunked file
    if file_detail_to_process.get('is_split_for_telegram'):
        telegram_chunks = file_detail_to_process.get('telegram_chunks', [])
        if not telegram_chunks:
            return None, "Record is marked as chunked but is missing 'telegram_chunks' data."
            
        for chunk in sorted(telegram_chunks, key=lambda c: c.get('part_number', 0)):
            best_id = _find_best_file_id(chunk.get('send_locations', []), PRIMARY_TELEGRAM_CHAT_ID)
            if not best_id:
                return None, f"Could not find a valid file_id for chunk number {chunk.get('part_number')}."
            list_of_file_ids_to_download.append(best_id)

    # Case B: It's a small, single file
    elif file_detail_to_process.get('telegram_send_locations'):
        locations = file_detail_to_process.get('telegram_send_locations', [])
        best_id = _find_best_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
        if not best_id:
            return None, "Could not find a valid file_id in 'telegram_send_locations'."
        list_of_file_ids_to_download.append(best_id)

    # Case C: The record is incomplete
    else:
        return None, "Record is incomplete; it has neither 'telegram_chunks' nor 'telegram_send_locations'."

    if not list_of_file_ids_to_download:
        return None, "Successfully parsed record, but found no valid file_ids to download."

    # 4. Download all the parts we found
    try:
        all_chunks_data = []
        for i, file_id in enumerate(list_of_file_ids_to_download):
            chunk_data, dl_error = download_telegram_file_content(file_id)
            if dl_error:
                logging.error(f"Failed to download chunk {i+1} (file_id: {file_id}) for '{filename}': {dl_error}")
                return None, f"Failed to download part {i+1}: {dl_error}"
            all_chunks_data.append(chunk_data)
        
        logging.info(f"Successfully downloaded {len(all_chunks_data)} parts for file '{filename}'.")
        return all_chunks_data, ""
    except Exception as e:
        logging.error(f"Unexpected error during download phase for '{filename}': {e}", exc_info=True)
        return None, "An unexpected error occurred during content download."