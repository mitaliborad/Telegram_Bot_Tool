# database/file_models.py
import logging
from typing import Optional, Dict, Any, List, Tuple
from bson import ObjectId
from pymongo.errors import PyMongoError, OperationFailure

# Import the function to get the metadata collection (user_files)
from .connection import get_metadata_collection

# --- Active File Metadata (user_files collection) Functions ---

def save_file_metadata(record: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Saves a single file upload record (document) to the metadata collection.
    Overwrites existing record if one with the same 'access_id' exists (upsert).
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return False, f"Failed to get collection: {error}"
    if "access_id" not in record:
        return False, "Record is missing 'access_id' field."
    try:
        result = collection.update_one(
            {"access_id": record["access_id"]},
            {"$set": record},
            upsert=True
        )
        if result.upserted_id:
            logging.info(f"Successfully inserted metadata for access_id: {record['access_id']}")
            return True, f"Metadata inserted (ID: {result.upserted_id})."
        elif result.modified_count > 0:
            logging.info(f"Successfully updated metadata for access_id: {record['access_id']}")
            return True, "Metadata updated."
        elif result.matched_count > 0:
            logging.info(f"Metadata for access_id {record['access_id']} already exists and is identical.")
            return True, "Metadata already up-to-date."
        else:
            # This case should ideally not be hit if upsert=True and access_id is present.
            # It implies the record was matched but no fields needed updating, and it wasn't a new insert.
            logging.warning(f"Upsert for access_id {record['access_id']} neither inserted nor modified directly (no change or unexpected).")
            return True, "Upsert completed (no change or unexpected state, but considered success)." # Adjusted to True for "no change"
    except OperationFailure as of: error_msg = f"Database operation failed saving metadata: {of}"; logging.exception(error_msg); return False, error_msg
    except Exception as e: error_msg = f"Unexpected error saving metadata: {e}"; logging.exception(error_msg); return False, error_msg

def find_metadata_by_username(username: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Finds all active metadata records for a given username from the 'user_files' collection.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get collection: {error}"
    try:
        # Consider adding sorting, e.g., by upload_timestamp
        records_cursor = collection.find({"username": username}).sort("upload_timestamp", -1)
        records_list = list(records_cursor)
        # Convert ObjectId to str for JSON serialization if necessary (often done in routes)
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
            if '_id' in record and isinstance(record['_id'], ObjectId): # Good practice for consistency
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

def get_all_file_metadata(search_query: Optional[str] = None) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Retrieves all documents from the 'user_files' (active metadata) collection,
    optionally filtered by a search query.
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        logging.error(f"Failed to get metadata collection for get_all_file_metadata: {error}")
        return None, f"Database error: {error}"
    query_filter = {}
    if search_query and search_query.strip():
        search_term = search_query.strip()
        # Assuming 're' module is imported if you need re.escape for complex regex
        # For simple case-insensitive substring search, $regex with $options "i" is fine.
        query_filter["$or"] = [
            {"access_id": {"$regex": search_term, "$options": "i"}},
            {"original_filename": {"$regex": search_term, "$options": "i"}},
            {"batch_display_name": {"$regex": search_term, "$options": "i"}},
            {"username": {"$regex": search_term, "$options": "i"}}
        ]
        logging.info(f"Searching active file metadata with query: '{search_term}' using filter: {query_filter}")
    else:
        logging.info("Fetching all active file metadata (no search query / empty search query).")
    try:
        records_cursor = collection.find(query_filter).sort("upload_timestamp", -1)
        records_list = list(records_cursor)
        for record in records_list:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Retrieved {len(records_list)} active file metadata record(s).")
        return records_list, ""
    except PyMongoError as e: error_msg = f"PyMongoError fetching all active file metadata: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error fetching all active file metadata: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg

logging.info("Active file models module (database/file_models.py) initialized.")