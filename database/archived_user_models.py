# database/archived_user_models.py
import logging
import re
from typing import Optional, List, Dict, Any, Tuple
from bson import ObjectId
from pymongo.errors import PyMongoError, OperationFailure

from .connection import get_archived_users_collection

def get_all_archived_users(search_query: Optional[str] = None) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Retrieves all documents from the 'archived_users' collection,
    optionally filtered by a search query on original_username or original_email.
    """
    collection, error = get_archived_users_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_users collection for get_all_archived_users: {error}")
        return None, f"Database error: {error}"

    query_filter = {}
    if search_query and search_query.strip():
        search_term = search_query.strip()
        escaped_query = re.escape(search_term)
        regex_pattern = re.compile(escaped_query, re.IGNORECASE)
        query_filter["$or"] = [
            {"original_username": {"$regex": regex_pattern}},
            {"original_email": {"$regex": regex_pattern}}
        ]
        logging.info(f"Searching archived_users with query: '{search_term}' using filter: {query_filter}")
    else:
        logging.info("Fetching all archived_users (no search query / empty search query).")

    try:
        records_cursor = collection.find(query_filter).sort("archived_at", -1)
        records_list = list(records_cursor)
        for record in records_list:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Retrieved {len(records_list)} archived user record(s).")
        return records_list, ""
    except PyMongoError as e:
        error_msg = f"PyMongoError fetching all archived users: {e}"
        logging.error(error_msg, exc_info=True)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error fetching all archived users: {e}"
        logging.error(error_msg, exc_info=True)
        return None, error_msg

def find_archived_user_by_original_id(original_user_id_str: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Finds a single archived user record by their original user_id
    (stored as 'original_user_id' in the archived_users collection).
    """
    collection, error = get_archived_users_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_users collection for original_user_id '{original_user_id_str}': {error}")
        return None, f"Database error: {error}"

    if not original_user_id_str:
        return None, "Original user ID string cannot be empty."

    try:
        record = collection.find_one({"original_user_id": original_user_id_str})
        if record:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
            logging.info(f"Found archived user record for original_user_id: {original_user_id_str}")
            return record, ""
        else:
            logging.info(f"No archived user record found for original_user_id: {original_user_id_str}")
            return None, "Archived user record not found by original ID."
    except OperationFailure as of:
        error_msg = f"DB op failed finding archived user by original_user_id '{original_user_id_str}': {of}"
        logging.exception(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error finding archived user by original_user_id '{original_user_id_str}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def delete_archived_user_permanently(archived_record_id_str: str) -> Tuple[int, str]:
    """
    Deletes a single archived user record from 'archived_users' collection
    using the _id of the archive record itself.
    Returns the number of deleted documents (0 or 1) and an error message.
    """
    collection, error = get_archived_users_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_users collection for deleting archived_record_id '{archived_record_id_str}': {error}")
        return 0, f"Database error: {error}"

    try:
        archive_oid = ObjectId(archived_record_id_str)
    except Exception as e:
        logging.error(f"Invalid ObjectId format for archived_record_id_str '{archived_record_id_str}': {e}")
        return 0, f"Invalid archive record ID format: {archived_record_id_str}"

    try:
        result = collection.delete_one({"_id": archive_oid})
        deleted_count = result.deleted_count
        if deleted_count == 1:
            logging.info(f"Permanently deleted archived user record with _id: {archive_oid}")
        elif deleted_count == 0:
            logging.warning(f"No archived user record found to permanently delete with _id: {archive_oid}")
        return deleted_count, ""
    except PyMongoError as e:
        error_msg = f"PyMongoError permanently deleting archived user record '{archived_record_id_str}': {e}" # Corrected log
        logging.exception(error_msg)
        return 0, error_msg
    except Exception as e:
        error_msg = f"Unexpected error permanently deleting archived user record '{archived_record_id_str}': {e}" # Corrected log
        logging.exception(error_msg)
        return 0, error_msg

logging.info("Archived User models module (database/archived_user_models.py) initialized.")