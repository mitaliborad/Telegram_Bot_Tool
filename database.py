# --- START OF FILE database.py ---

import os
import urllib.parse
import logging
from pymongo import MongoClient # Corrected import casing
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List, Tuple

# --- Load Environment Variables ---
# EXPECTS .env file with: ATLAS_USER, ATLAS_PASSWORD, ATLAS_CLUSTER_HOST
load_dotenv()

# --- Configuration ---
DATABASE_NAME = "Telegrambot"       # Your chosen database name
COLLECTION_NAME = "file_metadata"      # Name for the collection storing file info

# --- MongoDB Connection Setup ---
_client: Optional[MongoClient] = None
_db: Optional[Database] = None
_collection: Optional[Collection] = None

def _connect_to_db() -> Tuple[Optional[MongoClient], str]:
    """
    Establishes a connection to MongoDB Atlas using environment variables.
    Returns the client instance and an error message if connection fails.
    """
    global _client # Allow modification of the global variable

    if _client:
        # Optional: Add a ping check here if you want to verify existing connection
        # try:
        #     _client.admin.command('ping')
        #     return _client, "" # Already connected and ping successful
        # except ConnectionFailure:
        #     logging.warning("Existing MongoDB client failed ping. Attempting reconnect.")
        #     _client = None # Force reconnect
        # except Exception as e:
        #      logging.error(f"Error pinging existing MongoDB client: {e}")
        #      return _client, f"Error pinging existing MongoDB client: {e}" # Return client but signal potential issue
         return _client, "" 


    ATLAS_USER = os.getenv("ATLAS_USER")
    ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
    ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")
    print(f"--- DEBUG: Connecting with HOST = '{ATLAS_CLUSTER_HOST}' ---")

    if not ATLAS_USER: # Added check
         error_msg = "Database Error: ATLAS_USER environment variable not set or empty."
         logging.critical(error_msg)
         return None, error_msg
    if not ATLAS_PASSWORD: # Added check
         error_msg = "Database Error: ATLAS_PASSWORD environment variable not set or empty."
         logging.critical(error_msg)
         return None, error_msg
    if not ATLAS_CLUSTER_HOST: # Existing check is fine
         error_msg = "Database Error: ATLAS_CLUSTER_HOST environment variable not set or empty."
         logging.critical(error_msg)
         return None, error_msg
    
    encoded_user = urllib.parse.quote_plus(ATLAS_USER)
    encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)
    
    try:
        # URL Encode username and password
        encoded_user = urllib.parse.quote_plus(ATLAS_USER)
        encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)

        # Construct the connection string
        CONNECTION_STRING = f"mongodb+srv://{encoded_user}:{encoded_password}@{ATLAS_CLUSTER_HOST}/?retryWrites=true&w=majority&appName=Telegrambot"

        logging.info(f"Attempting to connect to MongoDB Atlas host: {ATLAS_CLUSTER_HOST}...")
        client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))

        # Ping to confirm connection
        client.admin.command('ping')
        logging.info("✅ Successfully connected and pinged MongoDB Atlas!")
        _client = client # Store the client globally
        return _client, "" # Return client, empty error message

    except ConnectionFailure as cf:
        error_msg = f"MongoDB Connection Failure: {cf}"
        logging.error(error_msg)
        return None, error_msg
    except OperationFailure as of: # Handles auth errors, etc. during ping
         error_msg = f"MongoDB Operation Failure (Auth/Permissions?): {of}"
         logging.error(error_msg)
         return None, error_msg
    except Exception as e:
        error_msg = f"An unexpected error occurred during MongoDB connection: {e}"
        logging.exception(error_msg) # Log full traceback for unexpected errors
        return None, error_msg

def get_db() -> Tuple[Optional[Database], str]:
    """
    Gets the database instance, connecting if necessary.
    Returns the database instance and an error message.
    """
    global _db
    if _db:
        return _db, ""

    client, error = _connect_to_db()
    if error or not client:
        return None, error

    try:
        _db = client[DATABASE_NAME]
        logging.info(f"Accessed database: {DATABASE_NAME}")
        return _db, ""
    except Exception as e:
        error_msg = f"Error accessing database '{DATABASE_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def get_metadata_collection() -> Tuple[Optional[Collection], str]:
    """
    Gets the file_metadata collection instance, connecting if necessary.
    Returns the collection instance and an error message.
    """
    global _collection
    if _collection is not None:
        return _collection, ""

    db_instance, error = get_db()
    if error or db_instance is None:
        return None, error

    try:
        _collection = db_instance[COLLECTION_NAME]
        logging.info(f"Accessed collection: {COLLECTION_NAME}")
        return _collection, ""
    except Exception as e:
        error_msg = f"Error accessing collection '{COLLECTION_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

# --- Application Specific Database Functions ---

def save_file_metadata(record: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Saves a single file upload record (document) to the metadata collection.
    Overwrites existing record if one with the same 'access_id' exists (upsert).

    Args:
        record: A dictionary containing the metadata for one uploaded file.
                Must include an 'access_id' key.

    Returns:
        A tuple (success: bool, message: str)
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return False, f"Failed to get collection: {error}"

    if "access_id" not in record:
        return False, "Record is missing 'access_id' field."

    try:
        # Use update_one with upsert=True to insert or replace based on access_id
        result = collection.update_one(
            {"access_id": record["access_id"]}, # Filter: find by unique access_id
            {"$set": record},                   # Data: the entire new record
            upsert=True                         # Insert if doesn't exist
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
            # This case should ideally not be reached with upsert=True unless there's a race condition or unusual setup
             logging.warning(f"Upsert for access_id {record['access_id']} neither inserted nor modified.")
             return False, "Upsert completed unexpectedly (no change)."


    except OperationFailure as of:
        error_msg = f"Database operation failed saving metadata: {of}"
        logging.exception(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error saving metadata: {e}"
        logging.exception(error_msg)
        return False, error_msg

def find_metadata_by_username(username: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Finds all metadata records for a given username.

    Args:
        username: The username to search for.

    Returns:
        A tuple (list_of_records or None, error_message)
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get collection: {error}"

    try:
        # Find all documents where the 'username' field matches
        records_cursor = collection.find({"username": username})
        records_list = list(records_cursor) # Convert cursor to list
        logging.info(f"Found {len(records_list)} metadata records for username: {username}")
        return records_list, ""

    except OperationFailure as of:
        error_msg = f"Database operation failed finding metadata by username: {of}"
        logging.exception(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error finding metadata by username: {e}"
        logging.exception(error_msg)
        return None, error_msg

def find_metadata_by_access_id(access_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Finds a single metadata record by its unique access_id.

    Args:
        access_id: The unique access ID to search for.

    Returns:
        A tuple (record_dictionary or None, error_message)
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return None, f"Failed to get collection: {error}"

    try:
        # Find one document where the 'access_id' field matches
        record = collection.find_one({"access_id": access_id})
        if record:
            logging.info(f"Found metadata record for access_id: {access_id}")
            return record, ""
        else:
            logging.info(f"No metadata record found for access_id: {access_id}")
            return None, "File record not found." # Specific message for not found

    except OperationFailure as of:
        error_msg = f"Database operation failed finding metadata by access_id: {of}"
        logging.exception(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error finding metadata by access_id: {e}"
        logging.exception(error_msg)
        return None, error_msg

def delete_metadata_by_filename(username: str, original_filename: str) -> Tuple[int, str]:
    """
    Deletes metadata record(s) matching a username and original filename.
    Note: This could potentially delete multiple records if a user uploads
          files with the same name. Consider using access_id for unique deletion.

    Args:
        username: The username.
        original_filename: The original filename stored in the record.

    Returns:
        A tuple (number_of_deleted_records, error_message)
    """
    collection, error = get_metadata_collection()
    if error or collection is None:
        return 0, f"Failed to get collection: {error}"

    try:
        # Delete documents matching both username and original_filename
        result = collection.delete_many({
            "username": username,
            "original_filename": original_filename
        })
        deleted_count = result.deleted_count
        if deleted_count > 0:
            logging.info(f"Deleted {deleted_count} metadata record(s) for user '{username}', filename '{original_filename}'.")
        else:
             logging.info(f"No metadata records found to delete for user '{username}', filename '{original_filename}'.")
        return deleted_count, ""

    except OperationFailure as of:
        error_msg = f"Database operation failed deleting metadata: {of}"
        logging.exception(error_msg)
        return 0, error_msg
    except Exception as e:
        error_msg = f"Unexpected error deleting metadata: {e}"
        logging.exception(error_msg)
        return 0, error_msg

def close_db_connection():
    """Closes the MongoDB client connection if it's open."""
    global _client
    if _client:
        try:
            _client.close()
            _client = None # Reset global variable
            logging.info("MongoDB connection closed.")
        except Exception as e:
            logging.error(f"Error closing MongoDB connection: {e}")

# --- Optional: Test Connection on Module Load (or call explicitly elsewhere) ---
# _, initial_conn_error = _connect_to_db()
# if initial_conn_error:
#     logging.error(f"Initial MongoDB connection failed on module load: {initial_conn_error}")
    # Depending on your app's needs, you might exit here or handle later

logging.info("Database module initialized.")

