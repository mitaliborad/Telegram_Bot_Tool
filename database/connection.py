# database/connection.py
import os
import urllib.parse
import logging
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv
from typing import Optional, Tuple

load_dotenv()

DATABASE_NAME = "Telegrambot"
USERINFO_COLLECTION_NAME = "userinfo"
METADATA_COLLECTION_NAME = "user_files"
ARCHIVED_COLLECTION_NAME = "archived_files"
ARCHIVED_USERS_COLLECTION_NAME = "archived_users"

_client: Optional[MongoClient] = None
_db: Optional[Database] = None
_userinfo_collection: Optional[Collection] = None
_metadata_collection: Optional[Collection] = None
_archived_files_collection: Optional[Collection] = None
_archived_users_collection: Optional[Collection] = None

def _connect_to_db() -> Tuple[Optional[MongoClient], str]:
    """
    Establishes a connection to MongoDB Atlas using environment variables.
    Returns the client instance and an error message if connection fails.
    """
    global _client

    if _client is not None:
        return _client, ""

    ATLAS_USER = os.getenv("ATLAS_USER")
    ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
    ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")
    logging.info(f"--- DEBUGGING DB CONNECTION ATTEMPT ---")
    logging.info(f"Read ATLAS_USER: '{ATLAS_USER}'")
    logging.info(f"Read ATLAS_PASSWORD: {'******' if ATLAS_PASSWORD else 'NOT FOUND'}")
    logging.info(f"Read ATLAS_CLUSTER_HOST: '{ATLAS_CLUSTER_HOST}'")

    if not ATLAS_USER:
        error_msg = "Database Error: ATLAS_USER environment variable not set or empty."
        logging.critical(error_msg)
        return None, error_msg
    if not ATLAS_PASSWORD:
        error_msg = "Database Error: ATLAS_PASSWORD environment variable not set or empty."
        logging.critical(error_msg)
        return None, error_msg
    if not ATLAS_CLUSTER_HOST:
         error_msg = "Database Error: ATLAS_CLUSTER_HOST environment variable not set or empty."
         logging.critical(error_msg)
         return None, error_msg

    try:
        encoded_user = urllib.parse.quote_plus(ATLAS_USER)
        encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)
        
        # Construct connection string
        CONNECTION_STRING = f"mongodb+srv://{encoded_user}:{encoded_password}@{ATLAS_CLUSTER_HOST}/?retryWrites=true&w=majority&appName=Telegrambot"
        client_instance = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))

        # Ping to confirm a successful connection.
        client_instance.admin.command('ping')
        logging.info("✅ Successfully connected and pinged MongoDB Atlas!")
        _client = client_instance 
        return _client, ""

    except ConnectionFailure as cf:
        error_msg = (f"MongoDB Connection Failure: {cf}. "
                     f"Hints: Check network connectivity to '{ATLAS_CLUSTER_HOST}', "
                     f"IP whitelist configuration on Atlas, and Atlas cluster status.")
        logging.error(error_msg, exc_info=True) 
        return None, error_msg
    except OperationFailure as of:
        error_msg = (f"MongoDB Operation Failure: {of}. "
                     f"Hints: Check authentication credentials (user/password), "
                     f"database/collection permissions, or if the cluster supports Stable API features used.")
        logging.error(error_msg, exc_info=True)
        return None, error_msg
    except Exception as e:
        error_msg = f"An unexpected error occurred during MongoDB connection: {e}"
        logging.error(error_msg, exc_info=True)
        return None, error_msg

def get_db() -> Tuple[Optional[Database], str]:
    """
    Gets the database instance, connecting if necessary.
    Returns the database instance and an error message.
    """
    global _db
    if _db is not None:
        return _db, ""

    client_instance, error = _connect_to_db()
    if error or not client_instance:
        return None, error 

    try:
        _db = client_instance[DATABASE_NAME]
        logging.info(f"Accessed database: {DATABASE_NAME}")
        return _db, ""
    except Exception as e:
        error_msg = f"Error accessing database '{DATABASE_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def get_userinfo_collection() -> Tuple[Optional[Collection], str]:
    """
    Gets the userinfo collection instance, connecting if necessary.
    Returns the collection instance and an error message.
    """
    global _userinfo_collection
    if _userinfo_collection is not None:
        return _userinfo_collection, ""

    db_instance, error = get_db()
    if error or db_instance is None:
        return None, error

    try:
        _userinfo_collection = db_instance[USERINFO_COLLECTION_NAME]
        logging.info(f"Accessed collection: {USERINFO_COLLECTION_NAME}")
        return _userinfo_collection, ""
    except Exception as e:
        error_msg = f"Error accessing collection '{USERINFO_COLLECTION_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def get_metadata_collection() -> Tuple[Optional[Collection], str]:
    """
    Gets the file_metadata (user_files) collection instance, connecting if necessary.
    Returns the collection instance and an error message.
    """
    global _metadata_collection
    if _metadata_collection is not None:
        return _metadata_collection, ""

    db_instance, error = get_db()
    if error or db_instance is None:
        return None, error

    try:
        _metadata_collection = db_instance[METADATA_COLLECTION_NAME]
        logging.info(f"Accessed collection: {METADATA_COLLECTION_NAME}")
        return _metadata_collection, ""
    except Exception as e:
        error_msg = f"Error accessing collection '{METADATA_COLLECTION_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def get_archived_files_collection() -> Tuple[Optional[Collection], str]:
    """
    Gets the archived_files collection instance, connecting if necessary.
    Returns the collection instance and an error message.
    """
    global _archived_files_collection
    if _archived_files_collection is not None:
        return _archived_files_collection, ""

    db_instance, error = get_db()
    if error or db_instance is None:
        logging.error(f"Failed to get DB instance for '{ARCHIVED_COLLECTION_NAME}' collection: {error}")
        return None, error

    try:
        _archived_files_collection = db_instance[ARCHIVED_COLLECTION_NAME]
        logging.info(f"Accessed collection: {ARCHIVED_COLLECTION_NAME}")
        return _archived_files_collection, ""
    except Exception as e:
        error_msg = f"Error accessing collection '{ARCHIVED_COLLECTION_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def get_archived_users_collection() -> Tuple[Optional[Collection], str]:
    """
    Gets the archived_users collection instance, connecting if necessary.
    Returns the collection instance and an error message.
    """
    global _archived_users_collection
    if _archived_users_collection is not None:
        return _archived_users_collection, ""

    db_instance, error = get_db()
    if error or db_instance is None:
        logging.error(f"Failed to get DB instance for '{ARCHIVED_USERS_COLLECTION_NAME}' collection: {error}")
        return None, error

    try:
        _archived_users_collection = db_instance[ARCHIVED_USERS_COLLECTION_NAME]
        logging.info(f"Accessed collection: {ARCHIVED_USERS_COLLECTION_NAME}")
        return _archived_users_collection, ""
    except Exception as e:
        error_msg = f"Error accessing collection '{ARCHIVED_USERS_COLLECTION_NAME}': {e}"
        logging.exception(error_msg)
        return None, error_msg

def close_db_connection():
    """Closes the MongoDB client connection if it's open and resets cached objects."""
    global _client, _db, _userinfo_collection, _metadata_collection, _archived_files_collection, _archived_users_collection
    if _client:
        try:
            _client.close()
            logging.info("MongoDB connection closed.")
        except Exception as e:
            logging.error(f"Error closing MongoDB connection: {e}", exc_info=True)
        finally:
            _client = None
            _db = None
            _userinfo_collection = None
            _metadata_collection = None
            _archived_files_collection = None
            _archived_users_collection = None
            logging.info("Global MongoDB client and collection instances reset.")

logging.info("Database connection module (database/connection.py) initialized.")