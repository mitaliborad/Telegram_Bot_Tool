# database/__init__.py

# --- Re-export core connection and collection access functions ---
from .connection import (
    get_db,
    close_db_connection,
    get_userinfo_collection,
    get_metadata_collection,
    get_archived_files_collection
)

# --- Re-export User model and key user-related functions ---
from .user_models import (
    User,
     get_all_users,
     find_user_by_id,
     find_user_by_id_str,
     find_user_by_email,
     find_user_by_username,
     save_user,
     update_user_password,
     delete_user_by_id,
     update_user_admin_status,
     update_user_details,
     find_user_by_email_excluding_id,    
     find_user_by_username_excluding_id
)

# --- Re-export Active File model functions ---
from .file_models import (
    save_file_metadata,
    find_metadata_by_username,       # For active files
    find_metadata_by_access_id,      # For active files
    delete_metadata_by_filename,     # For active files
    find_metadata_by_email,          # For active files
    delete_metadata_by_access_id,    # For active files
    get_all_file_metadata            # For active files
)

# --- Re-export Archived File model functions ---
from .archive_models import (
    # Renaming for clarity if functions have similar names but operate on different collections
    # For example, if find_metadata_by_username existed in archive_models for archived files:
    # find_archived_metadata_by_username as find_archived_files_by_username,
    find_archived_metadata_by_username, # Already distinct enough
    find_archived_metadata_by_access_id, # Already distinct enough
    delete_archived_metadata_by_access_id, # Already distinct enough
    get_all_archived_files
)

# --- Re-export Common Cross-Collection Operations ---
from .common_operations import (
    archive_file_record_by_access_id
)

import logging
logging.info("Database package (database/__init__.py) fully initialized and re-exporting components.")