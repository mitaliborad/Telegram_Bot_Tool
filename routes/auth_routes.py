"""Flask routes and core logic for the Telegram File Storage tool."""
import io
import os
import uuid
import time
import json
import zipfile
import tempfile
import shutil
from flask import send_from_directory
import logging
from bson import ObjectId
from database import User
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List, Generator, Union
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from flask import ( Blueprint,Flask, request, render_template, flash, redirect, url_for,
    make_response, jsonify, send_file, Response, stream_with_context )
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone
import re
from dateutil import parser as dateutil_parser
import io
from flask_jwt_extended import jwt_required, get_jwt_identity
import threading
import database
from database import User, find_user_by_id, find_user_by_email, find_user_by_username, save_user
from config import format_time
from flask_cors import CORS
from config import app 
from extensions import login_manager
from extensions import upload_progress_data
from extensions import download_prep_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, MAX_DOWNLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES
    ,format_bytes
)
from telegram_api import send_file_to_telegram, download_telegram_file_content
from flask_jwt_extended import create_access_token

# --- Type Aliases ---
Metadata = Dict[str, List[Dict[str, Any]]]
UploadProgressData = Dict[str, Any]; DownloadPrepData = Dict[str, Any]
SseEvent = str; ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
ChunkDataResult = Tuple[int, Optional[bytes], Optional[str]]

# --- Constants ---
DEFAULT_CHUNK_READ_SIZE = 4 * 1024 * 1024; STREAM_CHUNK_SIZE = 65536
auth_bp = Blueprint('auth', __name__)
admin_auth_bp = Blueprint('admin_auth', __name__)

def _ensure_username_in_user_doc(user_doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if user_doc and 'username' not in user_doc:
        doc_id = user_doc.get('_id', 'N/A')
        logging.warning(f"User document for _id {doc_id} is missing 'username'. Attempting to derive.")
        if 'email' in user_doc and isinstance(user_doc['email'], str):
            user_doc['username'] = user_doc['email'].split('@')[0]
            logging.info(f"Derived username '{user_doc['username']}' from email for _id {doc_id}.")
        elif 'firstName' in user_doc and 'lastName' in user_doc: # Fallback if email isn't suitable/present
            derived_username = f"{user_doc.get('firstName', '')}{user_doc.get('lastName', '')}".replace(" ", "").lower()
            if derived_username:
                user_doc['username'] = derived_username
                logging.info(f"Derived username '{user_doc['username']}' from firstName/lastName for _id {doc_id}.")
            else:
                # Last resort placeholder if derivation fails
                user_doc['username'] = f"user_{str(doc_id)[-6:]}" if doc_id != 'N/A' else "unknown_user"
                logging.warning(f"Could not derive a meaningful username for _id {doc_id}, using placeholder '{user_doc['username']}'.")
        else:
            # This is a critical situation if no username can be set.
            # The User class will likely still fail.
            logging.error(f"Cannot derive username for user document . User class instantiation will likely fail.")
            # To prevent a crash if User class is extremely strict, you might return None or raise an error here.
            # For now, we'll let it proceed, and the User class will raise its own error if it's still unhappy.
    return user_doc

@login_manager.user_loader
def load_user(user_id: str): 
    logging.debug(f"Attempting to load user with ID: {user_id}")
    if not user_id:
        return None
    try:
        user_obj_id = ObjectId(user_id) 
    except Exception:
        logging.error(f"Invalid ObjectId format for user_id: {user_id}")
        return None
    
    user_doc, error = find_user_by_id(user_obj_id) # find_user_by_id needs to be robust
    if error:
         logging.error(f"Error loading user by ID {user_id}: {error}")
         return None
    
    if user_doc:
        user_doc = _ensure_username_in_user_doc(user_doc) 
        if not user_doc or 'username' not in user_doc: 
            logging.error(f"Failed to ensure username for user_id {user_id}. User doc after attempt: {user_doc}")
            return None
        try:
            return User(user_doc)
        except ValueError as ve:
            logging.error(f"Failed to instantiate User for ID {user_id}: {ve}. Document was: {user_doc}")
            return None
    return None


@auth_bp.route('/register', methods=['GET'])
def show_register_page():
    logging.info("Serving registration page.")
    try:
        return render_template('register.html') # Assumes register.html is in templates/auth/
    except Exception as e:
        logging.error(f"Error rendering register.html: {e}", exc_info=True)
        return make_response("Error loading page.", 500)
    
@auth_bp.route('/register', methods=['POST'])
def register_user(): 
    logging.info("Received POST request for /register")
    try:
        data = request.get_json()
        if not data:
            logging.warning("Registration failed: No JSON data received.")
            return make_response(jsonify({"error": "Invalid request format. Expected JSON."}), 400)
        username = data.get('username', '').strip() 
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        confirmPassword = data.get('confirmPassword', '')
        agreeTerms = data.get('agreeTerms', False)
        understand_privacy = data.get('understandPrivacy', False)
    except Exception as e:
        logging.error(f"Error parsing registration JSON data: {e}", exc_info=True)
        return make_response(jsonify({"error": "Invalid request data received."}), 400)

    if not all([username, email, password, confirmPassword]):
        return make_response(jsonify({"error": "Username, email, and passwords are required."}), 400)
    if not re.match(r"^[a-zA-Z0-9_]{3,}$", username):
        return make_response(jsonify({"error": "Invalid username format (letters, numbers, _, min 3 chars)."}), 400)
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return make_response(jsonify({"error": "Invalid email format."}), 400)
    if password != confirmPassword:
        return make_response(jsonify({"error": "Passwords do not match."}), 400)
    if not (agreeTerms and understand_privacy):
        return make_response(jsonify({"error": "You must agree to all terms and privacy conditions."}), 400)

    try:
        existing_user_by_username, db_error_uname = find_user_by_username(username)
        if db_error_uname: raise Exception(db_error_uname) 
        if existing_user_by_username:
            return make_response(jsonify({"error": "Username is already taken."}), 409) 
        
        existing_user_by_email, db_error_email = find_user_by_email(email)
        if db_error_email: raise Exception(db_error_email)
        if existing_user_by_email:
            return make_response(jsonify({"error": "An account with this email address already exists."}), 409)
    except Exception as e:
        logging.error(f"Database error checking username/email '{username}/{email}': {e}", exc_info=True)
        return make_response(jsonify({"error": "Server error during registration check."}), 500)

    try:
        hashed_pw = generate_password_hash(password, method='pbkdf2:sha256')
    except Exception as e:
        logging.error(f"Password hashing failed: {e}", exc_info=True)
        return make_response(jsonify({"error": "Server error during registration processing."}), 500)

    new_user_data = {
        "username": username, "email": email, "password_hash": hashed_pw,
        "created_at": datetime.now(timezone.utc),
        "agreed_terms": agreeTerms, "understand_privacy": understand_privacy
    }
    try:
        save_success, save_msg = save_user(new_user_data)
        if not save_success:
            logging.error(f"Failed to save new user '{username}' / '{email}': {save_msg}")
            error_to_return = "Server error saving registration. Please try again."
            if "duplicate key error" in save_msg.lower():
                 if "username_1" in save_msg: error_to_return = "Username is already taken."
                 elif "email_1" in save_msg: error_to_return = "An account with this email address already exists."
                 else: error_to_return = "Username or Email already exists."
                 return make_response(jsonify({"error": error_to_return}), 409)
            return make_response(jsonify({"error": error_to_return}), 500)
    except Exception as e:
        logging.error(f"Unexpected error during database save for user '{username}': {e}", exc_info=True)
        return make_response(jsonify({"error": "Critical server error during final registration step."}), 500)

    logging.info(f"User '{username}' registered successfully.")
    return make_response(jsonify({
        "message": "Registration successful!",
        "user": {"username": username, "email": email}
    }), 201) 
    
    
@auth_bp.route('/api/auth/login', methods=['POST'])
def api_login():
    logging.info("Received POST request for /api/auth/login")
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"error": "Invalid request format. Expected JSON."}), 400)
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
    except Exception as e:
        logging.error(f"Error parsing API login JSON data: {e}", exc_info=True)
        return make_response(jsonify({"error": "Invalid request data received."}), 400)

    if not email or not password:
        return make_response(jsonify({"error": "Email and password are required."}), 400)

    logging.info(f"API Login attempt for email: {email}")
    user_doc, db_error = find_user_by_email(email)

    if db_error:
        logging.error(f"Database error during API login for {email}: {db_error}")
        return make_response(jsonify({"error": "Internal server error during login."}), 500)

    user_obj = None
    if user_doc:
        user_doc = _ensure_username_in_user_doc(user_doc)
        if not user_doc or 'username' not in user_doc:
            logging.error(f"Failed to ensure username for login (email: {email}). Doc: {user_doc}")
            return make_response(jsonify({"error": "Login failed due to inconsistent user data (username)."}), 500)
        try:
            user_obj = User(user_doc)
        except ValueError as e:
            logging.error(f"Failed to create User object for {email} from doc {user_doc}: {e}", exc_info=True)
            return make_response(jsonify({"error": "Login failed due to inconsistent user data."}), 500)

    if user_obj and user_obj.check_password(password):
        try:
            identity = user_obj.get_id()
            if not identity:
                logging.error(f"User object for {email} missing ID.")
                return make_response(jsonify({"error": "Internal server error - user identity missing."}), 500)
            access_token = create_access_token(identity=identity) 
            response_username = user_obj.username
            logging.info(f"User '{response_username}' ({email}) logged in successfully via API. Token created.")
            return make_response(jsonify({
                "message": "Login successful!",
                "user": {"username": response_username, "email": user_obj.email, "id": identity },
                "token": access_token  
            }), 200) 
        except Exception as e:
            logging.error(f"Error creating JWT for user {email}: {e}", exc_info=True)
            return make_response(jsonify({"error": "Internal server error during token generation."}), 500)
    else:
        logging.warning(f"Failed API login attempt for email: {email} (Invalid credentials or user not found)")
        return make_response(jsonify({"error": "Invalid email or password."}), 401)
    
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        if not email or not password:
            flash('Please enter both email and password.', 'warning')
            return render_template('login.html') # Assumes login.html in templates/auth/

        logging.info(f"Login attempt for email: {email}")
        user_doc, db_error = find_user_by_email(email)
        if db_error:
            logging.error(f"Database error during login for {email}: {db_error}")
            flash('An internal error occurred. Please try again later.', 'danger')
            return render_template('login.html')

        user_obj = None
        if user_doc:
             user_doc = _ensure_username_in_user_doc(user_doc) # Ensure username
             if not user_doc or 'username' not in user_doc:
                 logging.error(f"Login failed for {email}: username missing after ensure step.")
                 flash('Login failed due to inconsistent user data. Please contact support.', 'danger')
                 return render_template('login.html')
             try:
                user_obj = User(user_doc)
             except ValueError as e:
                 logging.error(f"Failed to create User object for {email}: {e}", exc_info=True)
                 flash('Login failed due to inconsistent user data. Please contact support.', 'danger')
                 return render_template('login.html')
        
        if user_obj and user_obj.check_password(password):
            login_user(user_obj, remember=False) # Set remember=True if you want "Remember Me" functionality
            logging.info(f"User '{user_obj.username}' ({email}) logged in successfully.")
            flash(f'Welcome back, {user_obj.username}!', 'success')
            next_page = request.args.get('next')
            # Basic protection against open redirect
            if next_page and (next_page.startswith('/') or next_page.startswith(request.host_url)):
                return redirect(next_page)
            return redirect(url_for('file.list_user_files_page')) # Example: redirect to a user's file page (adjust endpoint)
        else:
            logging.warning(f"Failed login attempt for email: {email}")
            flash('Invalid email or password. Please try again.', 'danger')
            return render_template('login.html')
    return render_template('auth/login.html')


@auth_bp.route('/logout')
# @login_required 
def logout():
    user_email_display = "User"
    if current_user and current_user.is_authenticated:
        user_email_display = current_user.email
    logout_user() 
    logging.info(f"{user_email_display} logged out.")
    flash('You have been successfully logged out.', 'success')
    return redirect(url_for('auth.login')) # Redirect to login page in this blueprint