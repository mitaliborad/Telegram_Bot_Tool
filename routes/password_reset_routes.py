# password_reset_routes.py
import logging
from flask import (
    Blueprint, request, jsonify, current_app, make_response, render_template, url_for, flash, redirect) # Added current_app
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from bson.objectid import ObjectId 
from datetime import datetime 
from database import find_user_by_email, find_user_by_id, update_user_password
from config import mail 
from flask_mail import Message
import os
from dotenv import load_dotenv
load_dotenv()

password_reset_bp = Blueprint('password_reset', __name__)

def get_serializer(secret_key=None):
    """Creates and returns a URLSafeTimedSerializer instance."""
    if secret_key is None:
        # Use the app's configured SECRET_KEY
        secret_key = current_app.config.get('SECRET_KEY') 
        if not secret_key:
            logging.critical("CRITICAL: SECRET_KEY is not set in Flask app configuration. Password reset serializer cannot be created.")
            # This will likely cause URLSafeTimedSerializer to fail or use a default weak key if not handled.
            # For robustness, you might want to raise an error here if secret_key is None.
            # raise ValueError("Application SECRET_KEY is not configured for the serializer.")
            # However, to maintain previous behavior if it somehow worked, we let it pass to the serializer.
    return URLSafeTimedSerializer(secret_key)


# --- Route to request a password reset link (API endpoint) ---
@password_reset_bp.route('/api/auth/request-password-reset', methods=['POST', 'OPTIONS'])
def request_password_reset_api(): 
    """
    API endpoint for users to request a password reset link.
    Expects a JSON payload with the user's email.
    """
    if request.method == 'OPTIONS':
        # Flask-CORS should handle preflight. This manual response might be redundant
        # but kept for consistency with original code. A simple make_response(), 204 is also fine.
        response = make_response(jsonify({"message": "CORS preflight successful"}))
        response.status_code = 204 # No Content for preflight
        return response

    if request.method == 'POST':
        data = request.get_json()
        if not data or 'email' not in data:
            logging.warning("API Password reset request failed: Missing email in JSON payload.")
            return make_response(jsonify({"error": "Email is required."}), 400)

        email = data['email'].strip().lower()
        logging.info(f"API Password reset requested for email: {email}")

        user_doc, db_error = find_user_by_email(email)

        if db_error:
            logging.error(f"DB error checking email {email} for API password reset: {db_error}")
            # Still return a generic message to avoid account enumeration
            return make_response(jsonify({"message": "If an account with that email exists, a reset link has been sent."}), 200)

        if not user_doc:
            logging.warning(f"API Password reset attempt for non-existent email: {email}")
            return make_response(jsonify({"message": "If an account with that email exists, a reset link has been sent."}), 200)

        s = get_serializer()
        token_data = {'user_id': str(user_doc['_id']), 'email': email} 
        try:
            token = s.dumps(token_data, salt='password-reset-salt')
            logging.info(f"Generated password reset token for {email} (user_id: {user_doc['_id']})")
        except Exception as e:
            logging.error(f"Error generating token for {email}: {e}", exc_info=True)
            return make_response(jsonify({"error": "Server error: Could not create reset token."}), 500)

        frontend_url = os.environ.get('FRONTEND_URL')
        if not frontend_url:
            logging.error("FRONTEND_URL environment variable is not set. Password reset links will be broken.")
            # Decide if you want to proceed or return an error. Proceeding will send a bad link.
            # For now, it proceeds, but this is a critical configuration.
            frontend_url = "YOUR_FRONTEND_URL_NOT_SET" # Placeholder to make the f-string work

        reset_url = f"{frontend_url}/reset-password/{token}"
        
        # Ensure PASSWORD_RESET_TOKEN_MAX_AGE is an int for display
        try:
            token_max_age_display = int(os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', 3600)) // 60
        except ValueError:
            token_max_age_display = 3600 // 60 # Default to 60 minutes if parsing fails
            logging.warning("Could not parse PASSWORD_RESET_TOKEN_MAX_AGE as int for email display, using default.")


        subject = "Password Reset Request - Telegram File Storage" 
        username_display = user_doc.get('username', 'User')
        html_body = f"""
        <p>Hello {username_display},</p>
        <p>You (or someone else) requested a password reset for your account.</p>
        <p>If this was you, please click the link below to set a new password:</p>
        <p><a href="{reset_url}">Reset Your Password</a></p>
        <p>This link will expire in approximately {token_max_age_display} minutes.</p>
        <p>If you did not request this, please ignore this email. Your password will not be changed.</p>
        <p>Thanks,<br>The Telegram File Storage Team</p>
        <hr>
        <p><small>If you're having trouble clicking the link, copy and paste this URL into your browser:<br>{reset_url}</small></p>
        """
        text_body = f"""
        Hello {username_display},

        You (or someone else) requested a password reset for your account.
        If this was you, please copy and paste the following link into your browser to set a new password:
        {reset_url}

        This link will expire in approximately {token_max_age_display} minutes.

        If you did not request this, please ignore this email. Your password will not be changed.

        Thanks,
        The Telegram File Storage Team
        """

        msg = Message(subject, recipients=[email], body=text_body, html=html_body)
        try:
            mail.send(msg)
            logging.info(f"Password reset email successfully sent to {email}")
        except Exception as e:
            logging.error(f"Failed to send password reset email to {email}: {e}", exc_info=True)
            # Still return a generic success message
            return make_response(jsonify({"message": "If an account with that email exists, a reset link has been sent."}), 200)
        return make_response(jsonify({"message": "If an account with that email exists, a reset link has been sent."}), 200)
    return make_response(jsonify({"error": "Method Not Allowed"}), 405)

# --- Route to handle the new password submission (API endpoint) ---
@password_reset_bp.route('/api/auth/reset-password/<token>', methods=['POST', 'OPTIONS'])
def reset_password_api(token: str): 
    """
    API endpoint to handle the submission of the new password.
    Validates the token, checks the new password, and updates it in the 
    Expects a JSON payload with 'password' and 'confirmPassword'.
    """
    if request.method == 'OPTIONS':
        response = make_response(jsonify({"message": "CORS preflight successful"}))
        response.status_code = 204
        return response

    if request.method == 'POST':
        s = get_serializer()
        try:
            # Ensure PASSWORD_RESET_TOKEN_MAX_AGE is an int for s.loads()
            max_age_env_val = os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', '3600')
            try:
                max_age_seconds = int(max_age_env_val)
            except ValueError:
                logging.error(f"Invalid value for PASSWORD_RESET_TOKEN_MAX_AGE: '{max_age_env_val}'. Using default 3600 seconds.")
                max_age_seconds = 3600
                
            token_data = s.loads(token, salt='password-reset-salt', max_age=max_age_seconds)
            user_id_str = token_data.get('user_id')
            token_email = token_data.get('email') 

            if not user_id_str:
                raise ValueError("Token data is missing 'user_id'.")

            logging.info(f"Processing password reset submission for token associated with user_id: {user_id_str} (email: {token_email})")

        except SignatureExpired:
            logging.warning(f"Password reset submission with EXPIRED token: {token}")
            return make_response(jsonify({"error": "Password reset link has expired. Please request a new one."}), 400)
        except BadTimeSignature: 
            logging.warning(f"Password reset submission with INVALID token (BadTimeSignature): {token}")
            return make_response(jsonify({"error": "Password reset link is invalid or has been tampered with."}), 400)
        except ValueError as ve: 
            logging.warning(f"Password reset submission with invalid token data ({ve}): {token}")
            return make_response(jsonify({"error": "Password reset link is invalid."}), 400)
        except Exception as e: 
            logging.error(f"Unexpected error validating token during reset submission {token}: {e}", exc_info=True)
            return make_response(jsonify({"error": "Server error: Could not validate reset link."}), 500)

        data = request.get_json()
        if not data or 'password' not in data or 'confirmPassword' not in data:
            logging.warning("API Password reset failed: Missing password fields in JSON payload.")
            return make_response(jsonify({"error": "New password and confirmation are required."}), 400)

        new_password = data['password']
        confirm_password = data['confirmPassword']
        
        if len(new_password) < 8: 
             return make_response(jsonify({"error": "Password must be at least 8 characters long."}), 400)
        if new_password != confirm_password:
            return make_response(jsonify({"error": "Passwords do not match."}), 400)

        try:
            user_oid = ObjectId(user_id_str)
        except Exception: 
            logging.error(f"Invalid ObjectId format in token for user_id: {user_id_str}")
            return make_response(jsonify({"error": "Invalid user identifier in reset link."}), 400)

        user_doc_verify, _ = find_user_by_id(user_oid)
        if not user_doc_verify:
            logging.error(f"User account with ID {user_oid} (from token) not found during password reset execution.")
            return make_response(jsonify({"error": "User account associated with this link no longer exists."}), 404) 
        update_success, update_msg = update_user_password(user_oid, new_password)

        if not update_success:
            logging.error(f"Failed to update password for user ID {user_oid}: {update_msg}")
            return make_response(jsonify({"error": "Server error: Could not update password."}), 500)

        logging.info(f"Password successfully reset for user ID {user_oid} (associated with token for email {token_email})")
        return make_response(jsonify({"message": "Your password has been reset successfully. You can now log in with your new password."}), 200)
    return make_response(jsonify({"error": "Method Not Allowed"}), 405)

@password_reset_bp.route('/forgot-password-form', methods=['GET'])
def show_forgot_password_form():
    """
    (Optional) Displays a simple HTML form (rendered by Flask) to enter an email
    for password reset. This is an alternative if Angular isn't handling this view.
    """
    logging.info("Serving Flask-rendered forgot password form.")
    return render_template('password_reset/forgot_password_form.html') 


@password_reset_bp.route('/reset-password-form/<token>', methods=['GET'])
def show_reset_password_form(token: str):
    """
    (Optional) Displays a simple HTML form (rendered by Flask) to enter a new password.
    Validates the token first. This is an alternative if Angular isn't handling this view.
    """
    s = get_serializer()
    try:
        max_age_env_val = os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', '3600')
        try:
            max_age_seconds = int(max_age_env_val)
        except ValueError:
            logging.error(f"Invalid value for PASSWORD_RESET_TOKEN_MAX_AGE: '{max_age_env_val}' for form. Using default 3600 seconds.")
            max_age_seconds = 3600
        s.loads(token, salt='password-reset-salt', max_age=max_age_seconds)
        logging.info(f"Token valid for displaying Flask-rendered reset form: {token}")
        return render_template('password_reset/reset_password_form.html', token=token)

    except SignatureExpired:
        logging.warning(f"Expired token for Flask-rendered reset form: {token}")
        flash('The password reset link has expired. Please request a new one.', 'danger')
        return redirect(url_for('password_reset.show_forgot_password_form'))
    except BadTimeSignature:
        logging.warning(f"Invalid token for Flask-rendered reset form: {token}")
        flash('The password reset link is invalid or corrupted. Please request a new one.', 'danger')
        return redirect(url_for('password_reset.show_forgot_password_form'))
    except Exception as e:
        logging.error(f"Error validating token for Flask-rendered reset form {token}: {e}", exc_info=True)
        flash('An error occurred. Please try again or request a new reset link.', 'danger')
        return redirect(url_for('password_reset.show_forgot_password_form'))