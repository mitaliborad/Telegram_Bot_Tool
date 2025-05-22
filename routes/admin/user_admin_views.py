# routes/admin/user_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
# from flask_login import current_user # Not strictly needed while security is off
from flask import redirect, url_for, request, flash # Keep flash for feedback
from math import ceil
import re
from database import (
    get_all_users,
    archive_user_account,
    find_user_by_id_str,
    update_user_admin_status,
    update_user_details,
    find_user_by_email_excluding_id,    
    find_user_by_username_excluding_id  
)
import json
from .forms import EditUserForm
from bson import ObjectId

class UserView(BaseView):
    def is_accessible(self):
        logging.warning("UserView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback called for {name} in UserView, but is_accessible is True.")
        flash(gettext('Access Denied (inaccessible_callback). This should not happen with current settings.'), 'warning')
        # When security is on, this should redirect based on current_user state
        return redirect(url_for('auth.login')) # Fallback redirect

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_users = 0
        total_pages = 1
        users_list_page = []
        error_message = None

        try:
            all_users_data, db_error = get_all_users(search_query=search_query)

            if db_error:
                error_message = f"Error fetching users: {db_error}"
            elif all_users_data:
                total_users = len(all_users_data)
                total_pages = ceil(total_users / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_users > 0: total_pages = 1

                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                users_list_page = all_users_data[start_index:end_index]

                if not users_list_page and page > 1:
                    pass
            else:
                # If search_query is present and no users, it's not an error, just no results
                if not search_query:
                    logging.info("[AdminUserView] No users found in the database (no search).")
                else:
                    logging.info(f"[AdminUserView] No users found matching search: '{search_query}'.")


        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminUserView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query:
            endpoint_args['q'] = search_query

        return self.render('admin/user_list.html',
                           users=users_list_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_users=total_users,
                           search_query=search_query,
                           endpoint_args=endpoint_args)

    @expose('/delete/<user_id_str>', methods=('POST',))
    def delete_user_view(self, user_id_str):
        log_prefix = f"[AdminArchiveUser-{user_id_str}]" # Changed log prefix
        admin_username_for_archive = "AdminPanelAction"
        try:
            # Find user to get username for flash message, and to confirm existence
            user_to_archive_doc, find_err = find_user_by_id_str(user_id_str)
            if find_err :
                flash(gettext('Error finding user for archiving: %(error)s', error=find_err), 'danger')
                logging.error(f"{log_prefix} Error finding user: {find_err}")
                return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
            if not user_to_archive_doc:
                flash(gettext('User with ID %(id)s not found. Already archived or deleted?', id=user_id_str), 'warning')
                logging.warning(f"{log_prefix} User not found with ID: {user_id_str}")
                return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))

            username_for_flash = user_to_archive_doc.get('username', user_id_str)

            # Call the archive_user_account function
            success, msg = archive_user_account(user_id_str, admin_username_for_archive)

            if success:
                flash(gettext('User "%(name)s" (ID: %(id)s) archived successfully.', name=username_for_flash, id=user_id_str), 'success')
                logging.info(f"{log_prefix} User '{username_for_flash}' (ID: {user_id_str}) archived by {admin_username_for_archive}.")
            else:
                flash(gettext('Error archiving user "%(name)s": %(error)s', name=username_for_flash, error=msg), 'danger')
                logging.error(f"{log_prefix} Failed to archive user '{username_for_flash}' (ID: {user_id_str}): {msg}")
        except Exception as e:
            flash(gettext('An unexpected error occurred during user archiving: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error: {str(e)}", exc_info=True)
        
        # Redirect back to the user list, preserving search and page context
        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
    
    @expose('/toggle-admin/<user_id_str>', methods=('POST',)) # Require POST
    def toggle_admin_view(self, user_id_str):
        # Security: Later, ensure current_user cannot demote themselves if they are the only admin, etc.
        # For now, keeping access open for dev.

        try:
            user_doc, find_err = find_user_by_id_str(user_id_str)
            if find_err or not user_doc:
                flash(gettext('User not found: %(error)s', error=find_err or "Unknown ID"), 'danger')
                return redirect(url_for('.index'))

            current_is_admin_status = user_doc.get('is_admin', False)
            new_is_admin_status = not current_is_admin_status # Toggle the status

            # Optional: Prevent demoting the last admin or self-demotion logic here
            # For example, if current_user.get_id() == user_id_str and new_is_admin_status == False:
            #    flash('You cannot remove your own admin status.', 'danger')
            #    return redirect(url_for('.index'))

            success, msg = update_user_admin_status(user_id_str, new_is_admin_status)

            if success:
                action = "promoted to admin" if new_is_admin_status else "removed from admin"
                flash(gettext('User "%(username)s" successfully %(action)s.',
                              username=user_doc.get('username', user_id_str), action=action), 'success')
                logging.info(f"[AdminUserToggle] User {user_id_str} ({user_doc.get('username')}) {action} by admin.")
            else:
                flash(gettext('Failed to update admin status for user "%(username)s": %(msg)s',
                              username=user_doc.get('username', user_id_str), msg=msg), 'danger')
                logging.error(f"[AdminUserToggle] Failed to update admin status for {user_id_str}: {msg}")

        except Exception as e:
            flash(gettext('An unexpected error occurred: %(error)s', error=str(e)), 'danger')
            logging.error(f"[AdminUserToggle] Unexpected error for user {user_id_str}: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
    
    
    
    @expose('/details/<user_id_str>')
    def user_details_view(self, user_id_str):
        user_doc = None
        error_message = None
        try:
            # Use the existing find_user_by_id_str helper
            user_data, db_error = find_user_by_id_str(user_id_str)
            if db_error:
                error_message = f"Error fetching user details for ID {user_id_str}: {db_error}"
                logging.error(f"[AdminUserDetailsView] {error_message}")
            elif user_data:
                user_doc = user_data
                # We already remove password_hash in get_all_users and find_user_by_id_str.
                # If find_user_by_id_str didn't, we would do it here:
                # if 'password_hash' in user_doc:
                #     del user_doc['password_hash']
            else:
                error_message = f"User with ID '{user_id_str}' not found."
                logging.warning(f"[AdminUserDetailsView] {error_message}")
                # Optionally: return abort(404)

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminUserDetailsView] Error for user {user_id_str}: {error_message}", exc_info=True)

        if not user_doc and not error_message:
            error_message = f"User with ID '{user_id_str}' not found."

        return self.render('admin/user_details.html',
                           user_doc=user_doc,
                           error_message=error_message,
                           json=json) # Pass json for potential future use
        
        
    @expose('/edit/<user_id_str>', methods=('GET', 'POST'))
    def edit_user_view(self, user_id_str):
        user_doc, find_err = find_user_by_id_str(user_id_str)
        if find_err or not user_doc:
            flash(gettext('User not found: %(error)s', error=find_err or "Unknown ID"), 'danger')
            return redirect(url_for('.index'))

        form = EditUserForm(obj=user_doc) # Pre-populate form with existing user data if names match

        if form.validate_on_submit(): # If form submitted and valid
            update_data = {
                'username': form.username.data,
                'email': form.email.data.lower(), # Store email as lowercase
                # 'is_admin': form.is_admin.data
                # Add other fields from the form here
            }
            
            # Ensure original_email and original_username are available for comparison
            original_email = user_doc.get('email')
            original_username = user_doc.get('username')

            # Check email uniqueness only if it changed
            if update_data['email'] != original_email:
                existing_user_email, _ = find_user_by_email_excluding_id(update_data['email'], ObjectId(user_id_str))
                if existing_user_email:
                    form.email.errors.append("This email address is already in use by another account.")
                    # Re-render form with error
                    return self.render('admin/user_edit_form.html', form=form, user_doc=user_doc, error_message=None) # Pass user_doc for title etc.
            
            # Check username uniqueness only if it changed
            if update_data['username'] != original_username:
                existing_user_username, _ = find_user_by_username_excluding_id(update_data['username'], ObjectId(user_id_str))
                if existing_user_username:
                    form.username.errors.append("This username is already taken by another account.")
                    # Re-render form with error
                    validation_passed = False
                    return self.render('admin/user_edit_form.html', form=form, user_doc=user_doc, error_message=None)
            if validation_passed:
                success, msg = update_user_details(user_id_str, update_data)
                if success:
                    flash(gettext('User "%(username)s" updated successfully.', username=update_data['username']), 'success')
                    return redirect(url_for('.user_details_view', user_id_str=user_id_str))
                else:
                    flash(gettext('Error updating user: %(msg)s', msg=msg), 'danger')
            else:
                flash('Please correct the errors below.', 'warning')

        elif request.method == 'POST' and not form.validate(): # If POST and validation failed
             flash('Please correct the errors below.', 'warning')


        # For GET request or if validation failed on POST, pre-populate form:
        if request.method == 'GET':
             form.username.data = user_doc.get('username')
             form.email.data = user_doc.get('email')
            #  form.is_admin.data = user_doc.get('is_admin', False)
             # Populate other form fields from user_doc
        current_role_for_display = user_doc.get('role', 'Free User')

        return self.render('admin/user_edit_form.html',
                           form=form,
                           user_doc=user_doc,
                           current_role=current_role_for_display)
