# routes/admin/archived_user_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask_login import current_user 
from flask import redirect, url_for, request, flash
from math import ceil

from database import (
    get_all_archived_users,
    restore_user_account,
    delete_archived_user_permanently,
    find_archived_user_by_original_id # To get details before action if needed
)

class ArchivedUserView(BaseView):
    def is_accessible(self):
        # --- TEMPORARILY MODIFIED FOR DEVELOPMENT ---
        logging.warning("ArchivedUserView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return current_user.is_authenticated and getattr(current_user, 'is_admin', False)

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback for ArchivedUserView.")
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'info')
            return redirect(url_for('admin_auth.login', next=request.url))
        else:
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('admin_auth.login'))

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_records = 0
        total_pages = 1
        archived_users_page = []
        error_message = None

        try:
            all_archived_users_data, db_error = get_all_archived_users(search_query=search_query)

            if db_error:
                error_message = f"Error fetching archived users: {db_error}"
                logging.error(f"[AdminArchivedUserView] {error_message}")
            elif all_archived_users_data:
                total_records = len(all_archived_users_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1 # Ensure at least one page if records exist

                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                archived_users_page = all_archived_users_data[start_index:end_index]
            else:
                if not search_query:
                    logging.info("[AdminArchivedUserView] No archived users found.")
                else:
                    logging.info(f"[AdminArchivedUserView] No archived users found matching search: '{search_query}'.")

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminArchivedUserView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query:
            endpoint_args['q'] = search_query
        
        # We will create 'admin/archived_user_list.html' in Phase 3
        return self.render('admin/archived_user_list.html',
                           records=archived_users_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
                           endpoint_args=endpoint_args)

    @expose('/restore/<original_user_id_str>', methods=('POST',))
    def restore_user_action(self, original_user_id_str):
        log_prefix = f"[AdminRestoreUser-{original_user_id_str}]"
        admin_username_for_restore = "AdminPanelAction" # Placeholder
        # When Flask-Login is integrated:
        # if not (hasattr(current_user, 'is_authenticated') and current_user.is_authenticated and getattr(current_user, 'is_admin', False)):
        #     flash('Permission denied.', 'danger')
        #     return redirect(url_for('.index'))
        # admin_username_for_restore = current_user.username
        
        username_for_flash = original_user_id_str # Fallback

        try:
            # Optional: Find user first to get their username for a nicer flash message
            archived_doc, find_err = find_archived_user_by_original_id(original_user_id_str)
            if find_err:
                logging.warning(f"{log_prefix} Could not fetch archived user details before restore: {find_err}. Proceeding with restore attempt.")
            if archived_doc:
                username_for_flash = archived_doc.get('original_username', original_user_id_str)

            success, msg = restore_user_account(original_user_id_str, admin_username_for_restore)

            if success:
                flash(gettext('User "%(name)s" (Original ID: %(id)s) restored successfully.', name=username_for_flash, id=original_user_id_str), 'success')
                logging.info(f"{log_prefix} User '{username_for_flash}' (Original ID: {original_user_id_str}) restored by {admin_username_for_restore}.")
            else:
                flash(gettext('Error restoring user "%(name)s": %(error)s', name=username_for_flash, error=msg), 'danger')
                logging.error(f"{log_prefix} Failed to restore user '{username_for_flash}' (Original ID: {original_user_id_str}): {msg}")
        
        except Exception as e:
            flash(gettext('An unexpected error occurred during user restoration: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))

    @expose('/permanently-delete/<archived_record_id_str>', methods=('POST',))
    def permanently_delete_action(self, archived_record_id_str):
        log_prefix = f"[AdminPermDeleteUser-{archived_record_id_str}]"
        # When Flask-Login is integrated:
        # if not (hasattr(current_user, 'is_authenticated') and current_user.is_authenticated and getattr(current_user, 'is_admin', False)):
        #     flash('Permission denied.', 'danger')
        #     return redirect(url_for('.index'))

        username_for_flash = archived_record_id_str # Fallback

        try:
            # Note: To get the username for flash, we'd need to fetch the archived record first by its *archive_record_id_str*.
            # The `delete_archived_user_permanently` function takes this ID.
            # For simplicity, we're not fetching just for the name here, but you could add it.
            
            deleted_count, msg = delete_archived_user_permanently(archived_record_id_str)

            if msg: # If there was an error message from the DB operation
                flash(gettext('Error permanently deleting archived user: %(error)s', error=msg), 'danger')
                logging.error(f"{log_prefix} DB error permanently deleting (Archive Record ID: {archived_record_id_str}): {msg}")
            elif deleted_count > 0:
                flash(gettext('Archived user record (Archive Record ID: %(id)s) permanently deleted successfully.', id=archived_record_id_str), 'success')
                logging.info(f"{log_prefix} Archived user record (Archive Record ID: {archived_record_id_str}) permanently deleted.")
            else:
                flash(gettext('Archived user record (Archive Record ID: %(id)s) not found for permanent deletion. Already deleted?', id=archived_record_id_str), 'warning')
                logging.warning(f"{log_prefix} Archived user record not found (Archive Record ID: {archived_record_id_str}).")

        except Exception as e:
            flash(gettext('An unexpected error occurred during permanent deletion: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))