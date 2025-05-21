# routes/admin/archive_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask_login import current_user # For security later
from flask import redirect, url_for, request, flash
from math import ceil
from database import (
    get_all_archived_files,
    find_archived_metadata_by_access_id,
    save_file_metadata,                     
    delete_archived_metadata_by_access_id   
)

class ArchivedFileView(BaseView):
    def is_accessible(self):
        # --- TEMPORARILY MODIFIED FOR DEVELOPMENT ---
        logging.warning("ArchivedFileView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return True
        # --- END OF TEMPORARY MODIFICATION ---

        # Real check (when security is back on):
        # if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        #     return False
        # return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback for ArchivedFileView. Current is_accessible: True")
        if hasattr(current_user, 'is_authenticated') and not current_user.is_authenticated: # Check if current_user has this attr
            flash(gettext('Please log in to access this page.'), 'warning')
            return redirect(url_for('auth.login', next=request.url))
        else:
            flash(gettext('You do not have permission to access this page.'), 'danger')
            if hasattr(current_user, 'username') and current_user.username:
                 return redirect(url_for('file.list_user_files_page', username=current_user.username))
            else:
                 return redirect(url_for('auth.login')) # Fallback

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = 20 # Number of archived items per page
        total_records = 0
        total_pages = 1
        archived_records_page = []
        error_message = None

        try:
            all_archived_data, db_error = get_all_archived_files(search_query=search_query)

            if db_error:
                error_message = f"Error fetching archived files: {db_error}"
                logging.error(f"[AdminArchivedFileView] {error_message}")
            elif all_archived_data:
                total_records = len(all_archived_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1

                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                archived_records_page = all_archived_data[start_index:end_index]

                if not archived_records_page and page > 1:
                    pass # Show empty list if page is too high
            else:
                if not search_query:
                    logging.info("[AdminArchivedFileView] No archived files found.")
                else:
                    logging.info(f"[AdminArchivedFileView] No archived files found matching search: '{search_query}'.")

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminArchivedFileView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query:
            endpoint_args['q'] = search_query

        # We will create 'admin/archived_file_list.html' in the next step
        return self.render('admin/archived_file_list.html',
                           records=archived_records_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
                           endpoint_args=endpoint_args)


    @expose('/restore/<access_id>', methods=('POST',))
    def restore_file_action(self, access_id):
        # When security is on, add admin check here
        # if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        #     flash('Permission denied.', 'danger')
        #     return redirect(url_for('.index'))

        log_prefix = f"[AdminRestoreFile-{access_id}]"
        requesting_username_for_log = "AdminPanelAction" # Placeholder if current_user is not yet used for this action
        # if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated and hasattr(current_user, 'username'):
        #     requesting_username = current_user.username
        
        display_name_for_flash = access_id # Fallback

        try:
            # 1. Find the record in the 'archived_files' collection
            
            archived_record, find_err = find_archived_metadata_by_access_id(access_id)
            if find_err:
                raise Exception(f"Error finding record to restore: {find_err}")
            if not archived_record:
                raise Exception(f"Archived record ID '{access_id}' not found.")
            
            display_name_for_flash = archived_record.get('batch_display_name', archived_record.get('original_filename', access_id))

            # 2. Prepare the record for 'user_files' collection
            record_to_restore = archived_record.copy()
            if "archived_timestamp" in record_to_restore: del record_to_restore["archived_timestamp"]
            if "archived_by_username" in record_to_restore: del record_to_restore["archived_by_username"]
            if '_id' in record_to_restore: del record_to_restore['_id'] # Original _id from archive

            # 3. Insert (or upsert) into 'user_files' collection
            success, message = save_file_metadata(record_to_restore) # Your existing function
            if not success:
                raise Exception(f"Failed to save restored record to user_files: {message}")
            logging.info(f"{log_prefix} Record (access_id: {access_id}) successfully saved back to user_files by {requesting_username_for_log}.")

            # 4. Delete from the 'archived_files' collection
            deleted_count, del_arc_err = delete_archived_metadata_by_access_id(access_id)
            if del_arc_err:
                logging.critical(f"{log_prefix} CRITICAL: Record restored BUT failed to delete from archive (access_id: {access_id}). Error: {del_arc_err}")
                # Don't raise exception here, flash a critical warning instead
                flash(gettext('CRITICAL: Record restored but failed to remove from archive for %(id)s. Please check logs.', id=access_id), 'danger')
            elif deleted_count == 0:
                logging.warning(f"{log_prefix} Record restored, but original (access_id: {access_id}) not found for deletion in archive.")
                flash(gettext('Record restored, but original archive entry not found for deletion (%(id)s).', id=access_id), 'warning')
            else:
                flash(gettext('Record "%(name)s" (%(id)s) restored successfully.', name=display_name_for_flash, id=access_id), 'success')
            
            logging.info(f"{log_prefix} Record '{display_name_for_flash}' (access_id: {access_id}) successfully restored by {requesting_username_for_log}.")

        except Exception as e:
            flash(gettext('Error restoring record "%(name)s": %(error)s', name=display_name_for_flash, error=str(e)), 'danger')
            logging.error(f"{log_prefix} Exception during restore: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
    
    
    @expose('/permanently-delete/<access_id>', methods=('POST',))
    def permanently_delete_view(self, access_id):
        # When security is on, add admin check here
        # if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        #     flash('Permission denied.', 'danger')
        #     return redirect(url_for('.index'))

        log_prefix = f"[AdminPermDelete-{access_id}]"
        display_name_for_flash = access_id # Fallback

        try:
            # Optional: Find the record first to get its display name for the flash message
            # This also confirms it exists before trying to delete.
            archived_record, find_err = find_archived_metadata_by_access_id(access_id)
            if find_err:
                # This error is less critical if the goal is just to ensure it's gone
                logging.warning(f"{log_prefix} Error finding record before permanent delete (continuing with delete attempt): {find_err}")
            if archived_record:
                display_name_for_flash = archived_record.get('batch_display_name', archived_record.get('original_filename', access_id))

            # Call the database function to delete from 'archived_files'
            deleted_count, db_error_msg = delete_archived_metadata_by_access_id(access_id)

            if db_error_msg:
                flash(gettext('Error permanently deleting record: %(error)s', error=db_error_msg), 'danger')
                logging.error(f"{log_prefix} DB error permanently deleting {access_id}: {db_error_msg}")
            elif deleted_count > 0:
                flash(gettext('Record "%(name)s" (%(id)s) permanently deleted successfully.', name=display_name_for_flash, id=access_id), 'success')
                logging.info(f"{log_prefix} Record {access_id} permanently deleted by admin.")
            else:
                flash(gettext('Record %(access_id)s not found in archive for permanent deletion. Already deleted?', access_id=access_id), 'warning')
                logging.warning(f"{log_prefix} Record {access_id} not found in archive for permanent deletion.")

        except Exception as e:
            flash(gettext('An unexpected error occurred during permanent deletion: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error permanently deleting {access_id}: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))