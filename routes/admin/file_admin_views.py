# routes/admin/file_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask import redirect, url_for, request, flash, abort 
import json
from flask_login import current_user
from math import ceil
from database import (
    get_all_file_metadata,
    find_metadata_by_access_id,
    archive_file_record_by_access_id, # For the delete/archive action
    find_archived_metadata_by_access_id # Used in delete_view to get name after archive
)

class FileMetadataView(BaseView):
    def is_accessible(self):
        logging.warning("FileMetadataView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return current_user.is_authenticated and getattr(current_user, 'is_admin', False)

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback called for {name} in FileMetadataView, but is_accessible is True.")
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'info')
            return redirect(url_for('admin_auth.login', next=request.url))
        else:
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('admin_auth.login'))

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip() 
        user_type_filter = request.args.get('user_type', None)
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_records = 0
        total_pages = 1
        records_list_page = []
        error_message = None

        try:
            all_records_data, db_error = get_all_file_metadata(search_query=search_query, user_type_filter=user_type_filter) 

            if db_error:
                error_message = f"Error fetching file metadata: {db_error}"
            elif all_records_data is not None: # Check for None explicitly
                total_records = len(all_records_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1


                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                records_list_page = all_records_data[start_index:end_index]

                if not records_list_page and page > 1 and total_records > 0:
                    logging.info(f"[AdminFileMetadataView] Requested page {page} is out of range for current filters.")
            else: # all_records_data is None (error occurred) or empty list (no match)
                log_msg_parts = []
                if search_query: log_msg_parts.append(f"search: '{search_query}'")
                if user_type_filter: log_msg_parts.append(f"user_type: '{user_type_filter}'")
                
                if log_msg_parts:
                    logging.info(f"[AdminFileMetadataView] No file metadata records found matching {', '.join(log_msg_parts)}.")
                else:
                    logging.info("[AdminFileMetadataView] No file metadata records found in the database at all.")


        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminFileMetadataView] Unexpected error: {error_message}", exc_info=True)


        endpoint_args = {}
        if search_query: 
            endpoint_args['q'] = search_query
        if user_type_filter: # <<< NEW
            endpoint_args['user_type'] = user_type_filter # <<< NEW

        return self.render('admin/file_metadata_list.html',
                           records=records_list_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
                           user_type_filter=user_type_filter, # Pass to template
                           endpoint_args=endpoint_args) 

    @expose('/details/<access_id>')
    def details_view(self, access_id):
        record = None
        error_message = None
        try:
            record_data, db_error = find_metadata_by_access_id(access_id)
            if db_error:
                error_message = f"Error fetching record details for {access_id}: {db_error}"
            elif record_data:
                record = record_data
            else:
                error_message = f"Record with Access ID '{access_id}' not found."
        except Exception as e:
            error_message = f"An unexpected error occurred while fetching details for {access_id}: {str(e)}"
        if not record and not error_message:
             error_message = f"Record with Access ID '{access_id}' not found."
        # For "Back to List" button, preserve filters
        back_to_list_args = {}
        if request.args.get('q'): back_to_list_args['q'] = request.args.get('q')
        if request.args.get('page'): back_to_list_args['page'] = request.args.get('page')
        if request.args.get('user_type'): back_to_list_args['user_type'] = request.args.get('user_type')
        return self.render('admin/file_metadata_details.html',
                           record=record,
                           error_message=error_message,
                           json=json,
                           back_to_list_args=back_to_list_args)

    @expose('/delete/<access_id>', methods=('POST',))
    def delete_view(self, access_id):
        log_prefix = f"[AdminArchiveFile-{access_id}]"
        admin_username_for_archive = "AdminPanelAction"
        display_name_for_flash = access_id

        try:
            original_record, _ = find_metadata_by_access_id(access_id)
            if original_record:
                display_name_for_flash = original_record.get('batch_display_name', original_record.get('original_filename', access_id))

            success, msg = archive_file_record_by_access_id(access_id, admin_username_for_archive)
        #     temp_archived_record, _ = find_archived_metadata_by_access_id(access_id) # Check if it's in archive
        #     if temp_archived_record and success: # If found in archive and operation was successful
        #          display_name_for_flash = temp_archived_record.get('batch_display_name', temp_archived_record.get('original_filename', access_id))
        #     elif not success: # if archive failed, try to get name from original location if still there
        #          original_record, _ = find_metadata_by_access_id(access_id)
        #          if original_record:
        #               display_name_for_flash = original_record.get('batch_display_name', original_record.get('original_filename', access_id))


        #     if success:
        #         flash(gettext('Record "%(name)s" (%(id)s) archived successfully.', name=display_name_for_flash, id=access_id), 'success')
        #         logging.info(f"{log_prefix} Record {access_id} archived by {admin_username_for_archive}.")
        #     else:
        #         flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')
        #         logging.error(f"{log_prefix} Failed to archive {access_id}: {msg}")

        # except Exception as e:
        #     flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
        #     logging.error(f"{log_prefix} Unexpected error archiving {access_id}: {str(e)}", exc_info=True)

        # return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
            if success:
                # If archive was successful, name might have been based on original. Let's re-check from archive for consistency if needed,
                # but usually, the original name is fine for the flash.
                flash(gettext('Record "%(name)s" (%(id)s) archived successfully.', name=display_name_for_flash, id=access_id), 'success')
                logging.info(f"{log_prefix} Record {access_id} archived by {admin_username_for_archive}.")
            else:
                flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')
                logging.error(f"{log_prefix} Failed to archive {access_id}: {msg}")

        except Exception as e:
            flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error archiving {access_id}: {str(e)}", exc_info=True)

        # --- MODIFIED: Preserve user_type filter on redirect ---
        return redirect(url_for('.index', 
                                q=request.args.get('q', ''), 
                                page=request.args.get('page', '1'), 
                                user_type=request.args.get('user_type')))