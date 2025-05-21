# routes/admin/file_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask import redirect, url_for, request, flash, abort 
import json
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
        return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback called for {name} in FileMetadataView, but is_accessible is True.")
        flash(gettext('Access Denied (inaccessible_callback). This should not happen with current settings.'), 'warning')
        # When security is on, this should redirect based on current_user state
        return redirect(url_for('auth.login')) # Fallback redirect

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip() 
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_records = 0
        total_pages = 1
        records_list_page = []
        error_message = None

        try:
            all_records_data, db_error = get_all_file_metadata(search_query=search_query) 

            if db_error:
                error_message = f"Error fetching file metadata: {db_error}"
            elif all_records_data:
                total_records = len(all_records_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1


                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                records_list_page = all_records_data[start_index:end_index]

                if not records_list_page and page > 1:
                    pass
            else:
                if not search_query:
                     logging.info("[AdminFileMetadataView] No file metadata records found.")
                else:
                     logging.info(f"[AdminFileMetadataView] No file metadata records found matching search: '{search_query}'.")


        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminFileMetadataView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query: 
            endpoint_args['q'] = search_query

        return self.render('admin/file_metadata_list.html',
                           records=records_list_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
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
        return self.render('admin/file_metadata_details.html',
                           record=record,
                           error_message=error_message,
                           json=json)

    @expose('/delete/<access_id>', methods=('POST',))
    def delete_view(self, access_id):
        log_prefix = f"[AdminArchiveFile-{access_id}]"
        admin_username_for_archive = "AdminPanelAction"
        display_name_for_flash = access_id

        try:
            success, msg = archive_file_record_by_access_id(access_id, admin_username_for_archive)
            temp_archived_record, _ = find_archived_metadata_by_access_id(access_id) # Check if it's in archive
            if temp_archived_record and success: # If found in archive and operation was successful
                 display_name_for_flash = temp_archived_record.get('batch_display_name', temp_archived_record.get('original_filename', access_id))
            elif not success: # if archive failed, try to get name from original location if still there
                 original_record, _ = find_metadata_by_access_id(access_id)
                 if original_record:
                      display_name_for_flash = original_record.get('batch_display_name', original_record.get('original_filename', access_id))


            if success:
                flash(gettext('Record "%(name)s" (%(id)s) archived successfully.', name=display_name_for_flash, id=access_id), 'success')
                logging.info(f"{log_prefix} Record {access_id} archived by {admin_username_for_archive}.")
            else:
                flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')
                logging.error(f"{log_prefix} Failed to archive {access_id}: {msg}")

        except Exception as e:
            flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error archiving {access_id}: {str(e)}", exc_info=True)

        return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
        