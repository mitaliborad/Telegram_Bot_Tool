# test_gdrive_upload.py
import io
import logging
import time
from google_drive_api import (
    upload_to_gdrive,
    download_from_gdrive,
    delete_from_gdrive
)

# Configure basic logging for the test script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_gdrive_test():
    logging.info("--- Starting Google Drive API Test ---")

    # 1. Create a dummy in-memory file for testing
    dummy_filename = f"test_file_{int(time.time())}.txt"
    dummy_content = b"Hello, Google Drive! This is a test file."
    file_stream = io.BytesIO(dummy_content)
    logging.info(f"Created dummy file: '{dummy_filename}' with {len(dummy_content)} bytes.")

    gdrive_file_id = None  # To store the ID of the uploaded file

    try:
        # 2. Test Upload
        logging.info(f"--- Testing Upload to Google Drive ---")
        gdrive_file_id, upload_error = upload_to_gdrive(file_stream, dummy_filename)

        if upload_error:
            logging.error(f"Upload FAILED: {upload_error}")
            return # Stop test if upload fails
        if not gdrive_file_id:
            logging.error(f"Upload FAILED: No Google Drive file ID was returned, though no explicit error message given.")
            return

        logging.info(f"Upload SUCCESSFUL. Google Drive File ID: {gdrive_file_id}")
        print(f"\nSUCCESS: File '{dummy_filename}' uploaded to Google Drive with ID: {gdrive_file_id}")
        print(f"Check your configured Google Drive temporary folder to see the file.")

        # --- Optional: Pause to allow manual verification in Google Drive ---
        # input("Press Enter to continue with download and delete test...")
        # ---

        # 3. Test Download (Optional, but good for verification)
        logging.info(f"\n--- Testing Download from Google Drive (File ID: {gdrive_file_id}) ---")
        downloaded_stream, download_error = download_from_gdrive(gdrive_file_id)

        if download_error:
            logging.error(f"Download FAILED: {download_error}")
            # Don't necessarily stop; still try to delete if upload was successful
        elif downloaded_stream:
            downloaded_content = downloaded_stream.getvalue()
            if downloaded_content == dummy_content:
                logging.info(f"Download SUCCESSFUL. Content matches original.")
                print("SUCCESS: File content downloaded and verified.")
            else:
                logging.warning(f"Download content MISMATCH. Original size: {len(dummy_content)}, Downloaded size: {len(downloaded_content)}")
                print("WARNING: Downloaded content does not match the original.")
            downloaded_stream.close()
        else:
            logging.error("Download FAILED: No stream returned and no explicit error.")


    except Exception as e:
        logging.error(f"An unexpected error occurred during the test: {e}", exc_info=True)
        print(f"AN UNEXPECTED ERROR OCCURRED: {e}")

    finally:
        # 4. Test Delete (Always attempt if gdrive_file_id was obtained)
        if gdrive_file_id:
            logging.info(f"\n--- Testing Deletion from Google Drive (File ID: {gdrive_file_id}) ---")
            time.sleep(2) # Small delay before deleting, sometimes GDrive API needs a moment
            delete_success, delete_error = delete_from_gdrive(gdrive_file_id)

            if delete_error:
                logging.error(f"Deletion FAILED: {delete_error}")
                print(f"ERROR: Failed to delete file from Google Drive: {delete_error}")
            elif delete_success:
                logging.info(f"Deletion SUCCESSFUL for file ID: {gdrive_file_id}")
                print(f"SUCCESS: File '{dummy_filename}' (ID: {gdrive_file_id}) deleted from Google Drive.")
            else:
                # This case should ideally be covered by delete_error, but as a fallback
                logging.error(f"Deletion status uncertain for file ID: {gdrive_file_id}")
                print(f"UNCERTAIN: Deletion status for file ID {gdrive_file_id} is unclear.")
        
        file_stream.close() # Close the initial dummy stream
        logging.info("--- Google Drive API Test Finished ---")

if __name__ == "__main__":
    # Ensure environment variables are loaded if not already handled by google_drive_api.py's import
    # from dotenv import load_dotenv
    # load_dotenv() # This is already in google_drive_api.py, so might be redundant here if run as main

    run_gdrive_test()