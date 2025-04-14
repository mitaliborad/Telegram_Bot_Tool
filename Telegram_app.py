import os
import requests 
from flask import Flask, request, render_template, flash, redirect, url_for, make_response, jsonify
import json
from datetime import datetime, timezone
import logging

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
TELEGRAM_CHAT_ID = '-4603853425'

# Basic check if placeholders are replaced
if TELEGRAM_BOT_TOKEN == '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' or TELEGRAM_CHAT_ID == '-4603853425':
    print("="*60)
    print("!!! WARNING: Please replace 'YOUR_BOT_TOKEN' and 'YOUR_CHAT_ID' in the script !!!")
    print("="*60)

#----------Metadata----------------------------------------------------------------
# Define the filename for storing metadata
METADATA_FILE = 'metadata.json'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_metadata():
    """Loads metadata from the JSON file (METADATA_FILE)."""
    try:
        # Check if the file exists. If not, it's not an error, just means no data yet.
        if not os.path.exists(METADATA_FILE):
            logging.info(f"Metadata file '{METADATA_FILE}' not found. Starting with empty data.")
            return {} # Return an empty dictionary if the file doesn't exist yet

        # Open the file for reading ('r') with UTF-8 encoding (good for different characters)
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            # Try to parse the text in the file as JSON into a Python dictionary
            data = json.load(f)
            logging.info(f"Successfully loaded metadata from '{METADATA_FILE}'.")
            return data # Return the loaded data

    # Handle errors if the file exists but contains invalid JSON
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from '{METADATA_FILE}'. File might be corrupted. Returning empty data.")
        return {} # Return empty data to prevent crashing

    # Handle errors if the program doesn't have permission to read the file
    except IOError as e:
        logging.error(f"Could not read metadata file '{METADATA_FILE}': {e}")
        return {} # Return empty data

    # Catch any other unexpected errors during loading
    except Exception as e:
        logging.error(f"An unexpected error occurred loading metadata: {e}", exc_info=True) # Log full error details
        return {}
    
def save_metadata(data):
    """Saves the provided Python dictionary 'data' to the JSON file (METADATA_FILE)."""
    try:
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully saved metadata to '{METADATA_FILE}'.")
        return True # Indicate success

    # Handle errors if the program doesn't have permission to write the file
    except IOError as e:
        logging.error(f"Could not write metadata file '{METADATA_FILE}': {e}")
        return False # Indicate failure

    # Handle cases where the 'data' contains something that can't be saved as JSON
    except TypeError as e:
        logging.error(f"Data type error while trying to save metadata (data might be invalid): {e}")
        return False # Indicate failure

    # Catch any other unexpected errors during saving
    except Exception as e:
        logging.error(f"An unexpected error occurred saving metadata: {e}", exc_info=True)
        return False # Indicate failure


    

# ----------- Flask App Setup ----------------------------------------------------------------
# We tell Flask to look for templates in the current directory ('.') instead of 'templates'
app = Flask(__name__, template_folder='.')
# Secret key is needed for flashing messages (for success/error feedback)
# Replace with a real random secret key if this becomes public
app.secret_key = 'a_simple_secret_key_for_now'

# --- Telegram API Function ---
def send_file_to_telegram(file_object, filename):
    """Sends the provided file object to the configured Telegram chat."""
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
    files_payload = {'document': (filename, file_object)}
    # Prepare the chat_id data
    data_payload = {'chat_id': TELEGRAM_CHAT_ID}

    print(f"Sending '{filename}' to chat ID: {TELEGRAM_CHAT_ID}") # Debug print

    try:
        # Make the POST request to Telegram
        # timeout=60 means wait up to 60 seconds for a response
        response = requests.post(api_url, data=data_payload, files=files_payload, timeout=60)

        # Check if Telegram reported an error (like bad token, chat not found, etc.)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)

        # Check the JSON response from Telegram to be sure
        response_json = response.json()
        if response_json.get('ok'):
            print(f"Telegram API reported success for '{filename}'!")
            return True, f"File '{filename}' sent successfully!", response_json
        else:
            error_desc = response_json.get('description', 'Unknown Telegram error')
            print(f"Telegram API Error: {error_desc} (Full Response: {response.text})")
            return False, f"Telegram API Error: {error_desc}", None

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out while sending '{filename}'.")
        return False, "Error: The request to Telegram timed out.", None
    except requests.exceptions.RequestException as e:
        # Handles network errors, connection errors, HTTP errors (like 404, 401)
        print(f"Error sending file via Telegram API: {e}")
        # Try to get more detail from the response if available
        error_details = str(e)
        if e.response is not None:
            error_details += f" | Response: {e.response.text}"
        return False, f"Network/Request Error: {error_details}", None
    except json.JSONDecodeError:
        # Handle cases where Telegram responds with non-JSON content unexpectedly
        logging.error(f"Error: Telegram returned a non-JSON response. Status: {response.status_code}, Body: {response.text}")
        return False, "Error: Received invalid response from Telegram.", None
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred during Telegram send: {e}")
        return False, f"An unexpected error occurred: {e}", None

# --- Flask Routes ---

# Route for the main page '/'
@app.route('/')
def index():
    """Serves the main HTML page."""
    try:
        # Use render_template, pointing to the current directory thanks to 'template_folder='.'
        return render_template('index.html')
    except Exception as e:
        print(f"Error rendering index.html: {e}")
        # Fallback if render_template fails for any reason
        return "Error: Could not load the upload page. Is index.html in the same folder?", 500


# --- Replace the OLD upload_file function with this NEW version ---

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file upload, sends to Telegram, extracts info, updates metadata."""

    # === Part 1: Get data from the incoming request ===

    # Check 1: Is there a 'file' part in the submitted form?
    if 'file' not in request.files:
        flash('No file part in the request!', 'error') # User feedback
        logging.warning("Upload attempt failed: 'file' part missing in request.") # Log details
        return redirect(url_for('index')) # Go back to upload page

    # Check 2: Is there a 'username' field in the submitted form?
    # .get('username', default_value) is safer than request.form['username'] which crashes if missing
    # .strip() removes leading/trailing whitespace
    username = request.form.get('username', '').strip()
    if not username: # Check if username is empty after stripping
        flash('Username is required!', 'error')
        logging.warning("Upload attempt failed: Username missing or empty.")
        return redirect(url_for('index'))

    # Get the file object itself
    file = request.files['file']

    # Check 3: Did the user actually select a file (or just submit an empty input)?
    if file.filename == '':
        flash('No file selected!', 'error')
        logging.warning(f"Upload attempt failed for user '{username}': No file selected.")
        return redirect(url_for('index'))

    # === Part 2: Process the file if all checks passed ===

    # Proceed only if 'file' exists and has a filename
    if file:
        # Get the original filename provided by the user's browser
        # Use secure_filename later if saving to your server's disk, but okay for sending to Telegram API
        original_filename = file.filename
        logging.info(f"Processing uploaded file: '{original_filename}' from user: '{username}'")

        # --- Action: Send the file to Telegram ---
        # IMPORTANT: We need to read the file content for Telegram.
        # The 'file' object acts like an open file handle. 'requests' will read it.
        # If you needed to read the file multiple times, you might use file.seek(0) first.
        success, message, tg_response_json = send_file_to_telegram(file, original_filename)

        # === Part 3: Handle the result of sending ===

        if success and tg_response_json:
            # --- Action: Extract required info from Telegram's response ---
            try:
                # Telegram's response structure is usually {'ok': True, 'result': { ... message details ... }}
                result_data = tg_response_json.get('result', {}) # Get the 'result' dictionary safely

                message_id = result_data.get('message_id')

                # File details are often nested inside 'document' (for documents), 'photo', 'video' etc.
                # We primarily expect 'document' here based on sendDocument
                document_data = result_data.get('document', {})
                # file_id is temporary, useful for getFile endpoint
                file_id = document_data.get('file_id')
                # file_unique_id is persistent across chats/bots, better for long-term identification
                file_unique_id = document_data.get('file_unique_id')

                # Make sure we got the essential IDs
                if not message_id or not file_unique_id:
                    logging.error(f"Upload SUCCESSFUL for '{original_filename}' (user: {username}) but FAILED to extract required IDs (msg_id={message_id}, unique_id={file_unique_id}) from Telegram response: {tg_response_json}")
                    # Inform user the file is sent, but tracking failed.
                    flash(f"File '{original_filename}' sent successfully, but failed to record tracking information. Please contact support.", 'warning')
                else:
                    logging.info(f"Extracted IDs for '{original_filename}': MessageID={message_id}, FileUniqueID={file_unique_id}")

                    # --- Action: Update the metadata file ---
                    metadata = load_metadata() # Load current data from metadata.json
                    timestamp = datetime.now(timezone.utc).isoformat() # Get current time in UTC format

                    # Create the dictionary for this specific file upload
                    new_file_record = {
                        "original_filename": original_filename,
                        "telegram_message_id": message_id,
                        "telegram_file_id": file_id, # Store this too, might be useful
                        "telegram_file_unique_id": file_unique_id, # Primary identifier
                        "upload_timestamp": timestamp # Record when it was uploaded
                    }

                    # Get the list of files already stored for this username.
                    # If the username is new, .setdefault() creates an empty list ([]) for them.
                    user_files_list = metadata.setdefault(username, [])

                    # Add the new file record to this user's list
                    user_files_list.append(new_file_record)

                    # Now, save the entire updated metadata structure back to the file
                    if save_metadata(metadata):
                        logging.info(f"Successfully saved metadata for '{original_filename}' (User: '{username}') to {METADATA_FILE}.")
                        flash(message, 'success') # Use the success message from send_file_to_telegram
                    else:
                        # Saving failed! File is in Telegram, but we lost track locally. Critical error.
                        logging.error(f"File '{original_filename}' (user: {username}) sent to Telegram (MsgID: {message_id}), BUT FAILED TO SAVE METADATA to {METADATA_FILE}!")
                        flash(f"File '{original_filename}' sent, but a CRITICAL error occurred saving tracking info. Please report this immediately!", 'error')

            except Exception as e:
                # Catch any unexpected errors during the processing of the Telegram response or metadata update
                logging.error(f"Error processing Telegram response or updating metadata for '{original_filename}' (user: {username}): {e}", exc_info=True)
                flash(f"File '{original_filename}' was sent, but an internal error occurred processing the result. Please check server logs.", 'error')

        else:
            # Sending to Telegram failed. 'message' already contains the error details.
            # The send_file_to_telegram function logged the detailed error.
            flash(message, 'error') # Show the error message (from step 4) to the user

        # Regardless of success or failure in sending/saving, always go back to the main page
        return redirect(url_for('index'))

    # Fallback case: Should not be reached if checks above are correct, but good to have.
    flash('An unexpected error occurred handling the uploaded file.', 'error')
    logging.error(f"Upload function reached fallback point unexpectedly for user '{username}'. File object: {file}")
    return redirect(url_for('index'))

# --- Make sure this block is present in your code ---
@app.route('/files/<username>', methods=['GET'])
def list_user_files(username):
    """API endpoint to list files for a given username."""
    logging.info(f"Request received to list files for user: '{username}'")
    metadata = load_metadata()
    user_files = metadata.get(username, []) # Get list for user, default to empty list
    if not user_files:
        logging.info(f"No files found in metadata for user: '{username}'")
        # Return JSON with an empty list and a 200 OK status
        return jsonify([]) # Return empty list, front-end handles 'no files' message
    else:
        logging.info(f"Found {len(user_files)} files for user '{username}'.")
        # Ensure we only return necessary info if needed, but full record is fine for now
        # You might want to filter what you send back later
        return jsonify(user_files) # Return the list of file records as JSON

# --- Also make sure the download route (even if not fully used yet) is there ---
# --- Replace the existing download_user_file function ---

@app.route('/download/<username>/<filename>', methods=['GET'])
def download_user_file(username, filename):
    """
    Handles download requests.
    1. Finds file metadata.
    2. Asks Telegram for a temporary file path using the file_id.
    3. Redirects the user's browser to the temporary Telegram download URL.
    """
    logging.info(f"Download request received for file: '{filename}' by user: '{username}'")

    # --- 1. Find the file metadata ---
    metadata = load_metadata()
    user_files = metadata.get(username, [])
    # Find the specific file record using original_filename
    file_info = next((f for f in user_files if f.get('original_filename') == filename), None)

    if not file_info:
        logging.warning(f"Download failed: File '{filename}' not found in metadata for user '{username}'.")
        # Use flash for user feedback on the page they are likely redirected back to
        flash(f"Error: File '{filename}' not found for user '{username}'.", 'error')
        # Redirecting to index might be better than showing a JSON error page
        return redirect(url_for('index'))
        # Or return a 404 JSON if the frontend should handle it explicitly
        # return jsonify({"error": "File not found for this user"}), 404

    # --- 2. Get the Telegram file_id ---
    # Prefer 'telegram_file_id' if available, as it's directly used by getFile
    telegram_file_id = file_info.get('telegram_file_id')
    if not telegram_file_id:
        logging.error(f"Download failed: Metadata for '{filename}' (user: '{username}') is missing 'telegram_file_id'. Record: {file_info}")
        flash(f"Error: Cannot download '{filename}'. File metadata is incomplete.", 'error')
        return redirect(url_for('index'))

    logging.info(f"Found file_id '{telegram_file_id}' for '{filename}' (user: '{username}')")

    # --- 3. Ask Telegram API for the file path using getFile ---
    get_file_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile'
    params = {'file_id': telegram_file_id}

    try:
        logging.info(f"Requesting file path from Telegram for file_id: {telegram_file_id}")
        response = requests.get(get_file_url, params=params, timeout=30) # Add timeout
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

        response_json = response.json()

        if response_json.get('ok'):
            # --- 4. Extract the file_path from Telegram's response ---
            # Response looks like: {'ok': True, 'result': {'file_id': '...', 'file_unique_id': '...', 'file_size': ..., 'file_path': 'documents/file_123.xyz'}}
            file_path = response_json.get('result', {}).get('file_path')

            if file_path:
                logging.info(f"Received file_path '{file_path}' from Telegram.")

                # --- 5. Construct the direct download URL ---
                direct_download_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}'
                logging.info(f"Redirecting user to temporary download URL: {direct_download_url}")

                # --- 6. Redirect the user's browser ---
                # Flask's redirect() function sends a 302 Found response.
                # The browser will automatically follow this URL.
                return redirect(direct_download_url)

            else:
                # This shouldn't happen if ok=True, but handle defensively
                logging.error(f"Download failed: Telegram responded OK but did not provide 'file_path'. Response: {response_json}")
                flash(f"Error: Could not get download link for '{filename}' from Telegram (missing path).", 'error')
                return redirect(url_for('index'))
        else:
            # Telegram API reported an error (ok=False)
            error_desc = response_json.get('description', 'Unknown Telegram error')
            logging.error(f"Download failed: Telegram API error (getFile): {error_desc}. Response: {response_json}")
            flash(f"Error: Telegram rejected the download request: {error_desc}", 'error')
            return redirect(url_for('index'))

    # --- Handle potential errors during the requests call ---
    except requests.exceptions.Timeout:
        logging.error(f"Error: Request timed out while calling Telegram getFile API for file_id: {telegram_file_id}")
        flash("Error: Timed out connecting to Telegram to prepare download.", 'error')
        return redirect(url_for('index'))
    except requests.exceptions.RequestException as e:
        error_details = str(e)
        if e.response is not None:
             error_details += f" | Status: {e.response.status_code} | Response: {e.response.text}"
        logging.error(f"Error calling Telegram getFile API: {error_details}")
        flash(f"Network/Request Error communicating with Telegram: {error_details}", 'error')
        return redirect(url_for('index'))
    except json.JSONDecodeError:
        logging.error(f"Error: Telegram getFile response was not valid JSON. Status: {response.status_code if 'response' in locals() else 'N/A'}, Body: {response.text if 'response' in locals() else 'N/A'}")
        flash("Error: Received an invalid response from Telegram.", 'error')
        return redirect(url_for('index'))
    except Exception as e:
        # Catch-all for any other unexpected errors
        logging.error(f"Unexpected error during download process for '{filename}' (user: {username}): {e}", exc_info=True)
        flash("An unexpected internal error occurred during download preparation.", 'error')
        return redirect(url_for('index'))

# Make sure other routes like /files/<username> and /upload are still present and correct

# --- Run the App ---
if __name__ == '__main__':
    print("Starting Flask server...")
    app.run(host='0.0.0.0', port=5000, debug=True)