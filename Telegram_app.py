import os
import requests # Library to make web requests (to Telegram API)
from flask import Flask, request, render_template, flash, redirect, url_for, make_response

# --- Configuration ---
# !! IMPORTANT: Replace these with your actual Bot Token and Chat ID !!
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' # Paste your Bot Token here (e.g., '1234567890:ABC...')
TELEGRAM_CHAT_ID = '-4603853425'     # Paste your Group Chat ID here (e.g., '-1001234567890') MUST BE A STRING

# Basic check if placeholders are replaced
if TELEGRAM_BOT_TOKEN == '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' or TELEGRAM_CHAT_ID == '-4603853425':
    print("="*60)
    print("!!! WARNING: Please replace 'YOUR_BOT_TOKEN' and 'YOUR_CHAT_ID' in the script !!!")
    print("="*60)
    # Consider adding 'import sys; sys.exit(1)' here to stop if not replaced

# --- Flask App Setup ---
# We tell Flask to look for templates in the current directory ('.') instead of 'templates'
app = Flask(__name__, template_folder='.')
# Secret key is needed for flashing messages (for success/error feedback)
# Replace with a real random secret key if this becomes public
app.secret_key = 'a_simple_secret_key_for_now'

# --- Telegram API Function ---
def send_file_to_telegram(file_object, filename):
    """Sends the provided file object to the configured Telegram chat."""
    # Construct the URL for the sendDocument method
    api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'

    # Prepare the file for sending. 'document' is the parameter name Telegram expects.
    # We send the filename and the file object itself.
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
            return True, f"File '{filename}' sent successfully!"
        else:
            error_desc = response_json.get('description', 'Unknown Telegram error')
            print(f"Telegram API Error: {error_desc} (Full Response: {response.text})")
            return False, f"Telegram API Error: {error_desc}"

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out while sending '{filename}'.")
        return False, "Error: The request to Telegram timed out."
    except requests.exceptions.RequestException as e:
        # Handles network errors, connection errors, HTTP errors (like 404, 401)
        print(f"Error sending file via Telegram API: {e}")
        # Try to get more detail from the response if available
        error_details = str(e)
        if e.response is not None:
            error_details += f" | Response: {e.response.text}"
        return False, f"Network/Request Error: {error_details}"
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred during Telegram send: {e}")
        return False, f"An unexpected error occurred: {e}"

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


# Route for handling the file upload, accessed via POST from the form
@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the file upload from the form."""

    # Check if the 'file' field is present in the form submission
    if 'file' not in request.files:
        flash('No file part in the request!', 'error')
        return redirect(url_for('index')) # Go back to the main page

    # Get the file object from the request
    file = request.files['file']

    # Check if the user submitted the form without selecting a file
    if file.filename == '':
        flash('No file selected!', 'error')
        return redirect(url_for('index')) # Go back to the main page

    # If a file was selected and has a name
    if file:
        filename = file.filename # Get the original filename
        print(f"Received file: {filename}")

        # Call our function to send the file to Telegram
        # We pass the file object directly; 'requests' handles reading it
        success, message = send_file_to_telegram(file, filename)

        # Store the result message to show it on the page after redirecting
        if success:
            flash(message, 'success') # Green message
        else:
            flash(message, 'error')   # Red message

        # Always redirect back to the main page ('/') after attempting the upload
        return redirect(url_for('index'))

    # Fallback if something unexpected happened with the file object
    flash('An unexpected error occurred during file processing.', 'error')
    return redirect(url_for('index'))

# --- Run the App ---
if __name__ == '__main__':
    print("Starting Flask server...")
    # Runs the Flask development server
    # host='0.0.0.0' makes it accessible from other devices on your local network
    # Use '127.0.0.1' to only allow access from your own computer
    # debug=True helps with development (shows errors, auto-reloads), but TURN OFF if public
    app.run(host='0.0.0.0', port=5000, debug=True)