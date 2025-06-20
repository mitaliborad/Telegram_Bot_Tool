 # Flask Telegram Storage Tool
  - This project is a sophisticated file storage and management backend built with Python and Flask. It uses Telegram as its primary, secure storage medium for files, leveraging its generous file size limits. Files are initially uploaded to a temporary location (Google Drive) and then transferred to Telegram in the background, allowing for a robust, non-blocking user experience.
  - The application features a complete user authentication system, a powerful admin dashboard for managing users and files, and a modular architecture for easy maintenance and extension.



# Core Features
  - Secure File Storage: Leverages the Telegram Bot API to store files securely and reliably.
  - Large File Support: Automatically splits large files into Telegram-compatible chunks during the background transfer process.
  - Two-Stage Upload Process:
    Initial Upload: Files are first uploaded to a temporary staging area (Google Drive) for speed and reliability.
    Background Transfer: A background worker process downloads the file from Google Drive and uploads it to designated Telegram chats.
  - User Management: Full JWT-based authentication for API endpoints (registration, login) and session-based authentication for the admin panel.
  - Comprehensive Admin Dashboard: Built with Flask-Admin, it allows administrators to:
    View, search, and filter all users.
    Edit user details (username, email).
    View, search, and filter all active file/batch uploads.
    Inspect detailed metadata for each upload.
    Archive and restore user accounts.
    Archive and restore file/batch records.
    Permanently delete archived records.
  - Password Reset: Secure password reset functionality via email, using timed tokens.
  - API-First Design: Core functionalities like upload, download, and file management are exposed through a clean, prefixed RESTful API.
  - Database Integration: Uses MongoDB (via PyMongo and MongoDB Atlas) to store all user information and file metadata.



# Technology Stack
  - Backend: Python 3, Flask
  - Database: MongoDB (designed for MongoDB Atlas)
  - Authentication: Flask-Login (Admin Panel), Flask-JWT-Extended (API)
  - Admin Interface: Flask-Admin
  - External Services:
    Telegram Bot API: For core file storage.
    Google Drive API: For temporary file staging during upload.
    SMTP Service (e.g., Brevo): For sending password reset emails.
  - Tooling: python-dotenv for environment configuration.



# Prerequisites
  - Before you begin, ensure you have the following installed:
  - Python 3.8+ and Pip
  - Git
  - A virtual environment tool (like venv)



# Setup and Installation
Follow these steps to get the application running on your local machine.

  1. Clone the Repository
    Generated bash
    git clone <your-repository-url>
    cd mitaliborad-telegram_bot_tool.git
  
  2. Create and Activate a Virtual Environment
    It is highly recommended to use a virtual environment to manage project dependencies.
    Generated bash
    # For Unix/macOS
    python3 -m venv venv
    source venv/bin/activate
  
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate
  
  3. Install Dependencies
    Install all the required Python packages from the requirements.txt file.
    pip install -r requirements.txt
  
  4. Configure Environment Variables
    This is the most crucial step. The application relies on an .env file to securely store configuration and API keys.
    First, create a copy of the example file. If one doesn't exist, create a new file named .env.
    Now, open the newly created .env file and fill in the values. The application will not run without these.
    Generated ini
    # .env
    
    # --- Flask Application Settings ---
    # Generate a strong, random secret key. You can use: python -c 'import secrets; print(secrets.token_hex())'
    FLASK_SECRET_KEY="your_strong_flask_secret_key"
    JWT_SECRET_KEY="your_strong_jwt_secret_key"
    FRONTEND_URL="http://localhost:4200" # URL of your frontend application for CORS and email links
    
    # --- Database (MongoDB Atlas) ---
    ATLAS_USER="your_mongodb_atlas_username"
    ATLAS_PASSWORD="your_mongodb_atlas_password"
    ATLAS_CLUSTER_HOST="your_atlas_cluster_url.mongodb.net" # e.g., cluster0.xxxxx.mongodb.net
    
    # --- Telegram Bot Configuration ---
    # Get this from Telegram's @BotFather
    TELEGRAM_BOT_TOKEN="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
    # The numerical ID(s) of the chat(s)/channel(s) where files will be stored.
    # Get the ID from a bot like @userinfobot. For channels, make the bot an admin.
    # Separate multiple IDs with a comma. The first ID is considered the primary chat.
    TELEGRAM_CHAT_IDS="-1001234567890,-1009876543210"
    
    # --- Google Drive API Credentials (for temporary uploads) ---
    # See "Detailed Configuration" section below for instructions on how to get these.
    GDRIVE_CLIENT_ID="your-google-client-id.apps.googleusercontent.com"
    GDRIVE_CLIENT_SECRET="your-google-client-secret"
    GDRIVE_REFRESH_TOKEN="your-google-refresh-token"
    # The ID of the Google Drive folder to use for temporary uploads.
    GDRIVE_TEMP_FOLDER_ID="your_google_drive_folder_id"
    
    # --- Email (SMTP) Configuration (for Password Resets) ---
    # Example uses Brevo (formerly Sendinblue)
    MAIL_SERVER="smtp-relay.brevo.com"
    MAIL_PORT=587
    MAIL_USE_TLS=true
    MAIL_USE_SSL=false
    MAIL_USERNAME_BREVO="your_brevo_login_email"
    MAIL_PASSWORD_BREVO="your_brevo_v3_api_key_or_smtp_password"
    MAIL_SENDER_NAME="Your App Name"
    MAIL_SENDER_EMAIL_BREVO="your_verified_sender_email@example.com"
    
    # --- Token Expiration ---
    # Max age of the password reset token in seconds (default is 24 hours)
    PASSWORD_RESET_TOKEN_MAX_AGE=86400
  
  5. Running the Application
    Once the dependencies are installed and the .env file is configured, you can start the Flask development server.
    Generated bash
    python app_setup.py
  
  
  The application will be running at http://0.0.0.0:5000 by default.




# Project Structure
    The project is organized into several key directories to promote separation of concerns.
    Generated code
    mitaliborad-telegram_bot_tool.git/
    ├── app_setup.py            # Main application setup: initializes Flask, Admin, LoginManager, and registers blueprints.
    ├── config.py               # Central configuration, loads from .env, sets up logging.
    ├── extensions.py           # Instantiates Flask extensions (JWT, LoginManager) to avoid circular imports.
    ├── google_drive_api.py     # Handles all interactions with the Google Drive API.
    ├── telegram_api.py         # Handles all interactions with the Telegram Bot API.
    ├── requirements.txt        # Python package dependencies.
    ├── database/               # All database-related logic.
    │   ├── connection.py       # Establishes and manages the MongoDB connection.
    │   ├── user_models.py      # User model, user-related DB operations.
    │   ├── file_models.py      # Active file metadata DB operations.
    │   ├── archive_models.py   # Archived file metadata DB operations.
    │   └── ...                 # Other model and operation files.
    ├── routes/                 # Contains all Flask Blueprints (application endpoints).
    │   ├── auth_routes.py      # User registration and login API endpoints.
    │   ├── upload_routes.py    # File upload handling.
    │   ├── download_routes.py  # File download and streaming.
    │   ├── ...
    │   └── admin/              # All routes and views for the Flask-Admin interface.
    └── templates/              # HTML templates.
        └── admin/              # Templates for the Flask-Admin interface.




# Admin Panel
  The application includes a powerful admin panel accessible at /admin.
  Creating the First Admin User
  The application does not have an automatic "first admin" creation script. You must create the first admin user manually in the database.

  1. Generate a Password Hash: Run the following command in your terminal (with your virtual environment activated) to securely hash your desired admin password.
    Generated bash
    python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('your_chosen_password'))"
    Use code with caution.
    Bash
    Copy the output string (it will start with pbkdf2:sha256:...).

 2. Connect to MongoDB Atlas: Use a tool like MongoDB Compass or the web UI to connect to your database cluster.
 3. Insert the Admin User: Navigate to the Telegrambot database and find the userinfo collection. Insert a new document with the following structure:
    Generated json
    {
        "username": "admin",
        "email": "admin@example.com",
        "password_hash": "paste_the_generated_hash_from_step_1_here",
        "role": "Admin",
        "created_at": { "$date": "2023-01-01T00:00:00.000Z" }
    }
    Use code with caution.
    Json
    You can adjust the username, email, and created_at date.

 4. Login: You can now navigate to http://localhost:5000/admin/login and log in with the username and password you chose.




# Detailed Configuration Guide
    Google Drive API Credentials (GDRIVE_*)
    Go to the Google Cloud Console.
    Create a new project.
    Go to APIs & Services -> Library and enable the Google Drive API.
    Go to APIs & Services -> OAuth consent screen.
    Choose External and create the consent screen. Provide an app name, user support email, and developer contact information.
    On the Scopes page, add the scope .../auth/drive.
    Add your own email address as a Test User.
    Go to APIs & Services -> Credentials.
    Click Create Credentials -> OAuth client ID.
    Select Web application.
    Add https://developers.google.com/oauthplayground to the Authorized redirect URIs.
    Create the client. Copy the Client ID and Client Secret into your .env file.
    Get the Refresh Token:
    Go to the OAuth 2.0 Playground.
    In the top right settings gear, check "Use your own OAuth credentials" and paste your Client ID and Secret.
    In Step 1, find the Google Drive API v3 section and select the https://www.googleapis.com/auth/drive scope. Click Authorize APIs.
    Allow access with the Google account you added as a test user.
    In Step 2, click Exchange authorization code for tokens.
    Copy the Refresh token and paste it into your .env file.
    Get the Folder ID: Create a folder in your Google Drive, open it, and copy the ID from the URL: https://drive.google.com/drive/folders/THIS_IS_THE_ID.




# Telegram Bot (TELEGRAM_*)
    Bot Token: Talk to @BotFather on Telegram. Use the /newbot command to create a new bot and get your API token.
    Chat IDs:
    Create one or more private channels to store your files.
    Add your newly created bot to each channel as an administrator.
    To get a channel's ID, first post a message in the channel. Then, forward that message to a bot like @userinfobot. It will show you the "Forwarded from" chat ID, which is what you need. It will be a negative number starting with -100....
    Add these IDs to your .env file. The first one will be used as the primary location for fetching files.




Email (Brevo Example)
    Sign up for a Brevo (formerly Sendinblue) account.
    Navigate to SMTP & API.
    Under the SMTP tab, you will find your server, port, login (email), and master password (or you can generate a specific SMTP key). Use these values for the MAIL_* variables in your .env file.
    You must verify your sender email address in Brevo for emails to be sent successfully.
