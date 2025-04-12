import logging
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs"

# /start command handler
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Print the chat ID to the console (useful for identifying where to send files)
    chat_id = update.effective_chat.id
    print(f"Chat ID: {chat_id}")

    await update.message.reply_text(
        "Hello! I am your File Storage Bot.\n\n"
        "Send me any file, and I'll store it locally."
    )

# /help command handler
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Help:\n\n"
        "- /start: Greet the bot.\n"
        "- /help: Show this message.\n"
        "Simply send me a file to store it locally."
    )

# Handles document/file uploads
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This check ensures there's actually a document in the message
    if not update.message.document:
        return

    document = update.message.document

    file_id = document.file_id
    file_name = document.file_name  # The original file name
    
    # Download the file from Telegram
    file_obj = await context.bot.get_file(file_id)

    # Save the file to your local folder (where bot.py is located)
    download_path = f"./{file_name}"
    await file_obj.download_to_drive(download_path)

    await update.message.reply_text(f"File '{file_name}' has been saved locally.")

def main():
    # Create the bot application
    app = ApplicationBuilder().token(TOKEN).build()

    # Add command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))

    # Add a message handler that filters for documents (any file type)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Start the bot (polling)
    app.run_polling()

if __name__ == "__main__":
    main()
