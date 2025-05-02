# --- Imports ---
import os
import json
import logging
import math
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
import pytz



# --- Formatting Utilities ---
def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds): # Added type/finite check
        logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
        return "--:--"
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if seconds < 0: seconds = 0 
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

