# logging_setup.py
import logging
import sys
import os
import io

# Import log settings from config
from config import LOG_LEVEL, LOG_FILE, FOLDER_NAME

def setup_logging():
    """
    Sets up the logging configuration.
    Logs to console and a file within the data folder, using UTF-8 encoding.
    """
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Ensure the log directory exists (within the data folder)
    # Add a basic print fallback if directory creation fails
    log_dir = FOLDER_NAME
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as e:
        print(f"ERROR: Could not create log directory {log_dir}: {e}", file=sys.stderr)
        # Fallback log file path to current directory if data folder fails
        log_dir = "."
        print(f"WARNING: Logging to current directory: {os.path.join('.', LOG_FILE)}", file=sys.stderr)

    log_filepath = os.path.join(log_dir, LOG_FILE)


    # File Handler - logs everything from the configured level using UTF-8
    try:
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8') # Specify UTF-8 encoding
        file_handler.setFormatter(log_formatter)
    except Exception as e:
         print(f"ERROR: Could not create log file handler: {e}", file=sys.stderr)
         file_handler = None # Disable file logging if fails


    # Console Handler - logs everything from the configured level using UTF-8
    # Directly setting encoding on StreamHandler is the standard way in modern Python logging (3.7+)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    if hasattr(console_handler.stream, 'reconfigure'): # Check for Python 3.7+ streams
        console_handler.stream.reconfigure(encoding='utf-8')
    else:
        # Fallback for older Python versions or streams without reconfigure
        try:
            console_handler.encoding = 'utf-8'
        except Exception as e:
             # This warning indicates console might still have encoding issues on some systems
             print(f"WARNING: Could not set console handler encoding to UTF-8: {e}. Console output may have encoding issues.", file=sys.stderr)


    # Get root logger and set level
    root_logger = logging.getLogger()
    try:
        # Convert log level string to logging constant (e.g., 'INFO' -> logging.INFO)
        log_level_int = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
        root_logger.setLevel(log_level_int)
    except Exception:
        root_logger.setLevel(logging.INFO)
        logging.warning(f"Invalid LOG_LEVEL '{LOG_LEVEL}' in config. Falling back to INFO.")

    # Add handlers only if they are not already added and are valid
    # Clear existing handlers first to prevent duplicate logs if setup_logging is called multiple times
    if root_logger.handlers:
        # Safely close handlers before removing
        for handler in root_logger.handlers[:]:
             try:
                 handler.acquire()
                 handler.flush()
                 handler.close()
             except Exception:
                  pass # Ignore errors during handler cleanup
             finally:
                 if handler.locked(): handler.release() # Release lock if acquired
             root_logger.removeHandler(handler)


    root_logger.addHandler(console_handler)
    if file_handler: # Only add if file handler was successfully created
        root_logger.addHandler(file_handler)


    # Optional: Set levels for specific loggers if they are too noisy
    # logging.getLogger('requests').setLevel(logging.WARNING)
    # logging.getLogger('urllib3').setLevel(logging.WARNING)
    # logging.getLogger('SmartApi').setLevel(logging.WARNING)


    logging.info("Logging setup complete.")
    logging.info(f"Log level: {LOG_LEVEL}")
    if file_handler:
        logging.info(f"Log file: {log_filepath}")
    else:
        logging.warning("File logging is disabled.")
    logging.info(f"Data folder: {FOLDER_NAME}")