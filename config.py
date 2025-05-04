# config.py
import os

# --- API Credentials ---
# It's strongly recommended to set these as environment variables for production.
# For local development convenience, you can use a .env file with the python-dotenv library.
# Example .env content:
# APIKEY='YOUR_ANGELONE_API_KEY'
# CLIENTID='YOUR_ANGELONE_CLIENT_ID'
# PASSWORD='YOUR_ANGELONE_PASSWORD_OR_PIN'
# LOGINTOKEN='YOUR_TOTP_SECRET_KEY'

APIKEY = os.getenv('APIKEY', 'YOUR_APIKEY_PLACEHOLDER')
CLIENTID = os.getenv('CLIENTID', 'YOUR_CLIENTID_PLACEHOLDER')
PASSWORD = os.getenv('PASSWORD', 'YOUR_PIN_PLACEHOLDER')
LOGINTOKEN = os.getenv('LOGINTOKEN', 'YOUR_TOTP_LOGINTOKEN_PLACEHOLDER')

# Check if placeholders are still present (basic check). Use explicit checks for None too.
if APIKEY is None or 'YOUR_APIKEY_PLACEHOLDER' in str(APIKEY):
    print("WARNING: API Key is not configured. Set the APIKEY environment variable for production use.")
if CLIENTID is None or 'YOUR_CLIENTID_PLACEHOLDER' in str(CLIENTID):
    print("WARNING: Client ID is not configured. Set the CLIENTID environment variable for production use.")
if PASSWORD is None or 'YOUR_PIN_PLACEHOLDER' in str(PASSWORD):
    print("WARNING: Password/PIN is not configured. Set the PASSWORD environment variable for production use.")
if LOGINTOKEN is None or 'YOUR_TOTP_LOGINTOKEN_PLACEHOLDER' in str(LOGINTOKEN):
    print("WARNING: TOTP Login Token is not configured. Set the LOGINTOKEN environment variable for production use.")


# --- Endpoints ---
NSE_CSV_URL = "https://archives.nseindia.com/content/indices/ind_nifty50list.csv"
ANGELONE_INSTRUMENT_LIST_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# --- Time Interval for Candle Data ---
# Valid intervals supported by Angel One API: ONE_MINUTE, THREE_MINUTE, FIVE_MINUTE, TEN_MINUTE,
# FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, ONE_DAY
TIME_INTERVAL = os.getenv('TIME_INTERVAL', 'ONE_HOUR')

# --- Retry and Delay Settings ---
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', 1))       # seconds between API retries on *same request* failure
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', 0.25))  # seconds between requests for different chunks/symbols

# --- Data Storage Settings ---
# Base folder name, interval will be appended
BASE_FOLDER_NAME = "NIFTY_50_DATA"
# Construct the final folder name including the time interval
FOLDER_NAME = f"{BASE_FOLDER_NAME}_{TIME_INTERVAL.upper()}" # Ensure uppercase consistency

CSV_EXTENSION = ".csv"
PARQUET_EXTENSION = ".parquet"

# --- Time Parameters ---
# Starting date for fetching historical data (YYYY-MM-DD). Data before this date is not fetched.
START_DATE = os.getenv('START_DATE', "2016-10-01")
# Note: Timezone is assumed to be IST (UTC+5:30) for all naive datetimes used in this script.

# --- Logging Settings ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper() # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILE = os.getenv('LOG_FILE', 'nifty_data_updater.log')