# main.py
import sys
import logging
from datetime import datetime
import time # Import time for sleep

# Optional: If using .env file for local development, uncomment the next two lines:
# from dotenv import load_dotenv
# load_dotenv()

# Import configuration
from config import CLIENTID, REQUEST_DELAY # Import REQUEST_DELAY

# Import modules with separated functionalities
from logging_setup import setup_logging # Import the logging setup function
from api_client import SmartApiClient
from symbol_fetcher import get_nifty50_tokens
from data_manager import update_symbol_data

logger = logging.getLogger(__name__) # Get logger for main script

def main():
    # ----------------------------
    # 0. Setup Logging
    # ----------------------------
    setup_logging()
    # Updated log message to be more explicit about the assumption
    logger.info("🚀 Starting data extraction and update process (Assuming Naive Datetimes Represent IST).")

    # Timezone setup is removed as we are using naive datetimes throughout

    # ----------------------------
    # 1. Initialize API Client and Authenticate
    # ----------------------------
    smart_api_client = None
    try:
        smart_api_client = SmartApiClient()
    except (ValueError, Exception) as e: # Catch exceptions raised during auth
        logger.critical(f"Fatal error during API authentication: {e}. Exiting.", exc_info=True)
        sys.exit("Exiting due to authentication failure.")

    # ----------------------------
    # 2. Fetch Nifty 50 symbols and tokens
    # ----------------------------
    nifty50_nse_df = get_nifty50_tokens()

    if nifty50_nse_df.empty:
        logger.error("🚨 Failed to fetch Nifty 50 tokens. Exiting.")
        if smart_api_client:
            smart_api_client.logout() # Attempt logout even on symbol fetch failure
        sys.exit("Exiting due to failure in fetching symbol list.")

    logger.info(f"Processing data for {len(nifty50_nse_df)} Nifty 50 equities.")

    # ----------------------------
    # 3. Process data for each symbol
    # ----------------------------
    processed_count = 0
    # Iterate over symbols and process each one
    for index, row in nifty50_nse_df.iterrows():
        try:
            symbol = row["name"]
            token = row["token"]
            logger.info(f"\n--- Starting processing for {symbol} ---") # Added log before processing symbol

            # Call the data manager function to handle update logic for this symbol
            update_symbol_data(symbol, token, smart_api_client)

            processed_count += 1
            logger.info(f"--- Finished processing {symbol}. Processed {processed_count}/{len(nifty50_nse_df)} symbols overall. ---")

        except Exception as e:
            # Catch any unexpected error during the processing of a single symbol
            # Log the error and continue with the next symbol
            logger.error(f"🚨 An unexpected error occurred while processing symbol {row.get('name', 'N/A')} (Token: {row.get('token', 'N/A')}): {e}", exc_info=True)
            logger.warning(f"Skipping symbol {row.get('name', 'N/A')} and proceeding to the next.")

        # --- Add a delay between processing each symbol ---
        # This helps comply with per-minute or per-hour rate limits when processing multiple items.
        if processed_count < len(nifty50_nse_df): # Don't delay after the last symbol
            logger.debug(f"Waiting for {REQUEST_DELAY} seconds before processing the next symbol.")
            time.sleep(REQUEST_DELAY)


    logger.info("\n🎉 Data extraction and update process completed.")

    # ----------------------------
    # 4. Logout from API
    # ----------------------------
    if smart_api_client:
        smart_api_client.logout()

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    main()