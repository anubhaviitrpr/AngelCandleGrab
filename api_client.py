# api_client.py
import time
import pyotp
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from SmartApi import SmartConnect
from SmartApi.smartExceptions import SmartAPIException, DataException # Import DataException

from config import (
    APIKEY, CLIENTID, PASSWORD, LOGINTOKEN,
    MAX_RETRIES, RETRY_DELAY, TIME_INTERVAL
)

logger = logging.getLogger(__name__)

class SmartApiClient:
    """
    Handles authentication and API calls to Angel One SmartApi.
    Assumes API times are implicitly IST and returns naive datetimes.
    """
    def __init__(self):
        self.smartApi = None
        self.client_id = CLIENTID
        self.interval = TIME_INTERVAL
        self._authenticate()

    def _authenticate(self):
        """Authenticates with the SmartApi."""
        logger.info("Attempting SmartApi authentication...")
        # Check for configured credentials before proceeding
        if APIKEY is None or 'YOUR_APIKEY_PLACEHOLDER' in str(APIKEY):
             logger.critical("API Key is not configured. Set the APIKEY environment variable.")
             raise ValueError("API Key not configured.")
        if CLIENTID is None or 'YOUR_CLIENTID_PLACEHOLDER' in str(CLIENTID):
             logger.critical("Client ID is not configured. Set the CLIENTID environment variable.")
             raise ValueError("Client ID not configured.")
        if PASSWORD is None or 'YOUR_PIN_PLACEHOLDER' in str(PASSWORD):
             logger.critical("Password/PIN is not configured. Set the PASSWORD environment variable.")
             raise ValueError("Password/PIN not configured.")
        if LOGINTOKEN is None or 'YOUR_TOTP_LOGINTOKEN_PLACEHOLDER' in str(LOGINTOKEN):
             logger.critical("TOTP Login Token is not configured. Set the LOGINTOKEN environment variable.")
             raise ValueError("TOTP Login Token not configured.")

        try:
            self.smartApi = SmartConnect(APIKEY)
            # Ensure LOGINTOKEN is treated as a string for pyotp.TOTP
            totp = pyotp.TOTP(str(LOGINTOKEN)).now()
            session_data = self.smartApi.generateSession(CLIENTID, PASSWORD, totp)

            if session_data and session_data.get('data'):
                logger.info("✅ SmartApi authentication successful.")
            elif session_data and "errorcode" in session_data:
                 error_code = session_data.get('errorcode')
                 message = session_data.get('message', 'Unknown API error')
                 logger.error(f"❌ SmartApi authentication failed. API Error {error_code}: {message}")
                 raise SmartAPIException(f"Authentication failed: {message}")
            else:
                 logger.error(f"❌ SmartApi authentication failed: Unexpected response format. Response: {session_data}")
                 raise Exception("Authentication failed: Unexpected API response.")

        except SmartAPIException as e:
             logger.critical(f"🚨 Angel One API exception during authentication: {e}", exc_info=True)
             raise
        except Exception as e:
            logger.critical(f"🚨 Unexpected error during SmartApi authentication: {e}", exc_info=True)
            raise

    # get_candle_data expects naive datetime objects and returns naive datetimes
    def get_candle_data(self, token: str, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        """
        Fetches candle data for a given token and naive datetime range.
        Handles retries and rate limiting. Assumes naive datetimes represent IST.
        Ensures returned DataFrame has a naive 'DateTime' column.

        Args:
            token: The symbol token.
            from_date: Start date/time (naive datetime).
            to_date: End date/time (naive datetime).

        Returns:
            A pandas DataFrame with columns ["DateTime", "Open", "High", "Low", "Close", "Volume"]
            with **naive** datetimes, if successful, otherwise an empty DataFrame.
        """
        # Format naive datetimes for API call. API expects naive datetime strings.
        # Use precise formatting to avoid ambiguity
        from_date_str = from_date.strftime('%Y-%m-%d %H:%M')
        to_date_str = to_date.strftime('%Y-%m-%d %H:%M')

        params = {
            "exchange": "NSE",
            "symboltoken": token,
            "interval": self.interval,
            "fromdate": from_date_str,
            "todate": to_date_str
        }

        logger.debug(f"Fetching data for token {token}, interval {self.interval} from {from_date_str} to {to_date_str} (assuming IST)")

        # Keep track of the last caught exception to potentially log details if all retries fail
        last_exception = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.smartApi.getCandleData(params)

                if not response:
                    logger.warning(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: Empty or None API response.")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    else:
                        # Permanent failure after retries for empty response
                        logger.error(f"🚨 Permanent failure for token {token} after {MAX_RETRIES} attempts: Empty API response.")
                        return pd.DataFrame()

                if "errorcode" in response:
                    error_code = response.get('errorcode')
                    message = response.get('message', 'Unknown API error')

                    if error_code == "AB1004":
                        logger.warning(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: Rate limit hit (AB1004). Waiting longer ({RETRY_DELAY * 2}s)...")
                        time.sleep(RETRY_DELAY * 2)
                        continue # Retry
                    elif error_code == "" and message == "SUCCESS":
                         # Success case, continue to process data below
                         pass
                    else:
                        # Handle other specific API errors if necessary
                        logger.error(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: API Error {error_code}: {message}")
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY)
                            continue # Retry on general API error
                        else:
                            # Permanent API error failure after retries
                            logger.error(f"🚨 Permanent API error failure for token {token} after {MAX_RETRIES} attempts: {error_code} - {message}")
                            return pd.DataFrame() # Return empty DataFrame on permanent failure

                if not response.get("data"):
                    # Success response but no data (e.g., outside market hours, holiday, no trades in interval)
                    logger.info(f"No data returned by API for token {token} for period {params['fromdate']} to {params['todate']}")
                    return pd.DataFrame() # Return empty DataFrame as there's no data

                # Data received successfully
                df = pd.DataFrame(
                    response["data"],
                    columns=["DateTime", "Open", "High", "Low", "Close", "Volume"]
                )

                # --- IMPORTANT ---
                # Ensure the 'DateTime' column is explicitly **naive** datetime64[ns].
                # Parse datetime strings, coercing errors to NaT.
                # If the string includes offset info, pd.to_datetime will create tz-aware.
                # We explicitly convert to naive, assuming the original timezone (+05:30) was IST.

                # Step 1: Attempt parsing, allowing timezone info initially
                df["DateTime"] = pd.to_datetime(df["DateTime"], errors='coerce')

                # Step 2: Convert any resulting tz-aware datetimes to naive, dropping timezone info
                # This assumes the source TZ was the one we want to represent as naive IST
                if pd.api.types.is_datetime64tz_dtype(df["DateTime"]):
                    logger.debug("API data parsed as tz-aware, converting to naive (assuming IST source).")
                    df["DateTime"] = df["DateTime"].dt.tz_convert(None) # Converts *from* its current timezone *to* naive

                # Step 3: Ensure the dtype is datetime64[ns] (naive) after potential conversion
                # If it's still not the correct dtype (e.g., object after failed parse), try again explicitly converting to naive
                if not pd.api.types.is_datetime64_ns_dtype(df["DateTime"]):
                     logger.warning(f"API DateTime column not datetime64[ns] after initial processing: {df['DateTime'].dtype}. Attempting final naive conversion.")
                     try:
                         # This might fail if the underlying data is problematic
                         df["DateTime"] = pd.to_datetime(df["DateTime"], errors='coerce').dt.tz_convert(None)
                     except Exception as e:
                         logger.error(f"🚨 Final attempt to convert API DateTime to naive failed: {e}", exc_info=True)
                         # If conversion fails critically, the column might be unusable.
                         # Let's proceed but rows with NaT will be dropped.


                # Drop rows where DateTime is NaT after parsing/conversion
                initial_rows = len(df)
                df.dropna(subset=["DateTime"], inplace=True)
                if len(df) < initial_rows:
                    logger.warning(f"Dropped {initial_rows - len(df)} rows with invalid/unparseable DateTime from API response.")

                # Final check for empty dataframe after cleaning
                if df.empty:
                    logger.warning("DataFrame from API became empty after DateTime cleaning.")
                    return pd.DataFrame()

                # --- Successful fetch and processing, return the DataFrame ---
                logger.debug(f"Successfully fetched and parsed {len(df)} rows (naive datetime) for token {token}")
                return df

            # --- Specific Exception Handling for non-JSON Rate Limit response ---
            except DataException as e:
                 last_exception = e # Store the exception
                 # Catch the specific DataException indicating JSON parsing failed
                 # Check if the message contains the rate limit text
                 if "exceeding access rate" in str(e):
                     logger.warning(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: Detected non-JSON Rate limit response. Waiting longer ({RETRY_DELAY * 2}s)...", exc_info=True)
                     time.sleep(RETRY_DELAY * 2)
                     continue # Retry
                 else:
                      # It's a DataException, but not related to rate limit text
                      logger.error(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: DataException during API call (non-rate limit): {e}. Retrying in {RETRY_DELAY}s.", exc_info=True)
                      if attempt < MAX_RETRIES:
                           time.sleep(RETRY_DELAY)
                           continue # Retry on other DataExceptions
                      else:
                           # Permanent failure after retries for this specific error
                           logger.error(f"🚨 Permanent DataException failure for token {token} after {MAX_RETRIES} attempts: {e}", exc_info=True)
                           return pd.DataFrame() # Return empty DataFrame on permanent failure

            except (SmartAPIException, requests.exceptions.RequestException) as e:
                last_exception = e # Store the exception
                # Catch Angel One's specific exceptions (excluding DataException now handled above) or general network errors
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: API or Network Exception: {e}. Retrying in {RETRY_DELAY}s.", exc_info=True)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue # Retry on network or other known API exception
                else:
                    # Permanent failure after retries for this exception type
                    logger.error(f"🚨 Permanent exception failure for token {token} after {MAX_RETRIES} attempts: {e}", exc_info=True)
                    return pd.DataFrame() # Return empty DataFrame on permanent failure

            except Exception as e:
                last_exception = e # Store the exception
                # Catch any other unexpected exceptions
                logger.error(f"Attempt {attempt}/{MAX_RETRIES} for token {token}: Unexpected Exception during API call: {e}. Retrying in {RETRY_DELAY}s.", exc_info=True)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue # Retry on any exception
                else:
                    # Permanent unexpected failure after retries
                    logger.error(f"🚨 Permanent unexpected failure for token {token} after {MAX_RETRIES} attempts: {e}", exc_info=True)
                    return pd.DataFrame() # Return empty DataFrame on permanent failure

        # --- If the loop finishes without returning, it means all retries failed ---
        # Log a final error message using the last caught exception, if any.
        if last_exception:
             logger.error(f"🚨 Failed to fetch data for token {token} after all {MAX_RETRIES} retry attempts. Last exception: {last_exception}", exc_info=True)
        else:
             # This case should ideally not be reached if MAX_RETRIES > 0 and exceptions are caught
             logger.error(f"🚨 Failed to fetch data for token {token} after all {MAX_RETRIES} retry attempts with unknown reason.")

        # Ensure an empty DataFrame is returned if all retries are exhausted
        return pd.DataFrame()


    def logout(self):
        """Logs out from the SmartApi session using terminateSession."""
        logger.info(f"Attempting SmartApi logout for client ID: {self.client_id}...")
        if self.smartApi:
            # Check if the terminateSession method exists and is callable
            if hasattr(self.smartApi, 'terminateSession') and callable(self.smartApi.terminateSession):
                try:
                    # Call terminateSession with the client ID (as a string)
                    logout_response = self.smartApi.terminateSession(str(self.client_id))
                    # terminateSession usually returns a response dict
                    if logout_response and logout_response.get('message') == 'SUCCESS':
                         logger.info("✅ SmartApi logout successful.")
                    elif logout_response and logout_response.get('errorcode'):
                         # Handle API error during logout
                         logger.error(f"❌ SmartApi logout failed. API Error {logout_response.get('errorcode')}: {logout_response.get('message', 'Unknown')}")
                    else:
                         # Log warning if the API reports logout was not a 'SUCCESS' and no error code
                         logger.warning(f"SmartApi logout attempted but neither SUCCESS nor Error reported. Response: {logout_response}")
                except Exception as e:
                    # Use logger.exception to log the traceback for unexpected errors during logout
                    logger.exception(f"🚨 SmartApi logout failed unexpectedly for client ID {self.client_id}.")
            else:
                 # Log a warning if terminateSession method is not found
                 logger.warning("SmartApi client object exists, but 'terminateSession' method is not available. Cannot perform API logout.")
        else:
            # Log a warning if the smartApi object wasn't initialized
            logger.warning("SmartApi client not initialized, cannot logout.")