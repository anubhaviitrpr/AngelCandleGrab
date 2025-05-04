# symbol_fetcher.py
import requests
import pandas as pd
import logging
from io import BytesIO

# Import config
from config import NSE_CSV_URL, ANGELONE_INSTRUMENT_LIST_URL

logger = logging.getLogger(__name__)

def get_nifty50_tokens() -> pd.DataFrame:
    """
    Fetches the list of Nifty 50 symbols and their corresponding
    Angel One tokens from NSE and Angel One sources.
    Returns a pandas DataFrame with 'name' and 'token' columns, or an empty DataFrame on failure.
    """
    logger.info("🔄 Fetching Nifty 50 symbols from NSE...")
    headers = {"User-Agent": "Mozilla/5.0"} # Sometimes required by websites
    nifty50_symbols = []
    try:
        # Added timeout and error checking for requests
        response = requests.get(NSE_CSV_URL, headers=headers, timeout=10)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        # Use Latin-1 encoding as NSE CSV often uses it, handle potential errors
        try:
             nifty50_df = pd.read_csv(BytesIO(response.content), encoding='latin-1')
        except Exception as e:
             logger.error(f"🚨 Failed to read/decode NSE CSV content: {e}", exc_info=True)
             return pd.DataFrame()


        if "Symbol" in nifty50_df.columns:
             # Filter out potential NaNs or empty strings in the 'Symbol' column
             nifty50_symbols = nifty50_df["Symbol"].dropna().astype(str).tolist()
             logger.info(f"✅ Fetched {len(nifty50_symbols)} potential symbols from NSE.")
        else:
             logger.error(f"NSE CSV does not contain 'Symbol' column. Columns found: {nifty50_df.columns.tolist()}")
             return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 Failed to fetch NIFTY 50 CSV from NSE: {e}", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on failure
    except Exception as e:
         logger.error(f"🚨 Unexpected error during NSE CSV processing: {e}", exc_info=True)
         return pd.DataFrame()

    if not nifty50_symbols:
        logger.warning("No Nifty 50 symbols fetched from NSE. Cannot proceed.")
        return pd.DataFrame()

    logger.info("🔄 Fetching instrument list from Angel One...")
    df_instruments = pd.DataFrame()
    try:
        # Added timeout and error checking for requests
        response = requests.get(ANGELONE_INSTRUMENT_LIST_URL, timeout=30)
        response.raise_for_status()
        instruments = response.json()
        df_instruments = pd.DataFrame(instruments)
        logger.info(f"✅ Fetched {len(df_instruments)} instruments from Angel One.")
    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 Failed to fetch instrument list from Angel One: {e}", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on failure
    except ValueError:
        logger.error("🚨 Failed to parse Angel One instrument list as JSON.", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on failure
    except Exception as e:
        logger.error(f"🚨 Unexpected error fetching Angel One instrument list: {e}", exc_info=True)
        return pd.DataFrame()

    if df_instruments.empty:
         logger.warning("Angel One instrument list is empty. Cannot match Nifty 50 symbols.")
         return pd.DataFrame()

    # Filter for NSE instruments that are also in the Nifty 50 symbol list
    nse_instruments = df_instruments[df_instruments["exch_seg"] == "NSE"].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Ensure 'name' and 'token' columns exist and are not null before selection
    required_cols = ["name", "token"]
    if not all(col in nse_instruments.columns for col in required_cols):
         missing = [col for col in required_cols if col not in nse_instruments.columns]
         logger.error(f"🚨 Angel One instrument list missing required columns: {missing}. Columns found: {nse_instruments.columns.tolist()}")
         return pd.DataFrame()

    # Ensure name and token columns are string type before filtering/selecting
    try:
        nse_instruments['name'] = nse_instruments['name'].astype(str)
        nse_instruments['token'] = nse_instruments['token'].astype(str)
    except Exception as e:
        logger.error(f"Error converting name or token columns to string: {e}", exc_info=True)
        return pd.DataFrame()


    # Filter for Nifty 50 symbols present in the Angel One NSE list
    # Ensure both 'name' and 'token' are not null
    nifty50_nse_df = nse_instruments[
        nse_instruments["name"].isin(nifty50_symbols) &
        nse_instruments['name'].notna() &
        nse_instruments['token'].notna()
    ][required_cols].reset_index(drop=True) # Reset index for clean output

    if nifty50_nse_df.empty:
        logger.warning("⚠️ Found 0 matching Nifty 50 symbols with valid tokens in the Angel One NSE list.")
    else:
        logger.info(f"Found {len(nifty50_nse_df)} Nifty 50 symbols with corresponding tokens.")

    return nifty50_nse_df