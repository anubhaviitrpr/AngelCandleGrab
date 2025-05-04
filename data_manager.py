# data_manager.py
import os
import time
import pandas as pd
from datetime import datetime, timedelta
import logging

from config import (
    FOLDER_NAME, CSV_EXTENSION, PARQUET_EXTENSION,
    REQUEST_DELAY, START_DATE
)
from data_utils import clean_ohlcv
from api_client import SmartApiClient

logger = logging.getLogger(__name__)

def read_existing_data(symbol: str) -> pd.DataFrame:
    """
    Reads existing data for a symbol, preferring CSV, falling back to Parquet.
    Attempts to parse DateTime into **naive datetime64[ns]**.
    Returns a DataFrame with a **naive datetime64[ns]** 'DateTime' column, or empty DataFrame.
    Applies minimal cleaning initially (parsing/dropna, basic OHLCV).
    Assumes datetimes in files are intended as IST.
    Ensures returned DataFrame has a naive 'DateTime' column of dtype datetime64[ns].
    """
    csv_filename = os.path.join(FOLDER_NAME, f"{symbol}{CSV_EXTENSION}")
    parquet_filename = os.path.join(FOLDER_NAME, f"{symbol}{PARQUET_EXTENSION}")
    existing_data = pd.DataFrame()
    data_read_attempted = False # Flag to know if we tried reading any file

    # 1. Try reading from CSV first (Primary)
    if os.path.exists(csv_filename):
        data_read_attempted = True
        logger.info(f"Attempting to read existing data for {symbol} from CSV: {csv_filename}")
        try:
            existing_data = pd.read_csv(csv_filename)
            logger.info(f"Read {len(existing_data)} rows from {csv_filename}.")

            # --- IMPORTANT ---
            # Handle date/time parsing and explicitly ensure **naive** datetime64[ns].
            if "DateTime" in existing_data.columns:
                existing_data["DateTime"] = pd.to_datetime(existing_data["DateTime"], errors='coerce')
                # If it parsed as tz-aware, convert to naive. Assumes original TZ was IST.
                if pd.api.types.is_datetime64tz_dtype(existing_data["DateTime"]):
                     logger.debug(f"CSV DateTime column for {symbol} read as tz-aware. Converting to naive.")
                     existing_data["DateTime"] = existing_data["DateTime"].dt.tz_convert(None)
                # If not datetime64[ns] after parse, attempt a final naive conversion
                elif not pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]):
                    logger.warning(f"CSV DateTime column for {symbol} not datetime64[ns] after parse: {existing_data['DateTime'].dtype}. Attempting re-parse to naive.")
                    # Use errors='coerce' in the re-parse as well
                    existing_data["DateTime"] = pd.to_datetime(existing_data["DateTime"], errors='coerce').dt.tz_convert(None)


            elif "Date" in existing_data.columns and "Time" in existing_data.columns:
                # Support old format and reconstruct DateTime to **naive**
                existing_data["DateTime"] = pd.to_datetime(existing_data["Date"] + " " + existing_data["Time"], errors='coerce')
                existing_data.drop(columns=["Date", "Time"], errors='ignore', inplace=True)
            else:
                logger.warning(f"Existing CSV for {symbol} in {FOLDER_NAME} does not have 'DateTime' or 'Date'/'Time' columns. Cannot use this file.")
                existing_data = pd.DataFrame() # Cannot use CSV

            # Check if DateTime column exists and is not all null AND is the correct naive dtype
            if "DateTime" in existing_data.columns and pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]) and not existing_data["DateTime"].isnull().all():
                 # CSV read successfully and has valid naive DateTime
                 # Now check for essential OHLCV columns
                 ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                 if all(col in existing_data.columns for col in ohlcv_cols):
                      # Data is usable from CSV
                      data_read_attempted = False # Reset to prevent fallback to Parquet if CSV was good
                      pass # CSV is good, proceed to basic cleaning below
                 else:
                      missing_ohlcv = [col for col in ohlcv_cols if col not in existing_data.columns]
                      logger.warning(f"CSV file for {symbol} has DateTime but missing essential OHLCV columns: {missing_ohlcv}. Will try Parquet if available.")
                      existing_data = pd.DataFrame() # Treat as unusable CSV
                      # data_read_attempted remains True to signal we tried CSV

            else:
                 # If DateTime column is missing, all null, or wrong dtype after CSV processing
                 if "DateTime" in existing_data.columns:
                     logger.warning(f"CSV file for {symbol} has unusable DateTime column (all null or wrong dtype: {existing_data['DateTime'].dtype}) after parsing. Will try Parquet if available.")
                 else:
                     logger.warning(f"CSV file for {symbol} has no DateTime column after parsing. Will try Parquet if available.")
                 existing_data = pd.DataFrame() # Treat as failed read if DateTime is bad
                 # data_read_attempted remains True to signal we tried CSV


        except Exception as e:
            logger.error(f"🚨 Error reading CSV file for {symbol} from {csv_filename}: {e}. Falling back to Parquet.", exc_info=True)
            existing_data = pd.DataFrame() # Clear data frame
            # data_read_attempted remains True to signal we tried CSV


    # 2. If CSV failed or doesn't exist/was unusable, try reading from Parquet (Fallback)
    # Only attempt if CSV wasn't usable (existing_data is empty) AND Parquet exists
    if existing_data.empty and os.path.exists(parquet_filename):
        data_read_attempted = True # Mark that we are attempting Parquet
        logger.info(f"Attempting to read existing data for {symbol} from Parquet: {parquet_filename}")
        try:
            # Parquet should ideally save/load with correct dtypes including datetime
            existing_data = pd.read_parquet(parquet_filename)
            logger.info(f"Read {len(existing_data)} rows from {parquet_filename}.")

            # --- IMPORTANT ---
            # Ensure 'DateTime' column exists and is **naive** datetime64[ns] after reading Parquet
            if "DateTime" not in existing_data.columns:
                 logger.warning(f"Parquet file for {symbol} is missing 'DateTime' column. Trying to reconstruct from 'Date'/'Time' (old format).")
                 if "Date" in existing_data.columns and "Time" in existing_data.columns:
                      existing_data["DateTime"] = pd.to_datetime(existing_data["Date"] + " " + existing_data["Time"], errors='coerce')
                      existing_data.drop(columns=["Date", "Time"], errors='ignore', inplace=True)
                 else:
                      logger.error(f"Parquet file for {symbol} is missing 'DateTime' and 'Date'/'Time' columns. Cannot use this file.")
                      existing_data = pd.DataFrame() # Cannot use Parquet
            else:
                 # Ensure the 'DateTime' column is actually datetime type after reading
                 existing_data['DateTime'] = pd.to_datetime(existing_data['DateTime'], errors='coerce')
                 # Convert to naive if it somehow came in as tz-aware
                 if pd.api.types.is_datetime64tz_dtype(existing_data["DateTime"]):
                      logger.debug(f"Parquet DateTime column for {symbol} read as tz-aware. Converting to naive.")
                      existing_data["DateTime"] = existing_data["DateTime"].dt.tz_convert(None)
                 # If not datetime64[ns] after parse, attempt a final naive conversion
                 elif not pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]):
                    logger.warning(f"Parquet DateTime column for {symbol} not datetime64[ns] after parse: {existing_data['DateTime'].dtype}. Attempting re-parse to naive.")
                    existing_data["DateTime"] = pd.to_datetime(existing_data["DateTime"], errors='coerce').dt.tz_convert(None)


            # Check if DateTime column exists and is not all null AFTER processing Parquet
            # Ensure it's the correct dtype
            if "DateTime" in existing_data.columns and pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]) and not existing_data["DateTime"].isnull().all():
                # Parquet read successfully and has valid naive DateTime
                # Now check for essential OHLCV columns
                ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(col in existing_data.columns for col in ohlcv_cols):
                     # Data is usable from Parquet
                     pass # Parquet is good, proceed to basic cleaning below
                else:
                     missing_ohlcv = [col for col in ohlcv_cols if col not in existing_data.columns]
                     logger.warning(f"Parquet file for {symbol} has DateTime but missing essential OHLCV columns: {missing_ohlcv}.")
                     existing_data = pd.DataFrame() # Treat as unusable Parquet
            else:
                 # If DateTime column is missing, all null, or wrong dtype after Parquet processing
                 if "DateTime" in existing_data.columns:
                      logger.warning(f"Parquet file for {symbol} has unusable DateTime column (all null or wrong dtype: {existing_data['DateTime'].dtype}) after parsing.")
                 else:
                     logger.warning(f"Parquet file for {symbol} has no DateTime column after parsing.")
                 existing_data = pd.DataFrame() # Treat as failed read


        except Exception as e:
            logger.error(f"🚨 Error reading Parquet file for {symbol} from {parquet_filename}: {e}. Treating as no existing data.", exc_info=True)
            existing_data = pd.DataFrame() # Clear data frame


    # --- Final check after attempting both files ---
    if existing_data.empty:
        # Only log "No usable file found" if we actually attempted to read at least one file
        if data_read_attempted:
             logger.info(f"No usable existing data file found (CSV or Parquet with naive datetime and OHLCV) for {symbol}.")
        # If data_read_attempted is False, it means neither file existed, which will be logged later when fetching from start_date
        return pd.DataFrame() # Return empty if neither file worked or data became empty

    # Apply minimal necessary cleaning after reading: Drop rows with NaT DateTime
    # This should have been handled in the parsing steps above, but adding a final check for safety
    if not pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]):
         logger.warning(f"DateTime column for {symbol} is not naive datetime64[ns] after reading, attempting one last conversion before dropping NaT: {existing_data['DateTime'].dtype}")
         existing_data["DateTime"] = pd.to_datetime(existing_data["DateTime"], errors='coerce').dt.tz_convert(None)


    initial_rows = len(existing_data)
    existing_data.dropna(subset=['DateTime'], inplace=True)
    if len(existing_data) < initial_rows:
        logger.warning(f"Dropped {initial_rows - len(existing_data)} rows due to invalid DateTime after reading file.")

    if not existing_data.empty:
        # Apply basic OHLCV cleaning before returning
        # This cleaning is minimal; full clean_ohlcv happens before saving.
        ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        # Check columns exist before cleaning
        if all(col in existing_data.columns for col in ohlcv_cols):
             existing_data = existing_data.ffill().dropna(subset=ohlcv_cols) # ffill and drop NaNs in OHLCV
             if (existing_data['Volume'] < 0).any():
                  existing_data['Volume'] = existing_data['Volume'].abs() # Make volume non-negative
        else:
             # This case should be caught earlier if essential columns are missing, but defensive
             logger.warning(f"Missing OHLCV columns in existing data for {symbol} after NaT drop. Skipping basic OHLCV cleaning.")


    # Return the DataFrame with **naive** 'DateTime' as a column (and potentially other columns)
    required_output_cols = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    # Ensure required columns exist and DateTime is naive datetime64[ns] before final return
    if not all(col in existing_data.columns for col in required_output_cols) or not pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]):
        # This check is largely redundant if parsing logic is correct, but defensive
        missing = [col for col in required_output_cols if col not in existing_data.columns]
        if 'DateTime' in existing_data.columns and not pd.api.types.is_datetime64_ns_dtype(existing_data["DateTime"]):
             missing.append(f"DateTime (wrong dtype: {existing_data['DateTime'].dtype})")
        elif 'DateTime' not in existing_data.columns:
             missing.append('DateTime (missing)')


        logger.error(f"Existing data for {symbol} is missing essential columns or DateTime is wrong dtype after final checks: {missing}. Cannot use this data.")
        return pd.DataFrame() # Return empty if essential columns are missing or DateTime dtype is wrong

    # Select and reorder columns for consistency if needed
    cols_to_return = [col for col in required_output_cols + [col for col in existing_data.columns if col not in required_output_cols] if col in existing_data.columns]
    return existing_data[cols_to_return]


def save_data(df: pd.DataFrame, symbol: str):
    """
    Applies final cleaning, sorts, removes duplicates, and saves data to
    CSV and Parquet files for a given symbol.
    Uses the constructed FOLDER_NAME based on interval.
    Assumes input df has a **naive** 'DateTime' column.
    Saves data with the **naive** 'DateTime' column.
    """
    if df.empty:
        logger.warning(f"⚠️ No data to save for {symbol}. Skipping save.")
        return

    logger.info(f"Initiating save process for {symbol} with {len(df)} rows.")

    try:
        os.makedirs(FOLDER_NAME, exist_ok=True)
        csv_filename = os.path.join(FOLDER_NAME, f"{symbol}{CSV_EXTENSION}")
        parquet_filename = os.path.join(FOLDER_NAME, f"{symbol}{PARQUET_EXTENSION}")
    except OSError as e:
        logger.error(f"🚨 Could not create data directory {FOLDER_NAME}: {e}. Cannot save data for {symbol}.", exc_info=True)
        return

    # Ensure 'DateTime' column is present and is **naive** datetime64[ns] before final cleaning
    if 'DateTime' not in df.columns or not pd.api.types.is_datetime64_ns_dtype(df['DateTime']):
         # Attempt to convert if present but wrong dtype
         if 'DateTime' in df.columns:
              logger.warning(f"DateTime column for {symbol} not naive datetime64[ns] before save processing: {df['DateTime'].dtype}. Attempting conversion.")
              df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce').dt.tz_convert(None)
         else:
             logger.error(f"🚨 Cannot save data for {symbol}: 'DateTime' column is missing.")
             return # Exit save function if DateTime is fundamentally unusable

    # Drop any NaT that resulted from parsing/conversion
    initial_rows_pre_clean = len(df)
    df.dropna(subset=['DateTime'], inplace=True)
    if len(df) < initial_rows_pre_clean:
        logger.warning(f"Dropped {initial_rows_pre_clean - len(df)} rows with invalid DateTime before final cleaning.")
    if df.empty:
         logger.warning(f"DataFrame became empty after DateTime cleaning before final processing. Skipping save for {symbol}.")
         return # Exit save function if dataframe is empty

    # At this point, df['DateTime'] should be pd.Series with dtype datetime64[ns] (or dataframe is empty)


    logger.debug(f"Applying final cleaning for {symbol}...")
    # Apply final cleaning (handles NaNs, duplicates, OHLC validation etc.)
    # clean_ohlcv expects DateTime as a column and returns it as a column, preserving naivety.
    # Pass a copy to avoid modifying the input df if clean_ohlcv does in place ops (though it copies internally)
    data_to_save = clean_ohlcv(df.copy())
    logger.debug(f"Final cleaning resulted in {len(data_to_save)} rows for {symbol}.")

    # Ensure DateTime column is still present and naive after clean_ohlcv
    if 'DateTime' not in data_to_save.columns or not pd.api.types.is_datetime64_ns_dtype(data_to_save['DateTime']):
         logger.error(f"🚨 Cannot save data for {symbol}: 'DateTime' column is missing or not naive datetime64[ns] after clean_ohlcv.")
         return # Exit save function


    # Sort by DateTime before saving
    if 'DateTime' in data_to_save.columns and not data_to_save['DateTime'].isnull().all(): # Check if not all values are NaT for sorting
        logger.debug(f"Sorting data by DateTime for {symbol}...")
        # Sort works correctly on naive DateTime
        data_to_save = data_to_save.sort_values("DateTime").reset_index(drop=True)
        logger.debug(f"Data sorted for {symbol}.")
    else:
         logger.warning("Cannot sort by DateTime as column is all null after final cleaning.")
         if data_to_save.empty:
              logger.warning("Cleaned data is empty, nothing to save.")
              return # Exit if empty
         # Continue attempting to save without sorting if DateTime is critically missing or all NaT


    # Drop duplicates based on DateTime after sorting - keep the last one
    if 'DateTime' in data_to_save.columns and not data_to_save['DateTime'].isnull().all(): # Check if not all values are NaT for dropping duplicates
        initial_rows = len(data_to_save)
        logger.debug(f"Dropping duplicates for {symbol} (initial count: {initial_rows})...")
        data_to_save.drop_duplicates(subset=["DateTime"], keep="last", inplace=True)
        if len(data_to_save) < initial_rows:
             logger.info(f"Dropped {initial_rows - len(data_to_save)} duplicate DateTime entries before saving.")
        else:
             logger.debug(f"No DateTime duplicates found for {symbol}.")
    else:
         logger.warning("Cannot drop duplicates by DateTime as column is all null.")


    # Select and reorder columns for the final output file
    output_cols = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    other_cols = [col for col in data_to_save.columns if col not in output_cols]
    final_output_order = output_cols + other_cols
    # Select only columns that actually exist in the dataframe
    cols_to_save = [col for col in final_output_order if col in data_to_save.columns]

    # Final check if essential columns are still present after all cleaning/dropping
    if data_to_save.empty or 'DateTime' not in cols_to_save or data_to_save['DateTime'].isnull().all():
         logger.warning(f"Final data for {symbol} is empty or missing/invalid essential columns ('DateTime'). Skipping save.")
         return # Exit save function

    data_to_save = data_to_save[cols_to_save]


    # Save the cleaned and formatted data
    if not data_to_save.empty: # Final check before saving
        logger.info(f"Saving final data ({len(data_to_save)} rows) for {symbol} to disk...")
        try:
            logger.info(f"Saving CSV to {csv_filename}...")
            # Use date_format argument to ensure naive datetime is saved in a standard format
            data_to_save.to_csv(csv_filename, index=False, date_format='%Y-%m-%d %H:%M:%S')
            logger.info(f"💾 Data saved successfully to {csv_filename}")
        except Exception as e:
            logger.error(f"🚨 Error saving CSV for {symbol} to {csv_filename}: {e}", exc_info=True)

        try:
            # Check if pyarrow is installed for parquet
            try:
                import pyarrow
            except ImportError:
                logger.warning("PyArrow not installed. Skipping Parquet save. Install with 'pip install pyarrow'.")
                pyarrow = None # Set to None to prevent saving

            if pyarrow:
                logger.info(f"Saving Parquet to {parquet_filename}...")
                # Parquet saves datetime64[ns] correctly as naive
                data_to_save.to_parquet(parquet_filename, index=False)
                logger.info(f"💾 Data saved successfully to {parquet_filename}")
        except Exception as e:
             logger.error(f"🚨 Error saving Parquet for {symbol} to {parquet_filename}: {e}", exc_info=True)
    else:
        logger.warning(f"Final data for {symbol} is empty after processing. Skipping save.")


def update_symbol_data(symbol: str, token: str, smart_api_client: SmartApiClient):
    """
    Reads existing data (preferring CSV, reads naive), determines the start date for new data,
    fetches new data (naive), concatenates (result is naive), cleans, and saves ONLY if new data was fetched.
    Assumes all naive datetimes represent IST.
    """
    # current_date is naive
    current_date = datetime.now()

    # Parse the start date from config as naive
    try:
        start_date_config = datetime.strptime(START_DATE, "%Y-%m-%d") # start_date_config is naive
    except ValueError as e:
        logger.critical(f"Invalid START_DATE format in config: {START_DATE}. Expected YYYY-MM-DD. Error: {e}. Exiting.")
        return # Cannot proceed with invalid start date


    logger.info(f"\n--- Processing {symbol} (Token: {token}, Interval: {smart_api_client.interval}) ---")
    logger.info(f"Data will be stored in folder: {FOLDER_NAME}")
    # Log the current date being used for comparisons
    logger.info(f"Current system date/time (assumed IST): {current_date.strftime('%Y-%m-%d %H:%M:%S')}")


    # ----------------------------
    # Read existing data (DateTime as naive datetime64[ns] column)
    # ----------------------------
    existing_data_naive = read_existing_data(symbol)

    data_changed = False # Flag to track if new data was added

    if not existing_data_naive.empty:
        # Find the latest date in the existing data's naive DateTime column
        # read_existing_data should ensure DateTime is naive datetime64[ns] and not all null
        # Drop NaT before finding max for safety, although read_existing_data should handle this
        existing_data_naive.dropna(subset=["DateTime"], inplace=True)

        if not existing_data_naive.empty: # Check again after dropping NaT
            last_date = existing_data_naive["DateTime"].max() # last_date is naive Timestamp

            logger.info(f"Existing data found. Last record (assumed IST): {last_date.strftime('%Y-%m-%d %H:%M:%S')}.")

            # Determine the start date for fetching *new* data (1 minute after the last timestamp)
            new_start_date = last_date + timedelta(minutes=1) # new_start_date is naive Timestamp

            # Use existing data (naive) as the base for concatenation
            all_data = existing_data_naive.copy()
        else:
             # If existing_data_naive became empty after parsing/dropping NaT or checks in read_existing_data
             logger.warning(f"Existing data for {symbol} became empty or unusable after initial processing. Treating as no existing data.")
             all_data = pd.DataFrame()
             new_start_date = start_date_config # Use config start date (naive Timestamp)
             data_changed = True # Mark as changed if starting fresh

    else:
        # No usable existing data found initially (empty or failed read_existing_data)
        logger.info(f"No usable existing data found for {symbol}. Fetching from config start date (assumed IST): {start_date_config.strftime('%Y-%m-%d %H:%M:%S')}")
        all_data = pd.DataFrame() # Start with empty DataFrame
        new_start_date = start_date_config # Use config start date (naive Timestamp)
        data_changed = True # Mark as changed if starting fresh


    # Determine the latest time to fetch data up to. (current time - buffer, naive)
    fetch_end_date_limit = current_date - timedelta(minutes=1)
    fetch_end_date = fetch_end_date_limit # fetch_end_date is naive Timestamp


    # Check if fetching is needed at all (compare naive Timestamps)
    # This comparison should now work correctly as both are naive.
    if new_start_date >= fetch_end_date:
        logger.info(f"Data for {symbol} is already up-to-date as of {new_start_date.strftime('%Y-%m-%d %H:%M:%S')} (latest fetchable: {fetch_end_date.strftime('%Y-%m-%d %H:%M:%S')}). Skipping new data fetch.")
        # If data_changed is True, it means we started fresh (no existing file or file was unusable), but found no data to fetch.
        # If data_changed is False, it means we read an existing file and it's up-to-date.
        if data_changed:
             # Only log this warning if data_changed is true AND all_data is still empty (meaning nothing was loaded/fetched)
             if all_data.empty:
                  logger.warning(f"No usable existing data and no new data found in the fetch range for {symbol}. Nothing to save.")
             # If data_changed is true but all_data is NOT empty, it means some invalid existing data was loaded but no NEW data was fetched.
             # In this case, all_data contains the potentially invalid existing data, and we skip save. The warning about unusable existing data was already logged in read_existing_data or above.
        else:
             # Existing data was present and is up-to-date. Skip save to save time.
             logger.info(f"Skipping save for {symbol} as existing data is up-to-date and no new data was fetched.")

        return # Exit processing for this symbol

    # Ensure start/end dates for API calls are suitable.
    # Pass the naive datetimes directly for the fetch bounds.
    fetch_start_dt = new_start_date # Naive Timestamp
    fetch_end_dt = fetch_end_date   # Naive Timestamp

    # Add a check to avoid fetching if the start date is after the end date (e.g., due to current time buffer)
    if fetch_start_dt > fetch_end_dt:
         logger.warning(f"Calculated fetch start date {fetch_start_dt.strftime('%Y-%m-%d %H:%M:%S')} is after fetch end date {fetch_end_dt.strftime('%Y-%m-%d %H:%M:%S')}. Skipping new data fetch entirely.")
         # If data_changed is True (started fresh), log warning about no data
         if data_changed and all_data.empty:
               logger.warning(f"No usable existing data and no new data found in the fetch range for {symbol}. Nothing to save.")
         # Otherwise (existing data exists), simply return, skip save is handled by data_changed = False
         return


    logger.info(f"Starting new data fetch in chunks from {fetch_start_dt.strftime('%Y-%m-%d %H:%M:%S')} up to {fetch_end_dt.strftime('%Y-%m-%d %H:%M:%S')} (assumed IST)")


    # ----------------------------
    # Fetch new data in monthly chunks
    # ----------------------------
    chunk_timedelta = timedelta(days=30) # Use days for chunk size base
    current_chunk_start_dt = fetch_start_dt # Start fetching from this naive datetime

    # Loop while the *start* of the current chunk is within or equal to the fetch end datetime
    while current_chunk_start_dt <= fetch_end_dt:
        # Determine chunk end date/time: max chunk_timedelta from start, or up to fetch_end_dt
        chunk_end_dt = min(current_chunk_start_dt + chunk_timedelta, fetch_end_dt)

        # API expects HH:MM format strings - get_candle_data handles this formatting internally
        # Pass the precise naive datetime objects for the range
        logger.info(f"Requesting data chunk from: {current_chunk_start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {chunk_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # Pass naive datetimes to the fetch function
        df_new = smart_api_client.get_candle_data(token, current_chunk_start_dt, chunk_end_dt) # df_new should have naive DateTime column

        if df_new.empty:
            logger.info(f"⚠️ No new data returned by API for chunk from {current_chunk_start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {chunk_end_dt.strftime('%Y-%m-%d %H:%M:%S')}.")
        else:
            # df_new should already have naive DateTime column from get_candle_data.
            # Just drop NaTs that may have occurred during conversion in get_candle_data.
            # Also ensure it is indeed naive datetime64[ns] as a final check here
            if not pd.api.types.is_datetime64_ns_dtype(df_new["DateTime"]):
                 logger.warning(f"New data DateTime column for {symbol} is not naive datetime64[ns] after fetch: {df_new['DateTime'].dtype}. Attempting correction.")
                 # Attempt to make it naive if possible, coercing errors
                 df_new["DateTime"] = pd.to_datetime(df_new["DateTime"], errors='coerce').dt.tz_convert(None)

            initial_rows = len(df_new)
            df_new.dropna(subset=["DateTime"], inplace=True)
            if len(df_new) < initial_rows:
                 logger.warning(f"Dropped {initial_rows - len(df_new)} rows with NaT DateTime in new data from chunk.")

            # Only keep data within the requested chunk range (inclusive start, inclusive end)
            # Filter using naive datetimes. This comparison should now work.
            if not df_new.empty: # Check if any data remains after dropping NaT
                initial_rows = len(df_new)
                df_new = df_new[
                    (df_new["DateTime"] >= current_chunk_start_dt) &
                    (df_new["DateTime"] <= chunk_end_dt)
                ].copy() # Use copy() after filtering to avoid SettingWithCopyWarning
                if len(df_new) < initial_rows:
                     logger.warning(f"Dropped {initial_rows - len(df_new)} rows outside the requested chunk range [{current_chunk_start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {chunk_end_dt.strftime('%Y-%m-%d %H:%M:%S')}].")


            if not df_new.empty:
                # Concatenate existing data (naive) with new data (naive).
                # The resulting DateTime column remains naive.
                initial_total_rows = len(all_data)
                all_data = pd.concat([all_data, df_new], ignore_index=True)
                data_changed = True # Mark that new data was successfully added
                logger.info(f"Fetched and added {len(df_new)} new rows. Total rows: {len(all_data)}")
            else:
                 logger.warning(f"Fetched data for chunk from {current_chunk_start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {chunk_end_dt.strftime('%Y-%m-%d %H:%M:%S')} was empty after cleaning/filtering.")

        # Move to the start of the next chunk: 1 minute after the end of the current chunk
        # Using chunk_end_dt + timedelta(minutes=1) ensures we request the candle starting
        # immediately after the last candle in the previous chunk.
        current_chunk_start_dt = chunk_end_dt + timedelta(minutes=1)

        # Add a delay between requests for different chunks
        if current_chunk_start_dt <= fetch_end_dt:
             time.sleep(REQUEST_DELAY)
        # Add an extra small delay *after* the last chunk if data was fetched,
        # to avoid hitting the next symbol's API call too fast.
        elif data_changed: # Delay only happens once after the last chunk is processed, *if* data was added
             time.sleep(REQUEST_DELAY * 2)


    # ----------------------------
    # Final Processing and Saving (Only if data_changed is True and DataFrame is not empty)
    # ----------------------------
    if data_changed and not all_data.empty:
        # At this point, all_data should contain combined data with a naive DateTime column
        logger.info(f"New data fetched. Finalizing and saving combined data ({len(all_data)} rows) for {symbol}...")
        save_data(all_data, symbol) # save_data expects and saves naive
        logger.info(f"Finished processing and saving data for {symbol}.")
    elif data_changed and all_data.empty:
         logger.warning(f"Attempted to fetch new data for {symbol} (started fresh or existing data invalid), but no usable data resulted. Nothing to save.")
    # Implicit else: data_changed is False, meaning no new data was added and existing data was usable. We already handled this at the beginning by returning early.