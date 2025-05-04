# data_utils.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans OHLCV data: ffill, dropna, remove duplicates based on DateTime,
    ensure datetime index, sort index, and validate OHLC relationships.
    Assumes 'DateTime' is a column to become the index temporarily.
    Returns cleaned DataFrame with 'DateTime' as a column.
    Assumes DateTime column is **naive datetime64[ns]** and attempts to ensure this.
    """
    df_clean = df.copy()

    # Ensure 'DateTime' column is present and is **naive datetime64[ns]**.
    # Attempt to convert if not, coercing errors.
    if 'DateTime' not in df_clean.columns:
        logger.warning("⚠️ 'DateTime' column not found for cleaning. Skipping cleaning steps that require it.")
        # Still try basic ffill/dropna if OHLCV columns exist
        ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if all(col in df_clean.columns for col in ohlcv_cols):
            df_clean = df_clean.ffill().dropna(subset=ohlcv_cols)
            df_clean['Volume'] = df_clean['Volume'].abs() # Still clean volume
        return df_clean # Cannot proceed with full cleaning

    # Attempt to convert to naive datetime64[ns]. If it's tz-aware, tz_convert(None) makes it naive.
    # If it's already naive but wrong dtype, pd.to_datetime might fix it.
    if not pd.api.types.is_datetime64_ns_dtype(df_clean['DateTime']):
        logger.debug(f"DateTime column is not naive datetime64[ns] before cleaning: {df_clean['DateTime'].dtype}. Attempting conversion.")
        df_clean['DateTime'] = pd.to_datetime(df_clean['DateTime'], errors='coerce')
        # If conversion results in tz-aware, convert to naive
        if pd.api.types.is_datetime64tz_dtype(df_clean['DateTime']):
             df_clean['DateTime'] = df_clean['DateTime'].dt.tz_convert(None)
        # If still not naive datetime64[ns] after trying, log warning
        if not pd.api.types.is_datetime64_ns_dtype(df_clean['DateTime']):
             logger.warning(f"DateTime column remains not naive datetime64[ns] after conversion attempts: {df_clean['DateTime'].dtype}. Proceeding, but issues possible.")


    # Drop rows where DateTime couldn't be parsed/converted or were NaT
    initial_rows = len(df_clean)
    df_clean.dropna(subset=['DateTime'], inplace=True)
    if len(df_clean) < initial_rows:
        logger.warning(f"Dropped {initial_rows - len(df_clean)} rows due to invalid DateTime format during cleaning.")

    if df_clean.empty:
        logger.warning("DataFrame is empty after initial DateTime cleaning.")
        return df_clean # Return empty dataframe

    # Set DateTime as index for cleaning operations - works with naive datetimes
    # Check if DateTime is already the index before setting
    # Ensure the column used for index is indeed datetime64[ns] and not all NaT
    if df_clean.index.name != 'DateTime' or not isinstance(df_clean.index, pd.DatetimeIndex):
        if pd.api.types.is_datetime64_ns_dtype(df_clean['DateTime']) and not df_clean['DateTime'].isnull().all():
             df_clean = df_clean.set_index('DateTime').sort_index()
        else:
             # If DateTime column is not the correct dtype or all null even after attempts, cannot set as index
             dtype_str = df_clean['DateTime'].dtype if 'DateTime' in df_clean.columns else 'Missing'
             logger.error(f"Cannot set 'DateTime' column as index for cleaning as it is not datetime64[ns] or is all null ({dtype_str}). Skipping further cleaning steps.")
             # Return the dataframe as is, minus rows with NaT DateTime (already handled)
             return df_clean # Return dataframe as is, cleaning limited


    else:
        # If already index, just ensure it's sorted
        df_clean = df_clean.sort_index()


    # Drop duplicates based on index (DateTime) - keep the last one in case of merges
    # Check index validity before dropping duplicates by index
    if isinstance(df_clean.index, pd.DatetimeIndex) and not df_clean.index.isnull().all():
        initial_rows = len(df_clean)
        df_clean = df_clean[~df_clean.index.duplicated(keep='last')]
        if len(df_clean) < initial_rows:
             logger.info(f"Dropped {initial_rows - len(df_clean)} duplicate entries based on DateTime during cleaning.")
    else:
         logger.warning("Cannot drop duplicates based on DateTime index as index is not DatetimeIndex or is all null.")


    # Apply basic cleaning - ffill for NaNs (common for OHLC in some datasets) then drop remaining
    df_clean = df_clean.ffill().dropna()

    # Validate OHLC relationships
    ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    # Check columns exist before validating
    if all(col in df_clean.columns for col in ohlcv_cols):
        # Validate High >= Low
        invalid_ohlc_count = len(df_clean[df_clean['High'] < df_clean['Low']])
        if invalid_ohlc_count > 0:
             logger.warning(f"Dropped {invalid_ohlc_count} rows where High < Low during cleaning.")
             df_clean = df_clean[df_clean['High'] >= df_clean['Low']]

        # Validate High is max of O, H, L, C
        invalid_high_open_close = len(df_clean[(df_clean['High'] < df_clean['Open']) | (df_clean['High'] < df_clean['Close'])])
        if invalid_high_open_close > 0:
            logger.warning(f"Dropped {invalid_high_open_close} rows where High was less than Open or Close during cleaning.")
            df_clean = df_clean[(df_clean['High'] >= df_clean['Open']) & (df_clean['High'] >= df_clean['Close'])]

        # Validate Low is min of O, H, L, C
        invalid_low_open_close = len(df_clean[(df_clean['Low'] > df_clean['Open']) | (df_clean['Low'] > df_clean['Close'])])
        if invalid_low_open_close > 0:
            logger.warning(f"Dropped {invalid_low_open_close} rows where Low was greater than Open or Close during cleaning.")
            df_clean = df_clean[(df_clean['Low'] <= df_clean['Open']) & (df_clean['Low'] <= df_clean['Close'])]

        # Ensure volume is non-negative
        if (df_clean['Volume'] < 0).any():
            df_clean['Volume'] = df_clean['Volume'].abs()
    else:
         logger.warning("Missing OHLCV columns after filtering/dropna in cleaning. Skipping OHLC validation.")


    # Reset index back to column before returning
    # Ensure the index is still datetime-like before resetting
    if isinstance(df_clean.index, pd.DatetimeIndex):
        df_clean = df_clean.reset_index()
    else:
         logger.error("Index is not DatetimeIndex after cleaning steps. Cannot reset 'DateTime' index properly.")
         # The 'DateTime' column might be missing or named differently if index couldn't be reset
         if 'DateTime' in df_clean.columns:
             logger.warning("Returning DataFrame without resetting DateTime index, but column exists.")
         else:
             logger.error("Returning DataFrame, but 'DateTime' column is missing.")


    return df_clean