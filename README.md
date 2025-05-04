# AngelCandleGrab: Nifty 50 Historical Data Fetcher using Angel One SmartAPI

This project, **AngelCandleGrab**, provides a Python utility to fetch, update, and clean historical OHLCV (Open, High, Low, Close, Volume) data for the Nifty 50 constituents using the Angel One SmartAPI. The data is saved in both CSV and Parquet formats, organized by symbol and candle interval.

**Important Note on Timezones:** **AngelCandleGrab** operates under the strict assumption that all datetime data received from the Angel One API and read from existing data files (CSV/Parquet) represents timestamps in the **Indian Standard Time (IST)** timezone (UTC+5:30). All datetime objects used and saved by the script are **timezone-naive** (`datetime64[ns]` in pandas). No explicit timezone handling (like `pytz` localization or conversion) is performed within the core data processing logic to simplify the code. This approach relies heavily on the consistency of the data source and file format. If the Angel One API ever changes its datetime format or implicit timezone, or if your existing files contain datetimes in a different timezone, this script may produce incorrect results.

## Features

*   Fetches the current list of Nifty 50 symbols from NSE India.
*   Finds corresponding instrument tokens for these symbols using the Angel One instrument list.
*   Connects to the Angel One SmartAPI to retrieve historical candle data.
*   Supports fetching data for various time intervals (e.g., 1-minute, 5-minute, 1-day) as supported by the Angel One API.
*   Efficiently updates existing data files by determining the last recorded timestamp and fetching only the new data.
*   Cleans data by handling missing values (forward fill, dropping NaNs), removing duplicate entries based on timestamp, and validating basic OHLC relationships (High >= Low, High >= Open/Close, Low <= Open/Close, non-negative Volume).
*   Stores data in a dedicated folder structure organized by the chosen time interval (e.g., `NIFTY_50_DATA_ONE_MINUTE`).
*   Saves data in both CSV and Parquet formats for flexibility.
*   Uses environment variables for secure storage of sensitive API credentials.
*   Implements robust logging for monitoring progress, warnings, and errors.
*   Includes retry logic for API calls to handle transient network issues and rate limits.

## Prerequisites

*   **Python 3.6+:** Ensure you have a compatible version of Python installed.
*   **An Active Angel One SmartAPI Account:** You need to register and get API access. Visit the [Angel One SmartAPI Documentation Portal](https://smartapi.angelbroking.com/docs) for information on signing up and managing your API key.
*   **Angel One API Credentials:** From your SmartAPI account, you will need the following:
    *   **API Key:** Your unique key to access the API.
    *   **Client ID:** Your Angel One client ID (trading account ID).
    *   **Password / PIN:** Your trading account password or PIN.
    *   **TOTP Login Token:** The base32 secret key used to generate Time-based One-Time Passwords (TOTP) for your account (used for the `LOGINTOKEN` credential in this script). This is typically provided during the API setup or linked to your preferred authenticator app.
*   **Internet Connectivity:** Required to fetch data from NSE and Angel One APIs.

## Installation

1.  **Clone the repository:**
    Open your terminal or command prompt and clone the project:
    ```bash
    git clone https://github.com/anubhaviitrpr/AngelCandleGrab.git
    cd AngelCandleGrab 
    ```

2.  **Create and activate a Python Virtual Environment (Recommended):**
    Virtual environments help manage dependencies and prevent conflicts with other Python projects.
    ```bash
    python -m venv venv
    # On Windows:
    venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```

3.  **Install Dependencies:**
    With your virtual environment activated, install the required libraries:
    ```bash
    pip install -r requirements.txt
    ```
    *Note: `pyarrow` is included in `requirements.txt` and is necessary for Parquet file support. If you only need CSV, you can remove `pyarrow` from the file before installing dependencies, but Parquet is generally more efficient for large datasets.*

## Configuration

Configuration is handled using a combination of `config.py` and **environment variables**. Using environment variables is the recommended and most secure way to manage your sensitive API credentials, especially in production environments.

**1. Setting Sensitive Credentials (Environment Variables):**

Set the following environment variables with your actual Angel One API credentials:

*   `APIKEY`
*   `CLIENTID`
*   `PASSWORD`
*   `LOGINTOKEN`

**How to set environment variables:**

*   **Directly in your terminal session (temporary):**
    ```bash
    # On macOS/Linux
    export APIKEY='YOUR_ANGELONE_API_KEY'
    export CLIENTID='YOUR_ANGELONE_CLIENT_ID'
    export PASSWORD='YOUR_ANGELONE_PASSWORD_OR_PIN'
    export LOGINTOKEN='YOUR_TOTP_SECRET_KEY'

    # On Windows (Command Prompt)
    set APIKEY=YOUR_ANGELONE_API_KEY
    set CLIENTID=YOUR_ANGELONE_CLIENT_ID
    set PASSWORD=YOUR_ANGELONE_PASSWORD_OR_PIN
    set LOGINTOKEN=YOUR_TOTP_SECRET_KEY

    # On Windows (PowerShell)
    $env:APIKEY='YOUR_ANGELONE_API_KEY'
    $env:CLIENTID='YOUR_ANGELONE_CLIENT_ID'
    $env:PASSWORD='YOUR_ANGELONE_PASSWORD_OR_PIN'
    $env:LOGINTOKEN='YOUR_TOTP_SECRET_KEY'
    ```
    These will only last for the current terminal session.
*   **Using a `.env` file (convenient for local development):**
    Install `python-dotenv` (`pip install python-dotenv`) and create a file named `.env` in the root directory of the project with your credentials.
    ```dotenv
    # .env
    APIKEY='YOUR_ANGELONE_API_KEY'
    CLIENTID='YOUR_ANGELONE_CLIENT_ID'
    PASSWORD='YOUR_ANGELONE_PASSWORD_OR_PIN'
    LOGINTOKEN='YOUR_TOTP_SECRET_KEY'
    # Add other configurations here if you want to override defaults, e.g.:
    # TIME_INTERVAL='FIVE_MINUTE'
    # START_DATE='2020-01-01'
    # MAX_RETRIES=10
    # RETRY_DELAY=5
    # REQUEST_DELAY=1.0
    # LOG_LEVEL='DEBUG' # Use DEBUG for detailed logs, INFO for standard progress
    # LOG_FILE='custom_nifty_log.log'
    ```
    Then, **uncomment** the two lines at the very top of `main.py`:
    ```python
    # from dotenv import load_dotenv # Uncomment this line
    # load_dotenv() # Uncomment this line
    ```
    **Important:** If you use a `.env` file, make sure you add `.env` to your `.gitignore` file to prevent accidentally committing your credentials to GitHub.
*   **System-wide (permanent):** Consult your operating system's documentation for setting permanent environment variables.

**2. Customizing Other Configurations (`config.py`):**

The `config.py` file contains default values for other settings. These defaults can also be overridden by setting environment variables with the same name.

*   `TIME_INTERVAL`: Specifies the candle interval for fetching data. Defaults to `'ONE_HOUR'`. Set the `TIME_INTERVAL` environment variable (e.g., `export TIME_INTERVAL='ONE_MINUTE'`) to override.
*   `START_DATE`: The historical date (YYYY-MM-DD) from which to start fetching data *if no existing data file is found* for a symbol. Defaults to `'2016-10-01'`. Set the `START_DATE` environment variable to override.
*   `MAX_RETRIES`: Maximum number of retries for failed API requests. Defaults to `5`. Set the `MAX_RETRIES` environment variable to override.
*   `RETRY_DELAY`: The delay in seconds between general API retries. Defaults to `1`. Set the `RETRY_DELAY` environment variable to override.
*   `REQUEST_DELAY`: The delay in seconds between fetching data chunks for a single symbol or processing different symbols. Defaults to `0.25`. Set the `REQUEST_DELAY` environment variable to override.
*   `LOG_LEVEL`: The minimum severity level for log messages (e.g., 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'). Defaults to `'INFO'`. Set the `LOG_LEVEL` environment variable to override.
*   `LOG_FILE`: The name of the log file to be saved within the data folder. Defaults to `'nifty_data_updater.log'`. Set the `LOG_FILE` environment variable to override.
*   `FOLDER_NAME`: This folder name is automatically constructed based on `BASE_FOLDER_NAME` and `TIME_INTERVAL`.

## Running the Script

1.  **Activate your virtual environment** (if you created one).
2.  **Ensure your environment variables are set** with your Angel One credentials (or that your `.env` file is correctly placed and loading is uncommented in `main.py`).
3.  **Run the main script:**
    ```bash
    python main.py
    ```

The script will authenticate with Angel One, fetch the list of Nifty 50 stocks, and then process each stock sequentially. For each stock, it will check for existing data (prioritizing CSV), determine the last recorded time, fetch new data from the API in chunks, combine the data, clean it, and save the updated data to CSV and Parquet files in the dedicated data folder (e.g., `NIFTY_50_DATA_ONE_HOUR`).

## Output

Data for each successfully processed Nifty 50 symbol will be saved within the folder specified by `FOLDER_NAME` (e.g., `NIFTY_50_DATA_ONE_HOUR`).

For each symbol (e.g., `RELIANCE`):
*   `RELIANCE.csv`
*   `RELIANCE.parquet` (Requires `pyarrow`)

These files will contain the following columns, with **timezone-naive** datetimes assumed to be in IST:

*   `DateTime`: Combined Date and Time (YYYY-MM-DD HH:MM:SS)
*   `Open`: Open price
*   `High`: High price
*   `Low`: Low price
*   `Close`: Close price
*   `Volume`: Trading volume

Log messages will be printed to your console and appended to the log file (e.g., `NIFTY_50_DATA_ONE_HOUR/nifty_data_updater.log`).

## Error Handling and Logging

The script uses Python's standard `logging` module for detailed output.

*   **INFO:** Provides general progress updates (starting process, fetching symbols, processing each stock, requesting data chunks, saving files).
*   **WARNING:** Indicates potential non-fatal issues (e.g., empty data returned for a period, existing file format issues, missing `pyarrow` for Parquet). Processing for the symbol usually continues.
*   **ERROR:** Signals errors that prevent processing for the *current* symbol or data chunk (e.g., API error responses other than rate limits, file read/write errors). The script logs the error and attempts to move to the next symbol.
*   **CRITICAL:** Represents severe errors that prevent the script from authenticating or running essential setup steps. These errors will cause the script to log the issue and exit.

API call failures and network errors are handled with retries (`MAX_RETRIES`, `RETRY_DELAY`). Rate limit errors (`AB1004` or non-JSON "exceeding access rate" responses) trigger a slightly longer delay (`RETRY_DELAY * 2`) before retrying.

If the data for a symbol is determined to be already up-to-date based on the last timestamp in the existing file, the script will log this and **skip the entire fetching and saving process** for that symbol, significantly reducing execution time on subsequent runs when minimal data needs updating.

## Customization

You can customize the behavior by setting environment variables or modifying `config.py`:

*   **Candle Interval:** Change `TIME_INTERVAL` (e.g., `'FIVE_MINUTE'`, `'ONE_DAY'`). This affects both the fetched data and the output folder name.
*   **Historical Start Date:** Change `START_DATE` (e.g., `'2022-01-01'`). Used only if no existing data is found.
*   **API Request Delays:** Adjust `MAX_RETRIES`, `RETRY_DELAY`, `REQUEST_DELAY` to fine-tune API interaction behavior based on your connection and API limits.
*   **Logging Verbosity:** Change `LOG_LEVEL` to `'DEBUG'` for extensive debugging information, or `'ERROR'` to see only significant errors.
*   **Log File Name:** Change `LOG_FILE`.

## Potential Improvements

*   Add command-line arguments for parameters like `TIME_INTERVAL` or specific symbols to process.
*   Implement parallel processing for fetching data for multiple symbols simultaneously (requires careful management of API rate limits and connections).
*   Add data validation beyond basic OHLC checks.
*   Implement incremental updates that only read/write new data segments rather than the entire file each time (more complex, especially with CSV).

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.

## Contributing

Contributions are welcome! Please feel free to fork the repository, make changes, and submit pull requests.