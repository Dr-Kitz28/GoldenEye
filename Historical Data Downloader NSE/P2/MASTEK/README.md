# Historical OHLCV Downloader

This utility downloads OHLCV (open, high, low, close, adjusted close, volume) data for MASTEK—or any symbol available on [Yahoo Finance](https://finance.yahoo.com/quote/MASTEK.NS/). It no longer requires Kite/Zerodha credentials. Data is fetched via the [`yfinance`](https://pypi.org/project/yfinance/) package and saved to a CSV file for analysis in Excel, Google Sheets, or your own tooling.

## Prerequisites

- Python 3.9 or later
- Internet access (the script queries Yahoo Finance)

Install dependencies once:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Usage

Inside the project folder run:

```powershell
python mastek_historical_data.py --symbol MASTEK.NS --start 2004-06-23 --interval 1d --output data\\mastek_daily.csv
```

Flags:

- `--symbol` — Yahoo Finance ticker (e.g. `MASTEK.NS` for NSE, `MASTEK.BO` for BSE)
- `--start` / `--end` — inclusive date range (YYYY-MM-DD). If omitted, the full history is fetched.
- `--interval` — candle interval (`1d`, `1wk`, `1mo`, minute intervals, etc.)
- `--auto-adjust` — adjust OHLC values for splits/dividends
- `--output` — CSV path to create; directories are created automatically
- `--incremental` — append only new candles to an existing CSV (auto-detects the last date and merges deduplicated rows)
- `--metadata` — optional JSON file summarising the download window and row count

> ⚠️ Minute-level intervals are limited by Yahoo Finance to roughly the last 60 days.

### Hourly interval downloader

For hourly candles (updated hourly):

```powershell
python mastek_timeInterval_OHLCV_data.py --symbol MASTEK.NS --interval 1h --output data\mastek_hourly.csv --metadata data\mastek_hourly.json --incremental
```

The script automatically refreshes the existing CSV by:

- Detecting the most recent `timestamp` in the current file
- Fetching a small overlap window from Yahoo Finance
- Merging and sorting rows while removing duplicates

Hourly mode respects Yahoo Finance's 730-day retention cap for intraday data and uses smart batch sizing to stay below the server limit.

### Incremental updates & automation

- Run the hourly script every hour with `--incremental` to keep `mastek_hourly.csv` current
- Run the daily script once per day with `--incremental` to append the latest session to `mastek_daily.csv`
- Both scripts can be wiredup via Windows Task Scheduler (see `TASK_SCHEDULER_SETUP.md` for ready-to-use examples). The hourly job should trigger every 60 minutes; the daily job can run a few minutes after market close.

## Output

The script produces a CSV with the following columns:

| Column | Description |
| --- | --- |
| `date` | ISO8601 timestamp of each candle |
| `open`, `high`, `low`, `close` | Raw OHLC values |
| `adj_close` | Adjusted close (blank when `--auto-adjust` is omitted) |
| `volume` | Trading volume |

## Troubleshooting

- Empty CSV: Confirm the ticker exists on Yahoo Finance and the interval is supported for the chosen range.
- SSL errors: Ensure your system clock is correct and you are not behind a restrictive firewall.

## License

MIT License © 2025 Nihaal Patnaik# Historical Data Downloader for Stock Holdings

This folder contains Python scripts to download historical data for your stock holdings using the Kite API.

## Files Included

1. `indianhume_historical_data.py` - Downloads minute-by-minute OHLC data with volume for INDIANHUME (BSE)
2. `mastek_historical_data.py` - Downloads minute-by-minute OHLC data with volume for MASTEK (NSE)

## How to Use These Scripts

### Prerequisites

1. Python installed on your computer
2. Zerodha Kite account with API access

### Steps to Download Historical Data

1. **Install Required Packages**:
   ```
   pip install kiteconnect
   ```

2. **Get API Credentials**:
   - Log in to your Kite Connect developer account
   - Create an API key and secret
   - Open the Python script for the stock you want to download
   - Replace `your_api_key` and `your_api_secret` with your actual credentials

3. **Run the Script**:
   - Uncomment the actual data download code in the script
   - Run the script:
     ```
     python indianhume_historical_data.py
     ```
     or
     ```
     python mastek_historical_data.py
     ```

4. **Access the Data**:
   - The scripts will create CSV files named:
     - `INDIANHUME_BSE_historical_data.csv`
     - `MASTEK_NSE_historical_data.csv`
   - These files can be opened in Excel or any spreadsheet software

## Important Notes

1. **API Limitations**: The Kite API has limits on how much historical data you can download at once and how many requests you can make per minute. The scripts handle this by downloading data in chunks.

2. **Data Availability**: Minute-by-minute data might not be available for the entire history of the stock, especially for older periods.

3. **Script Customization**: You can modify the scripts to change:
   - The time interval (e.g., from 'minute' to 'day' for daily data)
   - The date range
   - Output format

## Troubleshooting

If you encounter any issues:

1. Ensure your API credentials are correct
2. Check your internet connection
3. Make sure your Kite API access is active
4. Try downloading smaller date ranges if you're hitting API limits

For additional help, refer to the Kite Connect documentation at: https://kite.trade/docs/connect/v3/
