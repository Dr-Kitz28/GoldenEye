# NSE Bulk Historical Data Downloader

This workspace extends the single-company automation originally built for `MASTEK` and makes it easy to refresh data for any list of NSE-listed tickers. The helper script `bulk_download_nse.py` reuses the existing downloaders so you get the same folder layout (hourly CSV, full-history daily CSV, and metadata JSON) for every symbol.

## Prerequisites

- Python 3.10+
- Dependencies from the original template (install them once):
  ```powershell
  pip install -r MASTEK/requirements.txt
  ```
- A CSV file with one NSE ticker per line. The repository ships with `Tickers/nse_symbols_sample.csv` as a quick starting point. Symbols can be written as `MASTEK` or `MASTEK.NS`; the script normalises them to the `.NS` suffix automatically. Lines starting with `#` are ignored.

## Getting the full NSE symbol list

Run the helper fetcher to pull the latest `EQUITY_L.csv` directly from NSE India and convert it into a simple newline-delimited list that the bulk downloader can consume:

```powershell
python fetch_all_nse_symbols.py --show-count
```

By default the script writes `Tickers/nse_symbols_all.csv` with all `EQ`-series tickers and the `.NS` suffix appended. Useful flags:

- `--dry-run` previews the first few tickers without touching the filesystem.
- `--offline-sample` uses an embedded mini dataset—handy when testing in an environment without internet access.
- `--series` to include other series codes, e.g. `--series EQ,BE`.
- `--no-suffix` if you prefer bare ticker names.
- `--limit N` to truncate the list (useful for smoke tests).

## Running the bulk downloader

1. Review and edit `Tickers/nse_symbols_sample.csv`, adding or removing tickers as needed.
2. Execute the orchestrator from the workspace root:
  ```powershell
  python bulk_download_nse.py --symbols-file Tickers/nse_symbols_sample.csv
  ```

For each ticker, a folder named after the symbol (without the `.NS` suffix) is created. The two underlying scripts from `MASTEK/` generate the following artefacts inside that folder:

- `<SYMBOL>_complete_with_pct.csv`
- `<SYMBOL>_complete_metadata.json`
- `<SYMBOL>_complete_historical_1h_730days.csv`
- `<SYMBOL>_complete_historical_1h_metadata.json`

Re-running the bulk script keeps files up to date thanks to the `--incremental` mode of the individual downloaders.

## Useful flags

Run `python bulk_download_nse.py --help` for the full option list. Highlights:

- `--skip-hourly` or `--skip-daily` to refresh only one timeframe.
- `--hourly-days N` to request a smaller hourly window (default 730 days).
- `--output-root PATH` to mirror the generated folders elsewhere.
- `--dry-run` to print the commands without executing them (handy for large batches).

## Refreshing daily OHLCV+percentage for all tickers

When you need the complete day-by-day dataset (including percentage change columns) for every NSE symbol, the helper below wraps `mastek_historical_data.py` and iterates through the full ticker roster:

```powershell
python run_daily_pct_all.py --symbols-file Tickers/nse_symbols_all.csv --quiet --workers 8
```

Tips:

- Use `--limit N` while testing to process only the first _N_ tickers.
- Pass `--skip-existing` to avoid touching symbols that already have a fresh CSV.
- Combine with `--dry-run` to preview the workload before launching a long batch.
- Add `--full-refresh` (optionally with `--force`) when you want to overwrite any existing CSVs instead of doing incremental merges.
- Use `--workers K` (default up to 8 simultaneous tickers) to control how many downloads run in parallel. Set it to `1` to fall back to fully sequential mode if you encounter rate limits.
- The script defaults to writing into the repository root, creating (or reusing) one folder per ticker named after the symbol (without `.NS`).

## Automating the daily refresh (Windows Task Scheduler)

Use the helper script in `automation/register_data_refresh_tasks.ps1` to register two scheduled tasks under your Windows account—one for the daily dataset and one for the hourly dataset. The script assumes the project lives at `D:\Trading Strategies\Historical Data Downloader NSE` and that the virtual environment lives at `D:\Trading Strategies\.venv`.

```powershell
# Run once from an elevated PowerShell prompt
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser       # if needed
cd "D:\Trading Strategies\Historical Data Downloader NSE"
.\automation\register_data_refresh_tasks.ps1 -DailyTime 05:45 -HourlyTime 06:30 -DailyWorkers 6 -HourlyWorkers 6
```

- `-DailyTime` and `-HourlyTime` expect 24-hour `HH:mm` strings.
- The script unregisters any existing tasks with the same names before re-creating them.
- Add `-RunAfterRegister` to fire both tasks immediately after registration for a quick smoke test.
- Use `-Unregister` to remove both tasks when you no longer need them.

Both tasks run the virtualenv Python executable directly, so they inherit the same dependencies you use during manual runs. Review the Task Scheduler GUI (`taskschd.msc`) to confirm the triggers and choose “Run whether user is logged on or not” if you need overnight refreshes.

## Monitoring metadata compatibility

`run_hourly_all.py` now logs a warning if it encounters a metadata JSON structure it cannot recognise. If you see a message such as `Unable to locate timestamp in ... metadata keys=[...]`, inspect the referenced file and extend `_TIMESTAMP_KEYS` inside `run_hourly_all.py` so the script can keep parsing the correct update window.

## Extending further

- The script supports controlled concurrency—raise or lower `--workers` to balance speed with API limits.
- The per-symbol template still lives in `MASTEK/`. If you improve those scripts, the bulk downloader automatically benefits from the changes.
