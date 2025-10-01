"""Fetch Mastek (or any Yahoo Finance ticker) OHLCV data at a custom interval.

Examples
--------
python mastek_timeInterval_OHLCV_data.py --interval 15m --start 2024-01-01 --end 2024-01-15
python mastek_timeInterval_OHLCV_data.py --symbol MASTEK.BO --interval 1wk --output data/mastek_weekly.csv
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Optional, List

import pandas as pd

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    print("yfinance not found. Installing...", file=sys.stderr)
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])
    import yfinance as yf  # type: ignore  # noqa: E402


def parse_date(value: Optional[str]) -> Optional[dt.datetime]:
    """Parse a YYYY-MM-DD string into a datetime, returning None if blank."""
    if not value:
        return None
    return dt.datetime.strptime(value, "%Y-%m-%d")


def download_prices(
    symbol: str,
    interval: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    auto_adjust: bool = False,
    progress: bool = True,
):
    """Download OHLCV data using yfinance.download and return a DataFrame."""
    kwargs = {
        "interval": interval,
        "auto_adjust": auto_adjust,
        "progress": progress,
    }
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end

    df = yf.download(symbol, **kwargs)
    if df.empty:
        raise ValueError(
            f"No data returned for symbol={symbol} interval={interval} start={start} end={end}. "
            "Try a broader date range or a supported interval."
        )
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        # For single symbol downloads, take only the first level (OHLCV field names)
        # and ignore the repetitive ticker name in the second level
        df.columns = [col[0].lower().replace(" ", "_") if col[0] else "date" for col in df.columns]
    else:
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    df["symbol"] = symbol
    return df


def write_csv(df, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)


def write_metadata(df, output: Path, symbol: str, interval: str) -> None:
    timestamp_col = "timestamp" if "timestamp" in df.columns else (
        "datetime" if "datetime" in df.columns else "date"
    )
    start_ts = pd.to_datetime(df[timestamp_col].iloc[0]).isoformat()
    end_ts = pd.to_datetime(df[timestamp_col].iloc[-1]).isoformat()
    metadata = {
        "symbol": symbol,
        "rows": int(len(df)),
        "interval": interval,
        "start": start_ts,
        "end": end_ts,
    }
    output.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def generate_date_batches(start_date: str, end_date: str, batch_days: int = 50, interval: str = "1d") -> List[tuple]:
    """Generate date ranges for batch downloading with interval-aware batch sizing."""
    start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
    
    # For hourly intervals, Yahoo Finance has a ~730 hour limit (about 30 days)
    # We'll use smaller batches to stay well within this limit
    if interval in ['1h', '60m']:
        # Use 25-day batches for hourly data (25 days * 6.5 trading hours = ~162 hours per batch)
        # This leaves plenty of margin below the 730-hour limit
        batch_days = min(batch_days, 25)
        print(f"📊 Hourly interval detected: Using {batch_days}-day batches (Yahoo Finance 730-hour limit)")
    elif interval in ['30m']:
        # For 30-minute data, use 15-day batches
        batch_days = min(batch_days, 15)
        print(f"📊 30-minute interval detected: Using {batch_days}-day batches")
    elif interval in ['15m']:
        # For 15-minute data, use 10-day batches
        batch_days = min(batch_days, 10)
        print(f"📊 15-minute interval detected: Using {batch_days}-day batches")
    elif interval in ['5m']:
        # For 5-minute data, use 7-day batches
        batch_days = min(batch_days, 7)
        print(f"📊 5-minute interval detected: Using {batch_days}-day batches")
    elif interval in ['1m', '2m']:
        # For 1-2 minute data, use very small batches (3-5 days)
        batch_days = min(batch_days, 5)
        print(f"📊 High-frequency interval detected: Using {batch_days}-day batches")
    
    batches = []
    current = start_dt
    
    while current < end_dt:
        batch_end = min(current + dt.timedelta(days=batch_days), end_dt)
        batches.append((
            current.strftime("%Y-%m-%d"),
            batch_end.strftime("%Y-%m-%d")
        ))
        current = batch_end + dt.timedelta(days=1)
    
    return batches


def download_batch_data(
    symbol: str,
    interval: str,
    start: str,
    end: str,
    auto_adjust: bool = False,
    progress: bool = True,
    delay: float = 1.0
) -> Optional[pd.DataFrame]:
    """Download data for a single batch with error handling and delay."""
    try:
        print(f"  Downloading batch: {start} to {end}")
        df = download_prices(symbol, interval, start, end, auto_adjust, progress)
        time.sleep(delay)  # Rate limiting
        return df
    except Exception as e:
        print(f"  Failed to download batch {start} to {end}: {str(e)}")
        return None


def download_batched_data(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    auto_adjust: bool = False,
    progress: bool = True,
    batch_days: int = 50,
    delay: float = 1.0
) -> pd.DataFrame:
    """Download historical data in batches to overcome Yahoo Finance limitations."""
    print(f"🚀 Batch Downloading {symbol} [{interval}] from {start_date} to {end_date}")
    
    # Calculate total period and estimated hours for hourly intervals
    start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days
    
    if interval in ['1h', '60m']:
        # Estimate trading hours (assuming ~6.5 hours per trading day)
        estimated_hours = total_days * 6.5
        print(f"📅 Total period: {total_days} days (~{estimated_hours:.0f} trading hours)")
        if estimated_hours > 730:
            print(f"⚠️  Period exceeds Yahoo Finance 730-hour limit - using batch approach")
    
    batches = generate_date_batches(start_date, end_date, batch_days, interval)
    print(f"📦 Generated {len(batches)} batches (auto-sized for {interval} interval)")
    
    all_data = []
    successful_batches = 0
    
    for i, (batch_start, batch_end) in enumerate(batches, 1):
        print(f"Batch {i}/{len(batches)}: {batch_start} to {batch_end}")
        
        df = download_batch_data(
            symbol, interval, batch_start, batch_end, 
            auto_adjust, progress, delay
        )
        
        if df is not None and not df.empty:
            all_data.append(df)
            successful_batches += 1
            print(f"  Success: {len(df)} records")
        else:
            print(f"  Skipped: No data available")
    
    if not all_data:
        raise ValueError(f"No data downloaded for any batch. Check symbol {symbol} and date range.")
    
    print(f"\nCombining {successful_batches} successful batches...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates that might occur at batch boundaries
    # Handle both 'timestamp' and 'Datetime' column names
    datetime_col = 'timestamp' if 'timestamp' in combined_df.columns else 'Datetime'
    if datetime_col in combined_df.columns:
        combined_df = combined_df.drop_duplicates(subset=[datetime_col]).sort_values(datetime_col)
        print(f"Final dataset: {len(combined_df)} records from {combined_df[datetime_col].min()} to {combined_df[datetime_col].max()}")
    else:
        print(f"Final dataset: {len(combined_df)} records")
    
    return combined_df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download Yahoo Finance OHLCV data for MASTEK at a chosen interval.")
    parser.add_argument("--symbol", default="MASTEK.NS", help="Yahoo Finance ticker symbol (default: MASTEK.NS)")
    parser.add_argument("--interval", default="1d", help="Data interval (e.g., 1m, 5m, 1h, 1d, 1wk, 1mo)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD). Omit for full available history.")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Omit for latest available data.")
    parser.add_argument("--auto-adjust", action="store_true", help="Adjust OHLC for dividends/splits.")
    parser.add_argument("--output", default="mastek_interval_data.csv", help="Path to output CSV file.")
    parser.add_argument("--metadata", help="Optional JSON file to store summary metadata.")
    parser.add_argument("--no-progress", action="store_true", help="Disable yfinance progress bar.")
    parser.add_argument("--batch", action="store_true", help="Use batch downloading for long historical periods with intraday intervals.")
    parser.add_argument("--batch-days", type=int, default=50, help="Days per batch when using batch mode (default: 50)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between batch requests in seconds (default: 1.0)")
    parser.add_argument("--slow-mode", action="store_true", help="Enable slow mode with longer delays (5 seconds between requests)")
    parser.add_argument("--ultra-slow", action="store_true", help="Enable ultra-slow mode with very long delays (10 seconds between requests)")
    parser.add_argument("--extended-hourly", action="store_true", help="Enable extended hourly mode for 1460+ hours (forces batch mode with optimal settings for long periods)")
    parser.add_argument("--incremental", action="store_true", help="Merge freshly downloaded data into the existing output CSV (auto-detects start date).")
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    output_path = Path(args.output)
    existing_dataset: Optional[pd.DataFrame] = None

    start = args.start
    end = args.end

    if args.incremental:
        if output_path.exists():
            try:
                existing_dataset = pd.read_csv(output_path)
            except Exception as exc:  # pragma: no cover - defensive IO handling
                print(f"⚠️ Unable to read existing output for incremental merge: {exc}. Continuing with fresh download.")
                existing_dataset = None
            else:
                if "timestamp" not in existing_dataset.columns:
                    print("⚠️ Existing output is missing a 'timestamp' column; skipping incremental optimisation.")
                    existing_dataset = None
                else:
                    existing_dataset["timestamp"] = pd.to_datetime(
                        existing_dataset["timestamp"], utc=True, errors="coerce"
                    )
                    last_timestamp = existing_dataset["timestamp"].dropna().max()
                    if pd.isna(last_timestamp):
                        print("⚠️ Existing output does not contain valid timestamps; skipping incremental optimisation.")
                        existing_dataset = None
                    else:
                        if not start:
                            fetch_start = (last_timestamp - dt.timedelta(days=5)).date()
                            start = fetch_start.strftime("%Y-%m-%d")
                            args.start = start
                            print(f"📈 Incremental update enabled: fetching data starting {start}")
                        else:
                            print(f"ℹ️ Incremental mode active with user-specified start date {start}")
                        if not end:
                            end = dt.datetime.utcnow().strftime("%Y-%m-%d")
                            args.end = end
                            print(f"ℹ️ No end date supplied; defaulting to today ({end})")
        else:
            print("⚠️ Incremental update requested but existing output not found; performing full download.")

    if start:
        parse_date(start)
    if end:
        parse_date(end)

    # Handle extended hourly mode
    if args.extended_hourly:
        if args.interval not in ['1h', '60m']:
            print("⚠️  Extended hourly mode is designed for 1h/60m intervals. Consider using regular batch mode.")
        args.batch = True  # Force batch mode
        if args.delay == 1.0:  # Only set delay if user hasn't customized it
            delay = 2.0  # Slightly longer delay for extended periods
        print("🚀 Extended hourly mode enabled: Optimized for 1460+ hours with batch downloading")
    
    # Handle slow mode settings
    delay = args.delay
    if args.ultra_slow:
        delay = 10.0
        print("🐌 Ultra-slow mode enabled: 10 seconds between requests")
    elif args.slow_mode:
        delay = 5.0
        print("🚶 Slow mode enabled: 5 seconds between requests")
    elif delay > 1.0:
        print(f"⏱️ Custom delay enabled: {delay} seconds between requests")

    # Determine if we should use batch mode
    # For hourly data, auto-enable batch mode for periods > 30 days (to stay within 730-hour limit)
    # For other intraday intervals, use batch mode for periods > 60 days
    hourly_intervals = ['1h', '60m']
    intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']
    
    use_batch = args.batch
    if not use_batch and start and end:
        period_days = (dt.datetime.strptime(end, "%Y-%m-%d") - dt.datetime.strptime(start, "%Y-%m-%d")).days
        
        if args.interval in hourly_intervals and period_days > 30:
            use_batch = True
            print(f"🔄 Auto-enabling batch mode: {period_days} days with hourly data exceeds 30-day safe limit")
        elif args.interval in intraday_intervals and period_days > 60:
            use_batch = True
            print(f"🔄 Auto-enabling batch mode: {period_days} days with intraday data exceeds 60-day threshold")
    
    if use_batch and start and end:
        print(f"Using batch mode for {args.symbol} interval={args.interval} from {start} to {end}")
        df = download_batched_data(
            symbol=args.symbol,
            interval=args.interval,
            start_date=start,
            end_date=end,
            auto_adjust=args.auto_adjust,
            progress=not args.no_progress,
            batch_days=args.batch_days,
            delay=delay
        )
    else:
        print(f"Downloading {args.symbol} interval={args.interval} start={start} end={end or 'latest'}")
        df = download_prices(
            symbol=args.symbol,
            interval=args.interval,
            start=start,
            end=end,
            auto_adjust=args.auto_adjust,
            progress=not args.no_progress,
        )

    # Normalize datetime column name for different intervals
    if "datetime" in df.columns:
        df.rename(columns={"datetime": "timestamp"}, inplace=True)
    elif "date" in df.columns:
        df.rename(columns={"date": "timestamp"}, inplace=True)

    output_path = Path(args.output)
    final_df = df

    if args.incremental and existing_dataset is not None:
        print("🧩 Starting incremental merge with existing dataset...")
        existing = existing_dataset.copy()
        existing["timestamp"] = pd.to_datetime(existing["timestamp"], utc=True, errors="coerce")
        existing = existing.dropna(subset=["timestamp"])
        subset_cols = ["timestamp"]
        if "symbol" in existing.columns:
            subset_cols.append("symbol")
        existing = existing.drop_duplicates(subset=subset_cols, keep="last")

        new_data = df.copy()
        if "timestamp" not in new_data.columns:
            raise ValueError("Downloaded dataframe missing 'timestamp' column; cannot merge incrementally.")
        new_data["timestamp"] = pd.to_datetime(new_data["timestamp"], utc=True, errors="coerce")
        new_data = new_data.dropna(subset=["timestamp"])

        merged_columns = list(existing.columns)
        for col in new_data.columns:
            if col not in merged_columns:
                merged_columns.append(col)
        existing = existing.reindex(columns=merged_columns)
        new_data = new_data.reindex(columns=merged_columns)

        combined_df = pd.concat([existing, new_data], ignore_index=True, sort=False)
        combined_df = combined_df.dropna(subset=["timestamp"])
        combined_df = combined_df.drop_duplicates(subset=subset_cols, keep="last")
        combined_df = combined_df.sort_values("timestamp")

        added_rows = len(combined_df) - len(existing)
        if added_rows <= 0:
            print("ℹ️ No new rows detected; the dataset is already up to date.")
        else:
            print(f"📈 Incremental merge complete: added {added_rows} new rows.")

        final_df = combined_df.reset_index(drop=True)
        df = final_df.copy()
        print(f"💾 Prepared merged dataset with {len(final_df)} total rows.")

    write_csv(final_df, output_path)
    print(f"Saved data to {output_path.resolve()}")

    if args.metadata:
        metadata_path = Path(args.metadata)
        write_metadata(final_df, metadata_path, args.symbol, args.interval)
        print(f"Saved metadata to {metadata_path.resolve()}")


if __name__ == "__main__":
    main()
