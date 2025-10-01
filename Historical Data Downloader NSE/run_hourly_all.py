"""Refresh rolling 1-hour OHLCV datasets for all NSE tickers.

The script is meant to run on a daily schedule. It keeps each symbol's
`*_complete_historical_1h_730days.csv` file up to date by downloading the latest
candles from Yahoo Finance via :mod:`MASTEK.mastek_historical_data`. Existing
CSV/JSON files are merged incrementally, so the retained history extends beyond
the 730-day download window when the script is executed regularly.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Tuple

from MASTEK import mastek_historical_data

THIS_DIR = Path(__file__).resolve().parent
DEFAULT_SYMBOLS_FILE = THIS_DIR / "Tickers" / "nse_symbols_all.csv"
DEFAULT_OUTPUT_ROOT = THIS_DIR
DEFAULT_LOOKBACK_DAYS = 5
MAX_DEFAULT_WINDOW_DAYS = 730

logger = logging.getLogger("historical_data_downloader.hourly")
_WARNED_METADATA_PATHS: set[Path] = set()
_TIMESTAMP_KEYS: Tuple[str, ...] = (
    "end",
    "last",
    "end_date",
    "last_date",
    "endTime",
    "last_timestamp",
    "endTimestamp",
)


@dataclass(slots=True)
class Job:
    symbol: str
    folder: Path
    csv_path: Path
    metadata_path: Path

    @property
    def symbol_display(self) -> str:
        return self.symbol.upper()


def read_symbols(path: Path) -> List[str]:
    if not path.exists():
        raise SystemExit(f"Symbols file '{path}' was not found. Run fetch_all_nse_symbols.py first.")

    symbols: List[str] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            value = row[0].strip()
            if not value or value.startswith("#"):
                continue
            symbols.append(value.upper())
    if not symbols:
        raise SystemExit(f"No symbols found in '{path}'.")
    return symbols


def plan_jobs(symbols: Iterable[str], output_root: Path) -> Iterator[Job]:
    for symbol in symbols:
        base = symbol.split(".")[0].upper()
        folder = output_root / base
        yield Job(
            symbol=symbol,
            folder=folder,
            csv_path=folder / f"{base}_complete_historical_1h_730days.csv",
            metadata_path=folder / f"{base}_complete_historical_1h_730days_metadata.json",
        )


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh 1-hour OHLCV CSVs for NSE tickers.")
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS_FILE,
    help="CSV with one ticker per line (default: Tickers/nse_symbols_all.csv)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where per-symbol folders live (default: repository root).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N symbols (useful for smoke tests).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force overwrite of existing CSV/metadata before merging.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Download the full 730-day window every time (disables incremental merge).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Number of trailing days to re-fetch beyond the last saved candle when merging incrementally.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned work without downloading.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Silence verbose logging from the underlying downloader.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of tickers to refresh concurrently (default: up to 8, bounded by CPU cores).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _coerce_timestamp(raw: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        if raw.endswith("Z"):
            try:
                return dt.datetime.fromisoformat(raw[:-1] + "+00:00")
            except ValueError:
                return None
    return None


def _parse_metadata_timestamp(metadata_path: Path) -> Optional[dt.datetime]:
    try:
        with metadata_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    if isinstance(payload, list) and payload:
        record = payload[-1]
    elif isinstance(payload, dict):
        record = payload
    else:
        if metadata_path not in _WARNED_METADATA_PATHS:
            logger.warning("Unsupported metadata payload type %s in %s", type(payload).__name__, metadata_path)
            _WARNED_METADATA_PATHS.add(metadata_path)
        return None

    for key in _TIMESTAMP_KEYS:
        raw_value = record.get(key)
        if isinstance(raw_value, str):
            parsed = _coerce_timestamp(raw_value)
            if parsed is not None:
                return parsed

    if metadata_path not in _WARNED_METADATA_PATHS:
        logger.warning(
            "Unable to locate timestamp in %s metadata keys=%s", metadata_path, sorted(record.keys())
        )
        _WARNED_METADATA_PATHS.add(metadata_path)
    return None


def determine_window(job: Job, lookback_days: int, full_refresh: bool) -> Tuple[str, str]:
    today = dt.date.today()
    default_start = today - dt.timedelta(days=MAX_DEFAULT_WINDOW_DAYS)

    if full_refresh:
        start_date = default_start
    else:
        last_timestamp = _parse_metadata_timestamp(job.metadata_path)
        if last_timestamp is None:
            start_date = default_start
        else:
            last_date = last_timestamp.date()
            safety_buffer = dt.timedelta(days=max(0, lookback_days))
            start_candidate = last_date - safety_buffer
            start_date = max(start_candidate, default_start)

    return start_date.isoformat(), today.isoformat()


def run_single_job(
    job: Job,
    *,
    dry_run: bool,
    force: bool,
    quiet: bool,
    incremental: bool,
    start_date: str,
    end_date: str,
    log,
) -> int:
    argv: List[str] = [
        "--symbol",
        job.symbol,
        "--interval",
        "1h",
        "--start",
        start_date,
        "--end",
        end_date,
        "--output",
        str(job.csv_path),
        "--metadata",
        str(job.metadata_path),
    ]
    if incremental:
        argv.append("--incremental")
    if force:
        argv.append("--force")
    if quiet:
        argv.append("--quiet")

    if dry_run:
        log(
            f"DRY RUN :: would refresh {job.symbol_display} "
            f"[{start_date} → {end_date}] -> {job.csv_path}"
        )
        return 0

    try:
        return_code = mastek_historical_data.main(argv)
    except SystemExit as exc:
        return_code = int(exc.code or 1)

    if return_code != 0:
        log(f"⚠️  Hourly refresh failed for {job.symbol_display} (exit {return_code})")
    return return_code


def main(argv: Iterable[str] | None = None) -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

    args = parse_args(argv)
    symbols = read_symbols(args.symbols_file)

    if args.limit is not None and args.limit >= 0:
        symbols = symbols[: args.limit]

    jobs = list(plan_jobs(symbols, args.output_root.resolve()))

    total = len(jobs)
    if total == 0:
        print("No symbols to process.")
        return 0

    print_lock = threading.Lock()

    def log(message: str) -> None:
        with print_lock:
            print(message)

    print(f"Processing {total} tickers with hourly OHLCV refresh…")
    incremental = not args.full_refresh
    failures = 0

    default_workers = max(1, min(8, os.cpu_count() or 4))
    max_workers = args.workers if args.workers and args.workers > 0 else default_workers

    def worker(index: int, job: Job) -> Tuple[int, str, int]:
        start_date, end_date = determine_window(
            job,
            lookback_days=args.lookback_days,
            full_refresh=args.full_refresh,
        )
        log(f"[{index}/{total}] Updating {job.symbol_display} ({start_date} → {end_date})")
        code = run_single_job(
            job,
            dry_run=args.dry_run,
            force=args.force,
            quiet=args.quiet,
            incremental=incremental,
            start_date=start_date,
            end_date=end_date,
            log=log,
        )
        return index, job.symbol_display, code

    tasks: List[Tuple[int, Job]] = []
    prepared_folders: set[Path] = set()
    for index, job in enumerate(jobs, start=1):
        folder_path = job.folder.resolve()
        if folder_path not in prepared_folders:
            ensure_folder(job.folder)
            prepared_folders.add(folder_path)
        tasks.append((index, job))

    if not tasks:
        log("No eligible symbols to process.")
        return 0

    if max_workers == 1:
        for index, job in tasks:
            _, _, code = worker(index, job)
            if code != 0:
                failures += 1
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {executor.submit(worker, index, job): job for index, job in tasks}
            for future in as_completed(future_to_job):
                _, symbol_display, code = future.result()
                if code != 0:
                    failures += 1

    if failures:
        print(f"⚠️  Completed with {failures} failures. See logs above for details.")
        return 1

    print("✅ Hourly dataset refresh complete for all processed tickers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
