"""Bulk downloader for NSE-listed symbols using existing single-symbol scripts.

This orchestrator reads a list of NSE tickers (e.g. "MASTEK.NS") and, for each
symbol, invokes the hourly and daily downloader scripts found in the `MASTEK`
sub-directory. The resulting folder structure mirrors the single-company
checkout: every symbol receives its own directory with hourly/daily CSVs and
metadata files.

Usage example:

    python bulk_download_nse.py --symbols-file Tickers/nse_symbols_sample.csv

The script is intentionally sequential (one symbol at a time) to stay within
Yahoo Finance rate limits. It supports resuming via the incremental flags of the
underlying scripts.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parent
SINGLE_SYMBOL_DIR = ROOT / "MASTEK"
HOURLY_SCRIPT = SINGLE_SYMBOL_DIR / "mastek_timeInterval_OHLCV_data.py"
DAILY_SCRIPT = SINGLE_SYMBOL_DIR / "mastek_historical_data.py"
DEFAULT_SYMBOLS_FILE = ROOT / "Tickers" / "nse_symbols_sample.csv"


def _validate_single_symbol_scripts() -> None:
    missing = []
    if not HOURLY_SCRIPT.exists():
        missing.append(str(HOURLY_SCRIPT))
    if not DAILY_SCRIPT.exists():
        missing.append(str(DAILY_SCRIPT))
    if missing:
        missing_list = "\n - ".join([""] + missing)
        raise SystemExit(
            "Required single-symbol scripts are missing:" f"{missing_list}\n"
            "Ensure the MASTEK template folder is intact before running the bulk downloader."
        )


def _load_symbols(symbols_file: Path) -> List[str]:
    if not symbols_file.exists():
        raise SystemExit(f"Symbols file '{symbols_file}' not found. Please create it or point to an existing file.")

    symbols: List[str] = []
    with symbols_file.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            raw = row[0].strip()
            if not raw or raw.startswith("#"):
                continue
            symbol = raw.upper()
            if not symbol.endswith(".NS"):
                symbol = f"{symbol}.NS"
            symbols.append(symbol)

    if not symbols:
        raise SystemExit(f"No symbols found in '{symbols_file}'. Add at least one NSE ticker (e.g. MASTEK.NS).")
    return symbols


def _run_subprocess(command: Iterable[str]) -> int:
    process = subprocess.run(command, check=False)
    return process.returncode


def _hourly_date_range(hourly_days: int) -> tuple[str, str]:
    today = dt.date.today()
    start = today - dt.timedelta(days=hourly_days - 1)
    return start.isoformat(), today.isoformat()


def _symbol_directory(symbol: str, output_root: Path) -> Path:
    base_name = symbol.split(".")[0].upper()
    return output_root / base_name


def _download_hourly(symbol: str, target_dir: Path, hourly_days: int, *, dry_run: bool) -> None:
    start, end = _hourly_date_range(hourly_days)
    base_name = target_dir.name
    output_csv = target_dir / f"{base_name}_complete_historical_1h_{hourly_days}days.csv"
    metadata_json = target_dir / f"{base_name}_complete_historical_1h_metadata.json"

    command = [
        sys.executable,
        str(HOURLY_SCRIPT),
        "--symbol", symbol,
        "--interval", "1h",
        "--start", start,
        "--end", end,
        "--output", str(output_csv),
        "--metadata", str(metadata_json),
        "--extended-hourly",
        "--incremental",
        "--no-progress",
    ]

    if dry_run:
        print("DRY RUN ::", " ".join(command))
        return

    print(f"→ Hourly download {symbol} ({start} → {end})")
    return_code = _run_subprocess(command)
    if return_code != 0:
        print(f"⚠️  Hourly download failed for {symbol} (exit code {return_code})")


def _download_daily(symbol: str, target_dir: Path, *, dry_run: bool) -> None:
    base_name = target_dir.name
    output_csv = target_dir / f"{base_name}_complete_with_pct.csv"
    metadata_json = target_dir / f"{base_name}_complete_metadata.json"

    command = [
        sys.executable,
        str(DAILY_SCRIPT),
        "--symbol", symbol,
        "--interval", "1d",
        "--output", str(output_csv),
        "--metadata", str(metadata_json),
        "--incremental",
        "--quiet",
    ]

    if dry_run:
        print("DRY RUN ::", " ".join(command))
        return

    print(f"→ Daily download {symbol} (full history with pct)")
    return_code = _run_subprocess(command)
    if return_code != 0:
        print(f"⚠️  Daily download failed for {symbol} (exit code {return_code})")


def main(argv: Iterable[str] | None = None) -> int:
    _validate_single_symbol_scripts()

    parser = argparse.ArgumentParser(description="Bulk NSE downloader leveraging the single-symbol scripts.")
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS_FILE,
    help="CSV file with one NSE ticker per line (default: Tickers/nse_symbols_sample.csv)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=ROOT,
        help="Directory where per-symbol folders will be created (default: repository root).",
    )
    parser.add_argument(
        "--hourly-days",
        type=int,
        default=730,
        help="Number of calendar days to request for hourly data (max ~730 supported by Yahoo Finance).",
    )
    parser.add_argument(
        "--skip-hourly",
        action="store_true",
        help="Skip hourly downloads and only refresh daily data.",
    )
    parser.add_argument(
        "--skip-daily",
        action="store_true",
        help="Skip daily downloads and only refresh hourly data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands that would run without executing them.",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    symbols = _load_symbols(args.symbols_file)
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    print(f"Loaded {len(symbols)} NSE tickers from {args.symbols_file}")
    for symbol in symbols:
        target_dir = _symbol_directory(symbol, output_root)
        target_dir.mkdir(parents=True, exist_ok=True)
        print("\n==============================")
        print(f"Processing {symbol} → {target_dir}")
        print("==============================")

        if not args.skip_hourly:
            _download_hourly(symbol, target_dir, args.hourly_days, dry_run=args.dry_run)

        if not args.skip_daily:
            _download_daily(symbol, target_dir, dry_run=args.dry_run)

    print("\n✅ Bulk download routine complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
