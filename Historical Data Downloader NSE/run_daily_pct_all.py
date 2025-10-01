"""Generate full historical daily OHLCV+percentage datasets for NSE tickers.

This script reuses the battle-tested `mastek_historical_data.py` helper from the
`MASTEK/` template folder. For every ticker in the provided list it ensures a
folder exists, refreshes the daily CSV that includes percentage change columns,
and writes a fresh metadata JSON. Incremental mode keeps subsequent runs fast by
adding only the most recent candles.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple

from MASTEK import mastek_historical_data

THIS_DIR = Path(__file__).resolve().parent
DEFAULT_SYMBOLS_FILE = THIS_DIR / "Tickers" / "nse_symbols_all.csv"
DEFAULT_OUTPUT_ROOT = THIS_DIR


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
            csv_path=folder / f"{base}_complete_with_pct.csv",
            metadata_path=folder / f"{base}_complete_metadata.json",
        )


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_single_job(
    job: Job,
    *,
    dry_run: bool,
    force: bool,
    quiet: bool,
    incremental: bool,
    log,
) -> int:
    argv: List[str] = [
        "--symbol",
        job.symbol,
        "--interval",
        "1d",
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
        log(f"DRY RUN :: would refresh {job.symbol_display} -> {job.csv_path}")
        return 0

    try:
        return_code = mastek_historical_data.main(argv)
    except SystemExit as exc:  # mastek_historical_data uses SystemExit for fatal errors
        return_code = int(exc.code or 1)
    if return_code != 0:
        log(f"⚠️  Daily refresh failed for {job.symbol_display} (exit {return_code})")
    return return_code


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh daily OHLCV+percentage CSVs for NSE tickers.")
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
        "--skip-existing",
        action="store_true",
        help="Skip symbols whose target CSV already exists (useful for staged migrations).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force overwrite of existing CSV/metadata instead of incremental merge.",
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
        "--full-refresh",
        action="store_true",
        help="Disable incremental merging and download the full history for every processed ticker.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of tickers to refresh concurrently (default: up to 8, bounded by CPU cores).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
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

    print(f"Processing {total} tickers with daily OHLCV+percentage refresh…")
    incremental = not args.full_refresh
    failures = 0

    default_workers = max(1, min(8, os.cpu_count() or 4))
    max_workers = args.workers if args.workers and args.workers > 0 else default_workers

    def worker(index: int, job: Job) -> Tuple[int, str, int]:
        log(f"[{index}/{total}] Updating {job.symbol_display}")
        code = run_single_job(
            job,
            dry_run=args.dry_run,
            force=args.force,
            quiet=args.quiet,
            incremental=incremental,
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
        if args.skip_existing and job.csv_path.exists():
            log(f"[{index}/{total}] Skipping {job.symbol_display} (existing file detected).")
            continue
        tasks.append((index, job))

    if not tasks:
        log("No eligible symbols to process after skip filters.")
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

    print("✅ Daily dataset refresh complete for all processed tickers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
