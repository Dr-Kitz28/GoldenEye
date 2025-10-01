"""Utility to download the latest list of NSE equity tickers.

Fetches the official `EQUITY_L.csv` file from NSE India, filters to the common
`EQ` series, and writes a newline-delimited ticker list that can feed the bulk
historical downloader.

The script is resilient to minor network issues (adds a User-Agent header) and
supports a dry-run mode for previewing the first few tickers. For local testing
or offline experimentation you can use `--offline-sample`, which relies on a
small embedded data snippet mirroring the CSV structure published by NSE.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import textwrap
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List

# NSE endpoints - try multiple URLs since availability varies
NSE_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv", 
    "https://www1.nseindia.com/content/equities/EQUITY_L.csv"
]
DEFAULT_OUTPUT = Path("Tickers/nse_symbols_all.csv")
DEFAULT_SERIES = {"EQ"}
DEFAULT_SUFFIX = ".NS"
SAMPLE_DATA = textwrap.dedent(
    """
    SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
    MASTEK,Mastek Limited,EQ,1993-04-23,5,1,INE759A01021,5
    RELIANCE,Reliance Industries Limited,EQ,1977-01-01,10,1,INE002A01018,10
    INFY,Infosys Limited,EQ,1993-06-14,5,1,INE009A01021,5
    TCS,Tata Consultancy Services Limited,EQ,2004-08-25,1,1,INE467B01029,1
    HDFCBANK,HDFC Bank Limited,EQ,1995-05-19,1,1,INE040A01034,1
    """
).strip()


@dataclass(frozen=True)
class SymbolEntry:
    raw_symbol: str
    series: str

    @property
    def normalized_symbol(self) -> str:
        return self.raw_symbol.strip().upper()


def fetch_csv_bytes_with_fallback(urls: List[str]) -> bytes:
    """Try multiple NSE URLs until one succeeds."""
    for i, url in enumerate(urls):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except urllib.error.URLError as exc:
            if i == len(urls) - 1:  # Last URL failed
                raise
    raise RuntimeError("All NSE endpoints failed")


def load_rows(csv_payload: str) -> Iterator[SymbolEntry]:
    handle = io.StringIO(csv_payload)
    reader = csv.DictReader(handle)
    
    for row in reader:
        symbol = row.get("SYMBOL", "").strip()
        # Note: NSE CSV has leading space in column names
        series = row.get(" SERIES", row.get("SERIES", "")).strip().upper()
        if not symbol:
            continue
        yield SymbolEntry(raw_symbol=symbol, series=series)


def filter_series(entries: Iterable[SymbolEntry], allowed_series: set[str]) -> List[str]:
    tickers: List[str] = []
    for entry in entries:
        if entry.series in allowed_series:
            tickers.append(entry.normalized_symbol)
    return tickers


def maybe_suffix(symbols: Iterable[str], suffix: str | None) -> List[str]:
    if not suffix:
        return list(symbols)
    return [f"{symbol}{suffix}" for symbol in symbols]


def write_symbols(symbols: Iterable[str], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        for symbol in symbols:
            handle.write(f"{symbol}\n")


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download the latest list of NSE equity tickers.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    help="Destination path for the ticker list (default: Tickers/nse_symbols_all.csv)",
    )
    parser.add_argument(
        "--series",
        default="EQ",
        help="Comma-separated list of allowed series codes (default: EQ).",
    )
    parser.add_argument(
        "--no-suffix",
        action="store_true",
        help="Do not append the .NS suffix to tickers.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of tickers to output (useful for sampling).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse tickers but do not write the output file; prints a preview instead.",
    )
    parser.add_argument(
        "--offline-sample",
        action="store_true",
        help="Use an embedded mini sample instead of downloading the live NSE file.",
    )
    parser.add_argument(
        "--show-count",
        action="store_true",
        help="Print the total number of tickers discovered after filtering.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    allowed_series = {code.strip().upper() for code in args.series.split(",") if code.strip()}
    suffix = None if args.no_suffix else DEFAULT_SUFFIX

    try:
        if args.offline_sample:
            csv_payload = SAMPLE_DATA
        else:
            csv_bytes = fetch_csv_bytes_with_fallback(NSE_URLS)
            csv_payload = csv_bytes.decode("utf-8-sig")
    except urllib.error.URLError as exc:
        print(f"⚠️  Unable to download NSE ticker file: {exc}", file=sys.stderr)
        print("    Tip: retry with --offline-sample to validate the pipeline without network access.", file=sys.stderr)
        return 2

    entries = list(load_rows(csv_payload))
    symbols = filter_series(entries, allowed_series)
    symbols_with_suffix = maybe_suffix(symbols, suffix)

    if args.limit is not None:
        symbols_with_suffix = symbols_with_suffix[: max(args.limit, 0)]

    if args.show_count:
        print(f"Discovered {len(symbols_with_suffix)} tickers (series filter: {sorted(allowed_series)})")

    if args.dry_run:
        preview_count = min(10, len(symbols_with_suffix))
        print("Previewing tickers:")
        for symbol in symbols_with_suffix[:preview_count]:
            print(f" - {symbol}")
        if args.limit is None and len(symbols_with_suffix) > preview_count:
            print(f"… and {len(symbols_with_suffix) - preview_count} more")
        return 0

    write_symbols(symbols_with_suffix, args.output)
    print(f"Saved {len(symbols_with_suffix)} tickers to {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
