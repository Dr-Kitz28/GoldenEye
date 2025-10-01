"""Command-line utility to download Yahoo Finance OHLCV data into CSV files.

The tool is designed for long-running historical pulls, repeatable incremental
updates, and lightweight automation use. It supports multiple symbols,
optional adjusted close data, and JSON metadata summaries to assist data
pipelines.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from numbers import Number
from typing import Iterable, Iterator, Optional, Sequence

import pandas as pd

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - dependency bootstrap
    print("yfinance library not found. Installing...", flush=True)
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])
    import yfinance as yf  # type: ignore  # noqa: E402


DEFAULT_SYMBOL = "MASTEK.NS"
DEFAULT_INTERVAL = "1d"
DEFAULT_OUTPUT = "MASTEK_OHLCV.csv"
SUPPORTED_INTERVALS = {
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo",
}


@dataclass(slots=True)
class Config:
    symbols: tuple[str, ...]
    start: Optional[dt.date]
    end: Optional[dt.date]
    interval: str
    auto_adjust: bool
    output: str
    split_output: bool
    quiet: bool
    metadata_path: Optional[str]
    force: bool
    incremental: bool


def parse_args(argv: Optional[Iterable[str]] = None) -> Config:
    parser = argparse.ArgumentParser(
        description="Download OHLCV data from Yahoo Finance and export to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbol",
        dest="single_symbols",
        action="append",
        help="Yahoo Finance ticker symbol (can be provided multiple times).",
    )
    parser.add_argument(
        "--symbols",
        dest="comma_symbols",
        help="Comma-separated list of ticker symbols (alternative to --symbol).",
    )
    parser.add_argument(
        "--symbols-file",
        dest="symbols_file",
        help="Path to a text file with one ticker symbol per line.",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date in YYYY-MM-DD format. Defaults to earliest available.",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--interval",
        default=DEFAULT_INTERVAL,
        choices=sorted(SUPPORTED_INTERVALS),
        help="Candle interval to download.",
    )
    parser.add_argument(
        "--auto-adjust",
        action="store_true",
        help="Return adjusted OHLC values (default uses raw close).",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=(
            "Output CSV path. When --split-output is set, treated as a directory "
            "where files are emitted as <symbol>.csv"
        ),
    )
    parser.add_argument(
        "--split-output",
        action="store_true",
        help="Write one CSV per symbol instead of a combined file.",
    )
    parser.add_argument(
        "--metadata",
        dest="metadata_path",
        help="Optional path to JSON file with download metadata summary.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output (errors still print to stderr).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files without prompting.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Append only new candles to existing CSV output (auto-detects last saved date).",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    resolved_symbols = _resolve_symbols(
        cli_symbols=args.single_symbols,
        comma_symbols=args.comma_symbols,
        symbols_file=args.symbols_file,
    )

    if not resolved_symbols:
        resolved_symbols = (DEFAULT_SYMBOL,)

    return Config(
        symbols=resolved_symbols,
        start=args.start,
        end=args.end,
        interval=args.interval,
        auto_adjust=args.auto_adjust,
        output=args.output,
        split_output=args.split_output,
        quiet=args.quiet,
        metadata_path=args.metadata_path,
        force=args.force,
        incremental=args.incremental,
    )


def _resolve_symbols(
    *,
    cli_symbols: Optional[Sequence[str]],
    comma_symbols: Optional[str],
    symbols_file: Optional[str],
) -> tuple[str, ...]:
    symbols: list[str] = []

    if cli_symbols:
        for value in cli_symbols:
            symbols.extend(_split_symbol_list(value))

    if comma_symbols:
        symbols.extend(_split_symbol_list(comma_symbols))

    if symbols_file:
        try:
            with open(symbols_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        symbols.append(line)
        except OSError as exc:
            raise SystemExit(f"Unable to read symbols file '{symbols_file}': {exc}")

    # deduplicate while preserving order
    seen: set[str] = set()
    unique_symbols = []
    for symbol in symbols:
        symbol = symbol.strip()
        if not symbol:
            continue
        if symbol.upper() not in seen:
            seen.add(symbol.upper())
            unique_symbols.append(symbol)

    return tuple(unique_symbols)


def _split_symbol_list(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _parse_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover - argparse handles messaging
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format YYYY-MM-DD"
        ) from exc


def resolve_date_range(start: Optional[dt.date], end: Optional[dt.date]) -> tuple[
    Optional[dt.datetime], Optional[dt.datetime]
]:
    """Convert naive dates to timezone-aware datetimes suitable for yfinance."""

    tz = dt.timezone.utc
    start_dt = dt.datetime.combine(start, dt.time.min, tzinfo=tz) if start else None
    # yfinance treats end as exclusive, so include entire day when provided.
    end_dt = (
        dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min, tzinfo=tz)
        if end
        else None
    )
    return start_dt, end_dt


def download_symbol(
    symbol: str,
    *,
    interval: str,
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    auto_adjust: bool,
    quiet: bool,
):
    if not quiet:
        logging.info(
            "Fetching %s data from %s to %s at interval %s",
            symbol,
            start.date() if start else "earliest",
            (end - dt.timedelta(days=1)).date() if end else "today",
            interval,
        )

    download_kwargs = {
        "interval": interval,
        "auto_adjust": auto_adjust,
        "progress": False,
        "actions": False,
        "threads": False,
    }
    if start is not None:
        download_kwargs["start"] = start
    if end is not None:
        download_kwargs["end"] = end
    if start is None and end is None:
        download_kwargs["period"] = "max"

    df = yf.download(symbol, **download_kwargs)

    if df.empty:
        raise RuntimeError(
            f"No data returned for symbol '{symbol}'. Check the ticker or date range."
        )

    df.sort_index(inplace=True)
    df["Symbol"] = symbol
    return df


def _normalise_dataframe(df) -> Iterator[dict[str, object]]:
    # Calculate percentage changes while handling possible multi-index columns.
    prev_close: float | None = None
    prev_adj_close: float | None = None

    for timestamp, row in df.iterrows():
        def _extract_value(key):
            value = row.get(key)
            if hasattr(value, "iloc"):
                return value.iloc[0] if len(value) > 0 else None
            return value

        close_raw = _extract_value("Close")
        adj_close_raw = _extract_value("Adj Close")

        try:
            close_current = float(close_raw) if close_raw is not None else None
        except (TypeError, ValueError):
            close_current = None

        try:
            adj_close_current = float(adj_close_raw) if adj_close_raw is not None else None
        except (TypeError, ValueError):
            adj_close_current = None

        pct_change = ""
        adj_pct_change = ""

        if close_current is not None and prev_close is not None and prev_close != 0:
            pct_change = f"{((close_current - prev_close) / prev_close * 100):.4f}"

        if adj_close_current is not None and prev_adj_close is not None and prev_adj_close != 0:
            adj_pct_change = f"{((adj_close_current - prev_adj_close) / prev_adj_close * 100):.4f}"

        yield {
            "symbol": _extract_value("Symbol"),
            "date": timestamp.isoformat(),
            "open": _format_number(_extract_value("Open")),
            "high": _format_number(_extract_value("High")),
            "low": _format_number(_extract_value("Low")),
            "close": _format_number(close_raw),
            "adj_close": _format_number(adj_close_raw),
            "volume": _clean_volume(_extract_value("Volume")),
            "pct_change": pct_change,
            "adj_pct_change": adj_pct_change,
        }

        prev_close = close_current
        prev_adj_close = adj_close_current


def _build_normalised_dataframe(df, include_symbol: bool) -> pd.DataFrame:
    rows = list(_normalise_dataframe(df))
    normalised = pd.DataFrame(rows)
    if not include_symbol and "symbol" in normalised.columns:
        normalised = normalised.drop(columns=["symbol"])
    return normalised


def _merge_normalised(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    include_symbol: bool,
) -> pd.DataFrame:
    if existing_df.empty:
        return new_df.copy()

    work_existing = existing_df.copy()
    work_new = new_df.copy()

    all_columns: list[str] = list(work_existing.columns)
    for column in work_new.columns:
        if column not in all_columns:
            all_columns.append(column)
    work_existing = work_existing.reindex(columns=all_columns)
    work_new = work_new.reindex(columns=all_columns)

    work_existing["_merge_dt"] = pd.to_datetime(work_existing.get("date"), utc=True, errors="coerce")
    work_new["_merge_dt"] = pd.to_datetime(work_new.get("date"), utc=True, errors="coerce")

    combined = pd.concat([work_existing, work_new], ignore_index=True, sort=False)
    combined = combined.dropna(subset=["_merge_dt"])

    subset_cols = ["_merge_dt"]
    if include_symbol and "symbol" in combined.columns:
        subset_cols.insert(0, "symbol")

    combined = combined.drop_duplicates(subset=subset_cols, keep="last")
    combined = combined.sort_values(subset_cols)
    combined = combined.drop(columns=["_merge_dt"])
    combined.reset_index(drop=True, inplace=True)
    return combined


def _format_number(value) -> str:
    if value is None:
        return ""
    if isinstance(value, Number):
        float_value = float(value)
        if math.isnan(float_value):
            return ""
        return f"{float_value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _clean_volume(value) -> int:
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, Number):
        float_value = float(value)
        if math.isnan(float_value):
            return 0
        return int(round(float_value))
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def write_csv(
    df,
    *,
    output_path: str,
    include_symbol_column: bool,
    force: bool,
    incremental: bool,
):
    output_dir = os.path.dirname(os.path.abspath(output_path))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    normalised_df = _build_normalised_dataframe(df, include_symbol_column)

    if incremental and os.path.exists(output_path):
        try:
            existing_df = pd.read_csv(output_path)
        except Exception as exc:
            logging.warning("Unable to read existing output '%s' for incremental merge (%s). Recreating file.", output_path, exc)
            existing_df = pd.DataFrame(columns=normalised_df.columns)
        merged = _merge_normalised(existing_df, normalised_df, include_symbol_column)
        merged.to_csv(output_path, index=False)
        logging.info(
            "Merged %s new rows into %s",
            max(len(merged) - len(existing_df), 0),
            os.path.abspath(output_path),
        )
    else:
        if os.path.exists(output_path) and not force:
            raise FileExistsError(
                f"Output file '{output_path}' already exists. Use --force to overwrite."
            )
        normalised_df.to_csv(output_path, index=False)
        logging.info("Saved %s rows to %s", len(normalised_df), os.path.abspath(output_path))


def _write_metadata(
    *,
    metadata_path: str,
    results: list[dict[str, object]],
    force: bool,
):
    output_dir = os.path.dirname(os.path.abspath(metadata_path))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(metadata_path) and not force:
        raise FileExistsError(
            f"Metadata file '{metadata_path}' already exists. Use --force to overwrite."
        )

    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, default=_json_default)
        handle.write("\n")

    logging.info("Wrote metadata summary to %s", os.path.abspath(metadata_path))


def _json_default(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


def _build_metadata(symbol: str, df) -> dict[str, object]:
    index = df.index
    return {
        "symbol": symbol,
        "rows": int(len(df)),
        "start": index[0].isoformat() if len(index) else None,
        "end": index[-1].isoformat() if len(index) else None,
        "interval": df.attrs.get("Interval"),
    }


def _ensure_output_target(path: str, *, split_output: bool):
    if split_output:
        if path.lower().endswith(".csv"):
            logging.warning(
                "Treating output '%s' as directory because --split-output is set.",
                path,
            )
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        elif not os.path.isdir(path):
            raise SystemExit(
                "When --split-output is used the --output argument must be a directory"
            )
    else:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)


def main(argv: Optional[Iterable[str]] = None) -> int:
    cfg = parse_args(argv)

    logging.basicConfig(
        level=logging.ERROR if cfg.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if cfg.end and cfg.start and cfg.end < cfg.start:
        raise SystemExit("--end date must be on or after --start date")

    _ensure_output_target(cfg.output, split_output=cfg.split_output)

    combined_frames = []
    metadata_entries: list[dict[str, object]] = []
    downloaded_anything = False

    for symbol in cfg.symbols:
        symbol_start = cfg.start
        symbol_end = cfg.end

        if cfg.incremental:
            last_saved: Optional[dt.date] = None
            if cfg.split_output:
                target_path = os.path.join(cfg.output, f"{symbol.replace(':', '_')}.csv")
            else:
                target_path = cfg.output

            if target_path and os.path.exists(target_path):
                try:
                    existing_df = pd.read_csv(target_path)
                except Exception as exc:
                    logging.warning(
                        "Unable to read existing data for %s (%s). Skipping incremental optimisation.",
                        symbol,
                        exc,
                    )
                else:
                    if not existing_df.empty and "date" in existing_df.columns:
                        candidate_df = existing_df
                        if not cfg.split_output and "symbol" in existing_df.columns:
                            candidate_df = candidate_df[candidate_df["symbol"].str.upper() == symbol.upper()]

                        if not candidate_df.empty:
                            last_ts = pd.to_datetime(
                                candidate_df["date"], utc=True, errors="coerce"
                            ).dropna().max()
                            if pd.notna(last_ts):
                                last_saved = last_ts.date()

            if last_saved:
                next_day = last_saved + dt.timedelta(days=1)
                if symbol_start is None or next_day > symbol_start:
                    symbol_start = next_day
                effective_end = symbol_end or dt.datetime.now(dt.timezone.utc).date()
                if next_day > effective_end:
                    logging.info("%s already up to date through %s", symbol, last_saved)
                    continue

        start_dt, end_dt = resolve_date_range(symbol_start, symbol_end)

        try:
            df = download_symbol(
                symbol,
                interval=cfg.interval,
                start=start_dt,
                end=end_dt,
                auto_adjust=cfg.auto_adjust,
                quiet=cfg.quiet,
            )
        except Exception as exc:
            logging.error("Failed to download %s: %s", symbol, exc)
            continue

        df.attrs["Interval"] = cfg.interval
        metadata_entries.append(_build_metadata(symbol, df))
        downloaded_anything = True

        if cfg.split_output:
            output_file = os.path.join(cfg.output, f"{symbol.replace(':', '_')}.csv")
            write_csv(
                df,
                output_path=output_file,
                include_symbol_column=False,
                force=cfg.force,
                incremental=cfg.incremental,
            )
        else:
            combined_frames.append(df)

    if not cfg.split_output and combined_frames:
        merged = pd.concat(combined_frames)
        merged.sort_index(inplace=True)
        write_csv(
            merged,
            output_path=cfg.output,
            include_symbol_column=len(cfg.symbols) > 1,
            force=cfg.force,
            incremental=cfg.incremental,
        )

    if cfg.metadata_path and metadata_entries:
        _write_metadata(
            metadata_path=cfg.metadata_path,
            results=metadata_entries,
            force=cfg.force or cfg.incremental,
        )

    if not metadata_entries:
        if cfg.incremental and not downloaded_anything:
            logging.info("All symbols already up to date; no new records downloaded.")
            return 0
        logging.error("No data downloaded. See logs for errors.")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
