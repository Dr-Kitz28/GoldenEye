"""Analyze weekday direction probabilities, streak distributions, and formulate a PDE.

This module loads the historical MASTEK dataset, computes the probability of the
stock moving up or down on each weekday, derives streak-length distributions, and
saves visualizations alongside a conceptual PDE description that couples time,
frequency, daily percentage change, and streak length.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Callable, Dict, Iterable, List

import matplotlib

matplotlib.use("Agg")  # Headless backend suitable for file output
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import norm
from matplotlib.ticker import FuncFormatter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_PATH = Path(r"d:\Trading Strategies\historical_data_downloader\MASTEK_complete_with_pct.csv")
OUTPUT_DIR = Path(r"d:\Trading Strategies\Cleaning Data")
WEEKDAY_NAMES = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
}

sns.set_theme(style="whitegrid")


# ---------------------------------------------------------------------------
# Data loading and preprocessing
# ---------------------------------------------------------------------------
def load_data(path: Path) -> pd.DataFrame:
    """Load the CSV file, parse dates, and retain trading weekdays."""
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "pct_change"]).copy()

    df["weekday"] = df["date"].dt.dayofweek
    df = df[df["weekday"].isin(WEEKDAY_NAMES)].copy()

    df["weekday_name"] = df["weekday"].map(WEEKDAY_NAMES)
    df["movement"] = np.where(
        df["pct_change"] > 0,
        "Up",
        np.where(df["pct_change"] < 0, "Down", "Flat"),
    )
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Goal 1: Up/Down probabilities by weekday
# ---------------------------------------------------------------------------
def compute_weekday_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """Return probability metrics of up/down/flat moves for each weekday."""
    records = []
    for weekday, name in WEEKDAY_NAMES.items():
        subset = df[df["weekday"] == weekday]
        total = len(subset)
        if total == 0:
            continue

        up_count = int((subset["pct_change"] > 0).sum())
        down_count = int((subset["pct_change"] < 0).sum())
        flat_count = total - up_count - down_count

        records.append(
            {
                "Weekday": name,
                "Total Sessions": total,
                "Up Count": up_count,
                "Down Count": down_count,
                "Flat Count": flat_count,
                "Up Probability": up_count / total,
                "Down Probability": down_count / total,
                "Flat Probability": flat_count / total,
            }
        )

    result = pd.DataFrame(records)
    result.sort_values("Weekday", key=lambda col: [list(WEEKDAY_NAMES.values()).index(v) for v in col], inplace=True)
    return result.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Goal 2: Streak-length distributions by weekday
# ---------------------------------------------------------------------------
def _streak_lengths(values: Iterable[float], predicate: Callable[[float], bool]) -> List[int]:
    """Return a list of streak lengths for values satisfying predicate."""
    streaks: List[int] = []
    current = 0
    for value in values:
        if np.isnan(value):
            if current:
                streaks.append(current)
                current = 0
            continue

        if predicate(value):
            current += 1
        else:
            if current:
                streaks.append(current)
                current = 0
    if current:
        streaks.append(current)
    return streaks


def compute_weekday_streaks(df: pd.DataFrame) -> Dict[str, Dict[str, List[int]]]:
    """Produce up/down streak lists for each weekday."""
    streaks: Dict[str, Dict[str, List[int]]] = {}
    for weekday, name in WEEKDAY_NAMES.items():
        subset = df[df["weekday"] == weekday].copy()
        subset.sort_values("date", inplace=True)
        returns = subset["pct_change"].to_numpy()

        up_streaks = _streak_lengths(returns, lambda x: x > 0)
        down_streaks = _streak_lengths(returns, lambda x: x < 0)
        streaks[name] = {"up": up_streaks, "down": down_streaks}
    return streaks


def streaks_to_dataframe(streaks: Dict[str, Dict[str, List[int]]]) -> pd.DataFrame:
    """Flatten streak information into a tidy DataFrame."""
    rows = []
    for weekday, directions in streaks.items():
        for direction, lengths in directions.items():
            if not lengths:
                rows.append(
                    {
                        "Weekday": weekday,
                        "Direction": direction.capitalize(),
                        "Streak Length": np.nan,
                        "Frequency": 0,
                        "Relative Frequency": np.nan,
                        "Total Runs": 0,
                    }
                )
                continue

            counter = Counter(lengths)
            total_runs = sum(counter.values())
            for length, frequency in sorted(counter.items()):
                rows.append(
                    {
                        "Weekday": weekday,
                        "Direction": direction.capitalize(),
                        "Streak Length": length,
                        "Frequency": frequency,
                        "Relative Frequency": frequency / total_runs,
                        "Total Runs": total_runs,
                    }
                )
    df = pd.DataFrame(rows)
    df.sort_values(["Weekday", "Direction", "Streak Length"], inplace=True, na_position="last")
    return df.reset_index(drop=True)


def _normal_overlay(ax: plt.Axes, lengths: List[int], color: str) -> None:
    """Overlay a normal PDF scaled to histogram counts."""
    unique_lengths = set(lengths)
    if len(unique_lengths) < 2:
        ax.text(
            0.5,
            0.85,
            "Insufficient variance\nfor normal fit",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.7),
        )
        return

    mu, sigma = norm.fit(lengths)
    if sigma <= 0:
        return

    x = np.linspace(min(lengths), max(lengths), 500)
    pdf = norm.pdf(x, mu, sigma)
    scaled_pdf = pdf * len(lengths) * 1.0  # bin width = 1
    ax.plot(x, scaled_pdf, color=color, linewidth=2.2, label=f"Normal fit μ={mu:.2f}, σ={sigma:.2f}")


def plot_streak_distributions(streaks: Dict[str, Dict[str, List[int]]], direction: str, output_path: Path) -> None:
    """Create histogram + normal overlay plots for a given direction."""
    direction = direction.lower()
    assert direction in {"up", "down"}

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()
    palette = sns.color_palette("husl", len(WEEKDAY_NAMES))

    for idx, (weekday_idx, weekday_name) in enumerate(WEEKDAY_NAMES.items()):
        ax = axes[idx]
        lengths = streaks[weekday_name][direction]

        if lengths:
            max_len = max(lengths)
            bins = np.arange(0.5, max_len + 1.5, 1)
            ax.hist(
                lengths,
                bins=bins,
                color=palette[idx],
                edgecolor="black",
                alpha=0.65,
            )
            ax.set_xticks(np.arange(1, max_len + 1, 1))
            _normal_overlay(ax, lengths, color=palette[idx])
            ax.legend(loc="upper right", fontsize=9)
            ax.text(
                0.02,
                0.95,
                f"Runs: {len(lengths)}\nMax L: {max_len}",
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=9,
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.7),
            )
        else:
            ax.text(
                0.5,
                0.5,
                "No streaks recorded",
                ha="center",
                va="center",
                fontsize=11,
                transform=ax.transAxes,
            )

        ax.set_title(f"{weekday_name} ({direction.title()} streaks)", fontsize=12, fontweight="bold")
        ax.set_xlabel("Streak length (weeks)")
        ax.set_ylabel("Frequency")
        ax.grid(alpha=0.3)

    # Remove the unused subplot panel if present
    axes[-1].remove()

    fig.suptitle(
        f"MASTEK {direction.title()} Streak Length Distribution by Weekday",
        fontsize=18,
        fontweight="bold",
        y=0.98,
    )
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _normal_overlay_mirrored(
    ax: plt.Axes,
    lengths: List[int],
    color: str,
    sign: int,
    label_prefix: str,
) -> None:
    """Overlay a normal curve mirrored about the x-axis when sign=-1."""
    unique_lengths = set(lengths)
    if len(unique_lengths) < 2:
        return

    mu, sigma = norm.fit(lengths)
    if sigma <= 0:
        return

    x = np.linspace(1, max(lengths), 400)
    pdf = norm.pdf(x, mu, sigma) * len(lengths) * 1.0
    linestyle = "-" if sign > 0 else "--"
    ax.plot(
        x,
        sign * pdf,
        color=color,
        linewidth=2,
        linestyle=linestyle,
        label=f"{label_prefix} normal μ={mu:.2f}, σ={sigma:.2f}",
    )


def plot_mirrored_streak_distributions(streaks: Dict[str, Dict[str, List[int]]], output_path: Path) -> None:
    """Plot up streaks above the axis and down streaks mirrored below the axis."""

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()
    palette = sns.color_palette("husl", len(WEEKDAY_NAMES))

    for idx, (weekday_idx, weekday_name) in enumerate(WEEKDAY_NAMES.items()):
        ax = axes[idx]
        up_lengths = streaks[weekday_name]["up"]
        down_lengths = streaks[weekday_name]["down"]

        if not up_lengths and not down_lengths:
            ax.text(
                0.5,
                0.5,
                "No streak data",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title(weekday_name, fontsize=12, fontweight="bold")
            ax.set_xlabel("Streak length (weeks)")
            ax.set_ylabel("Frequency")
            ax.grid(alpha=0.3)
            continue

        max_len = 1
        if up_lengths:
            max_len = max(max_len, max(up_lengths))
        if down_lengths:
            max_len = max(max_len, max(down_lengths))

        centers = np.arange(1, max_len + 1)
        up_counter = Counter(up_lengths)
        down_counter = Counter(down_lengths)
        up_counts = [up_counter.get(length, 0) for length in centers]
        down_counts = [down_counter.get(length, 0) for length in centers]

        ax.bar(
            centers,
            up_counts,
            width=0.9,
            color=palette[idx],
            edgecolor="black",
            alpha=0.7,
            label="Up streaks",
        )
        ax.bar(
            centers,
            [-count for count in down_counts],
            width=0.9,
            color=palette[idx],
            edgecolor="black",
            alpha=0.4,
            label="Down streaks",
        )

        _normal_overlay_mirrored(ax, up_lengths, palette[idx], sign=1, label_prefix="Up")
        _normal_overlay_mirrored(ax, down_lengths, palette[idx], sign=-1, label_prefix="Down")

        ax.set_xticks(centers)
        ax.set_xlabel("Streak length (weeks)")
        ax.set_ylabel("Frequency")
        ax.set_title(f"{weekday_name} streaks", fontsize=12, fontweight="bold")
        ax.grid(alpha=0.3)
        ax.axhline(0, color="black", linewidth=1)

        max_height = max(up_counts + down_counts) if (up_counts or down_counts) else 1
        if max_height == 0:
            max_height = 1
        ax.set_ylim(-max_height * 1.25, max_height * 1.25)

        ax.yaxis.set_major_formatter(
            FuncFormatter(lambda val, _: f"{abs(val):.0f}" if abs(val) >= 1 else f"{abs(val):.1f}")
        )

        ax.text(
            0.02,
            0.95,
            f"Up runs: {len(up_lengths)}\nDown runs: {len(down_lengths)}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.7),
        )

        handles, labels = ax.get_legend_handles_labels()
        unique = dict(zip(labels, handles))
        ax.legend(unique.values(), unique.keys(), loc="upper right", fontsize=8)

    axes[-1].remove()

    fig.suptitle(
        "MASTEK Up vs Down Streak Lengths by Weekday",
        fontsize=18,
        fontweight="bold",
        y=0.98,
    )
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Goal 3: PDE formulation
# ---------------------------------------------------------------------------
PDE_TEXT = """# PDE Formulation for Weekday Momentum Dynamics

Let $\\Phi(t, f, p, L)$ denote the joint probability density of observing a streak
of length $L$ with frequency $f$ (counts per observation window) when the weekday
index is $t$ and the associated single-day percentage change is $p$. A parsimonious
continuity equation that couples these variables is:

$$
\\frac{\\partial \\Phi}{\\partial t}
+ \\frac{\\partial}{\\partial L}\\Big( (\\alpha_1 p - \\alpha_2 L)\\,\\Phi \\Big)
+ \\frac{\\partial}{\\partial p}\\Big( (\\beta_0 + \\beta_1 p)\\,\\Phi \\Big)
- D_f\\,\\frac{\\partial^2 \\Phi}{\\partial f^2}
= S(t, f, p, L).
$$

Where:

- $t$ indexes trading weekdays (0 for Monday through 4 for Friday) but can be
  treated as continuous for analytical convenience.
- $f$ is the observed frequency of a streak length within a rolling window.
- $p$ is the single-day percentage change for that weekday occurrence.
- $L$ is the integer streak length (consecutive weeks of the same direction).
- $\\alpha_1$ captures how stronger positive moves extend streaks; $\\alpha_2$ enforces
  mean reversion in streak length.
- $\\beta_0, \\beta_1$ describe drift in the daily return distribution.
- $D_f$ is a diffusion coefficient modelling dispersion of observed frequencies.
- $S(t, f, p, L)$ is an optional source term for exogenous shocks (earnings, events).

Boundary and initial conditions must be supplied based on empirical estimates,
for example: $\\Phi(0, f, p, L)$ fitted from historical data, reflecting the
observed Monday distribution at the start of the sample.
"""


def write_pde_formulation(path: Path) -> None:
    """Persist the PDE description to a markdown file."""
    path.write_text(PDE_TEXT, encoding="utf-8")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Weekday Direction & Streak Analysis")
    print("=" * 60)

    df = load_data(DATA_PATH)
    print(f"Loaded {len(df):,} weekday observations spanning {df['date'].min().date()} to {df['date'].max().date()}")

    # Goal 1: Probabilities
    prob_df = compute_weekday_probabilities(df)
    prob_path = OUTPUT_DIR / "weekday_direction_probabilities.csv"
    prob_df.to_csv(prob_path, index=False)
    print("\nUp/Down/Flat probabilities by weekday:")
    print(prob_df[["Weekday", "Up Probability", "Down Probability", "Flat Probability"]].to_string(index=False, formatters={
        "Up Probability": "{:.4f}".format,
        "Down Probability": "{:.4f}".format,
        "Flat Probability": "{:.4f}".format,
    }))
    print(f"Saved probability table to {prob_path}")

    # Goal 2: Streak distributions
    streak_dict = compute_weekday_streaks(df)
    streak_df = streaks_to_dataframe(streak_dict)
    streak_path = OUTPUT_DIR / "weekday_streak_lengths.csv"
    streak_df.to_csv(streak_path, index=False)
    print("\nSample of streak-length distribution:")
    print(streak_df.head(10).to_string(index=False))
    print(f"Saved streak summary to {streak_path}")

    up_plot_path = OUTPUT_DIR / "weekday_up_streak_distribution.png"
    combined_plot_path = OUTPUT_DIR / "weekday_combined_streak_distribution.png"
    plot_streak_distributions(streak_dict, "up", up_plot_path)
    plot_mirrored_streak_distributions(streak_dict, combined_plot_path)
    print(f"Saved streak distribution plots to {up_plot_path} and {combined_plot_path}")

    # Goal 3: PDE formulation
    pde_path = OUTPUT_DIR / "weekday_pde_formulation.md"
    write_pde_formulation(pde_path)
    print(f"PDE formulation written to {pde_path}")

    print("\nAnalysis complete.")


if __name__ == "__main__":
    main()
