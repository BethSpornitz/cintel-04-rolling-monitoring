"""
rolling_monitor_bethspornitz.py - Project script.

Author: Beth Spornitz
Date: 2026-03

Time-Series System Metrics Data

- Data is taken from a system that records operational metrics over time.
- Each row represents one observation at a specific timestamp.
- The CSV file includes these columns:
  - timestamp: when the observation occurred
  - requests: number of requests handled
  - errors: number of failed requests
  - total_latency_ms: total response time in milliseconds

Purpose

- Read time-series system metrics from a CSV file.
- Demonstrate rolling monitoring using a moving window.
- Compute rolling averages to smooth short-term variation.
- Save the resulting monitoring signals as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- How does system behavior change over time?
- Why might a rolling average reveal patterns that individual observations hide?
- How can smoothing short-term variation help us understand longer-term trends?

Paths (relative to repo root)

    INPUT FILE: data/system_metrics_timeseries_bethspornitz.csv
    OUTPUT FILE: artifacts/rolling_metrics_bethspornitz.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.rolling_monitor_bethspornitz_healthcare

"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "healthcare_analytics_patient_flow_data.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "rolling_healthcare_bethspornitz.csv"

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_FILE", DATA_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure artifacts directory exists
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ CSV DATA FILE INTO A POLARS DATAFRAME (TABLE)
    # ----------------------------------------------------
    df = pl.read_csv(DATA_FILE)

    df = df.with_columns(
        [
            (
                pl.col("Patient Admission Date").cast(pl.Utf8).str.strip_chars()
                + " "
                + pl.col("Patient Admission Time").cast(pl.Utf8).str.strip_chars()
            )
            .str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p", strict=False)
            .alias("timestamp"),
            pl.col("Patient Waittime")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
            .alias("Patient Waittime"),
            pl.col("Patient Satisfaction Score")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
            .alias("Patient Satisfaction Score"),
        ]
    )
    print(df.select(["timestamp", "Patient Waittime"]).head(10))
    print(df["timestamp"].null_count())
    LOG.info(f"Loaded {df.height} time-series records")

    # ----------------------------------------------------
    # STEP 2: SORT DATA BY TIME
    # ----------------------------------------------------
    # Time-series analysis requires observations to be ordered.
    df = df.sort("timestamp")

    LOG.info("Sorted records by timestamp")

    # ----------------------------------------------------
    # STEP 3: DEFINE ROLLING WINDOW RECIPES
    # ----------------------------------------------------
    WINDOW_SIZE: int = 50

    # ----------------------------------------------------
    # STEP 3.1: DEFINE ROLLING MEAN FOR PATIENT WAIT TIME
    # ----------------------------------------------------
    waittime_rolling_mean_recipe: pl.Expr = (
        pl.col("Patient Waittime")
        .rolling_mean(WINDOW_SIZE)
        .alias("waittime_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.2: DEFINE ROLLING MAX FOR PATIENT WAIT TIME
    # ----------------------------------------------------
    waittime_rolling_max_recipe: pl.Expr = (
        pl.col("Patient Waittime")
        .rolling_max(WINDOW_SIZE)
        .alias("waittime_rolling_max")
    )

    # ----------------------------------------------------
    # STEP 3.3: DEFINE ROLLING MEAN FOR SATISFACTION SCORE
    # ----------------------------------------------------
    satisfaction_rolling_mean_recipe: pl.Expr = (
        pl.col("Patient Satisfaction Score")
        .rolling_mean(WINDOW_SIZE)
        .alias("satisfaction_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.4: APPLY THE ROLLING RECIPES IN A NEW DATAFRAME
    # ----------------------------------------------------
    # with_columns() evaluates the recipes and adds the new columns
    df_with_rolling = df.with_columns(
        [
            waittime_rolling_mean_recipe,
            waittime_rolling_max_recipe,
            satisfaction_rolling_mean_recipe,
        ]
    )

    LOG.info("Computed rolling healthcare monitoring signals")

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    # Sort just to be safe
    df_plot = df_with_rolling.sort("timestamp")

    plt.figure(figsize=(10, 5))
    plt.plot(
        df_plot["timestamp"], df_plot["waittime_rolling_mean"], label="Rolling Mean"
    )
    plt.plot(df_plot["timestamp"], df_plot["waittime_rolling_max"], label="Rolling Max")

    plt.title("Patient Wait Time Trends")
    plt.xlabel("Time")
    plt.ylabel("Wait Time (minutes)")
    plt.legend()

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))

    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(ARTIFACTS_DIR / "waittime_trends.png")
    plt.show()
    plt.close()

    # ----------------------------------------------------
    # STEP 4: SAVE RESULTS AS AN ARTIFACT
    # ----------------------------------------------------
    df_with_rolling.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote rolling monitoring file: {OUTPUT_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
