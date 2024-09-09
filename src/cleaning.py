"""
Stage for filtering out the data that is not needed for the analysis.
"""

import os
import datetime

from rich.console import Console
import polars as pl
from rich.progress import track

from utils.dvc.params import get_params


console = Console()
params = get_params("cleaning")

if not os.path.exists(params["output_path"]):
    os.makedirs(params["output_path"])

OUTPUT_PATH = params["output_path"]
INPUT_PATH = params["input_path"]
TIMEDELTA_THRESHOLD = params["timedelta_threshold"]

TIMEDELTA_THRESHOLD = datetime.timedelta(minutes=TIMEDELTA_THRESHOLD)

# List all files in the folder with the .parquet extension. Each file is
# a daily partition of the dataset.
PARQUET_FILES = [
    file for file in os.listdir(INPUT_PATH) if file.endswith(".parquet")
]
# Sort the files to ensure that the order is consistent.
PARQUET_FILES.sort()
n_files = len(PARQUET_FILES)

console.log(f"Found {len(PARQUET_FILES)} files in {INPUT_PATH}")
console.rule("Starting cleaning process...")

# List files that are already present in the output folder.
OUTPUT_FILES = [
    file for file in os.listdir(OUTPUT_PATH) if file.endswith(".parquet")
]

# Only clean files that are not already present in the output folder.
PARQUET_FILES = [file for file in PARQUET_FILES if file not in OUTPUT_FILES]

previously_cleaned = n_files - len(PARQUET_FILES)

if previously_cleaned > 0:
    console.log(
        f"Skipped {previously_cleaned} files that were already in the"
        + "output folder."
    )

for file in track(PARQUET_FILES, description="Cleaning files..."):
    # Use lazy loading to avoid loading the entire dataset into memory.
    daily_data = pl.scan_parquet(os.path.join(INPUT_PATH, file))

    # Create a column with the difference between the timestamp and the
    # timestamp at the previous row.
    daily_data = daily_data.with_columns(
        timestamp_diff=pl.col("timestamp") - pl.col("timestamp").shift(1)
    )

    # Filter out rows where the timestamp difference is greater than 10 minutes
    daily_data = daily_data.with_columns(
        pl.when(
            (pl.col("timestamp_diff") > TIMEDELTA_THRESHOLD)
            & (pl.col("flight_id") == pl.col("flight_id").shift(1))
        )
        .then(1)
        .otherwise(0)
        .alias("jump_indicator")
    )

    # Find the unique flight ids where the jump indicator is 1.
    jump_ids = daily_data.filter(pl.col("jump_indicator") == 1).select(
        "flight_id"
    ).collect()

    # Filter out the rows where the flight id is in the list of jump ids.
    daily_data = daily_data.filter(
        pl.col("flight_id").is_in(jump_ids).not_()
    )

    daily_data = daily_data.drop(["timestamp_diff", "jump_indicator"])

    # Collect the data and write it to a new parquet file.
    daily_data.collect().write_parquet(os.path.join(OUTPUT_PATH, file))

console.rule(f"Cleaned data saved to {OUTPUT_PATH}")
