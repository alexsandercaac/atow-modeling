"""
Stage for filtering out the data that is not needed for the analysis.
"""

import os
import datetime
import glob

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
RESTART = params["restart"]

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
    if RESTART:
        console.log(
            "Restart flag is set to True. Cleaning all files in the output"
            + "folder."
        )
        files_to_rm = glob.glob(os.path.join(OUTPUT_PATH, "*.parquet"))
        for file in files_to_rm:
            os.remove(file)
    else:
        console.log(
            f"Skipped {previously_cleaned} files that were already in the"
            + "output folder."
        )

for curr_file in track(
        range(len(PARQUET_FILES)), description="Cleaning files..."):
    # Both the current file and the following day's file are loaded.
    # The tracks will be indexed by the date of departure. As one flight
    # can have a departure date that is different from the arrival date,
    # it is necessary to load the next day's data to ensure that the
    # tracks are complete.
    files_to_load = [PARQUET_FILES[curr_file], PARQUET_FILES[curr_file + 1]]
    # Use lazy loading to avoid loading the entire dataset into memory.
    files_paths = [os.path.join(INPUT_PATH, file) for file in files_to_load]
    daily_data = pl.scan_parquet(files_paths)
    # The currently analyzed date is the current file's name without the
    # .parquet extension.
    curr_date = PARQUET_FILES[curr_file].split(".")[0]
    curr_date = datetime.datetime.strptime(curr_date, "%Y-%m-%d").date()
    # Load the flight_id that are present in the current day's data.
    flight_ids = daily_data.filter(
        pl.col("timestamp").dt.date() == curr_date
    ).select("flight_id").collect()

    # Filter out the rows that are not in the list of flight ids.
    daily_data = daily_data.filter(pl.col("flight_id").is_in(flight_ids))

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
    daily_data.collect().write_parquet(
        os.path.join(OUTPUT_PATH, PARQUET_FILES[curr_file]))

console.rule(f"Cleaned data saved to {OUTPUT_PATH}")
