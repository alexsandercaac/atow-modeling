"""
    Stage for filtering out the data that is not needed for the analysis.

    A set of parquet files, which are daily partitions of the dataset, are
    loaded. The folllowing filters of data quality are applied to the dataset:

    * Row where the ground speed is equal to or less than zero are removed.

    * Tracks that have a time difference between consecutive measurements
        greater than a threshold are removed. This threshold is defined by the
        TIMEDELTA_THRESHOLD parameter. Note that this is the last filter to be
        applied, as the previous filters can generate missing data that would
        be removed by this filter.

    Since the data is partitioned daily, the tracks can be split between two
    files. To ensure that the tracks are complete, the data from the following
    day is also loaded.

    The tracks that are saved in the output folder are indexed by the date of
    departure, and no tracks will be split between two files.
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
# Threshold that defines the maximum time difference between consecutive
# measurements. If the difference is greater than this threshold, the track
# is considered incomplete and will be removed.
TIMEDELTA_THRESHOLD = params["timedelta_threshold"]
# Flag that determines if the cleaning process should be restarted. If set to
# True, all files in the output folder will be removed before the cleaning
# process starts. If set to False, the cleaning process will skip files that
# are already present in the output folder.
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

OUTPUT_FILES = [
    file for file in os.listdir(OUTPUT_PATH) if file.endswith(".parquet")
]

if RESTART and len(OUTPUT_FILES) > 0:
    console.log(
        "Restart flag is set to True. Cleaning all files in the output"
        + "folder."
    )
    files_to_rm = glob.glob(os.path.join(OUTPUT_PATH, "*.parquet"))
    for file in files_to_rm:
        os.remove(file)
else:
    # Only clean files that are not already present in the output folder.
    PARQUET_FILES = [
        file for file in PARQUET_FILES if file not in OUTPUT_FILES]
    previously_cleaned = n_files - len(PARQUET_FILES)
    console.log(
        f"Skipped {previously_cleaned} files that were already in the"
        + "output folder."
    )

total_number_of_tracks = 0
number_of_tracks_kept = 0
for curr_file in track(
        range(len(PARQUET_FILES) - 1), description="Cleaning files..."):
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

    flight_ids = daily_data.filter(
        pl.col("timestamp").dt.date() == curr_date
    ).select("flight_id").collect().unique()
    total_number_of_tracks += len(flight_ids)
    console.log(
        f"Found {len(flight_ids)} tracks for {curr_date}."
    )

    daily_data = daily_data.filter(pl.col("flight_id").is_in(flight_ids))

    daily_data = daily_data.filter(pl.col("groundspeed") > 0)

    daily_data = daily_data.with_columns(
        timestamp_diff=pl.col("timestamp") - pl.col("timestamp").shift(1)
    )

    daily_data = daily_data.with_columns(
        pl.when(
            (pl.col("timestamp_diff") > TIMEDELTA_THRESHOLD)
            & (pl.col("flight_id") == pl.col("flight_id").shift(1))
        )
        .then(1)
        .otherwise(0)
        .alias("jump_indicator")
    )

    jump_ids = daily_data.filter(pl.col("jump_indicator") == 1).select(
        "flight_id"
    ).collect()

    daily_data = daily_data.filter(
        pl.col("flight_id").is_in(jump_ids).not_()
    )

    daily_data = daily_data.drop(["timestamp_diff", "jump_indicator"])

    daily_data = daily_data.collect()

    number_of_tracks_kept += daily_data.select(
        "flight_id").unique().shape[0]
    console.log(f"Keeping {number_of_tracks_kept} tracks so far.")

    daily_data.write_parquet(
        os.path.join(OUTPUT_PATH, PARQUET_FILES[curr_file]))

console.rule(f"Cleaned data saved to {OUTPUT_PATH}")
console.log(f"Total number of tracks processed: {total_number_of_tracks}")
console.log(f"Number of tracks kept: {number_of_tracks_kept}")
