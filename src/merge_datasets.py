"""
Stage to merge aggregated tracks dataset with flight list information.
"""

import os

from rich.console import Console
import polars as pl
from rich.progress import track

from utils.dvc.params import get_params


console = Console()
params = get_params("merge_dataset")

OUTPUT_PATH = params["output_path"]
INPUT_PATH = params["input_path"]
TRACKS_PATH = INPUT_PATH["tracks"]
FLIGHT_LIST_PATH = INPUT_PATH["flight_list"]

if not os.path.exists(os.path.dirname(params["output_path"])):
    os.makedirs(os.path.dirname(params["output_path"]))

PARQUET_FILES = [
    file for file in os.listdir(TRACKS_PATH) if file.endswith(".parquet")]
PARQUET_FILES.sort()

n_files = len(PARQUET_FILES)

console.log(f"Found {len(PARQUET_FILES)} files in {TRACKS_PATH}")
console.rule("Starting dataset merging process...")

flight_list_data = pl.read_csv(FLIGHT_LIST_PATH)
merged_data = pl.DataFrame()

for file in track(PARQUET_FILES, description="Merging datasets..."):
    console.log(f"Processing file {file}...")
    tracks = pl.read_parquet(os.path.join(TRACKS_PATH, file))
    # Left join from tracks to flight list on flight_id
    merged = tracks.join(flight_list_data, on="flight_id", how="left")
    merged_data = pl.concat([merged_data, merged])

merged_data.write_csv(OUTPUT_PATH)
console.log(f"Dataset saved to {OUTPUT_PATH}")
