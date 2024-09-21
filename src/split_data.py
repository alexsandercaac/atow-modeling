"""
Split data into training and validation sets.
"""

import os

from rich.console import Console
import pandas as pd

from utils.dvc.params import get_params


console = Console()
params = get_params("split_data")

OUTPUT_PATH = params["output_path"]
INPUT_PATH = params["input_path"]
TRAIN_FRACTION = params["train_fraction"]
DTYPES = params["dtypes"]
RANDOM_STATE = params["random_state"]

if not os.path.exists(os.path.dirname(OUTPUT_PATH['train'])):
    os.makedirs(os.path.dirname(OUTPUT_PATH['train']))

console.rule("Splitting data into training and validation sets...")

data = pd.read_csv(
    INPUT_PATH,
    dtype=DTYPES
)

train_data = data.sample(frac=TRAIN_FRACTION, random_state=RANDOM_STATE)
val_data = data.drop(train_data.index)

train_data.to_csv(OUTPUT_PATH['train'], index=False)
val_data.to_csv(OUTPUT_PATH['val'], index=False)

console.log(f"Samples in training set: {len(train_data)}")
console.log(f"Samples in validation set: {len(val_data)}")

console.rule("Data split into training and validation sets.")
