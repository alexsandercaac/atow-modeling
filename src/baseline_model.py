"""
Train a linear regression model with all numerical features to serve as a
baseline model.

L1 regularization is used to select the best features for the model.

"""

import os
import pickle

from rich.console import Console
import pandas as pd
from sklearn.linear_model import Lasso

from utils.dvc.params import get_params


console = Console()
params = get_params("baseline_model")

RANDOM_STATE = params["random_state"]
OUTPUT_PATH = params["output_path"]
INPUT_PATH = params["input_path"]
CATEGORICAL_FEATURES = params["categorical_features"]
TARGET = params["target"]
DTYPES = params["dtypes"]
BASELINE_VALIDATION_FRAC = params["baseline_validation_frac"]
HPARAMS_GRID = params["hparams_grid"]

if not os.path.exists(os.path.dirname(OUTPUT_PATH)):
    os.makedirs(os.path.dirname(OUTPUT_PATH))

training_data = pd.read_csv(INPUT_PATH, dtype=DTYPES)
validation_data = training_data.sample(
    frac=BASELINE_VALIDATION_FRAC, random_state=RANDOM_STATE
)
training_data = training_data.drop(validation_data.index)

x_train = training_data.drop(TARGET, axis=1)
y_train = training_data[TARGET]

x_val = validation_data.drop(TARGET, axis=1)
y_val = validation_data[TARGET]

x_train = training_data.drop(TARGET, axis=1)
y_train = training_data[TARGET]

console.rule("Training baseline Lasso model...")

best_score = float("inf")
best_model = None
for alpha in HPARAMS_GRID["alpha"]:
    model = Lasso(alpha=alpha, random_state=RANDOM_STATE)
    model.fit(x_train, y_train)
    score = model.score(x_val, y_val)
    if score < best_score:
        best_score = score
        best_model = model

console.rule("Baseline Lasso model training completed.")
console.log(f"Best model score: {best_score}")

with open(OUTPUT_PATH, "wb") as f:
    pickle.dump(best_model, f)
