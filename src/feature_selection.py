"""
Step to select the best features for the model using SHAP values using the
CatBoost library.

"""

import os
import json

from rich.console import Console
import pandas as pd

from utils.dvc.params import get_params
from utils.models.feature_selection import (
    catboost_based_feature_selection_regression,
)

console = Console()
params = get_params("feature_selection")

OUTPUT_PATH = params["output_path"]
INPUT_PATH = params["input_path"]
CATEGORICAL_FEATURES = params["categorical_features"]
TARGET = params["target"]
RANDOM_STATE = params["random_state"]
FS_VALIDATION_FRAC = params["fs_validation_frac"]
DTYPES = params["dtypes"]

if not os.path.exists(os.path.dirname(OUTPUT_PATH)):
    os.makedirs(os.path.dirname(OUTPUT_PATH))

training_data = pd.read_csv(INPUT_PATH, dtype=DTYPES)

validation_data = training_data.sample(
    frac=FS_VALIDATION_FRAC, random_state=RANDOM_STATE
)
training_data = training_data.drop(validation_data.index)

x_train = training_data.drop(TARGET, axis=1)
y_train = training_data[TARGET]

x_val = validation_data.drop(TARGET, axis=1)
y_val = validation_data[TARGET]
console.rule("Feature selection started...")
feature_selection_results = catboost_based_feature_selection_regression(
    x_train,
    y_train,
    x_val,
    y_val,
    max_features_to_select=10,
    cat_features=CATEGORICAL_FEATURES,
)

console.rule("Feature selection completed.")
selected_columns_names = feature_selection_results["selected_columns_names"]

# Save the output dictionary to a json file.
with open(OUTPUT_PATH, "w") as f:
    json.dump(feature_selection_results, f)
