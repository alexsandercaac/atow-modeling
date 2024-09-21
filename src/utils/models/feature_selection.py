""" """

import pandas as pd
from catboost import (
    CatBoostRegressor,
    EFeaturesSelectionAlgorithm,
    EShapCalcType,
    Pool,
)
from rich.console import Console
from sklearn.metrics import mean_squared_error

console = Console()


def catboost_based_feature_selection_regression(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    x_val: pd.DataFrame,
    y_val: pd.Series,
    max_features_to_select: int,
    min_features_to_select: int = 1,
    cat_features: list = None,
    loss_function: str = "RMSE",
    verbose: bool = True,
) -> dict:
    """
    Perform feature selection using the CatBoost library.

    This function selects the optimal number of features using the Recursive
    Feature Selection (RFE) algorithm based on SHAP values. The optimal feature
    set is determined based on the minimum loss value.


    Args:
        x_train (pd.DataFrame): Training features.
        y_train (pd.Series): Training target.
        x_val (pd.DataFrame): Validation features.
        y_val (pd.Series): Validation target.
        max_features_to_select (int): Maximum number of features to select.
        min_features_to_select (int, optional): Minimum number of features to
          select. Defaults to 1.
        cat_features (list, optional): List of categorical features. Defaults
            to None.
        loss_function (str, optional): Loss function to optimize. Defaults to
            "RMSE".
        verbose (bool, optional): Whether to print verbose output. Defaults to
          False.

    Returns:
        dict: a dict with the following keys:
        - selected_columns_names: list of selected columns names
        - selected_columns_indices: list of selected columns indices
    """

    train_pool = Pool(x_train, y_train, cat_features=cat_features)
    val_pool = Pool(x_val, y_val, cat_features=cat_features)
    features_for_select = x_train.columns

    scores = {}
    selected_features_dict = {}
    if verbose:
        console.log(
            f"Selecting between {min_features_to_select} and "
            + f"{max_features_to_select} features..."
        )

    for n_feats in range(min_features_to_select, max_features_to_select):
        model = CatBoostRegressor(
            loss_function=loss_function, logging_level="Silent"
        )

        summary = model.select_features(
            train_pool,
            eval_set=val_pool,
            features_for_select=features_for_select,
            num_features_to_select=n_feats,
            algorithm=EFeaturesSelectionAlgorithm.RecursiveByShapValues,
            shap_calc_type=EShapCalcType.Regular,
            train_final_model=True
        )
        selected_features = summary["selected_features_names"]
        selected_cat = [
            col for col in selected_features if col in cat_features
        ]
        val_pool_selected = Pool(
            x_val[selected_features], y_val, cat_features=selected_cat
        )

        y_pred = model.predict(val_pool_selected)
        scores[n_feats] = mean_squared_error(y_val, y_pred, squared=False)
        selected_features_dict[n_feats] = selected_features
        if verbose:
            console.log(
                f"Selected {n_feats} feature(s) | "
                + f"RMSE: {scores[n_feats]:.2f}"
            )

    best_n_feats = min(scores, key=scores.get)
    best_feature_set = selected_features_dict[best_n_feats]

    if verbose:
        console.log(f"Best number of features: {best_n_feats}")
        console.log(f"Best feature set: {best_feature_set}")

    return_dict = {
        "selected_columns_names": best_feature_set,
        "selected_columns_indices": [
            x_train.columns.get_loc(col) for col in best_feature_set
        ],
    }

    return return_dict
