"""
Functions needed to load parameters from params.yaml tracked with DVC
"""
import sys
import os

import yaml


def get_params(stage_fn: str = None):
    """
    Reads parameters for a given DVC stage from params.yaml.

    The stage name is inferred from the name of the python file that calls this
    function.
    Args:
        stage_fn (str): Name of the stage. If None, the name of the file
            that calls this function is used. Defaults to None.
    Returns:
        dict with parameters for the stage
    Raises:
        KeyError: if the stage name is not found in params.yaml
    """

    if stage_fn is None:
        stage_fn = os.path.basename(sys.argv[0]).replace(".py", "")

    try:
        params = yaml.safe_load(open("params.yaml"))[stage_fn]
    except KeyError as exc:
        print(f'ERROR: Key "{stage_fn}" not in parameters.yaml.')
        raise KeyError(f"Is the stage file name ({sys.argv[0]}) " +
                       "the same as the stage name in params.yaml?") from exc
    try:
        all_params = yaml.safe_load(open("params.yaml"))['all']
        params = {**params, **all_params}
    except KeyError:
        print(
            'WARNING: Key "all" not in parameters.yaml.' +
            'Only returning stage parameters.')

    return params

