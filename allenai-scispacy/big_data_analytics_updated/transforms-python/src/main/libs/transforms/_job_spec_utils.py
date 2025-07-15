#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple, Union, cast

# Redefining here instead of importing from transforms.api._incremental_constants to avoid a circular import
BRANCH = "branch"
DATASET_LOCATOR = "datasetLocator"
DATASET_RID = "datasetRid"

_DatasetRID = str
_Branch = str
_InputSpecsMap = Dict[Tuple[_DatasetRID, Optional[_Branch]], Any]
_OutputSpecsMap = Dict[str, Dict[str, Any]]
_DatasetParamsMap = Dict[str, List[Union[str, None]]]


def extract_input_specs(
    input_specs: List[Dict[str, Any]], dataset_param_branches: _DatasetParamsMap
) -> _InputSpecsMap:
    """Convert input specs from list<map<string, any>> to <(rid, branch), spec> dictionary"""
    input_specs_map: _InputSpecsMap = {}
    for spec in input_specs:
        input_rid: str = spec[DATASET_LOCATOR][DATASET_RID]
        resolved_input_branch = spec.get(BRANCH)
        branches = dataset_param_branches.get(input_rid, None)
        if not branches:
            input_specs_map[(input_rid, None)] = spec
            # Back compatibility for jobspecs with no parameters.
            # These do not support multiple branches, let's overwrite as before, always None.
        elif resolved_input_branch in branches:  # branch specified
            input_specs_map[(input_rid, resolved_input_branch)] = spec
        else:  # no branch specified, use None
            input_specs_map[(input_rid, None)] = spec
    return input_specs_map


def extract_output_specs(output_specs: List[Dict[str, Any]]) -> _OutputSpecsMap:
    """Convert output specs from list<dict<string, any>> to <rid, spec> dictionary"""
    return {spec[DATASET_LOCATOR][DATASET_RID]: spec for spec in output_specs}


def get_dataset_param_branches(params: Union[Dict[str, Any], Any]) -> _DatasetParamsMap:
    """Convert params to dataset_rid -> [branch1, branch2, ...] dictionary"""
    dataset_param_branches: _DatasetParamsMap = {}
    for value in params.values():
        if not isinstance(value, Mapping):
            continue

        if DATASET_RID not in value:
            continue

        value = cast("Mapping[str, Any]", value)

        dataset_rid: str = value[DATASET_RID]
        dataset_branch: Optional[str] = value.get(BRANCH, None)
        if dataset_rid in dataset_param_branches:
            dataset_param_branches[dataset_rid].append(dataset_branch)
        else:
            dataset_param_branches[dataset_rid] = [dataset_branch]
    return dataset_param_branches
