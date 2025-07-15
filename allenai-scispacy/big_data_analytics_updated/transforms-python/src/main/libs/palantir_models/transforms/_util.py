#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import os
import shutil
import tempfile
from functools import lru_cache
from typing import Dict, List, Union

import yaml
from transforms._shrinkwrap import Shrinkwrap
from transforms.api import FileSystem

FILE_PATH = "/foundry/user_code/transforms-shrinkwrap.yml"

MAPPINGS_KEY = "mappings"
RID_KEY = "rid"
ALIAS_KEY = "alias"
MODES_KEY = "modes"


def copy_model_to_driver(filesystem: FileSystem) -> str:
    """Copy a Foundry Dataset FileSystem to a local temporary directory and return its path"""
    tmp_dir = tempfile.mkdtemp()
    for file_status in filesystem.ls():
        with filesystem.open(file_status.path, "rb") as fsrc:
            with open(os.path.join(tmp_dir, file_status.path), "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
    return tmp_dir


@lru_cache(maxsize=None)
def get_shrinkwrap() -> Shrinkwrap:
    if not os.path.exists(FILE_PATH):
        return Shrinkwrap({})
    with open(FILE_PATH, "r") as content:
        data = yaml.safe_load(content)
    raw_shrinkwrap: Dict[str, List[Dict[str, Union[str, List[str]]]]] = {MAPPINGS_KEY: []}
    for mapping in data[MAPPINGS_KEY]:
        rid = mapping[RID_KEY]
        alias = mapping[ALIAS_KEY]
        modes = mapping[MODES_KEY]
        new_modes = []
        for mode in modes:
            if mode == "BUILD":
                new_modes.append(Shrinkwrap.BUILD_MODE)
            elif mode == "READ":
                new_modes.append(Shrinkwrap.READ_MODE)
            else:
                raise ValueError(f"Unknown mode: {mode}")
        raw_shrinkwrap[MAPPINGS_KEY].append({RID_KEY: rid, ALIAS_KEY: alias, MODES_KEY: new_modes})
    return Shrinkwrap(raw_shrinkwrap)


def rid_from_alias(alias: str) -> str:
    if alias.startswith("ri.models"):
        return alias
    else:
        return get_shrinkwrap().rid_for_alias(alias)
