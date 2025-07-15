#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
"""Helper code to create a conda package from .py files and additional metadata"""

import hashlib
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
import time
from typing import Dict, Iterable, List, Tuple

import models_api.models_api_models as models_api

# Subset of pep440 for validation. Disallows v prefix, epochs, local versions, and dashes (conda does not allow them)
_SIMPLIFIED_PEP_440 = re.compile(
    (
        "^"
        "(?:[0-9]+(?:\\.[0-9]+)*)"
        "(?:[_\\.]?(?:a|b|c|rc|alpha|beta|pre|preview)[_\\.]?(?:[0-9]+)?)?"
        "(?:[_\\.]?(?:post|rev|r)[_\\.]?(?:[0-9]+)?)?"
        "(?:[_\\.]?dev[_\\.]?(?:[0-9]+)?)?"
        "$"
    ),
    re.IGNORECASE,
)

_CONDA_PACKAGE_NAME = re.compile(r"^[a-z_][a-z\d_-]*$")


def create_conda_noarch_package(
    name: str,
    base_version: str,
    depends: List[str],
    constrains: List[str],
    source_directory: str,
    output_directory: str,
) -> Tuple[models_api.CondaPackage, str]:
    """
    Creates a conda noarch package. source_directory should contain .py files to be copied into the packages'
    site-packages folder. Those should be directories containing plain .py files, or containing python modules.

    e.g., if you want to package a module my_module, you should pass its parent directory as a source_directory; if you
    want to package a python file my_module.py, you should pass its directory as a source_directory.

    Depends and constrains should be specified as conda match specs, such as "numpy >=1.0.0,<2.0.0" or "numpy==1.2.*".

    Version will be "{base_version}+{hash}" where hash is computed over the site-packages files and the timestamp.

    Build string is set to py_0.

    The method returns the resulting package information and path.

    For more information about the conda package format and noarch packages, see
    - https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/pkg-specs.html
    - https://www.anaconda.com/blog/condas-new-noarch-packages
    """
    assert is_package_base_version_valid(base_version), "Invalid base version"
    assert _CONDA_PACKAGE_NAME.match(name), "Invalid package name"

    package_files = _list_package_files([source_directory])

    for source in package_files.keys():
        assert os.path.isabs(source), "Not absolute path"
    for target in package_files.values():
        assert not os.path.isabs(target), "Target path is not relative"

    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(f"{temp_dir}/site-packages/")
        os.makedirs(f"{temp_dir}/info")

        # Copy the .py files to the module directory
        for source_path, target_relpath in package_files.items():
            target_path = os.path.join(temp_dir, "site-packages", target_relpath)
            target_dir = os.path.split(target_path)[0]
            os.makedirs(target_dir, exist_ok=True)
            shutil.copy(source_path, target_path)

        # info/files contains a list of all files in site-packages
        with open(f"{temp_dir}/info/files", "w") as files_file:
            files_file.writelines([f"site-packages/{target}\n" for target in package_files.values()])

        # build string is constant
        buildstring = "py_0"

        # use timestamp as buildstring
        timestamp = int(time.time())

        # append hash of all .py files + timestamp to base_version
        sha1_hash = _sha1_hash_files(package_files, timestamp)
        version = f"{base_version}+{sha1_hash[:7]}"

        # info/index.json contains conda package metadata
        index_json = {
            "name": name,
            "version": version,
            "build": buildstring,
            "build_number": 0,
            "depends": depends,
            "constrains": constrains,
            "platform": None,
            "subdir": "noarch",
            "noarch": "python",
            "timestamp": timestamp,
        }
        with open(f"{temp_dir}/info/index.json", "w") as index_file:
            json.dump(index_json, index_file, indent=2)

        # info/link.json indicates this is a pure python/noarch package
        link_json = {
            "noarch": {"type": "python"},
            "package_metadata_version": 1,
        }
        with open(f"{temp_dir}/info/link.json", "w") as link_file:
            json.dump(link_json, link_file, indent=2)

        # Create the .tar.bz2 archive
        package_filename = f"{name}-{version}-{buildstring}.tar.bz2"
        package_path = os.path.join(output_directory, package_filename)
        os.makedirs(output_directory, exist_ok=True)
        with tarfile.open(package_path, "w:bz2") as tar:
            tar.add(f"{temp_dir}/site-packages", arcname="site-packages")
            tar.add(f"{temp_dir}/info", arcname="info")

        package_metadata = models_api.CondaPackage(
            name=name,
            version=version,
            build_string=buildstring,
            platform="noarch",
            extension=".tar.bz2",
        )
        return package_metadata, package_path


def is_package_base_version_valid(base_version: str) -> bool:
    """
    Returns True iff base_version is a valid PEP 440 version string, False otherwise.
    Disallows v prefix, epochs, local versions, and dashes (conda does not allow them).
    """
    return _SIMPLIFIED_PEP_440.match(base_version) is not None


def _list_package_files(directories: Iterable[str]) -> Dict[str, str]:
    """
    Returns a mapping of (local .py file, target relative path) for all .py files in the given directories.
    Also includes plain text files such as .R files in the same directories.
    Recurses over directories and ignores hidden directories and files.
    """
    for directory in directories:
        assert os.path.isdir(directory), "Not a directory"
        assert os.path.isabs(directory), "Not absolute path"
    return {
        absolute_local_path: relative_package_path
        for directory in directories
        for absolute_local_path, relative_package_path in _non_hidden_package_files(directory)
    }


def _non_hidden_package_files(root_directory: str) -> Iterable[Tuple[str, str]]:
    for directory, _, filenames in os.walk(root_directory):
        if _is_hidden(directory):
            continue
        for filename in filenames:
            filepath = os.path.join(directory, filename)
            relative_filepath = os.path.relpath(filepath, start=root_directory)

            if _is_hidden(filename):
                continue
            if not _has_allowed_extension(filepath):
                continue
            if _is_larger_than(filepath, 10_000_000):
                logging.warning(f"Skipping packaging {relative_filepath} as part of adapter as it is larger than 10MB")
                continue

            yield (filepath, relative_filepath)


def _is_hidden(path: str):
    return os.path.split(path)[1].startswith(".")


def _has_allowed_extension(filename: str):
    return filename.endswith((".py", ".r", ".R"))


def _is_larger_than(filename: str, size: int):
    return os.path.getsize(filename) > size


def _sha1_hash_files(files: Dict[str, str], timestamp: int) -> str:
    sorted_files = sorted([(str(source), str(target)) for (source, target) in files.items()], key=lambda file: file[1])
    sha1 = hashlib.sha1()
    for source_path, target_relpath in sorted_files:
        sha1.update(target_relpath.encode("utf-8"))
        with open(source_path, "rb") as file:
            sha1.update(file.read())  # not optimized for large files
    sha1.update(str(timestamp).encode("utf-8"))
    return sha1.hexdigest()
