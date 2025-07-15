#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import json
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

try:
    import libmambapy
except ImportError:
    libmambapy = None

import models_api.models_api_models as models_api

from ._consts import CONDA_PACKAGE_EXCLUDE

PIP_METADATA_FILENAME = re.compile(".*/site-packages/[^/]+.(?:egg-info|dist-info)/(?:METADATA|PKG-INFO)")
PIP_METADATA_NAME_ENTRY = re.compile(r"Name:\s?([^\s]+)")

ARTIFACTS_REPOSITORY_REGEX = re.compile(r"ri\.artifacts\.repository\.[^\/]*")
LIBMAMBAPY_NOT_FOUND_MESSAGE = (
    "Mamba is required to get conda repositories\n"
    "Managed conda environments are not yet supported by the CWS integration"
)
DISABLE_PRUNE_PACKAGES_ENV_VAR = "DISABLE_MODELS_PRUNE_PACKAGES"


@dataclass(frozen=True)
class CondaPipEnvironment:
    """
    Requested specs and resolved locks for conda and pip packages in the environment.
    """

    conda_env_dir: str
    conda_specs: List[models_api.PythonLibraryConstraint]  # this is like python=3.11 or tensorflow>2.5.0
    conda_locks: List[models_api.CondaPackage]
    pip_specs: List[models_api.PythonLibraryConstraint]
    pip_locks: List[models_api.ResolvedPipPackage]


def get_conda_pip_environment(conda_env_dir: str) -> CondaPipEnvironment:
    """
    Get the conda and pip packages from the current environment.
    Relies on metadata present in conda-meta/ and site-packages/*.{egg,dist}-info/.
    Uses libmambapy and pip for parsing.

    See the following for more information:
    - https://setuptools.pypa.io/en/latest/deprecated/python_eggs.html
    """
    conda_specs = _get_conda_specs(conda_env_dir)
    conda_locks = _get_conda_locks(conda_env_dir, conda_specs)
    pip_packages_provided_by_conda = set(_get_pip_packages_provided_by_conda(conda_env_dir))
    pip_specs = _get_pip_specs(conda_env_dir, pip_packages_provided_by_conda)
    pip_locks = _get_pip_locks(conda_env_dir, pip_packages_provided_by_conda)
    return CondaPipEnvironment(
        conda_env_dir=conda_env_dir,
        conda_specs=conda_specs,
        conda_locks=conda_locks,
        pip_specs=pip_specs,
        pip_locks=pip_locks,
    )


def _get_conda_specs(conda_env_dir: str) -> List[models_api.PythonLibraryConstraint]:
    assert libmambapy, LIBMAMBAPY_NOT_FOUND_MESSAGE
    history = libmambapy.History(libmambapy.Path(conda_env_dir))
    return [
        models_api.PythonLibraryConstraint(name, spec.conda_build_form().lstrip(name).strip())
        for name, spec in history.get_requested_specs_map().items()
        if name not in CONDA_PACKAGE_EXCLUDE
    ]


def _get_conda_locks(
    conda_env_dir: str, conda_specs: List[models_api.PythonLibraryConstraint]
) -> List[models_api.CondaPackage]:
    assert libmambapy, LIBMAMBAPY_NOT_FOUND_MESSAGE

    prefix_data = libmambapy.PrefixData(libmambapy.Path(conda_env_dir))

    dependency_map: Dict[str, List[str]] = {}
    name_to_package_lock: Dict[str, models_api.CondaPackage] = {}
    for record in prefix_data.package_records.values():
        if record.fn and record.fn.endswith(".conda"):
            extension = ".conda"
        else:
            extension = ".tar.bz2"

        name_to_package_lock[record.name] = models_api.CondaPackage(
            name=record.name,
            version=record.version,
            build_string=record.build_string,
            extension=extension,
            platform=record.subdir,
        )
        dependency_map[record.name] = [_package_name_from_match_spec(dep) for dep in record.depends]

    return [package for package in name_to_package_lock.values() if package.name not in CONDA_PACKAGE_EXCLUDE]


def _package_name_from_match_spec(match_spec: str):
    assert libmambapy, LIBMAMBAPY_NOT_FOUND_MESSAGE
    return libmambapy.MatchSpec(match_spec).conda_build_form().split()[0]


def _get_pip_packages_provided_by_conda(conda_env_dir: str) -> List[str]:
    conda_metadata_dir = os.path.join(conda_env_dir, "conda-meta")

    conda_pip_metadata_files = []
    for filename in os.listdir(conda_metadata_dir):
        # this directory has a .json file for each package installed by conda/mamba
        #
        # format specification in:
        # - https://github.com/conda/conda/blob/24.1.2/conda/core/prefix_data.py#L84
        # - https://github.com/conda/conda/blob/24.1.2/conda/models/records.py#L477
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(conda_metadata_dir, filename), "r") as file:
            conda_package_metadata = json.load(file)

        # look for egg/dist info files pip will read in package files
        #
        # format specification in:
        # - https://packaging.python.org/en/latest/specifications/recording-installed-packages/
        # - https://packaging.python.org/en/latest/discussions/wheel-vs-egg/
        # - https://setuptools.pypa.io/en/latest/deprecated/python_eggs.html
        for package_filename in conda_package_metadata.get("files", []):
            if re.fullmatch(PIP_METADATA_FILENAME, package_filename):
                conda_pip_metadata_files.append(package_filename)

    pip_packages_provided_by_conda = []
    for filename in conda_pip_metadata_files:
        try:
            with open(os.path.join(conda_env_dir, filename), "r") as file:
                # format specification in:
                # - https://packaging.python.org/en/latest/specifications/core-metadata/
                # - https://peps.python.org/pep-0314/
                for line in file.readlines():
                    match = re.fullmatch(PIP_METADATA_NAME_ENTRY, line.strip())
                    if match:
                        pip_packages_provided_by_conda.append(match.group(1))
                        break
        except FileNotFoundError:
            # if conda metadata exists but pip metadata does not, ignore and continue, pip likely removed this package
            pass

    return pip_packages_provided_by_conda


def _get_pip_locks(conda_env_dir: str, packages_provided_by_conda: Set[str]) -> List[models_api.ResolvedPipPackage]:
    return [
        models_api.ResolvedPipPackage(name=name, version=version)
        for name, version in _get_pip_list(conda_env_dir, only_top_level=False)
        if name not in packages_provided_by_conda and name != "foundry-jupyter-extension"
    ]


def _get_pip_specs(
    conda_env_dir: str, packages_provided_by_conda: Set[str]
) -> List[models_api.PythonLibraryConstraint]:
    return [
        models_api.PythonLibraryConstraint(name=name, constraint=version)
        for name, version in _get_pip_list(conda_env_dir, only_top_level=True)
        if name not in packages_provided_by_conda and name != "foundry-jupyter-extension"
    ]


def _get_pip_list(conda_env_dir: str, only_top_level: bool) -> List[Tuple[str, str]]:
    result = subprocess.run(
        [
            os.path.join(conda_env_dir, "bin", "python"),
            "-m",
            "pip",
            "list",
            "--format=freeze",
            "--not-required" if only_top_level else "",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=True,
    )
    output = result.stdout.strip().split("\n")

    # returned format:
    # name==version
    return [tuple(line.split("==")) for line in output]  # type: ignore
