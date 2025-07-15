#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

# Logic reused from https://github.palantir.build/foundry/hawk/tree/9f5408b17bd36949826a0ae3b762d0f98ec62b4e/hawk/src/lockfile

import logging
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Tuple

import models_api.models_api_models as models_api
import yaml
from rattler import MatchSpec, PrefixRecord

from .._internal._runtime import RuntimeEnvironment
from ._consts import CONDA_PACKAGE_EXCLUDE

log = logging.getLogger("palantir_models")


@dataclass(frozen=True)
class HawkLockfile:
    conda_packages: List[models_api.CondaPackage]


@dataclass(frozen=True)
class CondaMetadataLine:
    hash: str
    platform: str
    extension: str
    channel: str


DISABLE_PRUNE_PACKAGES_ENV_VAR = "DISABLE_MODELS_PRUNE_PACKAGES"

_CONDA_SECTION_HEADER = "[conda]"
_CONDA_METADATA_SECTION_HEADER = "[conda-metadata]"
_PIP_SECTION_HEADER = "[pip]"
_PIP_INDICES_SECTION_HEADER = "[pip-indices]"
_VERSION_MARKER = "[conda lock version]"
_LOCKFILE_HEADERS = [
    _CONDA_SECTION_HEADER,
    _PIP_SECTION_HEADER,
    _CONDA_METADATA_SECTION_HEADER,
    _PIP_INDICES_SECTION_HEADER,
    _VERSION_MARKER,
]

# https://github.palantir.build/foundry/hawk/blob/9f5408b17bd36949826a0ae3b762d0f98ec62b4e/hawk/src/lockfile/conda_line.rs#L94
_CONDA_LINE_REGEX = re.compile(r"(.+?)=(.+?)=(.+?)\s@\s(.+)$")

# https://github.palantir.build/foundry/hawk/blob/9f5408b17bd36949826a0ae3b762d0f98ec62b4e/hawk/src/lockfile/hashed_conda_metadata.rs#L41
_CONDA_METADATA_LINE_REGEX = re.compile(r"(.+?) - (.+?) @ (.+?) @ (.+?)$")

# https://github.palantir.build/foundry/hawk/blob/9f5408b17bd36949826a0ae3b762d0f98ec62b4e/hawk/src/lockfile/pip_line.rs#L21
_PIP_LINE_REGEX = re.compile(r"(.+?)==(.+?)$")


def create_hawk_pack(conda_env_path: str, output_path: str) -> None:
    try:
        subprocess.run(
            ["hawk", "pack", "--prefix", conda_env_path, "-o", output_path], check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to package environment: {e.stdout}{e.stderr}")
        raise e


@dataclass(frozen=True)
class HawkEnvironment:
    conda_specs: List[models_api.PythonLibraryConstraint]  # this is like python=3.11 or tensorflow>2.5.0
    conda_locks: List[models_api.CondaPackage]
    pip_specs: List[models_api.PythonLibraryConstraint]
    pip_locks: List[models_api.ResolvedPipPackage]


def get_hawk_environment(runtime_env: RuntimeEnvironment, conda_env_path: str) -> HawkEnvironment:
    hawk_lockfile_lines, meta_yml = _read_hawk_files(runtime_env)
    split_lockfile = _split_lockfile(hawk_lockfile_lines, _LOCKFILE_HEADERS)

    conda_specs = _parse_conda_specs(meta_yml)
    pip_specs = _parse_pip_specs(meta_yml)

    conda_metadata, dependency_map = _parse_conda_metadata(
        split_lockfile.get(_CONDA_METADATA_SECTION_HEADER, []), conda_env_path
    )
    conda_packages = _parse_conda_packages(conda_metadata, split_lockfile.get(_CONDA_SECTION_HEADER, []))
    pip_packages = _parse_pip_packages(split_lockfile.get(_PIP_SECTION_HEADER, []))

    # raise python to be a spec - by default maestro does not provide a python version in meta.yaml
    # functionally does nothing other than ensuring in any re-solves we have the same version of python
    if next((spec for spec in conda_specs if spec.name == "python"), None) is None:
        python_metadata = next((pkg for pkg in conda_packages if pkg.name == "python"), None)
        if python_metadata is None:
            raise Exception("No python dependency defined in the environment")
        conda_specs.append(models_api.PythonLibraryConstraint(name="python", constraint="==" + python_metadata.version))

    conda_specs = [package for package in conda_specs if package.name not in CONDA_PACKAGE_EXCLUDE]

    return HawkEnvironment(
        conda_locks=conda_packages, pip_locks=pip_packages, conda_specs=conda_specs, pip_specs=pip_specs
    )


def _read_hawk_files(runtime_env: RuntimeEnvironment):
    meta_yml_path = runtime_env.get_path_to_maestro_meta_yml()
    hawk_lockfile_path = runtime_env.get_path_to_hawk_lockfile()
    if meta_yml_path is None or hawk_lockfile_path is None:
        raise Exception("cannot find meta.yaml and/or lock file")
    with open(hawk_lockfile_path, "r") as file:
        lockfile = file.readlines()

    with open(meta_yml_path, "r") as file:
        meta_yml = yaml.safe_load(file)

    return lockfile, meta_yml


def _parse_conda_metadata(
    conda_metadata_lines: List[str], conda_env_path: str
) -> Tuple[Dict[str, CondaMetadataLine], Dict[str, List[str]]]:
    dependency_map = {}
    try:
        for p in os.listdir(os.path.join(conda_env_path, "conda-meta")):
            if not p.endswith(".json"):
                continue
            path = os.path.join(conda_env_path, "conda-meta", p)
            record = PrefixRecord.from_path(path)  # type: ignore
            dependency_map[record.name.source] = [
                dependency_name.source for dep in record.depends if (dependency_name := MatchSpec(dep).name) is not None
            ]
    except Exception as e:
        log.warning(
            "Failed to build full conda dependency tree. This is not a failure, but the saved model environment cannot be pruned. Error: %s",
            e,
        )
        dependency_map = {}

    output = {}
    for line in conda_metadata_lines:
        match = _CONDA_METADATA_LINE_REGEX.match(line)
        if match:
            hash = match.group(1)
            platform = match.group(2)
            extension = match.group(3)
            channel = match.group(4)
            output[hash] = CondaMetadataLine(hash=hash, platform=platform, extension=extension, channel=channel)
    return output, dependency_map


def _parse_conda_packages(
    conda_metadata: Dict[str, CondaMetadataLine], conda_lines: List[str]
) -> List[models_api.CondaPackage]:
    output = []
    for line in conda_lines:
        match = _CONDA_LINE_REGEX.match(line)
        if match:
            name = match.group(1)
            version = match.group(2)
            build_string = match.group(3)
            hash = match.group(4)

            if hash not in conda_metadata:
                raise Exception(f"missing conda metadata for package {line}")

            metadata_entry = conda_metadata[hash]

            output.append(
                models_api.CondaPackage(
                    name=name,
                    version=version,
                    build_string=build_string,
                    extension=f".{metadata_entry.extension}",
                    platform=metadata_entry.platform,
                )
            )
    return output


def _parse_pip_packages(conda_lines: List[str]) -> List[models_api.ResolvedPipPackage]:
    output = []
    for line in conda_lines:
        match = _PIP_LINE_REGEX.match(line)
        if match:
            name = match.group(1)
            version = match.group(2)

            output.append(
                models_api.ResolvedPipPackage(
                    name=name,
                    version=version,
                )
            )
    return output


def _parse_conda_specs(meta_yml: Dict) -> List[models_api.PythonLibraryConstraint]:
    if "requirements" not in meta_yml:
        return []
    requirements = meta_yml["requirements"]

    if "run" not in requirements:
        return []

    run_reqs = requirements["run"]
    out = []
    for requirement in run_reqs:
        match_spec = MatchSpec(requirement)
        name = match_spec.name
        constraint = match_spec.version
        if name is not None:
            out.append(
                models_api.PythonLibraryConstraint(
                    name=name.source, constraint=constraint if constraint is not None else ""
                )
            )

    return out


def _parse_pip_specs(meta_yml: Dict) -> List[models_api.PythonLibraryConstraint]:
    if "requirements" not in meta_yml:
        return []
    requirements = meta_yml["requirements"]

    if "pip" not in requirements:
        return []

    pip_reqs = requirements["pip"]
    out = []
    for requirement in pip_reqs:
        try:
            req = _parse_pip_dependency(requirement)
            package_name = req["name"]
            constraints = req["constraints"]
            out.append(
                models_api.PythonLibraryConstraint(
                    name=package_name, constraint=constraints if constraints is not None else ""
                )
            )
        except Exception as e:
            raise Exception(
                f"Failed to parse pip dependency {requirement}. Please check the format in your meta.yaml file."
            ) from e

    return out


def _split_lockfile(lines: List[str], headers: List[str]):
    header_map: Dict[str, List[str]] = {}
    current_key = ""
    for line in lines:
        line = line.strip()
        if line in headers:
            current_key = line
            header_map[line] = []
        elif current_key in header_map:
            header_map[current_key].append(line)
    return header_map


# this is supposed to do pep 508 parsing
def _parse_pip_dependency(dependency_str):
    pattern = (
        r"^(?P<name>[a-zA-Z0-9_\-]+\s*(\[[a-zA-Z\-_,\s]+\])*)\s*(?P<constraints>(?:[><=]=?\s*[\d\w.\*-]+\s*,?\s*)*)$"
    )
    match = re.match(pattern, dependency_str)

    if not match:
        raise Exception(
            f"Failed to parse pip dependency {dependency_str}. Please check the format in your meta.yaml file."
        )

    package_name = match.group("name").strip()
    constraints = match.group("constraints").strip()

    return {"name": package_name, "constraints": constraints}
