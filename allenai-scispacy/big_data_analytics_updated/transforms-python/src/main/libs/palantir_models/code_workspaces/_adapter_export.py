#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import importlib
import importlib.util
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional, Union

import models_api.models_api_models as models_api

from .._internal._runtime import RuntimeEnvironment
from ..models._model_adapter import ModelAdapter
from ._conda_packager import create_conda_noarch_package, is_package_base_version_valid
from ._environment import CondaPipEnvironment, get_conda_pip_environment
from ._hawk import HawkEnvironment, get_hawk_environment

log = logging.getLogger("palantir_models")


@dataclass(frozen=True)
class ExportedAdapterPackage:
    """
    Information about the exported adapter package and the conda-pip environment for its dependencies.
    """

    package_info: models_api.CondaPackage
    package_filepath: str
    environment: Union[CondaPipEnvironment, HawkEnvironment]
    adapter_python_locator: models_api.PythonLocator


@dataclass(frozen=True)
class DiscoveredAdapterReference:
    """
    TODO(gsilvasimoes): add docstring and use this type when adapter reference exists
    """

    adapter_reference: models_api.ModelAdapterEnvironmentReference
    adapter_python_locator: models_api.PythonLocator


def export_adapter_package(
    runtime_env: RuntimeEnvironment,
    adapter: ModelAdapter,
    conda_env_dir: str,
    output_dir: str,
) -> Union[ExportedAdapterPackage, DiscoveredAdapterReference]:
    """
    Creates a conda package for the adapter code and returns necessary information to upload and persist this package.
    Throws if the adapter is defined interactively in __main__.
    """
    source_dir = os.getcwd()

    adapter_locator = _get_and_verify_adapter_locator(adapter, conda_env_dir)

    package_name = "cws_adapter"
    package_base_version = git_tag if is_package_base_version_valid(git_tag := _get_latest_git_tag()) else "0.0.0"

    environment: Union[CondaPipEnvironment, HawkEnvironment]
    if runtime_env.use_hawk():
        environment = get_hawk_environment(runtime_env, conda_env_dir)
    else:
        # fetch dependencies from conda and pip environment
        environment = get_conda_pip_environment(conda_env_dir)

    package_depends = [f"{spec.name} {spec.constraint}".strip() for spec in environment.conda_specs]

    # create conda package
    package_info, package_filepath = create_conda_noarch_package(
        name=package_name,
        base_version=package_base_version,
        depends=package_depends,
        constrains=[],
        source_directory=source_dir,
        output_directory=output_dir,
    )

    environment.conda_locks.append(
        models_api.CondaPackage(
            name=package_info.name,
            version=package_info.version,
            build_string=package_info.build_string,
            extension=package_info.extension,
            platform=package_info.platform,
        )
    )

    return ExportedAdapterPackage(package_info, str(package_filepath), environment, adapter_locator)


def _get_latest_git_tag() -> str:
    try:
        return subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except Exception:
        return ""


def _get_and_verify_adapter_locator(adapter: ModelAdapter, conda_env_dir: str) -> models_api.PythonLocator:
    assert isinstance(adapter, ModelAdapter), "Adapter must be an instance of ModelAdapter"
    adapter_class = adapter.__class__
    adapter_module = adapter_class.__module__

    # check if users have modified sys.path or cwd
    _check_no_path_modification(conda_env_dir)

    # check that autoreload has not failed and we are not packaging a broken .py file
    _assert_module_not_failed_to_autoreload(adapter_module)

    # assert adapter is defined in a .py file
    _assert_adapter_class_not_defined_interactively(adapter_class)

    # assert the adapter module is still importable
    _assert_module_is_importable(adapter_module)

    return models_api.PythonLocator(module=adapter_class.__module__, name=adapter_class.__name__)


def _assert_adapter_class_not_defined_interactively(adapter_class: type):
    assert adapter_class.__module__ != "__main__", (
        "Adapter class cannot be defined interactively.\n"
        "Please move your adapter code to a .py file in the same directory as the notebook and import from it.\n"
        "You may need to reinstantiate the adapter object if autoreload is disabled.\n"
    )


def _assert_module_is_importable(module_name: str):
    try:
        importlib.util.find_spec(module_name)
    except Exception as exception:
        raise AssertionError(
            f"Module {module_name} is not importable from the current directory. Unable to export adapter code as a package."
        ) from exception


def _assert_module_not_failed_to_autoreload(module_name: str):
    try:
        from IPython import get_ipython  # type: ignore
        from IPython.core.magic import MagicsManager  # type: ignore
        from IPython.extensions.autoreload import AutoreloadMagics, ModuleReloader  # type: ignore
    except ModuleNotFoundError:
        return

    module = sys.modules.get(module_name)
    if module is None:
        # could not find adapter module, skip check
        return

    interactive_shell = get_ipython()
    if interactive_shell is None:
        # may be None if outside ipython
        return

    magics_manager: MagicsManager = interactive_shell.magics_manager
    autoreload_magic: Optional[AutoreloadMagics] = magics_manager.registry.get(AutoreloadMagics.__name__)

    if autoreload_magic is None or not hasattr(autoreload_magic, "_reloader"):
        # guard against breaks on internal fields
        return

    module_reloader: ModuleReloader = autoreload_magic._reloader

    if not hasattr(module_reloader, "filename_and_mtime") or not hasattr(module_reloader, "failed"):
        # guard against breaks on internal fields
        return

    py_filename, pymtime = module_reloader.filename_and_mtime(module)
    if py_filename and pymtime and py_filename in module_reloader.failed:
        assert (
            module_reloader.failed.get(py_filename) != pymtime
        ), f"Adapter module {module_name} failed to reload, please fix any import errors before publishing."


def _check_no_path_modification(conda_env_dir: str):
    path_modification = []

    try:
        from IPython import get_ipython  # type: ignore

        interactive_shell = get_ipython()
        if interactive_shell is None:
            # may be None if outside ipython
            return

        history_manager = interactive_shell.history_manager  # type: ignore
        for block in history_manager.input_hist_parsed:  # type: ignore
            # check sys.path was not modified and os.chdir was not called
            for line in block.split("\n"):
                if re.search(r"sys\.path(?:\.append|\.extend|\.insert|\s*\+?=)", line):
                    path_modification.append(f"Modifying sys.path ({line})")
                if "os.chdir" in line:
                    path_modification.append(f"Modifying current working directory ({line})")
    except ModuleNotFoundError:
        pass

    # check that the path has not changed
    original_notebook_filepath = os.environ.get("JPY_SESSION_NAME", None)
    if original_notebook_filepath:
        original_notebook_dir = os.path.split(original_notebook_filepath)[0]
        if original_notebook_dir != os.getcwd():
            path_modification.append(
                f"Changing the current working directory since the notebook was started ({original_notebook_dir})"
            )

    # check that sys.path was not modified
    for entry in sys.path:
        if entry == "" or entry == os.getcwd():
            continue
        if os.path.commonpath([conda_env_dir, os.path.abspath(entry)]) == conda_env_dir:
            continue
        path_modification.append(f"Adding entries to sys.path ({entry})")

    if path_modification:
        log.warning(
            "Modifying the current working directory or the python path may cause the exported adapter to not be "
            "importable later. You may want to avoid:\n%s",
            "\n".join(" - " + line for line in path_modification),
        )
