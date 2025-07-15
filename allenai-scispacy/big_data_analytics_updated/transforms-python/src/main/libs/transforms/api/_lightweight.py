# Copyright 2023 Palantir Technologies, Inc.
# pylint: disable=too-many-lines
from __future__ import annotations

import inspect
import logging
import os
import re
import subprocess
import sys
import tempfile
from abc import ABC, ABCMeta, abstractmethod
from collections import Counter, defaultdict
from functools import lru_cache
from os.path import dirname
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union, overload

import yaml

from transforms._job_spec_utils import (
    extract_input_specs,
    extract_output_specs,
    get_dataset_param_branches,
)

from .._code_checks import (
    _execute_and_get_results_lightweight,
    _get_lightweight_unsupported_expectations,
    _get_transform_code_checks,
    abort_build_if_failed_expectations,
)
from .._utils import filter_instances
from ..foundry import _glob
from ._checks import Check
from ._dataset import Input, Output
from ._incremental import (
    AbstractNonCatalogIncrementalCompatibleInput,
    AbstractNonCatalogIncrementalCompatibleOutput,
    IncrementalSemanticVersion,
    _IncrementalCompute,
    _IncrementalComputeV2,
)
from ._incremental_semantic_version import SEMANTIC_VERSION_PROPERTY
from ._param import FoundryInputParam, FoundryOutputParam, Param, ParamContext
from ._transform import DataframeTransform, FileStatus, PandasTransform, Transform
from ._utils import _module_from_path, _raise_lib_python_import_error

BRANCH = "branch"
DATASET_RID = "datasetRid"
TABLE_INPUT_OUTPUT_TYPE = "table"
FOUNDRY_INPUT_OUTPUT_TYPE = "foundry"
MEDIA_INPUT_OUTPUT_TYPE = "media-set"
PYTHON_COMPUTE_FUNCTION_NAME_ENV_VAR = "PYTHON_COMPUTE_FUNCTION_NAME"
PATH_TO_WORKING_DIRECTORY_ENV_VAR = "USER_WORKING_DIR"
IS_CHILD_TRANSFORM_ENV_VAR = "__IS_CHILD_TRANSFORM"
MEMORY_LIMIT_IN_BYTES_ENV_VAR = "MEMORY_LIMIT_IN_BYTES"
MEMORY_OVERHEAD_RATIO = 0.1
JOB_CONFIG_FILE_PATH_ENV_VAR = "JOB_CONFIG_FILE"

GPU_TYPES = ["NVIDIA_T4", "NVIDIA_V100"]

log = logging.getLogger(__name__)


class ContainerTransformsConfiguration:
    _logger = logging.getLogger(__name__)
    _per_file_name_registry: "DefaultDict[str, Counter[str]]" = defaultdict(Counter)

    def __init__(
        self,
        transform: Transform,
        *,
        cpu_cores: Optional[float] = None,
        memory_mb: Optional[float] = None,
        memory_gb: Optional[float] = None,
        gpu_type: Optional[str] = None,
        container_image: Optional[str] = None,
        container_tag: Optional[str] = None,
        container_shell_command: Optional[str] = None,
    ):
        ContainerTransformsConfiguration._validate_transform(transform)
        self._transform = transform

        self._resources = ContainerTransformsConfiguration._serialise_resources(
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            memory_gb=memory_gb,
            gpu_type=gpu_type,
        )

        self._convert_to_pandas = isinstance(transform, PandasTransform)
        self.is_incremental = False

        if isinstance(transform.compute, (_IncrementalComputeV2, _IncrementalCompute)):
            self.is_incremental = True
            self.require_incremental = transform.compute._require_incremental
            self.snapshot_inputs = transform.compute._snapshot_inputs
            self.strict_append = transform.compute._strict_append
            self.allow_retention = transform.compute._allow_retention

        self._container_image = container_image
        self._container_tag = container_tag
        self._container_shell_command = container_shell_command
        self._validate_container_configuration()

        self._inputs = transform.inputs
        self._outputs = transform.outputs
        self._input_params = filter_instances(
            transform.parameters, LightweightInputParam
        )
        self._output_params = filter_instances(
            transform.parameters, LightweightOutputParam
        )
        self._parameters = transform.parameters
        self._use_context = transform._use_context
        self._user_code = transform._compute_func
        self._dataset_alias_to_param_name = {
            v.json_value["datasetRid"]: k
            for k, v in transform.parameters.items()
            if v._type
            in {
                FOUNDRY_INPUT_OUTPUT_TYPE,
                TABLE_INPUT_OUTPUT_TYPE,
                MEDIA_INPUT_OUTPUT_TYPE,
            }
        }

        self._file_name = ContainerTransformsConfiguration._get_user_code_file(
            transform._compute_func
        )

        self.__module__ = transform.__module__
        self.__doc__ = transform.__doc__
        self._user_code_file = ContainerTransformsConfiguration._get_user_code_file(
            self._user_code
        )
        self._code_checks = _get_transform_code_checks(transform)

        # this has to be the last line of the constructor as it may trigger a run
        self.__name__ = transform.__name__

    @staticmethod
    def _validate_transform(transform: Transform):
        try:
            from foundry import (
                transforms,
            )  # pylint: disable=import-outside-toplevel,import-error,unused-import
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)
        assert not isinstance(
            transform, DataframeTransform
        ), "@transform_df is not supported in Lightweight transforms"
        assert (
            not transform._profile
        ), "Profiles are not supported in Lightweight transforms"
        assert (
            not transform._run_as_user
        ), "Run as user is not supported in Lightweight transforms"
        assert (
            transform._allowed_run_duration is None
        ), "Run duration is not supported in Lightweight transforms"
        ContainerTransformsConfiguration._validate_inputs(transform.inputs)
        ContainerTransformsConfiguration._validate_outputs(transform.outputs)
        ContainerTransformsConfiguration._validate_params(transform.parameters)
        if isinstance(transform.compute, (_IncrementalComputeV2, _IncrementalCompute)):
            ContainerTransformsConfiguration._validate_incremental_parameters(transform)

    @staticmethod
    def _validate_inputs(inputs: Dict[str, Input]):
        for alias, input_param in inputs.items():
            if input_param.checks:
                unsupported_expectations = _get_lightweight_unsupported_expectations(
                    input_param.checks
                )
                assert not unsupported_expectations, (
                    f"Input checks contain expectations that are not yet supported"
                    f" for @lightweight transforms: {unsupported_expectations}"
                )
            assert (
                input_param.failure_strategy is None
            ), f"Input {alias} has failure_strategy set, this is not supported"

    @staticmethod
    def _validate_outputs(outputs: Dict[str, Output]):
        for alias, output_param in outputs.items():
            assert (
                output_param.sever_permissions is False
            ), f"Output {alias} has sever_permissions set, this is not supported"
            unsupported_expectations = _get_lightweight_unsupported_expectations(
                output_param.checks
            )
            assert not unsupported_expectations, (
                f"Output checks contain expectations that are not yet supported"
                f" for @lightweight transforms: {unsupported_expectations}"
            )

    @staticmethod
    def _validate_params(params: Dict[str, Param]):
        for param in params.values():
            if isinstance(param, FoundryInputParam) and param._type == "model":
                assert param.alias.startswith(
                    "ri.models.main.model."
                ), "Model parameters currently supported only if specified by rid in @lightweight transforms"

    @staticmethod
    def _validate_incremental_parameters(transform):
        if isinstance(transform.compute, _IncrementalCompute):
            assert (
                transform.compute._semantic_version == 1
            ), "Semantic version is not yet supported for @lightweight incremental transforms"

        for alias, parameter in transform.parameters.items():
            assert not isinstance(
                parameter, AbstractNonCatalogIncrementalCompatibleInput
            ) or (
                isinstance(parameter, IncrementalSemanticVersion)
                and parameter._properties.get(SEMANTIC_VERSION_PROPERTY) == str(1)
            ), (
                f"Type {type(parameter)} is not a supported input type for @lightweight incremental transforms"
                f" for input {alias}"
            )

            assert not isinstance(
                parameter, AbstractNonCatalogIncrementalCompatibleOutput
            ), (
                f"Type {type(parameter)} is not a supported output type for @lightweight incremental transforms"
                f" for output {alias}"
            )

    def _validate_container_configuration(self):
        needs_container_runtime = (
            self._container_image is not None
            or self._container_tag is not None
            or self._container_shell_command is not None
        )

        if needs_container_runtime:
            assert (
                self._container_image is not None
            ), "Container image must be specified"
            assert (
                self._container_tag is not None
            ), "Container image tag must be specified"

    @staticmethod
    def _serialise_resources(
        cpu_cores: Optional[float],
        memory_mb: Optional[float],
        memory_gb: Optional[float],
        gpu_type: Optional[str],
    ) -> Dict[str, Any]:
        resources = {}

        if cpu_cores is not None:
            assert cpu_cores > 0, "CPU cores must be positive"
            resources["cpu"] = f"{cpu_cores * 1000:.0f}m"

        if memory_mb is not None:
            assert memory_mb > 0, "Memory must be positive"
            assert memory_gb is None, "Cannot specify both memory_mb and memory_gb"
            resources["memory"] = f"{memory_mb}M"

        if memory_gb is not None:
            assert memory_gb > 0, "Memory must be positive"
            assert memory_mb is None, "Cannot specify both memory_mb and memory_gb"
            resources["memory"] = f"{round(memory_gb * 1000)}M"

        if gpu_type is not None:
            assert gpu_type in GPU_TYPES, f"GPU type must be one of {GPU_TYPES}"
            resources["gpu"] = {"type": gpu_type}

        return resources

    @classmethod
    def _create_unique_name(cls, file_name: str, proposed_name: str) -> str:
        unique_id = cls._per_file_name_registry[file_name][proposed_name]
        cls._per_file_name_registry[file_name][proposed_name] += 1
        return f"{proposed_name}_{unique_id}"

    @property
    def __name__(self) -> str:
        return self.__name

    @__name__.setter
    def __name__(self, value: str) -> None:
        """Hook into the __name__ setter to ensure the set name is unique and optionally trigger a run.

        At checks-time, the JobSpec is generated after all top-level code of the file has been executed,
        including all __name__ assignments. So the name of the transform in the JobSpec, and thus,
        contained within the PYTHON_COMPUTE_FUNCTION_NAME environment variable will be the last value
        assigned to __name__ in the file (or the function's original name).

        At the same time, a Lightweight transform must only get executed if its containing file is
        being run directly (thus it's the __main__ module) and either the PYTHON_COMPUTE_FUNCTION_NAME
        is unset or its value is equal to the __name__ of the transform. This logic ensures that
        transforms are only executed at runtime, not checks-time, and that only a single selected
        transform is executed even if a file contains multiple transforms.

        However, to decide whether the transform's name matches the PYTHON_COMPUTE_FUNCTION_NAME, we
        have to know the transform's name. By default, the name is just the decorated function's name,
        and if the the user doesn't manually update the ContainerTransformsConfiguration's name, we'll
        decide to execute the transform at runtime when the object is instantiated (thus, at the time
        of the @lightweight decorator being evaluated). However, if the user manually updates the name,
        we'll only decide to execute the transform at runtime when the __name__ attribute is set to the
        value of the PYTHON_COMPUTE_FUNCTION_NAME.

        The above behaviour won't cause any transforms to be run multiple times because Lightweight
        transforms have their names deduplicated per file, so it's impossible for the user to give the
        same name to multiple transforms (or the same transform multiple times) in the same file.

        It could be argued that running a transform shouldn't be a side-effect of setting its name, but
        this is the only way to ensure that the transform's file can be run as a script and that the
        transform names are consistent between checks-time and runtime.
        """
        self.__name = ContainerTransformsConfiguration._create_unique_name(
            file_name=self._file_name, proposed_name=value
        )
        self._execute_if_eligible()

    def _execute_if_eligible(self):
        entrypoint_name = os.environ.get(PYTHON_COMPUTE_FUNCTION_NAME_ENV_VAR)
        is_function_the_entrypoint = (
            True if entrypoint_name is None else entrypoint_name == self.__name__
        )

        if self._transform.__module__ == "__main__" and is_function_the_entrypoint:
            self.compute()

    @property
    def default_container_entrypoint(self) -> str:
        # pylint: disable=line-too-long
        # must match https://github.palantir.build/foundry/transforms-worker/blob/e6f5df6eb2655bbb52142a219cc3ad5902e4a637/transforms-worker/src/main/java/com/palantir/transforms/worker/publishing/ContainerRuntimeEntryPointResolver.java#L31-L44
        activate_environment = f"source ${PATH_TO_WORKING_DIRECTORY_ENV_VAR}/python_environment/bin/activate"
        set_path = f"PYTHONPATH=${PATH_TO_WORKING_DIRECTORY_ENV_VAR}/user_code"
        set_function_name = f"{PYTHON_COMPUTE_FUNCTION_NAME_ENV_VAR}={self.__name__}"
        execute_code = (
            f"python -m {_module_from_path(self._path_to_user_code, module_root='src')}"
        )
        return (
            f"{activate_environment} && {set_path} {set_function_name} {execute_code}"
        )

    @property
    def _path_to_user_code(self) -> Path:
        return self._get_safe_relative_path(
            path=Path(self._user_code_file), relative_to=dirname(os.getcwd())
        )

    @property
    def transform_spec(self) -> Dict[str, Any]:
        _source, line_number = inspect.getsourcelines(self._user_code)

        needs_container_runtime = self._container_image is not None
        if not self._container_shell_command:
            self._container_shell_command = self.default_container_entrypoint

        # TODO(tedcj): refactor api so we don't require conversion functions
        dataset_inputs = [
            ContainerTransformsConfiguration._convert_input_dataset(alias, input_param)
            for alias, input_param in list(self._inputs.items())
            + list(self._input_params.items())
            if input_param._type == FOUNDRY_INPUT_OUTPUT_TYPE
        ]

        media_inputs = [
            ContainerTransformsConfiguration._convert_input_media(alias, input_param)
            for alias, input_param in self._input_params.items()
            if input_param._type == MEDIA_INPUT_OUTPUT_TYPE
        ]

        language_model_inputs = [
            ContainerTransformsConfiguration._convert_input_language_model(
                alias, input_param
            )
            for alias, input_param in self._parameters.items()
            if input_param._type == "language-model"
        ]

        model_inputs = [
            ContainerTransformsConfiguration._convert_model_input(alias, param)
            for alias, param in self._parameters.items()
            if param._type == "model"
        ]

        dataset_outputs = [
            ContainerTransformsConfiguration._convert_output_dataset(
                alias, output_param
            )
            for alias, output_param in self._outputs.items()
            if output_param._type in FOUNDRY_INPUT_OUTPUT_TYPE
        ]

        table_outputs = [
            ContainerTransformsConfiguration._convert_output_table(alias, param)
            for alias, param in self._output_params.items()
            if param._type == TABLE_INPUT_OUTPUT_TYPE
        ]

        media_outputs = [
            ContainerTransformsConfiguration._convert_output_media(alias, output_param)
            for alias, output_param in self._output_params.items()
            if output_param._type == MEDIA_INPUT_OUTPUT_TYPE
        ]

        spec = {
            "inputs": [
                *dataset_inputs,
                *media_inputs,
                *language_model_inputs,
                *model_inputs,
            ],
            "outputs": [*dataset_outputs, *table_outputs, *media_outputs],
            "runtime": (
                self._get_container_runtime_configuration()
                if needs_container_runtime
                else self._get_python_runtime_configuration(self._path_to_user_code)
            ),
            "resources": self._resources,
            "sourceProvenance": {
                "filePath": str(self._path_to_user_code),
                "lineNumber": line_number,
            },
            "transformParameters": {
                name: parameter.json_value
                for name, parameter in self._parameters.items()
                if parameter.json_value is not None
            },
        }

        (
            credential_inputs,
            egress_inputs,
            export_control_outputs,
            sources,
        ) = self._get_external_systems_settings()

        if credential_inputs or egress_inputs or export_control_outputs:
            spec["externalSystems"] = {
                "credentials": credential_inputs,
                "egressPolicies": egress_inputs,
                "exportControl": export_control_outputs,
            }

        if sources:
            spec["externalSystemsV2"] = {
                "magritteSources": sources,
            }

        if self.is_incremental:
            spec["incrementalConfiguration"] = {
                "requireIncremental": self.require_incremental,
                "strictAppend": self.strict_append,
                "snapshotInputs": (
                    [] if not self.snapshot_inputs else self.snapshot_inputs
                ),
                "semanticVersion": 1,
                "allowRetention": self.allow_retention,
            }

        if self._code_checks:
            spec["codeChecks"] = self._code_checks

        return spec

    def _get_external_systems_settings(self):
        credential_inputs = self._convert_credentials()
        egress_inputs = self._convert_egress_policies()
        export_control_outputs = self._convert_export_control_outputs()
        sources = self._convert_sources()

        return (credential_inputs, egress_inputs, export_control_outputs, sources)

    @staticmethod
    def _convert_input_dataset(alias: str, param: FoundryInputParam) -> Dict[str, Any]:
        result = {
            "alias": alias,
            "properties": {
                "type": "dataset",
                "dataset": {"rid": param.json_value["datasetRid"]},
            },
        }

        if "branch" in param.json_value:
            result["properties"]["dataset"]["branch"] = param.json_value["branch"]

        unmarkings = [
            {
                "markingIds": unmarking_def.marking_ids,
                "branches": unmarking_def.branches,
            }
            for unmarking_def in [param.stop_propagating, param.stop_requiring]
            if unmarking_def
        ]

        if unmarkings:
            result["properties"]["dataset"]["assumedMarkings"] = unmarkings

        return result

    @staticmethod
    def _convert_input_media(alias: str, param: FoundryOutputParam) -> Dict[str, Any]:
        if param._type != MEDIA_INPUT_OUTPUT_TYPE:
            raise ValueError(f"Input {alias} is not a media set")
        result = {
            "alias": alias,
            "properties": {
                "type": "mediaset",
                "mediaset": {
                    "rid": param.json_value["datasetRid"],
                },
            },
        }
        if param.json_value.get("branch"):
            result["properties"]["mediaset"]["branch"] = param.json_value["branch"]
        return result

    @staticmethod
    def _convert_output_dataset(alias: str, param: Output) -> Dict[str, Any]:
        return {
            "alias": alias,
            "properties": {
                "type": "dataset",
                "dataset": {
                    "rid": param.json_value["datasetRid"],
                },
            },
        }

    @staticmethod
    def _convert_output_table(alias: str, param: FoundryOutputParam) -> Dict[str, Any]:
        if param._type != TABLE_INPUT_OUTPUT_TYPE:
            raise ValueError(f"Output {alias} is not a table")
        result = {
            "alias": alias,
            "properties": {
                "type": "table",
                "table": {
                    "rid": param.json_value["datasetRid"],
                },
            },
        }
        if param.json_value.get("source"):
            result["properties"]["table"]["source"] = param.json_value["source"]
        if param.json_value.get("table"):
            result["properties"]["table"]["table"] = param.json_value["table"]
        return result

    @staticmethod
    def _convert_output_media(alias: str, param: FoundryOutputParam) -> Dict[str, Any]:
        if param._type != MEDIA_INPUT_OUTPUT_TYPE:
            raise ValueError(f"Output {alias} is not a media set")
        return {
            "alias": alias,
            "properties": {
                "type": "mediaset",
                "mediaset": {
                    "rid": param.json_value["datasetRid"],
                },
            },
        }

    @staticmethod
    def _convert_model_input(alias: str, param: FoundryInputParam) -> Dict[str, Any]:
        result = {
            "alias": alias,
            "properties": {
                "type": "model",
                "model": {"alias": param.json_value["alias"]},
            },
        }
        if param.json_value.get("modelVersion"):
            result["properties"]["model"]["version"] = param.json_value["modelVersion"]
        return result

    @staticmethod
    def _convert_input_language_model(
        alias: str, param: FoundryInputParam
    ) -> Dict[str, Any]:
        return {
            "alias": alias,
            "properties": {
                "type": "languageModel",
                "languageModel": {"rid": param.json_value["datasetRid"]},
            },
        }

    def _get_container_runtime_configuration(self) -> Dict[str, Any]:
        return {
            "type": "container",
            "container": {
                "image": {
                    "name": self._container_image,
                    "tagOrDigest": {"type": "tag", "tag": self._container_tag},
                },
                "entryPoint": {
                    "type": "shellCommand",
                    "shellCommand": self._container_shell_command,
                },
            },
        }

    def _get_python_runtime_configuration(
        self, relative_path_from_project_root: Path
    ) -> Dict[str, Any]:
        return {
            "type": "pythonScript",
            "pythonScript": {
                "identifier": "Python",
                "filePath": str(relative_path_from_project_root),
                "computeFunctionName": self.__name__,
            },
        }

    def _convert_credentials(self) -> Dict[str, str]:
        result = {}
        transform_parameters = self._transform_parameters()
        for alias, param in self._parameters.items():
            if param._type == "credential":
                result[alias] = transform_parameters.get(alias, param.json_value)[
                    "datasetRid"
                ]

        return result

    def _convert_egress_policies(self) -> Dict[str, str]:
        return {
            alias: param.json_value["datasetRid"]
            for alias, param in self._parameters.items()
            if param._type == "network-egress-policy"
        }

    def _convert_export_control_outputs(self) -> Optional[Dict[str, Any]]:
        export_control_outputs = [
            {
                "rid": param.json_value["datasetRid"],
                "alias": alias,
                "exportableMarkings": param.json_value["exportableMarkings"],
            }
            for alias, param in self._parameters.items()
            if param._type == "magritte-export-output"
        ]

        assert len(export_control_outputs) <= 1

        if export_control_outputs:
            return export_control_outputs[0]

        return None

    def _extract_model_inputs(self) -> Dict[str, Any]:
        parameters = self._parameters

        if not parameters or not any(
            args_param._type == "model" for args_param in parameters.values()
        ):
            return {}
        param_context = ContainerTransformsConfiguration._context()
        return {
            alias: ContainerTransformsConfiguration._model_instance(
                model_param=args_param, context=param_context
            )
            for alias, args_param in parameters.items()
            if args_param._type == "model"
        }

    @staticmethod
    def _model_instance(model_param, context):
        # TODO(lmartini): Currently only rid supported, enable path support too.
        json_value = {"modelRid": model_param.json_value.get("alias")}
        model_version = model_param.json_value.get("modelVersion")
        if model_version is not None:
            json_value["modelVersion"] = model_version

        return model_param.instance(context=context, json_value=json_value)

    def _convert_sources(self) -> Optional[Dict[str, Any]]:
        result = {}
        transform_parameters = self._transform_parameters()
        for alias, param in self._parameters.items():
            if param._type == "magritte-source":
                result[alias] = transform_parameters.get(alias, param.json_value)[
                    "datasetRid"
                ]

        return result

    @staticmethod
    def _get_safe_relative_path(path: Path, relative_to: str) -> Path:
        try:
            return path.relative_to(relative_to)
        except ValueError:
            return path

    def __call__(self) -> None:
        return self.compute()

    def compute(self) -> None:
        ContainerTransformsConfiguration._set_up_logging()
        if os.environ.get(IS_CHILD_TRANSFORM_ENV_VAR) is None:
            return_code = subprocess.call(
                [
                    sys.executable,
                    "-m",
                    inspect.getmodule(self._user_code).__spec__.name,
                ],
                stderr=subprocess.STDOUT,
                env={**os.environ, IS_CHILD_TRANSFORM_ENV_VAR: "true"},
                # pylint: disable=subprocess-popen-preexec-fn
                preexec_fn=ContainerTransformsConfiguration._set_memory_limit,
            )

            self._log_return_code(return_code)
            if return_code != 0:
                sys.exit(return_code)
        else:
            self._compute()

    @staticmethod
    def _set_memory_limit() -> None:
        # `resource` is not available on Windows but we'd like to allow local development with t-p
        # on Windows even if a build couldn't be run there.
        import resource  # pylint: disable=import-outside-toplevel

        memory_limit_in_bytes = os.environ.get(MEMORY_LIMIT_IN_BYTES_ENV_VAR)
        if memory_limit_in_bytes is None:
            return

        memory_limit_in_bytes = round(
            int(memory_limit_in_bytes) * (1 - MEMORY_OVERHEAD_RATIO)
        )
        resource.setrlimit(
            resource.RLIMIT_AS, (memory_limit_in_bytes, memory_limit_in_bytes)
        )

    def _log_return_code(self, return_code: int) -> None:
        transform_name = f"Your @lightweight transform ({self.__name__})"

        if return_code == 0:
            ContainerTransformsConfiguration._logger.info(
                f"{transform_name} has completed successfully."
            )
        elif return_code in {-11, 137, -6}:
            ContainerTransformsConfiguration._logger.error(
                f"{transform_name} has encountered and OutOfMemory condition and was killed as a result of this"
            )
        else:
            ContainerTransformsConfiguration._logger.error(
                f"{transform_name} has failed with return code {return_code}."
            )

    def _get_kwargs(self, inputs, outputs) -> Dict[str, Any]:
        argspec = inspect.getfullargspec(self._user_code)

        if self._convert_to_pandas:
            kwargs = {
                k: v.pandas() if hasattr(v, "pandas") else v
                for k, v in inputs.items()
                if k in argspec.args or argspec.varkw is not None
            }
        else:
            kwargs = {
                k: v
                for k, v in {**inputs, **outputs}.items()
                if k in argspec.args or argspec.varkw is not None
            }

        if self._use_context:
            kwargs["ctx"] = LightweightContext()

        return kwargs

    def _compute(self) -> None:
        # TODO(tedcj): migrate all of the below to use LightweightInputParam/LightweightOutputParam
        inputs, outputs = self.extract_inputs_and_outputs()

        input_datasets = {
            self._dataset_alias_to_param_name.get(dataset_alias): (
                LightweightInput(alias, dataset_alias, branch)
                if not self.is_incremental
                else IncrementalLightweightInput(alias, dataset_alias, branch)
            )
            for alias, dataset_alias, branch in (
                # datasetRid field in param is actually alias
                (
                    alias,
                    input_param.json_value["datasetRid"],
                    input_param.json_value.get("branch"),
                )
                for alias, input_param in self._inputs.items()
            )
        }
        output_datasets = {
            self._dataset_alias_to_param_name.get(dataset_alias): (
                LightweightOutput(alias, dataset_alias, branch)
                if not self.is_incremental
                else IncrementalLightweightOutput(alias, dataset_alias, branch)
            )
            for alias, dataset_alias, branch in (
                # datasetRid field in param is actually alias
                (
                    alias,
                    output_param.json_value["datasetRid"],
                    output_param.json_value.get("branch"),
                )
                for alias, output_param in self._outputs.items()
            )
        }

        inputs = {**input_datasets, **inputs}
        outputs = {**output_datasets, **outputs}

        for alias, rid in self._convert_credentials().items():
            try:
                from foundry.transforms import (
                    Credential,
                )  # pylint: disable=import-outside-toplevel,import-error
            except ModuleNotFoundError as exc:
                _raise_lib_python_import_error(exc)
            inputs[alias] = Credential(rid)
        for alias, rid in self._convert_egress_policies().items():
            inputs[alias] = None
        for alias, rid in self._convert_sources().items():
            inputs[alias] = self._parameters[alias].instance(
                ContainerTransformsConfiguration._context(),
                {"datasetRid": rid},
            )

        for alias, param in self._parameters.items():
            if param._type == "language-model":
                inputs[alias] = param.instance(
                    ContainerTransformsConfiguration._context(),
                    param.json_value,
                )
            elif param._type == "magritte-export-output":
                outputs[alias] = None
        for alias, model_input in self._extract_model_inputs().items():
            inputs[alias] = model_input

        kwargs = self._get_kwargs(inputs, outputs)

        inputs_to_checks = self.get_checks_from_params(inputs, self._inputs.items())
        if inputs_to_checks:
            self.execute_and_publish_code_checks(inputs_to_checks)

        if self._convert_to_pandas:
            output = list(outputs.values())[0]
            output.write_pandas(self._user_code(**kwargs))
        else:
            self._user_code(**kwargs)

        output_to_checks = self.get_checks_from_params(outputs, self._outputs.items())
        if output_to_checks:
            self.execute_and_publish_code_checks(output_to_checks)

    @lru_cache(maxsize=1)
    def _transform_parameters(self):
        job_config = ContainerTransformsConfiguration.get_job_config_yaml()
        if job_config is None:
            return {}

        return (
            job_config.get("job-spec", {})
            .get("computationParameters", {})
            .get("transforms", {})
            .get("transformParameters", {})
        )

    @staticmethod
    def get_job_config_yaml():
        config_file_path = os.getenv(JOB_CONFIG_FILE_PATH_ENV_VAR)
        if config_file_path is None:
            log.warning(
                f"{JOB_CONFIG_FILE_PATH_ENV_VAR} envvar is not set, unable to extract container transforms job config."
            )
            return None

        if not os.path.isfile(config_file_path):
            log.warning(f"The job config file does not exist path {config_file_path}")
            return None

        with open(config_file_path, "r") as file:
            try:
                config_data = yaml.safe_load(file)
            except yaml.YAMLError as e:
                raise ValueError(f"Error decoding the job config file as yaml {e}")

        return config_data

    def extract_inputs_and_outputs(self):
        def create_input_obj(input_param: LightweightInputParam):
            dataset_rid = input_param.json_value.get(DATASET_RID)
            param_name = self._dataset_alias_to_param_name.get(dataset_rid)
            return param_name, input_param.lightweight_instance(
                self._context(), input_param.json_value
            )

        def create_output_obj(output_param: LightweightOutputParam):
            dataset_rid = output_param.json_value.get(DATASET_RID)
            param_name = self._dataset_alias_to_param_name.get(dataset_rid)
            return param_name, output_param.lightweight_instance(
                self._context(), output_param.json_value
            )

        inputs = {
            param_name: input_obj
            for input_param in self._input_params.values()
            for param_name, input_obj in [create_input_obj(input_param)]
        }

        outputs = {
            param_name: output_obj
            for output_param in self._output_params.values()
            for param_name, output_obj in [create_output_obj(output_param)]
        }

        return inputs, outputs

    @staticmethod
    def get_checks_from_params(
        dataframes: Dict[str, Union[LightweightInput, LightweightOutput]],
        transform_items: Iterable[Tuple[str, Union["Input", "Output"]]],
    ) -> List[Tuple["pl.LazyFrame", List[Check]]]:
        return [
            (
                (
                    dataframes[name].polars(lazy=True)
                    if isinstance(dataframes[name], LightweightInput)
                    else dataframes[name].read_unstaged_dataset_as_polars_lazy()
                ),
                titem.checks,
            )
            for name, titem in transform_items
            if len(titem.checks) > 0
        ]

    def execute_and_publish_code_checks(
        self, dfs_to_checks: List[Tuple["pl.LazyFrame", List[Check]]]
    ):
        try:
            from foundry.transforms import (
                CodeChecks,
            )  # pylint: disable=import-outside-toplevel,import-error
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)

        all_check_results = []
        results_that_should_fail_build = []
        exceptions = []
        for dataframe, checks in dfs_to_checks:
            results, exception = _execute_and_get_results_lightweight(dataframe, checks)
            if exception is not None:
                exceptions.append(exception)
            for check_result, should_fail_build in results:
                all_check_results.append(check_result)
                if should_fail_build:
                    results_that_should_fail_build.append(check_result["name"])

        code_checks = CodeChecks()
        code_checks.publish_results(all_check_results)

        results_dict = {
            "check_results": all_check_results,
            "results_that_should_fail_build": results_that_should_fail_build,
            "exceptions": exceptions,
        }

        abort_build_if_failed_expectations(results_dict)

    @staticmethod
    def _set_up_logging():
        logging.basicConfig(
            level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s"
        )

    @staticmethod
    def _get_user_code_file(user_code: callable) -> str:
        # make it overridable in case the user code has a non-transforms decorator
        return getattr(user_code, "_original_file_path", inspect.getfile(user_code))

    @staticmethod
    @lru_cache(maxsize=1)
    def _context():
        return LightweightParamContextCreator().get_lightweight_param_context(
            branch=None
        )


@overload
def lightweight(user_code: Callable[..., Any]) -> ContainerTransformsConfiguration:
    ...


@overload
def lightweight(
    *,
    cpu_cores: Optional[float] = 2,
    memory_mb: Optional[float] = None,
    memory_gb: Optional[float] = None,
    gpu_type: Optional[str] = None,
    container_image: Optional[str] = None,
    container_tag: Optional[str] = None,
    container_shell_command: Optional[str] = None,
) -> Callable[[Callable[..., Any]], ContainerTransformsConfiguration]:
    ...


def lightweight(
    _maybe_function=None,
    *,
    cpu_cores=2,
    memory_mb=None,
    memory_gb=None,
    gpu_type=None,
    container_image=None,
    container_tag=None,
    container_shell_command=None,
):
    """Turn a transform into a Lightweight transform.

    In order to use this decorator, `foundry-transforms-lib-python` must be added as a dependency.

    A Lightweight transform is a transform that runs without Spark, on a single node. Lightweight transforms are faster
    and more cost-effective for small to medium-sized datasets. Lightweight transforms also provide more methods for
    accessing datasets; however, they only support a subset of the API of a regular transforms, including Pandas and
    the filesystem API. For more information, see the public documentation available at
    https://www.palantir.com/docs/foundry/transforms-python/lightweight-overview

    Args:
        cpu_cores (float, optional): The number of CPU cores to request for the transform's container,
            can be a fraction.
        memory_mb (float, optional): The amount of memory to request for the container, in MB.
        memory_gb (float, optional): The amount of memory to request for the container, in GB. The default is 16 GB.
        gpu_type (str, optional): The type of GPU to allocate for the transform.
        container_image (str, optional): The image to use for the transform's container.
        container_tag (str, optional): The image tag to use for the transform's container.
        container_shell_command (str, optional): The shell command to execute inside the container after it has started.
            When left unspecified, a default command is generated resulting in executing the decorated transform. The
            default values is available through the decorated transform's default_container_entrypoint property.

    Notes:
        Either `memory_gb` or `memory_mb` can be specified, but not both.

        In case any of `container_image`, `container_tag` or `container_shell_command` is set, both `container_image`
        and `container_tag` must be set. In case `container_shell_command` isn't set, a default entrypoint will be used
        which will bootstrap a Python environment and execute the user code specified in the transform.

        Specifying the container_* arguments is referred to as a bring-your-own-container (BYOC) workflow. In this case,
        the main guarantees are that all files from the user's Code repository will be available inside
        $USER_WORKING_DIR/user_code at runtime and that a Python environment will be available as well.

        The container_image must be available from an Artifacts backing repository of the Code repository. For more
        details, check out the public docs. An example of a valid container_image's Dockerfile is:

        ```dockerfile
        FROM ubuntu:latest

        RUN apt update && apt install -y coreutils curl sed

        RUN useradd --uid 5001 user
        USER 5001
        ```

    Examples:
        >>> @lightweight
        ... @transform(
        ...     my_input=Input('/input'),
        ...     my_output=Output('/output')
        ... )
        ... def compute_func(my_input, my_output):
        ...     my_output.write_pandas(my_input.pandas())

        >>> @lightweight()
        ... @transform(
        ...    my_input=Input('/input'),
        ...    my_output=Output('/output')
        ... )
        ... def compute_func(my_input, my_output):
        ...     for file in my_input.filesystem().ls():
        ...         with my_input.filesystem().open(file.path) as f1:
        ...             with my_output.filesystem().open(file.path, "w") as f2:
        ...                 f2.write(f1.read())

        >>> @lightweight(cpu_cores=8, memory_gb=3.5, gpu_type='NVIDIA_T4')
        ... @transform_pandas(
        ...     Output('/output'),
        ...     my_input=Input('/input')
        ... )
        ... def compute_func(my_input):
        ...     return my_input

        >>> @lightweight(container_image='my-image', container_tag='0.0.1')
        ... @transform(my_output=Output('ri...my_output'))
        ... def run_data_generator_executable(my_output):
        ...     os.system('$USER_WORKING_DIR/data_generator')
        ...     my_output.write_table(pd.read_csv('data.csv'))
        ```
    """

    def _lightweight(transform) -> ContainerTransformsConfiguration:
        nonlocal memory_gb

        if not isinstance(transform, Transform):
            raise TypeError(
                "lightweight decorator must be used on a Transform object. "
                "Perhaps you didn't put @lightweight as the top-most decorator?"
            )

        if memory_gb is None and memory_mb is None:
            memory_gb = 16  # allow the user to set memory_mb without that conflicting with the memory_gb default

        return ContainerTransformsConfiguration(
            transform,
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            memory_gb=memory_gb,
            gpu_type=gpu_type,
            container_image=container_image,
            container_tag=container_tag,
            container_shell_command=container_shell_command,
        )

    return _lightweight if _maybe_function is None else _lightweight(_maybe_function)


class EnforceInheritanceMeta(ABCMeta):
    def __new__(mcs, name, bases, dct, *args, **kwargs):
        new_class = super().__new__(mcs, name, bases, dct, *args, **kwargs)
        if not any(isinstance(base, EnforceInheritanceMeta) for base in bases):
            return new_class

        def check_inheritance(base_class, required_class, error_message):
            if any(issubclass(base, base_class) for base in bases):
                if not any(issubclass(base, required_class) for base in bases):
                    raise TypeError(error_message)

        check_inheritance(
            LightweightInputParam,
            FoundryInputParam,
            f"{name} must inherit from FoundryInputParam",
        )
        check_inheritance(
            LightweightOutputParam,
            FoundryOutputParam,
            f"{name} must inherit from FoundryOutputParam",
        )

        return new_class


class LightweightInputParam(ABC, metaclass=EnforceInheritanceMeta):
    @staticmethod
    @abstractmethod
    def lightweight_instance(context: "ParamContext", json_value: Dict[str, Any]):
        """Instantiate an Input type from the resolved JSON value."""


class LightweightOutputParam(ABC, metaclass=EnforceInheritanceMeta):
    @staticmethod
    @abstractmethod
    def lightweight_instance(context: "ParamContext", json_value: Dict[str, Any]):
        """Instantiate an Output type from the resolved JSON value."""


class FoundryDataSidecarFile:
    def __init__(
        self,
        param: "LightweightParam",
        path: str,
        logical_path: str,
        mode: str,
        **kwargs,
    ):
        self._param = param
        self._path = path
        self._logical_path = logical_path
        self._mode = mode
        self._kwargs = kwargs
        # pylint: disable=consider-using-with, unspecified-encoding
        self._open_file = open(self._path, self._mode, **self._kwargs)

    def read(self, *args, **kwargs):
        """Read from the file.

        Mimics the API of `io.FileIO`.
        """
        return self._open_file.read(*args, **kwargs)

    def write(self, *args, **kwargs):
        """Write to the file.

        Mimics the API of `io.FileIO`.
        """
        raise ValueError("Use with statement to write to a file")

    def __enter__(self):
        return self._open_file

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self._open_file.close()

        if "w" in self._mode:
            self._param.dataset.upload_file(self._path, self._logical_path)

        return False


class FoundryDataSidecarFileSystem:
    def __init__(self, param: "LightweightParam", read_only=False, read_mode="current"):
        self._param = param
        self._read_only = read_only
        self._read_mode = read_mode
        self._tmp_dir = (
            tempfile.TemporaryDirectory()
        )  # pylint: disable=consider-using-with

    def open(self, path, mode="r", **kwargs):
        """Open a FoundryFS file in the given mode.

        Should be used in a `with` statement, especially when writing to a file.

        Args:
            path (str): The logical path of the file in the dataset.
            mode (str): File opening mode, defaults to read.
            **kwargs: Remaining keyword args passed to :func:`io.open`.

        Returns:
            File: a Python file-like object attached to the stream.
        """

        if "w" in mode:
            if self._read_only:
                raise ValueError(
                    "Cannot open a file for writing on a read-only file system"
                )

            tmp_path = Path(self._tmp_dir.name) / path
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            tmp_path = (
                self._safe_list_files(show_hidden_files=True).get(path).download()
            )

        return FoundryDataSidecarFile(
            self._param, path=str(tmp_path), logical_path=path, mode=mode, **kwargs
        )

    def ls(self, glob=None, regex=".*", show_hidden=False):
        """Recurses through all directories and lists all files matching the given patterns,
         starting from the root directory of the dataset.

        Args:
            glob (str, optional): A unix file matching pattern. Also supports globstar.
            regex (str, optional): A regex pattern against which to match filenames.
            show_hidden (bool, optional): Include hidden files, those prefixed with '.' or '_'.

        Yields:
            :class:`~transforms.api.FileStatus`: The logical path, file size, modified timestamp
        """
        if glob:
            matcher = re.compile(_glob.glob2regex(glob))
        else:
            matcher = re.compile(regex)

        for file in self._safe_list_files(show_hidden_files=show_hidden):
            if FoundryDataSidecarFileSystem._is_hidden(file.path) and not show_hidden:
                continue
            if not matcher.match(file.path):
                continue

            yield FileStatus(file.path, file.size_bytes, file.updated_time)

    def _safe_list_files(self, show_hidden_files):
        # TODO(lmartini): remove this when everyone is on newer versions of foundry-transforms-lib-python
        list_files_method = self._param.dataset.files
        sig = inspect.signature(list_files_method)
        supports_show_hidden_files = "show_hidden_files" in sig.parameters

        if supports_show_hidden_files:
            return list_files_method(
                mode=self._read_mode, show_hidden_files=show_hidden_files
            )
        else:
            return list_files_method(mode=self._read_mode)

    @staticmethod
    def _is_hidden(logical_path) -> bool:
        base = os.path.basename(logical_path)
        return base.startswith(".") or base.startswith("_")


class LightweightParam:
    def __init__(self, alias: str, rid: str, branch: Optional[str] = None):
        try:
            from foundry.transforms import (
                Dataset,
            )  # pylint: disable=import-outside-toplevel,import-error
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)

        self.dataset = Dataset.get(alias)
        self._rid = rid
        self._branch = branch

    def path(self) -> str:
        """Download the dataset's underlying files and return a Path to them."""
        return self._read_table(format="path")

    def dataframe(self) -> "pd.DataFrame":
        """A Pandas DataFrame containing the full view of the dataset."""
        return self.pandas()

    def pandas(self) -> "pd.DataFrame":
        """A Pandas DataFrame containing the full view of the dataset."""
        return self._read_table(format="pandas")

    def arrow(self) -> "pyarrow.Table":
        """A PyArrow table containing the full view of the dataset."""
        return self._read_table(format="arrow")

    def polars(
        self, lazy: Optional[bool] = False
    ) -> Union["pl.DataFrame", "pl.LazyFrame"]:
        """A Polars DataFrame or LazyFrame containing the full view of the dataset.

        Args:
            lazy (bool, optional): Whether to return a LazyFrame or DataFrame. Defaults to False.
        """
        return (
            self._read_table(format="lazy-polars")
            if lazy
            else self._read_table(format="polars")
        )

    def _read_table(self, *args, **kwarg):
        return self.dataset.read_table(*args, **kwarg)

    @lru_cache(maxsize=1)
    def filesystem(self) -> FoundryDataSidecarFileSystem:
        """Access the filesystem.

        Construct a `FoundryDataSidecarFileSystem` object
        for accessing the Dataset's files directly.
        """
        return FoundryDataSidecarFileSystem(self)

    @property
    def branch(self) -> str:
        """The branch of the dataset this parameter is associated with."""
        # TODO(lmartini): Currently populated only if manually set -> need to infer for outputs
        return self._branch  # pylint: disable=no-member

    @property
    def rid(self) -> str:
        """The unique Resource Identifier of the dataset this parameter is associated with."""
        # TODO(lmartini): Currently actually alias, so could show path if param is created with path
        return self._rid


class LightweightInput(LightweightParam):
    """The input object passed into Transform objects at runtime.

    Its aim is to mimic a subset of the API of `TransformInput` while
    providing access to the underlying `foundry.transforms.Dataset`.
    """

    def filesystem(self) -> FoundryDataSidecarFileSystem:
        """Access the filesystem in read-only mode.

        Construct a `FoundryDataSidecarFileSystem` object
        for accessing the Dataset's files directly.
        """
        return FoundryDataSidecarFileSystem(self, read_only=True)


class IncrementalLightweightInput(LightweightInput):
    def path(self, mode: Optional[str] = "added") -> str:
        """
        Download the dataset's underlying files and return a Path to them.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
                This argument is only applicable when @incremental is added and v2_semantics is True.
        """
        return self._read_table(format="path", mode=mode)

    def dataframe(self, mode: Optional[str] = "added") -> "pd.DataFrame":
        """
        A Pandas DataFrame containing the full view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
        """
        return self.pandas(mode=mode)

    def pandas(self, mode: Optional[str] = "added") -> "pd.DataFrame":
        """
        A Pandas DataFrame containing the full view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
        """
        return self._read_table(format="pandas", mode=mode)

    def arrow(self, mode: Optional[str] = "added") -> "pyarrow.Table":
        """
        A PyArrow table containing the full view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
        """
        return self._read_table(format="arrow", mode=mode)

    def polars(
        self, lazy: Optional[bool] = False, mode: Optional[str] = "added"
    ) -> Union["pl.DataFrame", "pl.LazyFrame"]:
        """
        A Polars DataFrame or LazyFrame containing the full view of the dataset.

        Args:
            lazy (bool, optional): Whether to return a LazyFrame or DataFrame. Defaults to False.
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
        """
        return (
            self._read_table(format="lazy-polars", mode=mode)
            if lazy
            else self._read_table(format="polars", mode=mode)
        )

    def filesystem(self, mode: Optional[str] = "added") -> FoundryDataSidecarFileSystem:
        """Access the filesystem in read-only mode.

        Construct a `FoundryDataSidecarFileSystem` object
        for accessing the Dataset's files directly.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `added`.
        """
        return FoundryDataSidecarFileSystem(self, read_only=True, read_mode=mode)

    def _read_table(self, *args, **kwarg):
        return self.dataset.read_table(*args, **kwarg)


class LightweightOutput(LightweightParam):
    """The output object passed to the user code at runtime.

    Its aim is to mimic a subset of the API of `TransformOutput` while
    providing access to the underlying `foundry.transforms.Dataset`.
    """

    def write_pandas(
        self, df: "pd.DataFrame", column_description: Optional[Dict[str, str]] = None
    ) -> None:
        """Write the given :class:`pandas.DataFrame` to the dataset."""
        self.write_table(df, column_description)

    def write_table(
        self, df, column_description: Optional[Dict[str, str]] = None
    ) -> None:
        """Write a Pandas DataFrame, Arrow Table, Polars DataFrame or LazyFrame, to a Foundry Dataset.

        Note:
            In case a path is specified, it must match the return value of ``path_for_write_table``.

        Args:
            df: pd.DataFrame, pa.Table, pl.DataFrame, pl.LazyFrame, or pathlib.Path with the data to upload
            column_description (Dict[str, str], optional): Map of column names to their string descriptions.
                This map is intersected with the columns of the DataFrame,
                and must include descriptions no longer than 800 characters.

        Returns:
            None
        """
        return self.dataset.write_table(df, column_description)

    @property
    def path_for_write_table(self):
        """Return the path for the dataset's files to be used with write_table"""
        return self.dataset.write_table_path

    def set_mode(self, mode: str) -> None:
        """Set the mode for the output dataset.

        Args:
            mode (str): The write mode, one of 'replace', 'modify' or 'append'.
                In modify mode anything written is appended to the dataset,
                this may also override existing files.
                In append mode, anything written is appended to the dataset and will not override existing files.
                In replace mode, anything written replaces the dataset.

                The write mode cannot be changed after data is written.
        """
        self.dataset.set_write_mode(mode)

    def read_unstaged_dataset_as_polars_lazy(self) -> "pl.LazyFrame":
        """Read the local version of the dataset as a Polars LazyFrame.

        This method is used when computing expectations on the dataset.
        It must happen before the dataset is commited since expectations
        can abort the build if failed.
        """

        return self.dataset.read_unstaged_dataframe("lazy-polars")


class IncrementalLightweightOutput(LightweightOutput):
    def path(self, mode: Optional[str] = "current") -> str:
        """
        Download the dataset's underlying files and return a Path to them.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `current`.
                This argument is only applicable when @incremental is added and v2_semantics is True.
        """
        return self._read_table(format="path", mode=mode)

    def dataframe(self, mode: Optional[str] = "current", schema=None) -> "pd.DataFrame":
        """
        A Pandas DataFrame containing the view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `current`.
            schema: The schema to read empty datasets with, only used if dataset is empty
        """
        return self.pandas(mode=mode, schema=schema)

    def pandas(self, mode: Optional[str] = "current", schema=None) -> "pd.DataFrame":
        """
        A Pandas DataFrame containing the view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `current`.
            schema: The schema to read empty datasets with, only used if dataset is empty
        """
        return self._read_table(format="pandas", mode=mode, schema=schema)

    def arrow(self, mode: Optional[str] = "current", schema=None) -> "pyarrow.Table":
        """
        A PyArrow table containing the view of the dataset.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `current`.
            schema: The schema to read empty datasets with, only used if dataset is empty
        """
        return self._read_table(format="arrow", mode=mode, schema=schema)

    def polars(
        self, lazy: Optional[bool] = False, mode: Optional[str] = "current", schema=None
    ) -> Union["pl.DataFrame", "pl.LazyFrame"]:
        """
        A Polars DataFrame or LazyFrame containing the view of the dataset.

        Args:
            lazy (bool, optional): Whether to return a LazyFrame or DataFrame. Defaults to False.
            mode (str, optional): The read mode, one of `current`, `previous`, `added`.  Defaults to `current`.
            schema: The schema to read empty datasets with, only used if dataset is empty
        """
        return (
            self._read_table(format="lazy-polars", mode=mode, schema=schema)
            if lazy
            else self._read_table(format="polars", mode=mode, schema=schema)
        )

    def _read_table(self, *args, **kwarg):
        kwarg["force_dataset_download"] = kwarg["mode"] != "previous"
        return self.dataset.read_table(*args, **kwarg)


class LightweightParamContextCreator:
    """A helper class to create a ParamContext object for a lightweight transform. Refer to the non-lightweight
    code paths for more information on how to use this class.
    """

    def __init__(self):
        try:
            from foundry.transforms import (
                JobSpecResource,
            )  # pylint: disable=import-outside-toplevel,import-error
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)
        self._jobspec_service = JobSpecResource()

    def job_rid(self) -> Optional[str]:
        """Extracts the job rid from the jobspec."""
        try:
            return self._jobspec_service.get_job_rid()
        except Exception as e:
            log.warning("Unable to extract job rid from jobspec.", e)
            return None

    def raw_params(self) -> Dict[str, Any]:
        """Extracts raw parameters from the jobspec."""
        return self._jobspec_service.get_params()

    def get_input_spec(self) -> List[Dict[str, Any]]:
        """Extracts input specs from the jobspec."""
        return self._jobspec_service.get_input_specs()

    def get_lightweight_param_context(
        self, branch: str, params: Union[Dict[str, Any], Any] = None
    ) -> ParamContext:
        """Creates and returns a ParamContext object for a lightweight transform.

        Args:
            branch (str): The current branch.
            params (Dict[str, Any], optional): The parameters to use, usually those would be the transformsParameters
                extracted from the jobspec. If none, those are inferred from the jobspec.
        """
        try:
            from foundry.transforms import (  # pylint: disable=import-outside-toplevel,import-error
                LightweightFoundryConnector,
            )
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)

        if params is None:
            params = self.raw_params()
        branch_params = get_dataset_param_branches(params)
        return ParamContext(
            foundry_connector=LightweightFoundryConnector(),
            input_specs=extract_input_specs(self.get_input_spec(), branch_params),
            output_specs=extract_output_specs(self._jobspec_service.get_output_specs()),
            branch=branch,
            job_rid=self.job_rid(),
        )


class LightweightContext(object):
    """Context object that can optionally be injected into the compute function of a lightweight transform."""

    def __init__(self):
        try:
            from foundry.transforms import (
                Context,
            )  # pylint: disable=import-outside-toplevel,import-error
        except ModuleNotFoundError as exc:
            _raise_lib_python_import_error(exc)

        self.context = Context()

    @property
    def auth_header(self) -> str:
        """The auth header used to run the transform."""
        if hasattr(self.context, "auth_header"):
            return self.context.auth_header

        return self.context._auth_header

    @property
    def is_incremental(self) -> bool:
        """Whether the transform is running incrementally."""
        return self.context.is_incremental
