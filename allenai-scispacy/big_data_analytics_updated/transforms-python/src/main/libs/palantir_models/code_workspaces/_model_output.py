#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Helpers for publishing model versions from code workspaces"""

import glob
import inspect
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Type, TypeVar, Union

import models_api.models_api_models as models_api
import models_api.models_sidecar_api as other_models_sidecar_api  # TODO(wsuppiger): fix import package
from conjure_python_client import ConjureDecoder, ConjureEncoder
from models_api import models_api_modelapi as conjure_api
from models_api import models_api_sidecar as models_sidecar_api
from palantir_layer_pack import MiceLayer

from palantir_models.code_workspaces._hawk import create_hawk_pack
from palantir_models.experiments import Experiment
from palantir_models.experiments._errors import ErrorHandlerType
from palantir_models.experiments._experiment import AbstractExperiment
from palantir_models.experiments._parameter import PARAMETER_VALUE
from palantir_models.experiments._utils import make_none_if_cant_publish

from .._internal._runtime import RuntimeEnvironment, _get_runtime_env
from ..models import ModelAdapter
from ..models._container_packaging import ModelContainerConsts
from ..models._state_accessors import TemporaryDirectoryBackedModelStateWriter
from ._adapter_export import DiscoveredAdapterReference, ExportedAdapterPackage, export_adapter_package
from ._model_input import ModelInput
from ._model_version_repr import _ModelVersionRepr
from ._publish_progress import ModelPublishProgressBar, Steps

_ConjureType = TypeVar("_ConjureType")


class ModelOutput:
    """A class for writing to Foundry Models from Code Workspaces."""

    __log = logging.getLogger("ModelOutput")
    __log.setLevel(logging.INFO)

    def __init__(self, model_alias: str):
        self.__runtime_env = _get_runtime_env()
        self.__model_alias = model_alias
        self.__model_rid = self.__runtime_env.get_imported_model_rid(model_alias)
        self.__model_sidecar_service = self.__runtime_env.get_sidecar_service(
            other_models_sidecar_api.FoundryDataSidecarModelsService
        )

    def create_experiment(
        self,
        name: str,
        error_handler_type: ErrorHandlerType = ErrorHandlerType.WARN,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]] = None,
    ) -> Experiment:
        """
        Creates an experiment for the given model that can be used to track metrics during model training.

        :param name: Unique name of the experiment.
        :param error_handler_type: Error handling mode.
        :param initial_parameters: Initial parameters to log to the experiment.
        :returns experiment: Experiment object to log parameters/metrics to.
        """

        return self.__runtime_env.create_experiment(
            model_rid=self.__model_rid,
            name=name,
            error_handler_type=error_handler_type,
            initial_parameters=initial_parameters,
        )

    def publish(
        self,
        model_adapter: ModelAdapter,
        *,
        experiment: Optional[AbstractExperiment] = None,
        notes: Optional[str] = None,
    ) -> _ModelVersionRepr:
        """
        Publish a Model Version to this Foundry Model resource.
        :param model_adapter: Model Adapter used to serialize and interact with the model
        :param notes: Optional version notes for this model version
        :returns: RID identifying the created Model Version
        """

        _validate_model_before_publishing(model_adapter, self.__model_alias)

        model_api = _unsafe_conjure_cast(
            model_adapter._api().to_service_api(),
            conjure_api.ModelApi,
        )

        user_working_dir = os.getenv("USER_WORKING_DIR", "/tmp/data")
        output_dir = tempfile.mkdtemp(prefix="model_version_publish_", dir=user_working_dir)
        progress_bar = ModelPublishProgressBar(include_zip=False)
        hawk_pack_dir: Optional[str] = None

        experiment = make_none_if_cant_publish(experiment)
        experiment_rid = experiment.close() if experiment is not None else None

        try:
            start_time = time.time()
            self.__log.info("Exporting model adapter package [%s]", _format_elapsed_time_since(start_time))
            progress_bar.step(Steps.EXPORTING_ADAPTER)

            adapter_export_or_reference = export_adapter_package(
                runtime_env=self.__runtime_env,
                adapter=model_adapter,
                conda_env_dir=sys.prefix,
                output_dir=output_dir,
            )

            model_adapter_locator = models_sidecar_api.ModelAdapterLocator(
                python=_unsafe_conjure_cast(
                    adapter_export_or_reference.adapter_python_locator, models_api.PythonLocator
                )
            )

            model_adapter_source = _get_adapter_source_from_export(self.__runtime_env, adapter_export_or_reference)

            docker_weights_layer_locator: models_sidecar_api.ModelDockerLayerLocator

            with TemporaryDirectoryBackedModelStateWriter() as state_writer:
                self.__log.info("Saving model state [%s]", _format_elapsed_time_since(start_time))
                progress_bar.step(Steps.SAVING_STATE)
                model_adapter.save(state_writer)

                with tempfile.TemporaryDirectory() as empty_tempdir:
                    shutil.make_archive(f"{output_dir}/model", "zip", empty_tempdir)
                empty_model_zip_path = f"{output_dir}/model.zip"

                progress_bar.step(Steps.CREATING_WEIGHTS_LAYER)
                docker_weights_layer_locator = self.create_docker_layer(
                    output_dir,
                    "weights",
                    state_writer.tmp_dir.name,
                    MiceLayer.from_dir,
                    MiceLayer.build_weights,
                    start_time,
                )

                progress_bar.step(Steps.CREATING_ENVIRONMENT_LAYER)
                if self.__runtime_env.use_hawk():
                    hawk_pack_dir = tempfile.mkdtemp()
                    tar_file_path = os.path.join(hawk_pack_dir, "env.tar")
                    create_hawk_pack(sys.prefix, tar_file_path)
                    docker_environment_layer_locator = self.create_docker_layer(
                        output_dir,
                        "environment",
                        tar_file_path,
                        MiceLayer.from_tar,
                        MiceLayer.build_conda,
                        start_time,
                    )
                else:
                    docker_environment_layer_locator = self.create_docker_layer(
                        output_dir, "environment", sys.prefix, MiceLayer.from_dir, MiceLayer.build_conda, start_time
                    )

                progress_bar.step(Steps.CREATING_ADAPTER_LAYER)
                docker_adapter_layer_locator = self.create_docker_layer(
                    output_dir, "adapter", os.getcwd(), MiceLayer.from_dir, MiceLayer.build_adapter, start_time
                )

                model_state_files_metadata = {}

                for path in glob.iglob(state_writer.tmp_dir.name + "/**", recursive=True):
                    if not os.path.isfile(path):
                        continue
                    relative_path = os.path.relpath(path, state_writer.tmp_dir.name)
                    model_state_files_metadata[relative_path] = os.path.getsize(path)

                progress_bar.step(Steps.PUBLISHING)
                self.__log.info("Publishing model version to Foundry [%s]", _format_elapsed_time_since(start_time))
                model_version_rid = self.__model_sidecar_service.publish_model_version(
                    auth_header=self.__runtime_env.get_sidecar_service_auth_header(),
                    model_rid=self.__model_rid,
                    request=models_sidecar_api.PublishModelVersionRequest(
                        absolute_path_to_model_weights_zip=empty_model_zip_path,
                        model_adapter_locator=model_adapter_locator,
                        model_adapter_source=model_adapter_source,
                        model_api=model_api,
                        model_docker_layer_locator=docker_weights_layer_locator,
                        model_docker_environment_locator=docker_environment_layer_locator,
                        model_docker_adapter_locator=docker_adapter_layer_locator,
                        version_notes=notes,
                        model_state_files_metadata=model_state_files_metadata,
                        experiment_rid=experiment_rid,
                    ),
                )
            progress_bar.step(Steps.PUBLISHED)
            return _ModelVersionRepr(self.__model_rid, model_version_rid)
        finally:
            progress_bar.close()
            shutil.rmtree(output_dir)
            if hawk_pack_dir is not None:
                shutil.rmtree(hawk_pack_dir)

    def create_docker_layer(self, output_dir, layer_type, layer_dir, from_method, build_method, start_time):
        self.__log.info(f"Creating model {layer_type} layer [%s]", _format_elapsed_time_since(start_time))

        layer_path = f"{output_dir}/{layer_type}.tar.gz"
        mice_layer = from_method(Path(layer_dir))
        built_layer = build_method(mice_layer, Path(layer_path))
        docker_layer_locator = models_sidecar_api.ModelDockerLayerLocator(
            digest=ModelContainerConsts.SHA_256_DIGEST_PREFIX + built_layer.compressed_hash,
            docker_name=ModelContainerConsts.DOCKER_WEIGHTS_NAME,
            layer_size=built_layer.compressed_size,
            path_to_layer=layer_path,
            tar_digest=ModelContainerConsts.SHA_256_DIGEST_PREFIX + built_layer.tar_hash,
        )

        return docker_layer_locator


def _get_adapter_source_from_export(
    runtime_env: RuntimeEnvironment,
    adapter_export_or_reference: Union[ExportedAdapterPackage, DiscoveredAdapterReference],
) -> models_sidecar_api.ModelAdapterSource:
    if isinstance(adapter_export_or_reference, DiscoveredAdapterReference):
        adapter_reference_source = _unsafe_conjure_cast(
            adapter_export_or_reference.adapter_reference, models_sidecar_api.ReferenceModelAdapterSource
        )
        return models_sidecar_api.ModelAdapterSource(reference=adapter_reference_source)

    # else this is an ExportedAdapterPackage

    package_info = adapter_export_or_reference.package_info
    environment = adapter_export_or_reference.environment

    adapter_local_source = models_sidecar_api.LocalModelAdapterSource(
        absolute_path_to_model_adapter_package=adapter_export_or_reference.package_filepath,
        solved_conda_environment=models_sidecar_api.SolvedCondaEnvironment(
            library=models_api.AdapterPythonLibrary(
                name=package_info.name,
                version=package_info.version,
                build_string=package_info.build_string,
                platform=package_info.platform,
                extension=package_info.extension,
                # TODO(gsilvasimoes): include _repo.py in package and use AdapterPythonLibrary for package_info
                repository_rid=runtime_env.get_repo_rid(),
            ),
            constraints=_unsafe_conjure_cast(environment.conda_specs, List[models_api.PythonLibraryConstraint]),
            packages=_unsafe_conjure_cast(
                environment.conda_locks,
                List[models_api.CondaPackage],
            ),
            pip_constraints=_unsafe_conjure_cast(
                environment.pip_specs,
                List[models_api.PythonLibraryConstraint],
            ),
            pip_packages=_unsafe_conjure_cast(
                environment.pip_locks,
                List[models_api.ResolvedPipPackage],
            ),
        ),
    )
    return models_sidecar_api.ModelAdapterSource(local=adapter_local_source)


def _unsafe_conjure_cast(source_object, target_type: Type[_ConjureType]) -> _ConjureType:
    """
    Re-serializes a conjure file to the target type.
    Drops fields in source object that are not defined in the target type.
    Will throw in case the objects are incompatible (e.g. a list is converted to a scalar).
    Will throw in case the target type requires fields that are not present in the source object.
    """
    try:
        return ConjureDecoder.do_decode(ConjureEncoder.do_encode(source_object), target_type)
    except Exception as exception:
        raise RuntimeError(f"Unable to cast object of type {type(source_object)} to {target_type}") from exception


def _format_elapsed_time_since(start_time: float) -> str:
    mins, secs = divmod(int(time.time() - start_time), 60)
    return f"{mins:02d}:{secs:02d}"


def _validate_model_before_publishing(model_adapter: ModelAdapter, alias: str):
    """
    Run simple validations that we can check in user code before publishing
    """
    try:
        model_adapter._api().validate_api_io_or_throw()
    except Exception as exception:
        raise ValueError("Model adapter api definition is invalid") from exception

    for arg_name, arg_value in model_adapter._get_auto_serializable_args().items():
        if isinstance(arg_value, ModelOutput) or isinstance(arg_value, ModelInput):
            raise ValueError(
                f"ModelOutput/ModelInput is not a valid model argument: {arg_name}. Please verify the inputs to your adapter.\n"
                f'If you are attempting to load an existing model into the adapter, use ModelInput("{alias}").initialize_adapter({model_adapter.__class__.__name__})'
            )

        for value_class in _recursive_iter_bases(type(arg_value)):
            if value_class.__module__.startswith("foundry_sdk_runtime"):
                raise ValueError(
                    f"Objects from the Foundry SDK or an Ontology SDK are not serializable: {arg_name}.\n"
                    "Please instantiate such objects in the predict method of your model adapter."
                )

    # inspect should work, but don't want to throw if it doesn't for whatever reason
    predict_source: str = ""
    adapter_file_source: str = ""
    try:
        predict_source = inspect.getsource(model_adapter.predict)

        adapter_filename = inspect.getfile(model_adapter.__class__)
        with open(adapter_filename, "r") as adapter_file:
            adapter_file_source = adapter_file.read()
    except (OSError, TypeError):
        # inspect will throw an OSError or TypeError if it cannot find the source
        pass

    if "raise NotImplementedError" in predict_source:
        raise ValueError(
            "Model adapter predict method throws a NotImplementedError. Please implement the predict method."
        )

    if predict_source.count("FoundryClient()") != adapter_file_source.count("FoundryClient()"):
        raise ValueError(
            "FoundryClient() must be instantiated in the predict method. Please move the instantiation there in your adapter class."
        )


def _recursive_iter_bases(cls: Type) -> Iterator[Type]:
    visited: Set[Type] = set()

    def _iter_bases(c: Type) -> Iterator[Type]:
        if c not in visited:
            visited.add(c)
            yield c
            for base in c.__bases__:
                yield from _iter_bases(base)

    yield cls
    yield from _iter_bases(cls)
