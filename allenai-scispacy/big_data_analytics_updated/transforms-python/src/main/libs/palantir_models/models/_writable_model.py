#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines the WritableModel interface class which wraps create model version service api call"""

import abc
import glob
import importlib
import logging
import os
import shutil
import tempfile
from enum import Enum
from typing import Dict, List, Optional

import requests
from models_api import models_api_common as common
from models_api import models_api_executable as executable_api
from models_api import models_api_experiments as experiments_api
from models_api import models_api_models as models_api

from palantir_models.experiments._errors import ErrorHandlerType
from palantir_models.experiments._experiment import AbstractExperiment, Experiment
from palantir_models.experiments._parameter import PARAMETER_VALUE
from palantir_models.experiments._utils import make_none_if_cant_publish

from ._container_packaging import ModelContainerConsts, package_mice_weights_layer
from ._model_adapter import ModelAdapter
from ._state_accessors import TemporaryDirectoryBackedModelStateWriter
from .rpc import ArtifactsService

log = logging.getLogger(__name__)


class ModelVersionChangeType(Enum):
    """
    Compatible with service api's model version change type
    """

    MAJOR = 1
    MINOR = 2
    PATCH = 3


class WritableModel(abc.ABC):
    """
    Retrieves all information needed to create a new model version.
    """

    @abc.abstractmethod
    def publish(
        self,
        model_adapter: ModelAdapter,
        *,
        model_adapter_config: Optional[dict],
        notes: Optional[str],
        with_docker: bool,
    ) -> models_api.ModelVersionRid:
        """
        Creates model version from given model adapter.
        :param model_adapter: python model adapter
        :param model_adapter_config: custom config to the model adapter
        :param notes: Optional version notes specified by the user
        :param with_docker: does this model use docker
        :returns: created model version rid
        """


class ModelsBackedWritableModel(WritableModel):
    """
    Creates a model version by calling the Models service.
    """

    MAVEN_VERSION = "-1.-1.-1"

    def __init__(
        self,
        model_service: models_api.ModelService,
        experiment_service: experiments_api.ExperimentService,
        executable_format_service: executable_api.ModelExecutableFormatService,
        artifacts_service: ArtifactsService,
        auth_header: str,
        model_rid: common.ModelRid,
        model_version_rid: models_api.ModelVersionRid,
        staged_experiments: List[common.ExperimentRid],
    ):
        self.__model_service = model_service
        self.__experiment_service = experiment_service
        self.__executable_format_service = executable_format_service
        self.__artifacts_service = artifacts_service
        self.__auth_header = auth_header
        self.__model_rid = model_rid
        self.__model_version_rid = model_version_rid
        self.__staged_experiments = staged_experiments

    @property
    def model_rid(self):
        return self.__model_rid

    @property
    def model_version_rid(self):
        return self.__model_version_rid

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

        if len(self.__staged_experiments) == 0:
            raise Exception(
                "No staged experiments exist for this model. create_experiment may only be called once per job."
            )

        return Experiment.initialize_from_staged(
            name=name,
            staged_experiment_rid=self.__staged_experiments.pop(0),
            auth_header=self.__auth_header,
            error_handler_type=error_handler_type,
            experiment_service=self.__experiment_service,
            initial_parameters=initial_parameters,
        )

    def _upload_model_zip(self, maven_coordinate, model_zip):
        log.info("Uploading zipped model to artifacts repository")
        try:
            self.__artifacts_service.put_maven_artifact(
                auth_header=self.__auth_header,
                repository_rid=self.__model_version_rid,
                maven_coordinate=maven_coordinate,
                content=model_zip,
            )
        except requests.exceptions.ConnectionError as e:
            trace_id = (
                e.request.headers.get("X-Trace-ID") if hasattr(e, "request") and hasattr(e.request, "headers") else None
            )
            log.error(f"ConnectionError occurred. Trace ID: {trace_id}")
            raise e
        log.info("Completed uploading zipped model to artifacts repository")

    def _upload_model_weights_layer(self, layer_file, layer_metadata):
        log.info("Uploading model layer to artifacts repository")
        try:
            upload_uuid = self.__artifacts_service.start_blob_upload(
                auth_header=self.__auth_header,
                repository_rid=self.__model_version_rid,
                docker_name=ModelContainerConsts.DOCKER_WEIGHTS_NAME,
                blob_digest=layer_metadata.gzip_digest,
            )
            self.__artifacts_service.upload_blob_complete(
                auth_header=self.__auth_header,
                repository_rid=self.__model_version_rid,
                docker_name=ModelContainerConsts.DOCKER_WEIGHTS_NAME,
                upload_identifier=upload_uuid,
                blob_digest=layer_metadata.gzip_digest,
                size=layer_metadata.gzip_size,
                data=layer_file,
            )
        except requests.exceptions.ConnectionError as e:
            trace_id = (
                e.request.headers.get("X-Trace-ID") if hasattr(e, "request") and hasattr(e.request, "headers") else None
            )
            log.error(f"ConnectionError occurred. Trace ID: {trace_id}")
            raise e
        log.info("Completed uploading model layer to artifacts repository")

    def publish(
        self,
        model_adapter: ModelAdapter,
        *,
        experiment: Optional[AbstractExperiment] = None,
        model_adapter_config: Optional[dict] = None,
        change_type: Optional[ModelVersionChangeType] = None,
        notes: Optional[str] = None,
        with_docker: bool = True,
    ) -> models_api.ModelVersionRid:
        if model_adapter_config is None:
            model_adapter_config = {}

        model_adapter._api().validate_api_io_or_throw()

        maven_coordinate = f"com.palantir.models:{self.__model_version_rid}:{ModelsBackedWritableModel.MAVEN_VERSION}"

        layer_locators = []

        model_state_files_metadata = {}

        experiment = make_none_if_cant_publish(experiment)
        experiment_rid = experiment.close() if experiment is not None else None

        with TemporaryDirectoryBackedModelStateWriter() as state_writer:
            model_adapter.save(state_writer)

            for path in glob.iglob(state_writer.tmp_dir.name + "/**", recursive=True):
                if not os.path.isfile(path):
                    continue
                relative_path = os.path.relpath(path, state_writer.tmp_dir.name)
                model_state_files_metadata[relative_path] = os.path.getsize(path)

            if with_docker:
                with package_mice_weights_layer(state_writer.tmp_dir.name) as (layer_file, layer_metadata):
                    self._upload_model_weights_layer(layer_file, layer_metadata)

                layer_locators.append(
                    models_api.DockerLayerLocator(
                        source_repository_rid=self.__model_version_rid,
                        docker_name=ModelContainerConsts.DOCKER_WEIGHTS_NAME,
                        size=layer_metadata.gzip_size,
                        digest=layer_metadata.gzip_digest,
                        tar_digest=layer_metadata.tar_digest,
                    )
                )
                binary_model_locator = None

            else:
                with tempfile.TemporaryDirectory() as zip_dir:
                    zipfile_path = os.path.join(zip_dir, "model")
                    shutil.make_archive(zipfile_path, "zip", state_writer.tmp_dir.name)
                    with open(f"{zipfile_path}.zip", "rb") as model_zip:
                        self._upload_model_zip(maven_coordinate, model_zip)
                binary_model_locator = models_api.ModelAssetLocator(
                    repository_rid=self.__model_version_rid,
                    maven_coordinate=maven_coordinate,
                )

        adapter_environment_reference = _maybe_get_adapter_environment_reference(model_adapter)

        request = models_api.StageModelVersionRequest(
            model_adapter=models_api.StagedModelAdapter(python_conda=model_adapter.to_service_api().python_conda),
            model_adapter_config=model_adapter_config,
            adapter_environment=adapter_environment_reference,
            source=models_api.StagedModelVersionSource(binary=models_api.BinaryModel(binary_model_locator)),
            model_api=model_adapter._api().to_service_api(),
            version_notes=notes,
            model_weights_docker_layers=layer_locators or None,
            model_state_files_metadata=model_state_files_metadata,
            experiment=experiment_rid,
        )

        response = self.__model_service.stage_model_version(
            auth_header=self.__auth_header,
            model_rid=self.__model_rid,
            model_version_rid=self.__model_version_rid,
            request=request,
        )

        return response.model_version.rid

    def _get_mice_enabled_feature_flag(self) -> bool:
        try:
            return self.__executable_format_service.is_mice_enabled_v2(self.__auth_header)
        except Exception:
            # default to disabled if we can't reach the endpoint for any reason
            return False


def _maybe_get_adapter_environment_reference(
    model_adapter: ModelAdapter,
) -> Optional[models_api.ModelAdapterEnvironmentReference]:
    adapter_module_name = model_adapter.__class__.__module__
    parent_module_name = adapter_module_name.split(".")[0]
    try:
        repository = importlib.import_module(f"{parent_module_name}._repo").__repo__
        version = importlib.import_module(f"{parent_module_name}._version").__version__
        return models_api.ModelAdapterEnvironmentReference(repository, version)
    except ModuleNotFoundError:
        return None
