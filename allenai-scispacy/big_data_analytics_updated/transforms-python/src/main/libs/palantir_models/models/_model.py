#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines classes for working with Models."""

import importlib
import inspect
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Type, TypeVar

import models_api.models_api_common as common
import models_api.models_api_executable as executable_api
import models_api.models_api_models as models_api

from ._model_adapter import ModelAdapter
from ._state_accessors import AbstractModelStateReader, LocalDirectoryModelStateReader, ZipModelStateReader
from ._tar_utils import _extract_model_layer_tarfiles
from .rpc import ArtifactsService

_LOAD_ERROR_MSG = "'{}:{}' load method returned None but must return a ModelAdapter instance, unable to continue."

log = logging.getLogger(__name__)

_ModelAdapter = TypeVar("_ModelAdapter", bound=ModelAdapter)


class Model:
    """A class for interacting with Foundry Models."""

    __model_service: models_api.ModelService
    __executable_service: executable_api.ModelExecutableFormatService
    __artifacts_service: ArtifactsService
    __auth_header: str
    __model_rid: common.ModelRid

    def __init__(
        self,
        model_service: models_api.ModelService,
        executable_service: executable_api.ModelExecutableFormatService,
        artifacts_service: ArtifactsService,
        auth_header: str,
        model_rid: common.ModelRid,
    ):
        self.__model_service = model_service
        self.__executable_service = executable_service
        self.__artifacts_service = artifacts_service
        self.__auth_header = auth_header
        self.__model_rid = model_rid

    def get_model_adapter_for_model_version(
        self,
        *,
        model_version_rid: models_api.ModelVersionRid,
        try_docker=True,
    ) -> ModelAdapter:
        """
        Loads a specific model version and provides an executable ModelAdapter object.

        :param model_version_rid: Rid for the desired model version.
        :return: ModelAdapter with transform method capable of scoring on the model.
        """
        model_version = fetch_model_version(
            self.__model_service,
            self.__executable_service,
            self.__artifacts_service,
            self.__auth_header,
            self.__model_rid,
            model_version_rid,
            try_docker=try_docker,
        )

        try:
            if model_version.model_weights_dir is not None:
                with LocalDirectoryModelStateReader(model_version.model_weights_dir) as model_state_reader:
                    model_adapter_instance = instantiate_adapter(
                        model_version.model_adapter_class,
                        model_state_reader,
                        model_version.container_context,
                        model_version.external_context,
                    )
            elif model_version.model_state_zip is not None:
                with ZipModelStateReader(model_version.model_state_zip) as model_state_reader:
                    model_adapter_instance = instantiate_adapter(
                        model_version.model_adapter_class,
                        model_state_reader,
                        model_version.container_context,
                        model_version.external_context,
                    )
        finally:
            if model_version.model_weights_dir is not None:
                shutil.rmtree(model_version.model_weights_dir)
            elif model_version.model_state_zip is not None:
                model_version.model_state_zip.close()

        model_adapter_instance._api().validate_api_io_or_throw()

        return model_adapter_instance


@dataclass(frozen=True)
class ModelVersion:
    """
    A dataclass containing all the necessary objects for instantiating a model version's adapter.
    model_state_zip is a temporary file which will be deleted when closed or when the reference is garbage collected.

    If `model_weights_layers_dir` is present, then the weights have been downloaded using docker layers.
    If not, fallback to `model_state_zip`
    """

    model_adapter_class: Type[ModelAdapter]
    container_context: Optional[executable_api.ContainerizedApplicationContext]
    external_context: Optional[executable_api.ExternalModelExecutionContext]
    model_state_zip: Optional["tempfile._TemporaryFileWrapper[bytes]"]
    model_weights_dir: Optional[str]


def fetch_model_version(
    model_service: models_api.ModelService,
    executable_service: executable_api.ModelExecutableFormatService,
    artifacts_service: ArtifactsService,
    auth_header: str,
    model_rid: common.ModelRid,
    model_version_rid: models_api.ModelVersionRid,
    try_docker=True,
) -> ModelVersion:
    """
    Fetches necessary context for instantiating a model version's adapter and downloads the model state locally to a
    temporary file.

    If try_docker is true, this will attempt to use the docker layers stored in artifacts instead of the model zip,
    and will fallback if it fails. If it passes, the returned ModelVersion's `model_weights_dir` will contain the unpacked
    weights. Consumers are expected to handle deletion of those files once they are no longer needed (for example, after
    using LocalDirectoryModelStateReader)
    """
    details = model_service.get_model_version_details(auth_header, model_rid, model_version_rid)

    assert details.model_adapter.python_conda is not None, "Only python model adapters are supported"

    module = importlib.import_module(details.model_adapter.python_conda.locator.module)
    model_adapter_class = getattr(module, details.model_adapter.python_conda.locator.name)

    maybe_container_context = executable_service.get_containerized_representation_context(
        auth_header, model_rid, model_version_rid
    )

    maybe_external_context = executable_service.get_external_model_execution_context(
        auth_header, model_rid, model_version_rid
    )

    if try_docker:
        try:
            weights_layers = executable_service.get_model_layer_locators(auth_header, model_version_rid)
            if weights_layers is not None and len(weights_layers) > 0:
                with tempfile.TemporaryDirectory() as layers_tempdir:
                    unpacked_tempdir = tempfile.mkdtemp()
                    _fetch_model_weights_layers(auth_header, weights_layers, artifacts_service, layers_tempdir)
                    _extract_model_layer_tarfiles(layers_tempdir, unpacked_tempdir)
                return ModelVersion(
                    model_adapter_class, maybe_container_context, maybe_external_context, None, unpacked_tempdir
                )
        except Exception as e:
            log.warning("Failed to download docker weights layers. Falling back to model state zip. Error: {}", e)

    if details.model_asset is None:
        raise ValueError("This model version only supports docker layers, but the runtime does not support them.")

    model_state_zip = tempfile.NamedTemporaryFile("w+b", prefix="model", suffix=".zip")
    with artifacts_service.get_maven_artifact(
        auth_header=auth_header,
        repository_rid=details.model_asset.repository_rid,
        maven_coordinate=details.model_asset.maven_coordinate,
    ) as response:
        shutil.copyfileobj(response.body, model_state_zip)  # type: ignore
    model_state_zip.seek(0)

    return ModelVersion(model_adapter_class, maybe_container_context, maybe_external_context, model_state_zip, None)


def instantiate_adapter(
    model_adapter_class: Type[_ModelAdapter],
    model_state_reader: AbstractModelStateReader,
    container_context: Optional[executable_api.ContainerizedApplicationContext] = None,
    external_model_context: Optional[executable_api.ExternalModelExecutionContext] = None,
) -> _ModelAdapter:
    """
    Instantiates the user implementation of the `load()` method. Depending on the number of positional arguments
    provided, contexts for containers, and external models are injected. The positional instantiation is a
    temporary measure to avoid breaking back-compat for external model support. Future context additions
    should be done using the ModelAdapterV2 definition.
    """
    load_method = model_adapter_class.load
    parameters = inspect.signature(load_method).parameters
    num_args = len(parameters)

    if num_args == 1:
        instance = load_method(model_state_reader)
    elif num_args == 2:
        instance = load_method(model_state_reader, container_context)
    elif num_args == 3:
        instance = load_method(model_state_reader, container_context, external_model_context)
    else:
        raise ValueError("Invalid load method signature")

    if instance is None:
        raise Exception(_LOAD_ERROR_MSG.format(model_adapter_class.__module__, model_adapter_class.__name__))

    return instance


def _fetch_model_weights_layers(
    auth_header: str,
    weights_layers: List[models_api.DockerLayerLocator],
    artifacts_service: ArtifactsService,
    target_download_dir: str,
):
    for layer in weights_layers:
        with artifacts_service.get_blob(
            auth_header=auth_header,
            repository_rid=layer.source_repository_rid,
            docker_name=layer.docker_name,
            docker_digest=layer.digest,
        ) as response:
            with open(os.path.join(target_download_dir, layer.digest.replace("sha256:", "") + ".tar.gz"), "wb") as f:
                f.write(response.body.read())
