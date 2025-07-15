#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines classes for working with Models through transforms."""

import functools
import logging
from typing import Any, Dict, Optional, Type, TypeVar, Union

import mio_api.mio as mio_api
from conjure_python_client import RequestsClient, Service, ServiceConfiguration
from models_api import models_api_executable as executable_api
from models_api import models_api_experiments as experiments_api
from models_api import models_api_models as models_api
from transforms.api import (
    FoundryInputParam,
    FoundryOutputParam,
    LightweightInputParam,
    LightweightOutputParam,
    ParamContext,
)
from transforms.api._abstract_incremental_params import (
    AbstractNonCatalogIncrementalCompatibleTransformInput,
    AbstractNonCatalogIncrementalCompatibleTransformOutput,
)

from palantir_models._internal._jobspecs import (
    _get_latest_model_version_rid_from_dataset_properties,
    _get_model_input_dataset_properties,
    _get_model_version_rid_from_context_output_specs,
    _get_sidecar_location_from_dataset_properties,
    _get_staged_experiment_rids_from_output_specs,
    _maybe_get_port_from_dataset_properties,
)
from palantir_models._internal._runtime._code_workspaces import _not_in_transforms_repo
from palantir_models.models import Model, ModelAdapter, ModelsBackedWritableModel, WritableModel
from palantir_models.models.api._types import ServiceContext
from palantir_models.models.rpc import ArtifactsService
from palantir_models.transforms._service import construct_service
from palantir_models.transforms._util import rid_from_alias
from palantir_models.transforms.adapterless._mice_sidecar_client import MiceSidecarClient

from ._schemas import MODEL_SCHEMA

log = logging.getLogger("palantir_models")


class ModelInput(FoundryInputParam, LightweightInputParam, AbstractNonCatalogIncrementalCompatibleTransformInput):
    """
    A TransformInput which loads a specific model and passes it to the transform
    """

    def __init__(
        self,
        alias,
        model_version: Optional[str] = None,
        use_sidecar: Optional[str] = None,
        sidecar_resources: Optional[Dict[str, Union[int, float]]] = None,
    ):
        """
        Connects a transform to a model for reading.
        Transform will be given a ModelAdapter corresponding to the specified model.
        :param alias: (string) path or rid of model resource to load from
        :param model_version: (Optional, string) Rid or semver version of the specific model version
        :param use_sidecar: (Optional, string) Specifies if the model should run in a separate container as a sidecar to the
            transform. If not specified will load the latest successful model version for the provided alias.
            Lightweight transforms do not support sidecar and must use None (default). Supports the following options:
            - None: Will not use a sidecar, used by default.
            - "driver": Will run a sidecar for Spark driver.
            - "executor": Will run a sidecar for each Spark executor.
            - "both": Will run a sidecar for each Spark executor and driver.
        :param sidecar_resources: (Optional, dict) Resource configuration sidecar, use_sidecar must not be None when
            this is passed. Supports the following options:
            - "cpus": (float) Number of CPUs for each sidecar
            - "memory_gb": (float) Number of GBs of memory for each sidecar
            - "gpus": (int) Number of GPUs for each sidecar
        """
        if _not_in_transforms_repo():
            raise Exception(
                "palantir_models.transforms.ModelInput is not supported outside of transforms. "
                + "Use palantir_models.code_workspaces.ModelInput instead."
            )
        # only inject the modelVersion into the InputSpec properties if one was specified by the user
        properties: Dict[str, Any] = {} if model_version is None else {"modelVersion": model_version}

        # Alert people who are using use_sidecar early as a boolean to change to str
        if use_sidecar and not isinstance(use_sidecar, str):
            raise Exception("use_sidecar must be a string, see documentation")

        if use_sidecar and use_sidecar not in ("driver", "executor", "both"):
            raise Exception('use_sidecar must be one of: None, "driver", "executor", or "both"')
        properties["useSidecar"] = use_sidecar is not None
        properties["sidecarLocation"] = "none" if not use_sidecar else use_sidecar

        if sidecar_resources:
            if not use_sidecar:
                raise Exception("use_sidecar must be not be None when sidecar_resources are specified")
            if "cpus" in sidecar_resources:
                if sidecar_resources["cpus"] <= 0:
                    raise Exception("sidecar_resources.cpus must be greater than 0")
                properties["sidecarNumCpus"] = sidecar_resources["cpus"]
            if "memory_gb" in sidecar_resources:
                if sidecar_resources["memory_gb"] <= 0:
                    raise Exception("sidecar_resources.memory_gb must be greater than 0")
                properties["sidecarMemoryMbs"] = int(sidecar_resources["memory_gb"] * 1000)
            if "gpus" in sidecar_resources:
                if sidecar_resources["gpus"] < 0:
                    raise Exception("sidecar_resources.gpus must not be negative")
                properties["sidecarNumGpus"] = sidecar_resources["gpus"]

        super().__init__(aliases=[alias], type="model", properties=properties)
        self.alias = alias
        self.model_version = model_version
        self.use_sidecar = use_sidecar

    @property
    def json_value(self):
        # json_value in instance method is this + modelRid (gets injected by gradle-transforms)
        # not including use_sidecar here, as it's part of resolved dataset properties
        return {
            "alias": self.alias,
            "datasetRid": self.alias,  # other LightweightInputParams have this, here for consistency in transforms
            "modelVersion": self.model_version,
        }

    @property
    def schema(self):
        return MODEL_SCHEMA

    @staticmethod
    def instance(context: ParamContext, json_value) -> ModelAdapter:
        auth_header = context.foundry_connector.auth_header
        model_rid = json_value["modelRid"]
        service_context = TransformsServiceContext(
            auth_header=auth_header, resolver=context.foundry_connector._resolve_role
        )

        executable_service = construct_service(context, "models-api", executable_api.ModelExecutableFormatService)
        model_service = construct_service(context, "models-api", models_api.ModelService)
        artifacts_service = construct_service(context, "foundry-artifacts-api", ArtifactsService)

        dataset_properties = _get_model_input_dataset_properties(context.input_specs, model_rid)
        if json_value.get("modelVersion"):
            model_version = json_value["modelVersion"]
        else:
            # no model version passed fetch from context
            model_version = _get_latest_model_version_rid_from_dataset_properties(
                auth_header, executable_service, model_rid, dataset_properties
            )

        # If resolved port is present and non-null, we should use the sidecar client
        resolved_sidecar_port = _maybe_get_port_from_dataset_properties(dataset_properties)
        if resolved_sidecar_port:
            sidecar_location = _get_sidecar_location_from_dataset_properties(dataset_properties)
            client = MiceSidecarClient(resolved_sidecar_port, service_context, sidecar_location)
            client._register_input_model_version(model_version_rid=model_version)
            return client

        model = Model(model_service, executable_service, artifacts_service, auth_header, model_rid)

        model_adapter: ModelAdapter = model.get_model_adapter_for_model_version(model_version_rid=model_version)

        model_adapter._register_input_model_version(model_version_rid=model_version)
        model_adapter._register_service_context(service_context)
        return model_adapter

    @staticmethod
    def lightweight_instance(context: ParamContext, json_value: Dict[str, Any]):
        json_value["modelRid"] = rid_from_alias(json_value["alias"])
        return ModelInput.instance(context, json_value)

    @property
    def rid(self) -> str:
        return rid_from_alias(self.alias)

    def was_change_incremental(self, incremental_input_resolution):
        return (False, "ModelInput does not support incremental changes")

    def get_incremental(self, incremental_input_resolution):
        return self

    def get_non_incremental(self):
        return self


_Service = TypeVar("_Service", bound=Service)


class TransformsServiceContext(ServiceContext):
    """
    A ServiceContext that can be used to construct services for a transform.
    """

    def __init__(self, auth_header, resolver):
        self.auth_header_str = auth_header
        self.resolver_fn = resolver

    def media_set_service(self):
        return self._construct_service("mio-api", mio_api.MediaSetService)

    def binary_media_set_service(self):
        return self._construct_service("mio-api", mio_api.BinaryMediaSetService)

    @property
    def auth_header(self):
        return self.auth_header_str

    @functools.lru_cache(maxsize=64)
    def _construct_service(self, role: str, service_class: Type[_Service]) -> _Service:
        """
        Constructs conjure client for a given service. Caches
        clients to avoid proliferation of RequestsClients.

        :return: Instance of service_class connected to the base uri.
        """
        config = ServiceConfiguration()
        config.uris = self.resolver_fn(role)
        return RequestsClient.create(service_class, "palantir-models-transforms", config)


class ModelOutput(FoundryOutputParam, LightweightOutputParam, AbstractNonCatalogIncrementalCompatibleTransformOutput):
    """
    Connects a transform to an existing Model for writing.
    Transform will be given a WritableModel object.
    :param alias: path or rid of model resource to load from
    """

    def __init__(self, alias):
        if _not_in_transforms_repo():
            raise Exception(
                "palantir_models.transforms.ModelOutput is not supported outside of transforms. "
                + "Use palantir_models.code_workspaces.ModelOutput instead."
            )
        super().__init__(aliases=[alias], type="model")
        self.alias = alias

    @property
    def json_value(self):
        return {
            "alias": self.alias,
            "datasetRid": self.alias,  # other LightweightOutputParams have this, here for consistency in transforms
        }

    @property
    def schema(self):
        return MODEL_SCHEMA

    @staticmethod
    def instance(context: ParamContext, json_value) -> WritableModel:
        auth_header = context.foundry_connector.auth_header
        model_service = construct_service(context, "models-api", models_api.ModelService)
        experiments_service = construct_service(context, "models-api", experiments_api.ExperimentService)
        executable_format_service = construct_service(
            context, "models-api", executable_api.ModelExecutableFormatService
        )
        artifacts_service = construct_service(context, "foundry-artifacts-api", ArtifactsService)
        model_rid = json_value["modelRid"]
        return ModelsBackedWritableModel(
            auth_header=auth_header,
            model_service=model_service,
            experiment_service=experiments_service,
            executable_format_service=executable_format_service,
            artifacts_service=artifacts_service,
            model_rid=model_rid,
            model_version_rid=_get_model_version_rid_from_context_output_specs(context.output_specs, model_rid),
            staged_experiments=_get_staged_experiment_rids_from_output_specs(context.output_specs, model_rid),
        )

    @staticmethod
    def lightweight_instance(context: ParamContext, json_value: Dict[str, Any]):
        json_value["modelRid"] = rid_from_alias(json_value["alias"])
        return ModelOutput.instance(context, json_value)

    @property
    def rid(self) -> str:
        return rid_from_alias(self.alias)

    def get_incremental(self, strict_append):
        return self

    def get_non_incremental(self):
        return self
