#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
from typing import Any, Dict, List, Optional, Type

import yaml
from models_api.models_api_experiments import ExperimentService

from palantir_models._internal._jobspecs import _get_staged_experiment_rids_from_output_specs
from palantir_models.experiments._errors import ErrorHandlerType
from palantir_models.experiments._experiment import Experiment
from palantir_models.experiments._parameter import PARAMETER_VALUE

from ...conjure.foundry_data_sidecar_api.foundrydatasidecar_api_context import FoundryDataSidecarContextService
from ...conjure.foundry_data_sidecar_api.foundrydatasidecar_api_jobspec import FoundryDataSidecarJobSpecService
from ._runtime_environment import RuntimeEnvironment, RuntimeEnvironmentType
from ._utils import PartialServiceConfig, _assert_foundry_env, _construct_service_for_uris, _Service, _sidecar_uri

BASE_NAME_DISCOVERY_MAPPING = {
    "models": "models_service",
    # TODO(tuckers): add LMS to discovery and then use here
}


class ContainerTransformsRuntimeEnvironment(RuntimeEnvironment):
    # static variable to share with all instances
    __output_specs: Optional[Dict[str, Any]] = None
    __experiment_rids_for_model: Dict[str, Optional[List[str]]] = {}

    def get_runtime_environment_type(self) -> RuntimeEnvironmentType:
        return RuntimeEnvironmentType.CONTAINER_TRANSFORMS

    def get_sidecar_service_auth_header(self) -> str:
        token = _assert_foundry_env("FOUNDRY_TOKEN")
        return f"Bearer {token}"

    def get_discovered_service_auth_header(self) -> str:
        with open(_assert_foundry_env("TEMPORARY_JOB_TOKEN_FILE"), "r") as file:
            return f"Bearer {file.read()}"

    def get_discovered_service(
        self,
        service_base_name: str,
        service_class: Type[_Service],
        partial_config: Optional[PartialServiceConfig] = None,
    ) -> _Service:
        with open(_assert_foundry_env("FOUNDRY_SERVICE_DISCOVERY_V2"), "r") as file:
            uris = yaml.safe_load(file)[_get_discovery_mapping(service_base_name)]
        return _construct_service_for_uris(uris, service_class, partial_config)

    def get_sidecar_service(self, service_class: Type[_Service]) -> _Service:
        return _construct_service_for_uris([f"{_sidecar_uri()}/foundry-data-sidecar/api"], service_class)

    def get_repo_rid(self) -> str:
        context_service = self.get_sidecar_service(FoundryDataSidecarContextService)
        repo_rid = context_service.get_repository_rid(self.get_sidecar_service_auth_header())
        if repo_rid is None:
            raise Exception(
                "No repository rid found. This is an infrastructure error, please contact Palantir support for assistance."
            )
        return repo_rid

    def get_branch(self) -> str:
        # TODO(tuckers): fix this
        raise Exception("Cannot get branch in container transforms")

    def write_model_input_spec(self, alias: str, branch: Optional[str], model_version_rid: Optional[str]):
        """This is a no-op for container transforms"""
        pass

    def create_experiment(
        self,
        model_rid: str,
        name: str,
        error_handler_type: ErrorHandlerType,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]],
    ):
        experiment_rids = self._load_experiment_info_for_model(model_rid)
        if len(experiment_rids) == 0:
            raise Exception(
                "No staged experiments exist for this model. create_experiment may only be called once per job."
            )

        experiment_rid = experiment_rids[0]
        ContainerTransformsRuntimeEnvironment.__experiment_rids_for_model[model_rid] = experiment_rids[1:]

        return Experiment.initialize_from_staged(
            name=name,
            staged_experiment_rid=experiment_rid,
            auth_header=self.get_discovered_service_auth_header(),
            error_handler_type=error_handler_type,
            experiment_service=self.get_discovered_service("models", ExperimentService),
            initial_parameters=initial_parameters,
        )

    def _load_experiment_info_for_model(self, model_rid):
        if ContainerTransformsRuntimeEnvironment.__output_specs is None:
            jobspec_service = self.get_sidecar_service(FoundryDataSidecarJobSpecService)
            ContainerTransformsRuntimeEnvironment.__output_specs = jobspec_service.get_output_specs(
                self.get_sidecar_service_auth_header()
            ).output_specs
        return self._get_staged_experiment_rids(model_rid)

    def _get_staged_experiment_rids(self, model_rid):
        if model_rid not in ContainerTransformsRuntimeEnvironment.__experiment_rids_for_model:
            rids = _get_staged_experiment_rids_from_output_specs(
                ContainerTransformsRuntimeEnvironment.__output_specs, model_rid
            )
            ContainerTransformsRuntimeEnvironment.__experiment_rids_for_model[model_rid] = rids
        return ContainerTransformsRuntimeEnvironment.__experiment_rids_for_model[model_rid]


def _get_discovery_mapping(service_base_name: str) -> str:
    if service_base_name in BASE_NAME_DISCOVERY_MAPPING:
        return BASE_NAME_DISCOVERY_MAPPING[service_base_name]
    raise Exception(f'Unable to locate service for context path "{service_base_name}".')
