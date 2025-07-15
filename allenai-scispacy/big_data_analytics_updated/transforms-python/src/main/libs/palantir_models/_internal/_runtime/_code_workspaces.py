#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import json
import os
import subprocess
from typing import Dict, Optional, Type

from models_api.models_api_experiments import ExperimentCodeWorkspaceSource, ExperimentService, ExperimentSource

from palantir_models.experiments._errors import ErrorHandlerType
from palantir_models.experiments._experiment import Experiment
from palantir_models.experiments._parameter import PARAMETER_VALUE

from ...conjure.build2_api.foundry_build2_api import BranchFallbacks
from ...conjure.build2_api.foundry_build2_jobspecgraph_api import GetJobSpecForDatasetRequest, JobSpecService
from ...conjure.foundry_container_service_api.fcs_api import (
    ModelVersionInputBranchResolution,
    ModelVersionInputResolution,
    ModelVersionInputVersionResolution,
)
from ._runtime_environment import RuntimeEnvironment, RuntimeEnvironmentType
from ._utils import (
    PartialServiceConfig,
    _assert_foundry_env,
    _construct_service_for_uris,
    _Service,
    _sidecar_uri,
    rid_pattern,
)

_STEMMA_RID_PATTERN = rid_pattern("stemma", "repository")
_MODEL_VERSION_RID_PATTERN = rid_pattern("models", "model-version")
_SANDBOX_BRANCH_PREFIX = "code-workspace-sandbox/"
_TEMPLATE_CONFIG_PATH = "/home/user/repo/templateConfig.json"


class CodeWorkspacesRuntimeEnvironment(RuntimeEnvironment):
    def get_runtime_environment_type(self) -> RuntimeEnvironmentType:
        return RuntimeEnvironmentType.CODE_WORKSPACES

    def get_sidecar_service_auth_header(self) -> str:
        token = _assert_foundry_env("FOUNDRY_TOKEN")
        return f"Bearer {token}"

    def get_discovered_service_auth_header(self) -> str:
        return self.get_sidecar_service_auth_header()

    def get_discovered_service(
        self,
        service_base_name: str,
        service_class: Type[_Service],
        partial_config: Optional[PartialServiceConfig] = None,
    ) -> _Service:
        return _construct_service_for_uris(
            [f"{_proxy_uri()}/{service_base_name}/api"], service_class, partial_config=partial_config
        )

    def get_sidecar_service(self, service_class: Type[_Service]) -> _Service:
        return _construct_service_for_uris([f"{_sidecar_uri()}/api"], service_class)

    def get_repo_rid(self) -> str:
        try:
            git_remotes = subprocess.check_output(["git", "remote", "-v"], text=True, cwd=self.get_user_home()).strip()
            return _STEMMA_RID_PATTERN.findall(git_remotes)[0]
        except Exception as exception:
            raise RuntimeError("Unable to obtain repository for this workspace") from exception

    def get_branch(self) -> str:
        try:
            git_branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True, cwd=self.get_user_home()
            ).strip()
            return git_branch
        except Exception as exception:
            raise RuntimeError("Unable to obtain git branch for this workspace") from exception

    def write_model_input_spec(self, alias: str, branch: Optional[str], model_version_rid: Optional[str]):
        aliases = self._read_aliases_file()
        try:
            file_alias = aliases[alias]
            for input_resolution in file_alias.input_resolutions:
                if _does_match(branch, model_version_rid, input_resolution):
                    return
            file_alias.input_resolutions.append(_create_input_resolution(branch, model_version_rid))
            aliases[alias] = file_alias
            self._write_aliases_file(aliases)
        except KeyError as exception:
            raise KeyError(
                f'No model found for alias "{alias}". Please import the model through the Models sidebar.'
            ) from exception

    def create_experiment(
        self,
        model_rid: str,
        name: str,
        error_handler_type: ErrorHandlerType,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]],
    ):
        auth_header = self.get_discovered_service_auth_header()
        experiment_service = self.get_discovered_service(
            "models",
            ExperimentService,
            PartialServiceConfig(max_retries=0, read_timeout=120, connect_timeout=60, backoff_slot_size=300),
        )
        container_args_path = _assert_foundry_env("CONTAINER_ARGUMENTS")
        with open(container_args_path, "r") as f:
            data = json.load(f)
        container_rid = data["containerRid"]
        deployment_rid = data["deploymentRid"]
        branch = self._get_correct_experiment_branch(model_rid)
        exp = Experiment.create_new(
            name=name,
            model_rid=model_rid,
            auth_header=auth_header,
            job_rid="ri.build2.main.job.this-will-get-patched",
            experiment_service=experiment_service,
            experiment_source=ExperimentSource(
                code_workspace=ExperimentCodeWorkspaceSource(
                    container_rid=container_rid,
                    deployment_rid=deployment_rid,
                    branch=branch,
                ),
            ),
            error_handler_type=error_handler_type,
            initial_parameters=initial_parameters,
        )
        return exp

    def _get_correct_experiment_branch(self, model_rid: str) -> str:
        """
        If sandbox branches are enabled and the model has a jobspec, we must fallback to using sandbox branches
        as that is where the model version will be published.
        """
        branch = self.get_branch()
        svc = self.get_discovered_service("build2", JobSpecService)
        maybe_job_spec = svc.get_job_spec_for_dataset_in_graph(
            self.get_discovered_service_auth_header(),
            GetJobSpecForDatasetRequest(
                branch=branch, branch_fallbacks=BranchFallbacks(branches=[]), dataset_rid=model_rid
            ),
        )
        if maybe_job_spec is not None:
            return _SANDBOX_BRANCH_PREFIX + branch
        return branch


def _proxy_uri() -> str:
    return f"https://{_assert_foundry_env('FOUNDRY_PROXY_URL')}"


def _does_match(branch: Optional[str], model_version_rid: Optional[str], input_resolution: ModelVersionInputResolution):
    if model_version_rid is not None:
        if input_resolution.version is not None:
            if input_resolution.version.model_version == model_version_rid:
                return True
        return False
    if input_resolution.branch is not None:
        if input_resolution.branch.branch == branch:
            return True
        return False
    return False


def _create_input_resolution(branch: Optional[str], model_version_rid: Optional[str]) -> ModelVersionInputResolution:
    if model_version_rid is not None:
        if not _MODEL_VERSION_RID_PATTERN.match(model_version_rid):
            raise ValueError(f"{model_version_rid} is not a valid model version rid")
        return ModelVersionInputResolution(version=ModelVersionInputVersionResolution(model_version_rid))
    return ModelVersionInputResolution(branch=ModelVersionInputBranchResolution(branch))


def _not_in_transforms_repo() -> bool:
    """Checks if we are definitively not in a transforms repo."""
    if not os.path.exists(_TEMPLATE_CONFIG_PATH):
        return False
    with open(_TEMPLATE_CONFIG_PATH, "r") as f:
        template_config = json.load(f)
        parent_template_id = template_config.get("parentTemplateId", None)
        return parent_template_id is not None and parent_template_id != "transforms"
