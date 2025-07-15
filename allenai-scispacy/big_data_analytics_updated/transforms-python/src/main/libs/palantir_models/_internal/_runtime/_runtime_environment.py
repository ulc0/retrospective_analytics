#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import abc
import enum
import os
from typing import Dict, Optional, Type

import yaml
from conjure_python_client import ConjureDecoder, ConjureEncoder

from palantir_models.experiments._errors import ErrorHandlerType
from palantir_models.experiments._parameter import PARAMETER_VALUE

from ...conjure.foundry_container_service_api.fcs_api import Alias, ModelAssetDefinition
from ._utils import PartialServiceConfig, _Service, rid_pattern

_USER_HOME_DIR = "/home/user/repo"
_MODELS_YML_PATH = ".foundry/model-assets.yml"
_MODEL_RID_PATTERN = rid_pattern("models", "model")


class RuntimeEnvironmentType(enum.Enum):
    CODE_WORKSPACES = 1
    CONTAINER_TRANSFORMS = 2


class RuntimeEnvironment(abc.ABC):
    """Runtime env"""

    def get_user_home(self):
        return _USER_HOME_DIR

    def get_user_working_dir(self) -> str:
        return os.environ["USER_WORKING_DIR"]

    def use_hawk(self) -> bool:
        if "ENABLE_MANAGED_CONDA_ENVIRONMENTS" in os.environ:
            return os.environ["ENABLE_MANAGED_CONDA_ENVIRONMENTS"] == "true"
        return False

    def get_path_to_hawk_lockfile(self) -> Optional[str]:
        if "HAWK_LOCKFILE_PREFIX" in os.environ:
            lock_file_path = os.path.join(os.environ["HAWK_LOCKFILE_PREFIX"], "hawk.lock")
            if os.path.exists(lock_file_path):
                return lock_file_path
            return os.path.join(os.environ["HAWK_LOCKFILE_PREFIX"], "conda.lock")
        return None

    def get_path_to_maestro_meta_yml(self) -> Optional[str]:
        if "MAESTRO_MANIFEST_DIR" in os.environ:
            return os.path.join(os.environ["MAESTRO_MANIFEST_DIR"], "meta.yaml")
        return None

    def get_imported_model_rid(self, model_alias: str):
        try:
            aliases = self._read_aliases_file()
            model_rid = aliases[model_alias].rid
        except (KeyError, FileNotFoundError) as exception:
            raise KeyError(
                f'No model found for alias "{model_alias}". Please import the model through the Models sidebar.'
            ) from exception

        assert _MODEL_RID_PATTERN.match(model_rid), f'Invalid Model RID for alias "{model_alias}": {model_rid}'
        return model_rid

    @abc.abstractmethod
    def get_runtime_environment_type(self) -> RuntimeEnvironmentType:
        """Returns the environment type"""

    @abc.abstractmethod
    def get_repo_rid(self) -> str:
        """Returns the repository rid for the current code"""

    @abc.abstractmethod
    def get_branch(self) -> str:
        """Returns the repository branch for the current code"""

    @abc.abstractmethod
    def get_discovered_service_auth_header(self) -> str:
        """Returns the auth header needed for discovered services"""

    @abc.abstractmethod
    def get_sidecar_service_auth_header(self) -> str:
        """Returns the auth header needed for sidecar services"""

    @abc.abstractmethod
    def get_discovered_service(
        self,
        service_base_name: str,
        service_class: Type[_Service],
        partial_config: Optional[PartialServiceConfig] = None,
    ) -> _Service:
        """Returns an initialized conjure service for the given parameters"""

    @abc.abstractmethod
    def get_sidecar_service(self, service_class: Type[_Service]) -> _Service:
        """Returns an initialized conjure service for the sidecar service"""

    @abc.abstractmethod
    def write_model_input_spec(self, alias: str, branch: Optional[str], model_version_rid: Optional[str]):
        """Writes the input spec to the aliases file"""

    @abc.abstractmethod
    def create_experiment(
        self,
        model_rid: str,
        name: str,
        error_handler_type: ErrorHandlerType,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]],
    ):
        """Creates experiment"""

    def _read_aliases_file(self) -> Dict[Alias, ModelAssetDefinition]:
        with open(os.path.join(self.get_user_home(), _MODELS_YML_PATH), "r") as models_yml:
            models_dict = yaml.safe_load(models_yml)

        try:
            return ConjureDecoder.do_decode(ConjureEncoder.do_encode(models_dict), Dict[Alias, ModelAssetDefinition])
        except Exception as exception:
            raise RuntimeError("Unable to read model aliases file due to invalid format.") from exception

    # temporarily making this a no-op, might come back to this and make a temp file for it
    def _write_aliases_file(self, aliases: Dict[Alias, ModelAssetDefinition]):
        pass
        # primitive_dict = ConjureEncoder.do_encode(aliases)
        # with open(os.path.join(self.get_user_home(), _MODELS_YML_PATH), "w") as models_yml:
        #     yaml.safe_dump(primitive_dict, models_yml)
