import os
import shutil
import tarfile
from typing import Optional, Type, TypeVar

import models_api.models_sidecar_api as models_sidecar_api_services  # TODO(wsuppiger): fix import package
from conjure_python_client import ConjureHTTPError

from .._internal._runtime import _get_runtime_env
from ..models._model import instantiate_adapter
from ..models._model_adapter import ModelAdapter
from ..models._state_accessors import LocalDirectoryModelStateReader, ZipModelStateReader
from ._deployment_client import DeploymentClient

_VALID_ROOT_PATHS = ["/app/data/model/", "app/data/model/"]

SOURCE_TYPE_UNSUPPORTED_EXCEPTION = "Models:ModelSourceTypeUnsupportedInCws"

GENERIC_RESOLUTION_FAILED_ERROR = "Model version resolution failed. Please verify you have proper permissions on the model and that it is either a container model or an in-platform produced model."


_ModelAdapter = TypeVar("_ModelAdapter", bound=ModelAdapter)


# TODO(tuckers): add a _repr_html_ for this class
class ModelInput:
    """A class for reading Foundry Models in Code Workspaces."""

    def __init__(self, model_alias: str, model_version: Optional[str] = None, branch: Optional[str] = None):
        self.__runtime_env = _get_runtime_env()
        self.__model_alias = model_alias

        self.__sidecar_service = self.__runtime_env.get_sidecar_service(
            models_sidecar_api_services.FoundryDataSidecarModelsService
        )
        self.__model_rid = self.__runtime_env.get_imported_model_rid(model_alias)

        try:
            resolve_response = self.__sidecar_service.resolve_model_version(
                self.__runtime_env.get_sidecar_service_auth_header(),
                model_rid=self.__model_rid,
                branch_id=branch,
                model_version_rid=model_version,
            )
        except ConjureHTTPError as conjure_error:
            # we likely won't actually catch this, as build2 will suppress and throw its own
            if conjure_error.error_name == SOURCE_TYPE_UNSUPPORTED_EXCEPTION:
                source_type = conjure_error.parameters["modelSourceType"]
                raise Exception(
                    f"Model version resolution failed due to the resolved model version being a {source_type} model, which is unsupported in Code Workspaces. Only importing container models and in-platform produced binary models are supported."
                ) from conjure_error
            raise Exception(GENERIC_RESOLUTION_FAILED_ERROR) from conjure_error
        except Exception as e:
            raise Exception(GENERIC_RESOLUTION_FAILED_ERROR) from e

        self.__runtime_env.write_model_input_spec(alias=model_alias, branch=branch, model_version_rid=model_version)

        self.__model_version_rid = resolve_response.model_version_rid
        self.__model_branch = resolve_response.branch

    @property
    def alias(self) -> str:
        return self.__model_alias

    @property
    def model_rid(self) -> str:
        return self.__model_rid

    @property
    def model_version(self) -> str:
        return self.__model_version_rid

    @property
    def branch(self) -> str:
        return self.__model_branch

    def download_files(self) -> str:
        """
        Downloads any serialized files that are stored in the resolved model version.
        :return: The path to the downloaded files.
        """

        download_response = self.__sidecar_service.download_model_version_weights(
            self.__runtime_env.get_sidecar_service_auth_header(), self.__model_rid, self.__model_version_rid
        )
        paths = download_response.weights_layer_paths

        if len(paths) == 0:
            raise Exception("model files download returned no data")

        path_prefix = _extract_path_prefix_from_download_path(self.__runtime_env.get_user_working_dir(), paths[0])
        path_to_extract = os.path.join(path_prefix, self.__model_rid, self.__model_version_rid, "files")

        if os.path.exists(path_to_extract):
            shutil.rmtree(path_to_extract)

        os.makedirs(path_to_extract, exist_ok=True)

        if len(paths) == 1 and paths[0].endswith(".zip"):
            with open(paths[0], "rb") as model_zip:
                ZipModelStateReader(model_zip).extract(path_to_extract)
        else:
            for tar_file_path in paths:
                with tarfile.open(tar_file_path, "r:gz") as tar:
                    for member in tar.getmembers():
                        _extract_member_if_valid(tar, member, path_to_extract)

        return path_to_extract

    def initialize_adapter(self, adapter_class: Type[_ModelAdapter]) -> _ModelAdapter:
        """
        Initializes a model adapter by downloading the serialized files and loading them into the adapter.
        :param adapter_class: The adapter class type.
        :return: An initialized model adapter.
        """

        path = self.download_files()
        with LocalDirectoryModelStateReader(path) as state_reader:
            return instantiate_adapter(adapter_class, state_reader, None, None)

    def deploy(
        self,
        cpu: Optional[float] = None,
        memory: Optional[int] = None,
        gpu: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ):
        """
        Attempts to deploy a model for inference in Code Workspaces.
        :param cpu: The number of vCPUs to use for the deployment.
        :param memory: The amount of memory to use for the deployment (ex: "4G").
        :param gpu: The number of GPUs per replica to use for the deployment.
        :param max_replicas: The maximum number of replicas the deployment can scale to.
        :return: A deployment client that allows running inference against the model, scaling the deployment, disabling, etc.
        """

        deployment_client = DeploymentClient(self.__model_rid, self.__model_version_rid, self.__runtime_env)
        deployment_client.deploy(cpu=cpu, memory=memory, gpu=gpu, max_replicas=max_replicas)
        return deployment_client


# extracts the tar member to path specified
# relativizes the path such that something at /app/data/model/something is extracted to {path_to_extract}/something
def _extract_member_if_valid(tar: tarfile.TarFile, member: tarfile.TarInfo, path_to_extract: str):
    for path in _VALID_ROOT_PATHS:
        if member.path.startswith(path):
            member.path = os.path.relpath(member.path, path)
            tar.extract(member, path=path_to_extract)


# this is really meant to extract the random part from the download path, which random per
# session such that users dont rely on the paths (it will change from session to session).
# we want to extract the layers into a different path and keep the downloaded data stable so we
# can avoid doing duplicate downloads.
# _extract_path_prefix_from_download_path(
#   "/foundry/working-dir",
#   "/foundry/working-dir/randomstuff/thisiswhereidownloaded"
# ) -> "/foundry/working-dir/randomstuff
def _extract_path_prefix_from_download_path(working_dir: str, path: str) -> str:
    parts = path.replace(working_dir, "").split(os.sep)
    parts = [part for part in parts if part]

    if len(parts) >= 1:
        new_path = os.path.join(working_dir, parts[0])
        return new_path
    else:
        raise ValueError("Path does not contain enough parts to extract.")
