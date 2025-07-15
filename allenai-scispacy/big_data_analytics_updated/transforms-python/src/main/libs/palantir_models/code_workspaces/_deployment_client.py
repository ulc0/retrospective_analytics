#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import logging
import time
from io import BytesIO
from typing import Callable, Optional, TypeVar, Union, cast

from conjure_python_client import ConjureHTTPError, Service
from models_api.models_api_models import ModelService
from models_api.models_api_sidecar import (
    GetOrCreateLiveDeploymentRequest,
    LiveDeploymentComputeResources,
    LiveDeploymentGpuConfiguration,
)
from models_api.models_sidecar_api import FoundryDataSidecarModelsService
from urllib3 import HTTPResponse
from urllib3.exceptions import ConnectionError

from .._internal._runtime._runtime_environment import RuntimeEnvironment, RuntimeEnvironmentType
from ..conjure.foundry_ml_live_api.foundry_ml_live_inference_api import InferenceService
from ..transforms.adapterless._chunked_remote_adapter import ArrowRemoteChunkingAdapter
from ..transforms.adapterless._remote_adapter import RemoteInferenceException
from ._human_readable_bytes import parse_human_readable_bytes

_MemoryType = Optional[Union[int, float, str]]

_LIVE_RID_ASSERT_MESSAGE = (
    "Deployment client must be connected to a deployment using the `deploy` method before calling this method."
)

T = TypeVar("T")
_Service = TypeVar("_Service", bound=Service)

log = logging.getLogger(__name__)


class RetryableNetworkException(Exception):
    """
    Retryable network errors. These are network errors (timeouts, connection) errors that are usually retryable
    (basically errors that are thrown on http requests that are not conjure errors)
    """


class DeploymentClient(ArrowRemoteChunkingAdapter):
    """
    A class for interacting with model deployments in Code Workspaces
    """

    def __init__(self, model_rid: str, model_version_rid: str, runtime_env: RuntimeEnvironment):
        self.__model_rid = model_rid
        self.__model_version_rid = model_version_rid
        self.__runtime_env = runtime_env
        self.__sidecar_service = self.__runtime_env.get_sidecar_service(FoundryDataSidecarModelsService)
        self.__inference_service = self.__runtime_env.get_discovered_service("foundry-ml-live", InferenceService)
        self.__live_rid = None

        super().__init__(self._load_conjure_api(), None, use_chunking=False)

    @property
    def live_rid(self):
        return self.__live_rid

    def is_ready(self, timeout=15):
        """
        Checks if the deployment is ready for inference.
        :param timeout: The number of seconds to wait for a response.
        :return: A boolean indicating whether the deployment is ready to accept inference requests or not.
        """

        try:
            readiness = self.get_readiness(timeout)
            return readiness.is_ready
        except (ConjureHTTPError, RetryableNetworkException):
            return False

    def wait_for_readiness(self, max_wait_seconds=600):
        start_time = time.time()
        while True:
            if time.time() - start_time > max_wait_seconds:
                raise Exception(
                    "Deployment did not become ready within wait duration. Please check the deployment logs using the Models sidebar to investigate further."
                )
            try:
                if self.is_ready(1):
                    return
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                pass
            time.sleep(1)

    def get_health_checks(self, timeout=15):
        """
        Returns the health check statuses of the deployment.
        :param timeout: The number of seconds to wait for a response.
        :return: Health check status object.
        """

        assert self.__live_rid is not None, _LIVE_RID_ASSERT_MESSAGE
        return _execute_with_modified_read_timeout(
            self.__inference_service,
            timeout,
            lambda service: service.health(
                self.__runtime_env.get_discovered_service_auth_header(), cast(str, self.__live_rid)
            ),
            "Network connection failure when calling get_health_checks. Please check the deployment status in the Code Workspaces sidebar, or increase the timeout duration.",
        )

    def get_readiness(self, timeout=15):
        """
        Returns the readiness status of the deployment.
        :param timeout: The number of seconds to wait for a response.
        :return: Readiness status object.
        """

        assert self.__live_rid is not None, _LIVE_RID_ASSERT_MESSAGE
        return _execute_with_modified_read_timeout(
            self.__inference_service,
            timeout,
            lambda service: service.readiness(
                self.__runtime_env.get_discovered_service_auth_header(), cast(str, self.__live_rid)
            ),
            "Network connection failure when calling get_readiness. Please check the deployment status in the Code Workspaces sidebar, or increase the timeout duration.",
        )

    def deploy(
        self,
        cpu: Optional[float] = None,
        memory: _MemoryType = None,
        gpu: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ) -> "DeploymentClient":
        """
        Deploys the model for inference.
        :param cpu: The number of vCPUs per replica to use for the deployment.
        :param memory: The amount of memory per replica to use for the deployment (ex: "4G").
        :param gpu: The number of GPUs per replica to use for the deployment.
        :param max_replicas: The maximum number of replicas the deployment can scale to.
        :return: The deployment client.
        """
        if self.__runtime_env.get_runtime_environment_type() != RuntimeEnvironmentType.CODE_WORKSPACES:
            raise RuntimeError(
                "The provided code is attempting to call .deploy() outside of interactive Code Workspaces context. "
                "Unfortunately, this is not supported yet."
            )

        if self.__live_rid is not None:
            return self.scale(cpu=cpu, memory=memory, gpu=gpu, max_replicas=max_replicas)

        log.info("Deploying model")

        gpu_config = None if gpu is None else LiveDeploymentGpuConfiguration(_validate_argument_is_int(gpu, "gpu"))
        # because FSMs are not instantaneous, it takes a bit of time for the live deployment to reach a state
        # where `startDeployedAppBuild` can be called. `get_or_create_live_deployment` is a no-op when the deployment already exists,
        # and will instead just call `startDeployedAppBuild`, so retrying here is totally safe.
        try:
            self.__live_rid = _retry(
                lambda: self.__sidecar_service.get_or_create_live_deployment(
                    self.__runtime_env.get_sidecar_service_auth_header(),
                    GetOrCreateLiveDeploymentRequest(
                        model_rid=self.__model_rid,
                        model_version_rid=self.__model_version_rid,
                        compute_resource_request=LiveDeploymentComputeResources(
                            cpu_request=cpu,
                            memory_request=_parse_memory_input(memory),
                            gpu_request=gpu_config,
                            max_replicas=max_replicas,
                        ),
                    ),
                ),
                [4, 8, 16],
                ConjureHTTPError,
            )
            log.warning(
                "Deployment created and will begin starting soon, use the sidebar to monitor the deployment status"
            )
            return self
        except Exception as e:
            raise Exception("Model deployment failed. Please retry starting the deployment.", e)

    def scale(
        self,
        cpu: Optional[float] = None,
        memory: _MemoryType = None,
        gpu: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ) -> "DeploymentClient":
        """
        Scales the deployments resources the model for inference.
        :param cpu: The number of vCPUs per replica to use for the deployment.
        :param memory: The amount of memory per replica to use for the deployment (ex: "4G").
        :param gpu: The number of GPUs per replica to use for the deployment.
        :param max_replicas: The maximum number of replicas the deployment can scale to.
        :return: The deployment client.
        """

        assert self.__live_rid is not None, _LIVE_RID_ASSERT_MESSAGE
        log.info("Scaling model deployment")

        gpu_config = None if gpu is None else LiveDeploymentGpuConfiguration(_validate_argument_is_int(gpu, "gpu"))
        self.__sidecar_service.scale_deployment(
            self.__runtime_env.get_sidecar_service_auth_header(),
            self.__live_rid,
            LiveDeploymentComputeResources(
                cpu_request=cpu,
                memory_request=_parse_memory_input(memory),
                gpu_request=gpu_config,
                max_replicas=max_replicas,
            ),
        )
        return self

    def disable(self):
        """
        Disables the deployment. This is equivalent to shutting down the deployment.
        """

        if self.__live_rid is None:
            log.info("Deployment is already disabled, skipping")
            return
        log.info("Disabling model deployment")
        self.__sidecar_service.disable_deployment(
            self.__runtime_env.get_sidecar_service_auth_header(),
            self.__live_rid,
        )
        self.__live_rid = None

    def _load_conjure_api(self):
        model_service = self.__runtime_env.get_discovered_service("models", ModelService)
        model_version = model_service.get_model_version_details(
            self.__runtime_env.get_discovered_service_auth_header(), self.__model_rid, self.__model_version_rid
        )
        return model_version.model_api

    def _internal_arrow_transform(self, arrow_bytes: BytesIO) -> bytes:
        # purposely going through sidecar instead of straight to proxy because we collect metrics on inference requests
        # in the sidecar and the conjure generated bindings for the inference service are somehow wrong (content-type is json
        # not octet-stream).
        assert self.__live_rid is not None, _LIVE_RID_ASSERT_MESSAGE
        live_rid = self.__live_rid
        try:
            result_bytes: Union[HTTPResponse, None] = _retry(
                lambda: self.__sidecar_service.transform_arrow(
                    self.__runtime_env.get_discovered_service_auth_header(), live_rid, arrow_bytes
                ),
                [1],
                RetryableNetworkException,
            )

            if result_bytes is None:
                raise Exception("Deployment returned an empty response")

            return result_bytes.read()
        except ConjureHTTPError as err:
            raise RemoteInferenceException.from_conjure(err)


def _parse_memory_input(memory: _MemoryType):
    if memory is None:
        return None
    if isinstance(memory, str):
        return parse_human_readable_bytes(memory)
    return int(memory)


def _retry(callback, seconds_to_sleep, retry_on):
    exception = None
    for seconds in seconds_to_sleep + [0]:
        try:
            return callback()
        except retry_on as e:
            log.debug(f"Call failed. Retrying in {seconds} seconds")
            exception = e
            time.sleep(seconds)
    if exception is not None:
        raise exception


def _execute_with_modified_read_timeout(
    service: _Service, timeout: int, callback: Callable[[_Service], T], network_exception_message: str
):
    initial_read_timeout = service._read_timeout
    service._read_timeout = timeout
    result = _error_handling_request(lambda: callback(service), network_exception_message)
    service._read_timeout = initial_read_timeout
    return result


def _error_handling_request(callback: Callable[[], T], network_exception_message):
    try:
        return callback()
    except (TimeoutError, ConnectionError) as e:
        raise RetryableNetworkException(network_exception_message) from e


def _validate_argument_is_int(value: int, arg_name: str):
    if value == int(value):
        return int(value)
    raise TypeError(f"Argument {arg_name} is expected to be an integer.")
