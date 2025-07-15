#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import logging
import os
import time
from io import BytesIO
from typing import Set

import requests
from conjure_python_client import ConjureDecoder
from models_api.models_api_modelapi import ModelApi as ConjureModelApi
from requests.exceptions import RequestException

from ._remote_adapter import ArrowRemoteAdapter, RemoteInferenceException
from ._util import convert_conjure_model_api_to_internal

log = logging.getLogger(__name__)

DECODER = ConjureDecoder()

MAX_READINESS_MILLIS = 30000
READINESS_POLL_DELAY_MILLIS = 200
MAX_READINESS_ATTEMPTS = MAX_READINESS_MILLIS // READINESS_POLL_DELAY_MILLIS

SERVICE_URI = "https://localhost:{}/model-inference-runner/api/"
READINESS_ENDPOINT_SUFFIX = "status/readiness"
MODEL_API_ENDPOINT_SUFFIX = "model/api"
TRANSFORM_ENDPOINT_SUFFIX = "transform/arrow"
DRIVER_LOCATION = "driver"
EXECUTOR_LOCATION = "executor"
BOTH_LOCATION = "both"

# Assume these are present in only one of driver/executor.
EXECUTOR_ENV_VAR = "SPARK_EXECUTOR_POD_NAME"
DRIVER_ENV_VAR = "SPARK_DRIVER_BIND_ADDRESS"

ERROR_STATUS_CODE = 400
ERROR_PARAMETERS = "parameters"
ERROR_MESSAGE = "unsafeMessage"


class MiceSidecarClient(ArrowRemoteAdapter):
    """
    Used by transforms to proxy inference requests to the MICE image running as a transforms sidecar instead of having
    the adapter loaded directly in the transform.
    """

    def __init__(self, port: int, service_context, sidecar_location: str):
        self.port = port
        self._service_context = service_context
        self.sidecar_location = sidecar_location
        self.ready_locations: Set[str] = set()
        self.initialized = False

    def _wait_for_readiness(self):
        """
        Checks if the sidecar container is available by making repeated network calls to
        the readiness endpoint. Continues checking at intervals until the container is ready or
        the timeout period (MAX_READINESS_MILLIS) is reached. Returns immediately if the sidecar
        for a spark location (e.g. driver/executor) has already been confirmed ready.
        """
        current_location = os.environ.get(EXECUTOR_ENV_VAR, DRIVER_LOCATION)
        if current_location in self.ready_locations:
            return

        # Since certs are injected into /etc/ssl/ we shouldn't need to explicitly add them to our requests
        # Not using the built-in liveness endpoint since that would require us to also expose the management port and
        # increases complexity
        readiness_url = self._get_service_uri(READINESS_ENDPOINT_SUFFIX)

        for _ in range(MAX_READINESS_ATTEMPTS):
            try:
                response = requests.get(readiness_url, timeout=1)
                response.raise_for_status()
                log.info("Sidecar container is ready")
                self.ready_locations.add(current_location)
                return
            except Exception:
                log.info("Readiness check failed", exc_info=True)
                time.sleep(READINESS_POLL_DELAY_MILLIS / 1000)
        raise RuntimeError(f"Sidecar container not available after {MAX_READINESS_MILLIS}ms.")

    def _populate_model_api(self) -> ConjureModelApi:
        """
        Makes a network call to retrieve the model_api for the deployed model.
        """
        model_api_url = self._get_service_uri(MODEL_API_ENDPOINT_SUFFIX)
        try:
            response = requests.get(model_api_url)
            response.raise_for_status()
            return DECODER.decode(response.json(), ConjureModelApi)
        except RequestException as e:
            raise RuntimeError(f"Failed to populate model_api: {e}")

    def _initialize(self):
        if EXECUTOR_ENV_VAR in os.environ:
            spark_location = EXECUTOR_LOCATION
        elif DRIVER_ENV_VAR in os.environ:
            spark_location = DRIVER_LOCATION
        else:
            log.warning(
                f"Neither {EXECUTOR_ENV_VAR} or {DRIVER_ENV_VAR} found in environment variables, defaulting setting location to driver."
            )
            spark_location = DRIVER_LOCATION
        if self.sidecar_location != spark_location and self.sidecar_location != BOTH_LOCATION:
            raise Exception(f"Model not available on {spark_location} as use_sidecar set to {self.sidecar_location}.")

        # If sidecar is only running on executor, we can only make these network calls
        # if we're in a Spark executor context, otherwise they'll fail.
        self._wait_for_readiness()
        if not self.initialized:
            self.model_api = self._populate_model_api()
            self.internal_model_api = convert_conjure_model_api_to_internal(self.model_api)
            super().__init__(self.model_api, self._service_context)
            self.initialized = True

    def _internal_arrow_transform(self, arrow_bytes: BytesIO) -> bytes:
        headers = {"Content-Type": "application/octet-stream"}
        transform_url = self._get_service_uri(TRANSFORM_ENDPOINT_SUFFIX)
        try:
            response = requests.post(transform_url, data=arrow_bytes, headers=headers)

            if response.status_code >= ERROR_STATUS_CODE:
                raise RemoteInferenceException(self._try_parse_unsafe_message(response))

            return response.content
        except RequestException as e:
            raise RuntimeError(f"Remote call to transform was unsuccessful: {e}")

    def _try_parse_unsafe_message(self, response):
        try:
            return response.json()[ERROR_PARAMETERS][ERROR_MESSAGE]
        except KeyError:
            return response.json()

    def _get_service_uri(self, endpoint_suffix):
        return f"{SERVICE_URI.format(self.port)}{endpoint_suffix}"
