#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import abc
from io import BytesIO
from typing import Any, NamedTuple, Tuple

from conjure_python_client import ConjureHTTPError
from models_api.models_api_modelapi import ModelApi as ConjureModelApi

from ...models._model_adapter import ModelAdapter, _map_arguments
from ._arrow_util import ArrowModelInputHandler, ArrowModelOutputHandler
from ._util import convert_conjure_model_api_to_internal

FML_LIVE_INFERENCE_EXCEPTION = "FoundryMlLiveInference:InferenceException"


class PredictNotSupportedException(Exception):
    """
    Exception raised when attempting to run inference using predict method when predict isnt supported (multiple output models)
    """


class RemoteInferenceException(Exception):
    """
    Inference exception from a remote model.
    """

    def __init__(self, message: Any, is_stack_trace=False):
        self.message = message
        self.is_stack_trace = is_stack_trace

    @classmethod
    def from_conjure(cls, conjure_error: ConjureHTTPError):
        if conjure_error.error_name == FML_LIVE_INFERENCE_EXCEPTION:
            if (
                "unsafeMessage" in conjure_error.parameters
                and conjure_error.parameters["unsafeMessage"] != "No unsafe message"
            ):
                unsafe_message = conjure_error.parameters["unsafeMessage"]
                return cls(unsafe_message, is_stack_trace="Traceback (most recent call last)" in unsafe_message)
            return cls(conjure_error)
        raise conjure_error

    def __str__(self):
        message = "Remote model inference failed. "
        if self.is_stack_trace:
            stack_trace_message = self.message.replace("Model inference executed unsuccessfully:", "")
            message += f"The stacktrace below has been proxied from the remote model deployment. This most likely indicates there is a bug in the model adapter.\n{stack_trace_message}"
        else:
            message += str(self.message)
        return message


class ArrowRemoteAdapter(ModelAdapter, abc.ABC):
    """
    Base remote adapter that handles setting up the api from a conjure api, and handles all inputs -> arrow bytes
    and arrow bytes -> output transformations. Subclasses only need to implement `_internal_arrow_transform`.
    """

    def __init__(self, conjure_model_api: ConjureModelApi, service_context):
        self._conjure_model_api = conjure_model_api
        self.__internal_model_api = convert_conjure_model_api_to_internal(conjure_model_api)
        self._service_context = service_context

    @classmethod
    def load(cls, state_reader):
        # no-op for remote adapter
        return None

    def save(self, state_writer) -> None:
        # throwing as users should not try to publish these
        raise Exception("Remote adapters cannot be serialized.")

    @classmethod
    def api(cls):
        """
        No api defined here since calls to get the api for an adapter
        call `_api()` which delegates to this method. We have the model api so we can
        just return it there.
        """
        return [], []

    def _initialize(self):
        """
        Subclasses can override to run initialization code which requires network access to sidecar.

        Should only be necessary for subclasses which don't always have network access to their sidecar on
        initialization (e.g. when running only executor sidecars). Will be called for each predict or run_inference
        call, so should likely be idempotent.
        """
        pass

    def _check_support_predict_method(self) -> None:
        if not len(self._api().outputs) == 1:
            raise PredictNotSupportedException(
                'Model api does not support calling "predict" because the api defines multiple outputs. Please use "transform" instead.'
            )

    def _api(self):
        return self.__internal_model_api

    @abc.abstractmethod
    def _internal_arrow_transform(self, arrow_bytes: BytesIO) -> bytes:
        """
        Given some arrow bytes, send to live/mice container and return the response bytes
        """

    def _input_arrow_bytes_hook(self, arrow_bytes: BytesIO) -> BytesIO:
        """
        Hook for subclasses to modify/check the arrow bytes before sending to the model.
        """
        return arrow_bytes

    def predict(self, *args, **kwargs):
        self._initialize()
        self._check_support_predict_method()
        inputs, output = self._preprocess_io_for_inference(args, kwargs)
        self.run_inference(inputs, output)
        return self._preprocess_outputs_for_predict(output)

    def run_inference(self, inputs: NamedTuple, outputs: NamedTuple) -> None:
        self._initialize()
        arrow_bytes = ArrowModelInputHandler(self._conjure_model_api).transform_inputs_to_arrow(inputs)
        arrow_bytes = self._input_arrow_bytes_hook(arrow_bytes)
        results = self._internal_arrow_transform(arrow_bytes)

        output_tables = ArrowModelOutputHandler(self._conjure_model_api).get_arrow_outputs(results)
        for key, value in output_tables.items():
            # namedtuples dont support indexing
            writable_value = getattr(outputs, key)
            writable_value.write(value)

    def _preprocess_io_for_inference(self, args, kwargs) -> Tuple[NamedTuple, NamedTuple]:
        api_inputs = {io.name: io for io in self._api().inputs}

        mapped_arguments = _map_arguments(api_inputs, args, kwargs)
        # Binding args returns the _actual_ values, spark/pandas dataframe, parameter value, etc.
        return self._api().bind_args(mapped_arguments, context=self._service_context)

    def _preprocess_outputs_for_predict(self, output):
        inference_outputs = {name: output._return_result() for name, output in output._asdict().items()}
        return inference_outputs[self._api().outputs[0].name]
