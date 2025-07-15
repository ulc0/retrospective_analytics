#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines the model adapter"""

import abc
import inspect
from collections import OrderedDict
from inspect import Parameter, Signature
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import models_api.models_api_executable as executable_api
import models_api.models_api_models as models_api

from ._state_accessors import (
    AbstractModelStateReader,
    ModelStateWriter,
    SubdirectoryModelStateReader,
    SubdirectoryModelStateWriter,
)
from .api._types import ServiceContext
from .api.user._model_api import ModelApi
from .api.user._param_io import ParameterInput, ParameterOutput
from .api.user._tabular_io import TabularOutput

CUSTOM_SAVE_LOAD_NOT_SUPPORTED_ERR_MSG = """\
Incorrect usage of @auto_serializer. ModelAdapters cannot have both the @auto_serializer decorator
and custom save() and load() methods.
"""
ADAPTER_NOT_COMPATIBLE_WITH_PREDICT_ERR_MSG = """\
Model adapters whose API contains outputs whose type is neither tabular nor parameter 
must define custom run_inference() logic.
"""

_ModelAdapter = TypeVar("_ModelAdapter", bound="ModelAdapter")


class ModelAdapterConfigurationException(Exception):
    """
    Exception raised when the user has configured a Model Adapter incorrectly, e.g not overriding the necessary methods
    or using the auto_serializer decorator incorrectly.
    """


class InferenceResults:
    """
    Class to access results from running inference using a model.
    Results are accessible as either attributes or items as follows:
        - inference_results.key -> value
        - inference_results["key"] -> value
    """

    __lock: bool = False

    def __init__(self, results_dict: dict):
        self.__results = results_dict.copy()
        self.__lock = True

    def asdict(self):
        return self.__results

    def __getitem__(self, item):
        return self.__results[item]

    def __setitem__(self, item, value):
        raise AttributeError("Inference results are immutable")

    def __getattr__(self, item):
        if item in self.__results:
            return self.__results[item]
        return super().__getattribute__(item)

    def __setattr__(self, key, value):
        if not self.__lock:
            super().__setattr__(key, value)
        else:
            raise AttributeError("Inference results are immutable")


class ModelAdapter(abc.ABC):
    """
    Base class for a foundry Model Adapter.
    """

    _service_context: Optional[ServiceContext] = None
    __model_version: Optional[str] = None

    # set by auto_serializer decorator
    __serializable_args__: Dict[str, Any] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # If __module__ == "__main__", the ModelAdapter is defined in the file which is being run, likely due to this
        # being a lightweight transform. If that is the case, we can get and set the module name if python was invoked
        # via `python3 -m this.module.name`. If python was invoked some other way we leave __module__ = "__main__" to
        # avoid weird breaks.
        if cls.__module__ == "__main__":
            module_spec = inspect.getmodule(cls).__spec__
            if module_spec:
                cls.__module__ = module_spec.name

        # validates that the user has not overridden the save() or load() methods when using @auto_serialize
        if hasattr(cls.__init__, "__auto_serializers__"):
            if cls.save != ModelAdapter.save or cls.load.__func__ != ModelAdapter.load.__func__:
                raise ModelAdapterConfigurationException(CUSTOM_SAVE_LOAD_NOT_SUPPORTED_ERR_MSG)

        # any adapter defined inside palantir_models should know how to properly implement the interface.
        # and can ignore the next check. added for the remote inference adapter which allows both predict and run_inference
        if cls.__module__.startswith("palantir_models"):
            return

        # if run_inference() was not overridden, validate that the api is compatible with predict()
        if not inspect.isabstract(cls) and cls.run_inference == ModelAdapter.run_inference:
            api = ModelApi.from_user_api(cls.api())
            if any([not isinstance(api_output, (TabularOutput, ParameterOutput)) for api_output in api.outputs]):
                raise ModelAdapterConfigurationException(ADAPTER_NOT_COMPATIBLE_WITH_PREDICT_ERR_MSG)

    @property
    def model_version_rid(self):
        """
        Returns none if no model version rid has been registered.
        """
        if self.__model_version is None:
            return None
        return self.__model_version

    @classmethod
    def load(
        cls: Type[_ModelAdapter],
        state_reader: AbstractModelStateReader,
        container_context: Optional[executable_api.ContainerizedApplicationContext] = None,
        external_model_context: Optional[executable_api.ExternalModelExecutionContext] = None,
    ) -> _ModelAdapter:
        """
        Factory method for an model adapter, called by transforms to load a model asset.
        :param state_reader: ModelStateReader object to extract the contents of the associated model.zip.
        :param container_context: The optional container context object for the model version. The context object
        includes a mapping from container name to service uris & the shared directory mount path. It is only present
        if the model version has a container source type, otherwise it will be 'None'.
        :param external_model_context: The optional external_model context object for the model version.
        The context contains credentials, and connection information for the externally hosted model.
        :return: A ModelAdapter for the given model.
        """
        if serializers := cls._get_auto_serializers():
            deserialized_args = {
                arg_name: serializer.deserialize(SubdirectoryModelStateReader(arg_name, state_reader))
                for arg_name, serializer in serializers.items()
            }
            return cls(**deserialized_args)

        raise ModelAdapterConfigurationException(
            "ModelAdapter.load() method not overridden and no auto_serializer decorator detected."
        )

    def save(self, state_writer: ModelStateWriter) -> None:
        """
        Save a locally available file or directory to this model version's artifacts repository
        :param state_writer: ModelStateWriter object to write local files to artifacts
        """
        if serializers := self._get_auto_serializers():
            for arg_name, arg_value in self.__serializable_args__.items():
                serializers[arg_name].serialize(SubdirectoryModelStateWriter(arg_name, state_writer), arg_value)
            return

        raise ModelAdapterConfigurationException(
            "ModelAdapter.save() method not overridden and no auto_serializer decorator detected."
        )

    @classmethod
    def _get_auto_serializers(cls):
        return getattr(cls.__init__, "__auto_serializers__", None)

    def _get_auto_serializable_args(self) -> Dict[str, Any]:
        return self.__serializable_args__

    @classmethod
    @abc.abstractmethod
    def api(cls) -> Union[ModelApi, Tuple[List, List], Tuple[Dict, Dict]]:
        """
        Defines inputs/outputs of this model adapter's run_inference method.
        Inputs and outputs can be defined as two lists or two dictionaries of input/output types.
        For lists, each input or output must be provided a name.
        For dictionaries, names must not be provided as the key value will be used as the name instead.
        :return: ModelApi object for the model, or a tuple of inputs, outputs
        """

    def _api(self) -> ModelApi:
        """
        Internal method to handle divergent api() return values. Call this method
        directly instead of .api().
        """
        return ModelApi.from_user_api(self.api())

    def run_inference(self, inputs, outputs) -> None:
        """
        Runs inference on the associated model using the input
        and output objects defined in this model adapter's API.
        :param inputs: NamedTuple of input objects.
        :param outputs: NamedTuple of output objects.
        """
        predict_outputs = self.predict(**inputs._asdict())
        if isinstance(predict_outputs, tuple):
            assert len(predict_outputs) == len(
                outputs
            ), "predict() must return a result for each output defined in the model API"
            for output, predict_output in zip(outputs, self.predict(**inputs._asdict())):
                output.write(predict_output)
        else:
            outputs[0].write(predict_outputs)

    def predict(self, *args, **kwargs):
        """
        Runs a prediction step on provided inputs, returning a single tabular or parameter output as a result.
        The signature of this method should include an argument for each input defined in the API using the same name.
        This method is callable by run_inference in the case of single-output adapters.
        :return: A pandas/spark dataframe or parameter containing the result of the inference step.
        """
        raise Exception(f"{self.__class__.__name__} does not implement predict.")

    def run_inference_stream(self, inputs) -> Iterable:
        """
        Runs inference on the associated model using the input
        objects defined in this model adapter's API. Must return
        an iterable stream of objects that can be serialized into
        json strings using the json.dumps() method.

        :param inputs: NamedTuple of input objects.
        """
        raise Exception(f"{self.__class__.__name__} does not implement run_inference_stream.")

    def transform(self, *args: Any, **kwargs: Any) -> InferenceResults:
        """
        Applies the model's logic to the input arguments.
        Calls run_inference() method with the provided arguments, and
        returns the inference results. Arguments are bound to their
        expected inputs and outputs as defined in the API before being
        passed to run_inference().
        """
        api_inputs = {io.name: io for io in self._api().inputs}
        mapped_arguments = _map_arguments(api_inputs, args, kwargs)
        inputs, outputs = self._api().bind_args(mapped_arguments, context=self._service_context)
        self.run_inference(inputs, outputs)
        inference_outputs = {name: output._return_result() for name, output in outputs._asdict().items()}
        return InferenceResults(inference_outputs)

    def transform_write(self, *args: Any, auth_header: Optional[str] = None, **kwargs: Any) -> None:
        """
        Applies the model's logic to the input arguments and writes
        inference results to the output arguments.
        Calls run_inference() method with the provided arguments, and
        writes inference results to their respective outputs. Arguments
        are bound to their expected inputs and outputs as defined in the
        API before being passed to run_inference().
        """
        if auth_header is not None:  # Users should not be able to override build tokens.
            raise ValueError(
                "transform_write no longer supports an auth_header override. Please remove the provided token."
            )

        api_io = {io.name: io for io in self._api().inputs + self._api().outputs}
        mapped_arguments = _map_arguments(api_io, args, kwargs)
        inputs, outputs = self._api().bind_args(mapped_arguments, context=self._service_context)
        self.run_inference(inputs, outputs)
        for output in outputs:
            output._perform_write()

    def transform_stream(self, *args: Any, auth_header: Optional[str] = None, **kwargs: Any) -> Iterable:
        """
        Applies the model's logic to the input arguments returns a
        response stream generator.
        Arguments are bound to their expected inputs as defined in the
        API before being passed to run_inference_stream().
        """
        if auth_header is not None:  # Users should not be able to override build tokens.
            raise ValueError(
                "transform_stream no longer supports an auth_header override."
                "Please remove the provided header value."
            )

        api_inputs = {io.name: io for io in self._api().inputs}
        mapped_arguments = _map_arguments(api_inputs, args, kwargs)
        inputs, _ = self._api().bind_args(mapped_arguments, context=self._service_context)
        return self.run_inference_stream(inputs)

    def to_service_api(self) -> models_api.ModelAdapter:
        """
        Converts the object to its respective conjure-defined object
        :return: conjure-defined model adapter object
        """
        return models_api.ModelAdapter(
            models_api.PythonCondaModelAdapter(
                environment=models_api.CondaEnvironment(conda_package_dependencies=[]),
                locator=models_api.PythonLocator(module=self.__class__.__module__, name=self.__class__.__name__),
            )
        )

    def _register_service_context(self, service_context: ServiceContext):
        self._service_context = service_context

    def _register_input_model_version(self, model_version_rid: str):
        self.__model_version = model_version_rid


def _map_arguments(api_io_dict, args, kwargs) -> OrderedDict:
    """
    Map positional and keyword arguments to the provided api input/output objects.
    :return: OrderedDict of mapped arguments.
    """
    signature = Signature([Parameter(name, Parameter.POSITIONAL_OR_KEYWORD) for name in api_io_dict.keys()])
    mapped_args = signature.bind_partial(*args, **kwargs).arguments

    # Apply parameter defaults if not provided in bound input args
    for io_name, io_obj in api_io_dict.items():
        if io_name not in mapped_args and isinstance(io_obj, ParameterInput):
            mapped_args[io_name] = io_obj.default

    return mapped_args
