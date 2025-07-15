#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import functools
from collections import Counter, OrderedDict, namedtuple
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union, get_origin

import pandas as pd
from models_api import models_api_modelapi as conjure_api

from .._types import ApiType, ModelIO, ServiceContext, UnassignedOutput, UserDefinedApiItem
from ._filesystem_io import FileSystemInput, FileSystemOutput
from ._media_io import MediaReference, MediaReferenceInput, ModelsMediaReference
from ._param_io import ParameterInput, ParameterOutput
from ._tabular_io import TabularInput, TabularOutput

# This map enables service-propagated column types to be referenced via more user-friendly imports
# eg. pm.MediaReference for both the API I/O type and a column type
_COLUMN_TYPE_MAP = {MediaReference: ModelsMediaReference}


class ModelApi(NamedTuple):
    """
    A description of the inputs and outputs for a model adapter.
    :param inputs: List of ApiType objects describing the name, type, and schema of model adapter inputs.
    :param outputs: List of ApiType objects describing the name, type, and schema of model adapter outputs.

    This is utilized by model deployment systems to determine what
    IO to bind to inputs/outputs.
    """

    inputs: List[ApiType]
    outputs: List[ApiType]

    @classmethod
    def from_user_api(
        cls, user_api_return_value: Union[Tuple[List, List], Tuple[Dict, Dict], "ModelApi"]
    ) -> "ModelApi":
        """
        Handle divergent return values from ModelAdapter.api().
        Returns a ModelApi instantiated with appropriate inputs/outputs,
        or throws if the return value is invalid.
        """
        if isinstance(user_api_return_value, ModelApi):
            return user_api_return_value
        if not isinstance(user_api_return_value, tuple):  # Received one output that is not a ModelApi
            raise ValueError("Unexpected return value of model adapter api(). Expected a ModelApi OR inputs, outputs")
        inputs, outputs = _get_api_io_from_tuple(user_api_return_value)
        return cls(inputs=inputs, outputs=outputs)

    @classmethod
    def from_service_api(cls, service_api: conjure_api.ModelApi) -> "ModelApi":
        """
        Converts from conjure-defined ModelApi to python ModelApi.
        This is not 100% accurate given the original conversion from python -> conjure is lossy,
        however any edge cases are handled by expanding the type of the object. For example,
        time and datetime are treated as "timestamp" in conjure. To ensure this doesnt break that assumption,
        those values are treated as Union[datetime, time] here.
        """
        return cls(
            inputs=[_from_service_api(inp) for inp in service_api.inputs],
            outputs=[_from_service_api(out) for out in service_api.outputs],
        )

    def to_service_api(self) -> conjure_api.ModelApi:
        """
        Converts this Api object to a conjure-defined ModelApi object
        :return: conjure-defined ModelApi object
        """
        return conjure_api.ModelApi(
            inputs=[inp._to_service_api() for inp in self.inputs],
            outputs=[out._to_service_api() for out in self.outputs],
        )

    def validate_api_io_or_throw(self) -> None:
        """
        Validate this API's input/output schema.
        Throws if checks fail.
        Checks to perform:
        - Duplicate input/output names
        - Duplicate column names
        - Invalid default parameter values
        """
        api_ios = self.inputs + self.outputs

        api_names = [api_io.name for api_io in api_ios]
        duplicate_names = {name for name, count in Counter(api_names).items() if count > 1}
        if duplicate_names:
            raise Exception(f"Duplicate names defined in model adapter API: {duplicate_names}.")

        for api_io in api_ios:
            if isinstance(api_io, (TabularInput, TabularOutput)):
                _validate_model_api_tabular_type(api_io)
            if isinstance(api_io, (ParameterInput, ParameterOutput)):
                _validate_model_api_parameter_type(api_io)

    def check_args_compatible_with_api_or_throw(self, kwargs: Dict[str, Any]) -> None:
        """
        Check if a dict of named arguments is compatible with this API's I/O schema.
        Throws if checks fail.
        Checks to perform:
        - Required input/output not found in kwargs
        - Provided named arguments not defined in API
        """
        # Required api-defined names not found in provided named arguments or mapped positional arguments
        required_names = [io.name for io in self.inputs + self.outputs if io.required]
        missing_names = [name for name in required_names if name not in kwargs.keys()]
        if missing_names:
            raise Exception(f"Required I/O defined in model adapter API not found in transform I/O: {missing_names}.")

        # Named arguments are not defined in the model adapter API
        api_names = {io.name for io in self.inputs + self.outputs}
        undefined_names = set(kwargs.keys()) - api_names
        if undefined_names:
            raise Exception(f"Provided named arguments not defined in model adapter API: {undefined_names}")

    def bind_args(
        self, mapped_arguments: OrderedDict, context: Optional[ServiceContext]
    ) -> Tuple[NamedTuple, NamedTuple]:
        """
        Binds named ModelIO arguments to API-requested types.
        Ex. TransformIO -> Spark DataFrame, requested with the following API input:
        ModelInput.Tabular(name="df_in", df_type=DFType.SPARK, ...)
        :param mapped_arguments: OrderedDict of api input/output name to input/output value
        :param maybe_auth_header: Auth header used to construct services for ServicePropagatedObjects
        return: NamedTuples of bound inputs and outputs
        """

        api_inputs = {io.name: io for io in self.inputs}
        api_outputs = {io.name: io for io in self.outputs}
        inputs_namedtuple = namedtuple(typename="ModelAdapterInputs", field_names=api_inputs.keys())  # type: ignore
        outputs_namedtuple = namedtuple(typename="ModelAdapterOutputs", field_names=api_outputs.keys())  # type: ignore

        bound_inputs = {
            name: api_input.bind(ModelIO.create(mapped_arguments[name]), context)
            for name, api_input in api_inputs.items()
        }

        if context:
            _bind_media_reference_to_tabular_column(bound_inputs, api_inputs, context)

        bound_outputs = {
            name: api_output.bind(
                ModelIO.create(mapped_arguments[name] if name in mapped_arguments else UnassignedOutput()),
                context,
            )
            for name, api_output in api_outputs.items()
        }

        inference_inputs = inputs_namedtuple(**bound_inputs)
        inference_outputs = outputs_namedtuple(**bound_outputs)
        return inference_inputs, inference_outputs


def _get_api_io_from_tuple(api_tuple: Tuple[Union[List, Dict, ModelApi], ...]) -> Tuple[List[ApiType], List[ApiType]]:
    if len(api_tuple) != 2:
        raise ValueError(
            f"Unexpected return value for api(). "
            f"Expected two values: 'inputs, outputs' but instead received {len(api_tuple)}"
        )
    if isinstance(api_tuple[0], list) and isinstance(api_tuple[1], list):
        return _get_api_io_from_lists(*api_tuple)  # type: ignore
    if isinstance(api_tuple[0], dict) and isinstance(api_tuple[1], dict):
        return _get_api_io_from_dicts(*api_tuple)  # type: ignore
    raise ValueError(
        f"Expected inputs, outputs to both be lists or dictionaries "
        f"but instead received {type(api_tuple[0])}, {type(api_tuple[1])}."
    )


def _get_api_io_from_lists(
    inputs_list: List[Union[ApiType, UserDefinedApiItem]], outputs_list: List[Union[ApiType, UserDefinedApiItem]]
) -> Tuple[List[ApiType], List[ApiType]]:
    inputs = []
    outputs = []

    def _throw():
        raise ValueError(
            "All elements of inputs, outputs lists must be api items with name parameters "
            "(e.g. pm.ModelInput.Tabular, pm.Pandas)."
        )

    for inp in inputs_list:
        if isinstance(inp, UserDefinedApiItem):
            inputs.append(inp.as_input())
        elif isinstance(inp, ApiType):
            inputs.append(inp)
        else:
            _throw()
    for out in outputs_list:
        if isinstance(out, UserDefinedApiItem):
            outputs.append(out.as_output())
        elif isinstance(out, ApiType):
            outputs.append(out)
        else:
            _throw()
    return inputs, outputs


def _get_api_io_from_dicts(
    inputs_dict: Dict[str, UserDefinedApiItem], outputs_dict: Dict[str, UserDefinedApiItem]
) -> Tuple[List[ApiType], List[ApiType]]:
    if not all(isinstance(key, str) for key in [*inputs_dict.keys(), *outputs_dict.keys()]):
        raise ValueError("All keys of inputs, outputs dictionaries must be strings.")
    if not all(isinstance(value, UserDefinedApiItem) for value in [*inputs_dict.values(), *outputs_dict.values()]):
        raise ValueError(
            "All values of inputs, outputs dictionaries must be ApiTypes without name parameters (e.g. pm.Pandas)."
        )
    inputs: List[ApiType] = [value.as_input(name) for name, value in inputs_dict.items()]
    outputs: List[ApiType] = [value.as_output(name) for name, value in outputs_dict.items()]
    return inputs, outputs


def _bind_media_reference_to_tabular_column(
    bound_inputs: Dict[str, Any], api_inputs: Dict[str, ApiType], service_context: ServiceContext
):
    for name, bound_input in bound_inputs.items():
        api_input = api_inputs.get(name)
        if isinstance(api_input, TabularInput):
            for column in api_input.columns:
                # Convert column.type to an internal type if the type map contains it,
                # otherwise continue with column.type unmodified
                column_type = _COLUMN_TYPE_MAP.get(column.type)  # type: ignore
                if not isinstance(column_type, type):
                    # Ex: type=typing.Any
                    continue
                if issubclass(column_type, ModelsMediaReference):
                    if isinstance(bound_input, pd.DataFrame):
                        input_instance = MediaReferenceInput(name=name)
                        bound_input[column.name] = bound_input[column.name].apply(
                            functools.partial(
                                _bind_column_element, media_input=input_instance, service_context=service_context
                            )
                        )
                    else:
                        raise RuntimeError(f"MediaReferences for type {column_type} only supported for Pandas input.")


def _bind_column_element(column_element, media_input: MediaReferenceInput, service_context: ServiceContext):
    return media_input.bind(io_object=ModelIO.create(column_element), context=service_context)


def _validate_model_api_tabular_type(tabular_type: Union[TabularInput, TabularOutput]) -> None:
    column_names = [col.name for col in tabular_type.columns]
    column_counts = Counter(column_names)
    duplicate_column_names = {name for name, count in column_counts.items() if count > 1}

    if duplicate_column_names:
        raise ValueError(
            f"Duplicate column names detected in '{tabular_type.name}' of tabular type: "
            f"{', '.join(duplicate_column_names)}."
        )


def _validate_model_api_parameter_type(parameter: Union[ParameterInput, ParameterOutput]) -> None:
    if parameter.default is None:
        return

    parameter_type = get_origin(parameter.type) or parameter.type
    if not isinstance(parameter.default, parameter_type):
        raise ValueError(
            f"Invalid default value for parameter '{parameter.name}'. "
            f"Expected type {parameter_type}, but received {type(parameter.default)}."
        )


def _from_service_api(service_api_type) -> "ApiType":
    if isinstance(service_api_type, conjure_api.ModelApiInput):
        return _from_model_api_input(service_api_type)
    if isinstance(service_api_type, conjure_api.ModelApiOutput):
        return _from_model_api_output(service_api_type)

    raise Exception("expected ModelApiInput or ModelApiOutput")


def _from_model_api_input(service_api_type: conjure_api.ModelApiInput) -> ApiType:
    if service_api_type.type.media_reference is not None:
        return MediaReferenceInput._from_service_api(service_api_type)
    if service_api_type.type.filesystem is not None:
        return FileSystemInput._from_service_api(service_api_type)
    if service_api_type.type.tabular is not None:
        return TabularInput._from_service_api(
            service_api_type=service_api_type,
        )
    if service_api_type.type.parameter is not None:
        return ParameterInput._from_service_api(
            service_api_type=service_api_type,
        )
    raise Exception(f'unknown api input type "{service_api_type.type}"')


def _from_model_api_output(service_api_type: conjure_api.ModelApiOutput) -> ApiType:
    if service_api_type.type.filesystem is not None:
        return FileSystemOutput._from_service_api(service_api_type)
    if service_api_type.type.tabular is not None:
        service_api_type.type.tabular
        return TabularOutput._from_service_api(
            service_api_type=service_api_type,
        )
    if service_api_type.type.parameter is not None:
        return ParameterOutput._from_service_api(
            service_api_type=service_api_type,
        )
    raise Exception(f'unknown api output type "{service_api_type.type}"')
