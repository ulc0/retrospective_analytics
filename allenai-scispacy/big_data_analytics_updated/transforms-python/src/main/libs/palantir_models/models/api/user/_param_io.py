#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API Parameter Input and Output objects"""

from typing import Any, Optional

import models_api.models_api_modelapi as conjure_api

from .._types import ApiType, ModelIO, ServiceContext, UserDefinedApiItem, get_value_as_type
from .._util import _value_type_to_python_type, convert_to_conjure_value_type


class ParameterInput(ApiType):
    """
    An ApiType for parameters
    """

    __type: type
    __default: Any

    def __init__(self, *, name: str, type: type, default: Any = None, required: bool = True):
        super().__init__(name=name, required=required)
        self.__type = type
        self.__default = default

    @property
    def type(self):
        """The parameter's type"""
        return self.__type

    @property
    def default(self):
        """The parameter's default value"""
        return self.__default

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]):
        """
        Binds a ParameterIO object to an argument of the model adapter's run_inference() method
        :param io_object: The ParameterIO object
        :returns: The parameter input passed in by the user with the specified type
        Throws if the parameter value is incompatible with the specified type.
        """
        return get_value_as_type(io_object.parameter(), self.type, self._required)

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        api_input_type = conjure_api.ModelApiInputType(
            parameter=conjure_api.ModelApiParameterType(convert_to_conjure_value_type(self.type))
        )
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.parameter is not None, "must provide a parameter input type"
        return cls(
            name=service_api_type.name,
            required=service_api_type.required,
            default=None,
            type=_value_type_to_python_type(service_api_type.type.parameter.type),
        )


class ParameterOutput(ApiType):
    """
    An output ApiType for parameters
    """

    __type: type
    __default: Any

    def __init__(self, *, name: str, type: type, default: Any = None, required: bool = True):
        super().__init__(name=name, required=required)
        self.__type = type
        self.__default = default

    @property
    def type(self):
        """The parameter's type"""
        return self.__type

    @property
    def default(self):
        """The parameter's default value"""
        return self.__default

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]):
        """
        Binds a ParameterIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: The ParameterIO object
        :returns: A WritableParameterOutput
        """
        return io_object.writable_parameter(self.__type, self._required)

    def _to_service_api(self) -> conjure_api.ModelApiOutput:
        api_output_type = conjure_api.ModelApiOutputType(
            parameter=conjure_api.ModelApiParameterType(convert_to_conjure_value_type(self.__type))
        )
        return conjure_api.ModelApiOutput(name=self.name, required=self.required, type=api_output_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiOutput):
        assert service_api_type.type.parameter is not None, "must provide a parameter output type"
        return cls(
            name=service_api_type.name,
            required=service_api_type.required,
            default=None,
            type=_value_type_to_python_type(service_api_type.type.parameter.type),
        )


class Parameter(UserDefinedApiItem):
    """
    UserDefinedApiItem for a parameter. Is converted to a ParameterInput or a ParameterOutput.

    Supported init signatures for Parameter():
    - Parameter(float)
    - Parameter(name="param_name")
    - Parameter(name="param_name", param_type="float")
    """

    __type: type
    __default: Any

    def __init__(
        self,
        type: type = Any,  # type: ignore
        name: Optional[str] = None,
        default: Any = None,
        required: bool = True,
    ):
        super().__init__(name, required)
        self.__type = type
        self.__default = default

    def _to_input(self) -> ParameterInput:
        return ParameterInput(name=self.name, type=self.__type, default=self.__default, required=self.required)

    def _to_output(self) -> ParameterOutput:
        return ParameterOutput(name=self.name, type=self.__type, default=self.__default, required=self.required)
