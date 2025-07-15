import json
from abc import abstractmethod
from typing import Any, Dict, Mapping, Optional, Set, Union

from ._param import Param

DESCRIPTION = "description"
ALLOWED_VALUES = "allowedValues"


class ParameterSpecInput(Param):
    _TYPE = None

    def __init__(self, *, description: str = None):
        """Specification to ParameterSpec definition used as an input to a transform.
        Args:
            description (str, optional): Parameter Description
        """
        super().__init__(description=description)
        self.description = description

    @property
    def json_value(self) -> Union[Dict[str, Any], None]:
        if self._TYPE is None:
            raise ValueError(
                "TYPE for parameter spec is not defined, the subclass must declare its parameter type"
            )
        val = {
            "type": {
                "type": self._TYPE,
                self._TYPE: self._render_parameter_default_value(),
            }
        }

        if self.description is not None:
            val[DESCRIPTION] = self.description

        return val

    @staticmethod
    def instance(context, json_value: Mapping[str, Any], runtime_override=None):
        override_value = (
            runtime_override[runtime_override["type"]]
            if runtime_override is not None
            else None
        )
        parameter_type = json_value["type"]
        default_value = parameter_type[parameter_type["type"]]["default"]
        return ParamValueInput(
            override_value if override_value is not None else default_value
        )

    @abstractmethod
    def _render_parameter_default_value(self):
        raise NotImplementedError(
            "Abstract method that should be overridden by a subclass"
        )


class BooleanParam(ParameterSpecInput):
    _TYPE = "boolean"

    def __init__(self, default: bool, *, description: str = None):
        """Specification to ParameterSpec definition used as an input to a transform.
        Args:
            parameter_id (str): a string value given to identify the parameter spec and get its associated value.
            default (bool): the default value for the parameter.
            description (str, optional): Parameter Description
        """
        super().__init__(description=description)
        self.default = default

    def _render_parameter_default_value(self):
        return {"default": json.dumps(self.default)}


class IntegerParam(ParameterSpecInput):
    _TYPE = "integer"

    def __init__(self, default: int, *, description: str = None):
        """Specification to ParameterSpec definition used as an input to a transform.
        Args:
            parameter_id (str): a string value given to identify the parameter spec and get its associated value.
            default (int): the default value for the parameter.
            description (str, optional): Parameter Description
        """
        super().__init__(description=description)
        self.default = default

    def _render_parameter_default_value(self):
        return {"default": self.default}


class StringParam(ParameterSpecInput):
    _TYPE = "string"

    def __init__(
        self,
        default: str,
        *,
        description: str = None,
        allowed_values: Optional[Set[str]] = None,
    ):
        """Specification to ParameterSpec definition used as an input to a transform.
        Args:
            parameter_id (str): a string value given to identify the parameter spec and get its associated value.
            default (str): the default value for the parameter.
            allowed_values (Optional[Set[str]]): allowed values for the parameter, as a set of strings.
            description (str, optional): Parameter Description
        """
        super().__init__(description=description)
        self.default = default
        if default == "":
            raise ValueError("Default value for StringParam cannot be an empty string")

        self.allowed_values = allowed_values
        if allowed_values is not None and default not in allowed_values:
            raise ValueError(
                "Default value must also be in allowed values for StringParam"
            )

    def _render_parameter_default_value(self):
        val = {"default": self.default}
        if self.allowed_values is not None:
            val[ALLOWED_VALUES] = list(self.allowed_values)
        return val


class FloatParam(ParameterSpecInput):
    # Python floats are IEEE 754 double precisions which is typed as "double" in conjure
    # https://github.palantir.build/foundry/shrinkwrap-service/blob/develop/transforms-user-defined-parameter-api
    # /src/main/conjure/user-defined-parameter-api.yml#L53
    _TYPE = "double"

    def __init__(self, default: float, *, description: str = None):
        """Specification to ParameterSpec definition used as an input to a transform.
        Args:
            parameter_id (str): a string value given to identify the parameter spec and get its associated value.
            default (float): the default value for the parameter.
            description (str, optional): Parameter Description
        """
        super().__init__(description=description)
        self.default = default

    def _render_parameter_default_value(self):
        return {"default": self.default}


class ParamValueInput:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value
