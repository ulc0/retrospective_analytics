#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
from datetime import date, datetime, time
from typing import Any, Dict, List, NamedTuple, Set, Tuple, TypeVar, Union

from models_api import models_api_modelapi as conjure_api
from numpy import double

from palantir_models.models.api.user._media_io import MediaReference, ModelsMediaReference


class ModelApiStructField(NamedTuple):
    """
    NamedTuple for struct fields.
    Compatible with the conjure API struct field definition.
    """

    name: str
    type: type
    required: bool = True


class ModelApiStruct(NamedTuple):
    """
    NamedTuple for structs.
    Compatible with the conjure API struct definition.
    """

    fields: List[ModelApiStructField]


def convert_to_conjure_value_type(
    python_api_type: Union[type, Any, ModelApiStruct],
) -> conjure_api.ModelApiValueType:
    """
    Convert the provided api type to its conjure-defined representation.
    :param python_api_type: Type to convert
    :return: Conjure defined ModelApiValueType object
    """
    converted_type = None
    if isinstance(python_api_type, ModelApiStruct):
        converted_type = _handle_struct(python_api_type)
    elif hasattr(python_api_type, "__origin__"):
        converted_type = _handle_type_alias(python_api_type)
    else:
        converted_type = _handle_simple_type(python_api_type)

    if not converted_type:
        raise Exception(f"Unable to convert type {python_api_type} to ModelApiValueType")

    return converted_type


def _handle_struct(struct_type: ModelApiStruct):
    struct_fields = struct_type.fields
    api_struct_fields: List[conjure_api.StructField] = []
    for struct_field in struct_fields:
        api_struct_fields.append(
            conjure_api.StructField(
                struct_field.name, struct_field.required, convert_to_conjure_value_type(struct_field.type)
            )
        )
    return conjure_api.ModelApiValueType(struct=conjure_api.StructType(fields=api_struct_fields))


def _handle_type_alias(type_alias):
    if type_alias._name in ["List", "Set"]:
        subtype = type_alias.__args__[0] if hasattr(type_alias, "__args__") else None
        if getattr(subtype, "_name", None) == "Dict":
            raise Exception(f"Dict type cannot be nested in {type_alias._name} type")
        if not subtype or isinstance(subtype, TypeVar):
            return conjure_api.ModelApiValueType(
                array=conjure_api.ArrayType(conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()))
            )
        return conjure_api.ModelApiValueType(array=conjure_api.ArrayType(convert_to_conjure_value_type(subtype)))
    if type_alias._name in ["Tuple"]:
        subtype = type_alias.__args__[0] if hasattr(type_alias, "__args__") and len(type_alias.__args__) == 1 else None
        if getattr(subtype, "_name", None) == "Dict":
            raise Exception(f"Dict type cannot be nested in {type_alias._name} type")
        if not subtype:
            return conjure_api.ModelApiValueType(
                array=conjure_api.ArrayType(conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()))
            )
        return conjure_api.ModelApiValueType(array=conjure_api.ArrayType(convert_to_conjure_value_type(subtype)))
    if type_alias._name in ["Dict"]:
        key_type, value_type = type_alias.__args__ if hasattr(type_alias, "__args__") else (Any, Any)
        if getattr(key_type, "_name", None) == "Dict" or getattr(value_type, "_name", None) == "Dict":
            raise Exception(f"Dict type cannot be nested in {type_alias._name} type")
        if isinstance(key_type, TypeVar):
            return conjure_api.ModelApiValueType(
                map=conjure_api.MapType(
                    conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()),
                    conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()),
                )
            )
        return conjure_api.ModelApiValueType(
            map=conjure_api.MapType(convert_to_conjure_value_type(key_type), convert_to_conjure_value_type(value_type))
        )
    return None


def _handle_simple_type(python_api_type):
    # ruff: noqa: E721
    if python_api_type == bool:
        return conjure_api.ModelApiValueType(boolean=conjure_api.EmptyParameterType())
    if python_api_type == date:
        return conjure_api.ModelApiValueType(date=conjure_api.EmptyParameterType())
    if python_api_type in (double, float):
        return conjure_api.ModelApiValueType(double=conjure_api.EmptyParameterType())
    if python_api_type == int:
        return conjure_api.ModelApiValueType(integer=conjure_api.EmptyParameterType())
    if python_api_type == str:
        return conjure_api.ModelApiValueType(string=conjure_api.EmptyParameterType())
    if python_api_type == time:
        return conjure_api.ModelApiValueType(timestamp=conjure_api.EmptyParameterType())
    if python_api_type == datetime:
        return conjure_api.ModelApiValueType(timestamp=conjure_api.EmptyParameterType())
    if python_api_type == Any:
        return conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType())
    if python_api_type in [list, set, tuple]:
        return conjure_api.ModelApiValueType(
            array=conjure_api.ArrayType(conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()))
        )
    if python_api_type == dict:
        return conjure_api.ModelApiValueType(
            map=conjure_api.MapType(
                conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()),
                conjure_api.ModelApiValueType(any=conjure_api.EmptyParameterType()),
            )
        )
    if python_api_type in [MediaReference, ModelsMediaReference]:
        return conjure_api.ModelApiValueType(media_reference=conjure_api.EmptyParameterType())
    return None


def _value_type_to_python_type(value_type: conjure_api.ModelApiValueType) -> type:
    if value_type.any is not None:
        return Any  # type: ignore
    if value_type.boolean is not None:
        return bool
    if value_type.date is not None:
        return date
    if value_type.double is not None:
        return Union[float, double]  # type: ignore  # conversion from python -> double supports both
    if value_type.float is not None:
        return Union[float, double]  # type: ignore
    if value_type.integer is not None:
        return int
    if value_type.long is not None:
        return int
    if value_type.string is not None:
        return str
    if value_type.timestamp is not None:
        return Union[datetime, time]  # type: ignore
    if value_type.map is not None:
        return Dict[  # type: ignore
            _value_type_to_python_type(value_type.map.key_type), _value_type_to_python_type(value_type.map.value_type)
        ]
    if value_type.array is not None:
        return Union[  # type: ignore
            List[_value_type_to_python_type(value_type.array.value_type)],  # type: ignore
            Set[_value_type_to_python_type(value_type.array.value_type)],  # type: ignore
            Tuple[_value_type_to_python_type(value_type.array.value_type), ...],  # type: ignore
        ]

    raise Exception(f'Unknown type "{value_type.type}" when converting from service api to python api')
