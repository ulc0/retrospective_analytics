from __future__ import annotations
from pyspark.sql import types as T
import polars as pl
from typing import NamedTuple, List, Union
from ._schema import (
    PolarsOrSparkSchemaField,
    SparkOrPolarsSchema,
    PolarsOrSparkDataType,
)


class CastColumn(NamedTuple):
    old: PolarsOrSparkSchemaField
    new: PolarsOrSparkSchemaField


class SchemaDifference(NamedTuple):
    added_columns: List[PolarsOrSparkSchemaField]
    dropped_columns: List[PolarsOrSparkSchemaField]
    cast_columns: List[CastColumn]


def schema_difference(
    old_schema: SparkOrPolarsSchema, new_schema: SparkOrPolarsSchema
) -> SchemaDifference:
    """Computes the difference between two schemas and returns it in a structured format.
    We ignore the nullability and metadata of each schema, caring only about differences in the
    field names or their data types"""
    old_names = set(old_schema.fieldNames())
    new_names = set(new_schema.fieldNames())

    return SchemaDifference(
        added_columns=[new_schema[name] for name in new_names - old_names],
        dropped_columns=[old_schema[name] for name in old_names - new_names],
        cast_columns=[
            CastColumn(old_schema[name], new_schema[name])
            for name in old_names.intersection(new_names)
            if not compare_types(old_schema[name].dataType, new_schema[name].dataType)
        ],
    )


def compare_types(
    old_type: Union[PolarsOrSparkDataType],
    new_type: Union[PolarsOrSparkDataType],
) -> bool:
    """Evaluates whether two types are equal ignoring nullability.
    We must recurse over the higher order types, those being StructType, StructField, Map, and Array.
    For base types we can check for simple equality"""

    old_type_class = old_type.__class__
    new_type_class = new_type.__class__

    if old_type_class != new_type_class:
        return False

    if isinstance(old_type, T.StructField):
        return _compare_struct_fields(old_type, new_type)
    elif isinstance(old_type, T.StructType):
        return _compare_struct_types(old_type, new_type)
    elif isinstance(old_type, pl.Struct):
        return _compare_struct_fields_polars(old_type, new_type)
    elif isinstance(old_type, T.ArrayType):
        return _compare_array_types(old_type, new_type)
    elif isinstance(old_type, (pl.Array, pl.List)):
        return _compare_array_types_polars(old_type, new_type)
    elif isinstance(old_type, T.MapType):
        return _compare_map_types(old_type, new_type)

    return old_type == new_type


def _compare_struct_fields(old_type: T.StructField, new_type: T.StructField) -> bool:
    """Compare equality of T.StructFields"""
    return old_type.name == new_type.name and compare_types(
        old_type.dataType, new_type.dataType
    )


def _compare_struct_fields_polars(old_type: pl.Struct, new_type: pl.Struct) -> bool:
    """Compare equality of pl.Struct"""
    old_fields_dict = {field.name: field for field in old_type.fields}
    new_fields_dict = {field.name: field for field in new_type.fields}

    return old_fields_dict.keys() == new_fields_dict.keys() and all(
        compare_types(old_fields_dict[name], new_fields_dict[name])
        for name in old_fields_dict.keys()
    )


def _compare_struct_types(old_type: T.StructType, new_type: T.StructType) -> bool:
    """
    Recurse over the struct type by evaluating each struct field therein.
    """
    old_names = old_type.fieldNames()
    new_names = new_type.fieldNames()

    return old_names == new_names and all(
        compare_types(old_type[name].dataType, new_type[name].dataType)
        for name in old_names
    )


def _compare_array_types(old_type: T.ArrayType, new_type: T.ArrayType) -> bool:
    """Compare the element types of ArrayTypes"""
    return compare_types(old_type.elementType, new_type.elementType)


def _compare_array_types_polars(
    old_type: Union[pl.Array, pl.List], new_type: Union[pl.Array, pl.List]
) -> bool:
    """Compare the element types of polars list types"""

    if isinstance(old_type, pl.Array) and old_type.shape != new_type.shape:
        return False

    if old_type.inner.is_nested():
        return compare_types(old_type.inner, new_type.inner)
    else:
        # Polars represents inner non nested types as a private "DataTypeClass"
        # That class does not have the same properties as the public DataType class
        # and the only way to do a viable comparison is via the .is method
        return old_type.inner.is_(new_type.inner)


def _compare_map_types(old_type: T.MapType, new_type: T.MapType) -> bool:
    """Compare the key and value types of MapTypes"""
    return compare_types(old_type.keyType, new_type.keyType) and compare_types(
        old_type.valueType, new_type.valueType
    )
