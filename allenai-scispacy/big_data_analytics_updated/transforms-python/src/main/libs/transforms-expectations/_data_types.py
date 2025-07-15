#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from typing import Dict, Any, Union
from pyspark.sql import types as T
import polars as pl
from transforms.expectations.wrapper import (
    PolarsOrSparkDataType,
    is_spark_data_type,
    is_polars_data_type,
)


# See https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/types/DataTypes.html
# + https://github.palantir.build/foundry/data-health/blob/develop/data-health-api/src/main/conjure/expectation-api.yml
def get_conjure_data_type(
    data_type: PolarsOrSparkDataType,
) -> Dict[str, Any]:
    """Get Conjure representation of spark/polars data type."""
    if is_spark_data_type(data_type):
        return get_conjure_spark_data_type(data_type)
    elif is_polars_data_type(data_type):
        return get_conjure_polars_data_type(data_type)
    else:
        raise TypeError(
            "Expected either a spark or polars data type, instead got: ", data_type
        )


def get_conjure_polars_data_type(data_type: pl.DataType) -> Dict[str, Any]:
    # https://docs.pola.rs/api/python/stable/reference/datatypes.html

    if verify_polars_data_type(data_type, pl.Binary):
        return _primitive_type("BINARY")
    elif verify_polars_data_type(data_type, pl.Boolean):
        return _primitive_type("BOOLEAN")
    elif verify_polars_data_type(data_type, pl.Int8):
        return _primitive_type("BYTE")
    elif verify_polars_data_type(data_type, pl.Date):
        return _primitive_type("DATE")
    elif verify_polars_data_type(data_type, pl.Float64):
        return _primitive_type("DOUBLE")
    elif verify_polars_data_type(data_type, pl.Float32):
        return _primitive_type("FLOAT")
    if verify_polars_data_type(data_type, pl.Int32):
        return _primitive_type("INTEGER")
    elif verify_polars_data_type(data_type, pl.Int64):
        return _primitive_type("LONG")
    elif verify_polars_data_type(data_type, pl.Null):
        return _primitive_type("NULL_TYPE")
    elif verify_polars_data_type(data_type, pl.Int16):
        return _primitive_type("SHORT")
    elif verify_polars_data_type(data_type, pl.String):
        return _primitive_type("STRING")
    elif verify_polars_data_type(data_type, pl.Utf8):
        return _primitive_type("STRING")
    elif verify_polars_data_type(data_type, pl.Datetime):
        return _primitive_type("TIMESTAMP")
    elif verify_polars_data_type(data_type, pl.Decimal):
        return _decimal_type(data_type)
    elif verify_polars_data_type(data_type, pl.Struct):
        return _polars_struct_type(data_type)
    elif verify_polars_data_type(data_type, pl.Field):
        return _polars_struct_field(data_type)
    elif verify_polars_data_type(data_type, pl.Array):
        return _polars_array_type(data_type)
    # now polars types with no direct equivalent to spark types
    elif verify_polars_data_type(data_type, pl.List):
        return _polars_array_type(data_type)
    elif verify_polars_data_type(data_type, pl.UInt64):
        return _primitive_type("UNSIGNED_LONG")
    elif verify_polars_data_type(data_type, pl.UInt32):
        return _primitive_type("UNSIGNED_INTEGER")
    elif verify_polars_data_type(data_type, pl.UInt16):
        return _primitive_type("UNSIGNED_SHORT")
    elif verify_polars_data_type(data_type, pl.UInt8):
        return _primitive_type("UNSIGNED_BYTE")
    elif verify_polars_data_type(data_type, pl.Time):
        return _primitive_type("TIME")
    elif verify_polars_data_type(data_type, pl.Duration):
        return _primitive_type("DURATION")
    elif verify_polars_data_type(data_type, pl.Categorical):
        return _primitive_type("CATEGORICAL")
    elif verify_polars_data_type(data_type, pl.Enum):
        return _primitive_type("ENUM")

    return (
        None if not data_type else {"type": "unknown", "unknown": str(type(data_type))}
    )


def verify_polars_data_type(data_type: pl.DataType, expected_type):
    """
    Verify if the polars data type is of the given class.
    Polars represents data types as either 'DataType' which are instances of types or as
    'DataTypeClass' which are just class references. These must be handled differently.
    """
    if isinstance(data_type, pl.DataType):
        return isinstance(data_type, expected_type)
    elif isinstance(data_type, pl.datatypes.classes.DataTypeClass):
        return data_type.is_(expected_type)
    else:
        raise TypeError(
            "Polars type must either be DataType or DataTypeClass. Instead got",
            data_type,
        )


def get_conjure_spark_data_type(data_type: T.DataType) -> Dict[str, Any]:
    if isinstance(data_type, T.BinaryType):
        return _primitive_type("BINARY")
    elif isinstance(data_type, T.BooleanType):
        return _primitive_type("BOOLEAN")
    elif isinstance(data_type, T.ByteType):
        return _primitive_type("BYTE")
    elif isinstance(data_type, T.DateType):
        return _primitive_type("DATE")
    elif isinstance(data_type, T.DoubleType):
        return _primitive_type("DOUBLE")
    elif isinstance(data_type, T.FloatType):
        return _primitive_type("FLOAT")
    elif isinstance(data_type, T.IntegerType):
        return _primitive_type("INTEGER")
    elif isinstance(data_type, T.LongType):
        return _primitive_type("LONG")
    elif isinstance(data_type, T.NullType):
        return _primitive_type("NULL_TYPE")
    elif isinstance(data_type, T.ShortType):
        return _primitive_type("SHORT")
    elif isinstance(data_type, T.StringType):
        return _primitive_type("STRING")
    elif isinstance(data_type, T.TimestampType):
        return _primitive_type("TIMESTAMP")
    elif isinstance(data_type, T.DecimalType):
        return _decimal_type(data_type)
    elif isinstance(data_type, T.StructType):
        return _struct_type(data_type)
    elif isinstance(data_type, T.StructField):
        return _struct_field(data_type)
    elif isinstance(data_type, T.ArrayType):
        return _array_type(data_type)
    elif isinstance(data_type, T.MapType):
        return _map_type(data_type)

    return (
        None if not data_type else {"type": "unknown", "unknown": data_type.typeName()}
    )


def _primitive_type(primitive_type: str):
    return {"type": "primitive", "primitive": primitive_type}


def _map_type(map_type: T.MapType):
    return {
        "type": "map",
        "map": {
            "keyType": get_conjure_data_type(map_type.keyType),
            "valueType": get_conjure_data_type(map_type.valueType),
            "valueContainsNull": map_type.valueContainsNull,
        },
    }


def _decimal_type(decimal_type: Union[pl.Decimal, T.DecimalType]):
    return {
        "type": "decimal",
        "decimal": {"precision": decimal_type.precision, "scale": decimal_type.scale},
    }


def _struct_type(struct_type: T.StructType):
    return {
        "type": "struct",
        "struct": {
            "fields": [_internal_struct_field(field) for field in struct_type.fields]
        },
    }


def _polars_struct_type(struct_type: pl.Struct):
    return {
        "type": "struct",
        "struct": {
            "fields": [
                _polars_internal_struct_field(field) for field in struct_type.fields
            ]
        },
    }


def _struct_field(field_type: T.StructField):
    return {"type": "field", "field": _internal_struct_field(field_type)}


def _polars_struct_field(field_type: pl.Field):
    return {"type": "field", "field": _polars_internal_struct_field(field_type)}


def _internal_struct_field(field_type: T.StructField):
    return {
        "name": field_type.name,
        "fieldType": get_conjure_data_type(field_type.dataType),
        "nullable": field_type.nullable,
    }


def _polars_internal_struct_field(field_type: pl.Field):
    return {
        "name": field_type.name,
        "fieldType": get_conjure_data_type(field_type.dtype),
        "nullable": True,
    }


def _array_type(data_type: T.ArrayType):
    return {
        "type": "array",
        "array": {
            "elementType": get_conjure_data_type(data_type.elementType),
            "containsNull": data_type.containsNull,
        },
    }


def _polars_array_type(data_type: Union[pl.Array, pl.List]):
    return {
        "type": "array",
        "array": {
            "elementType": get_conjure_data_type(data_type.inner),
            "containsNull": not data_type.inner,
        },
    }


def get_simple_string_representation_of_type(data_type: PolarsOrSparkDataType):
    """Get Conjure representation of the data type."""
    if is_spark_data_type(data_type):
        return get_simple_string_representation_of_spark_type(data_type)
    elif is_polars_data_type(data_type):
        return get_simple_string_representation_of_polars_type(data_type)
    else:
        raise TypeError(
            "Expected either a spark or polars data type, instead got: ", data_type
        )


def get_simple_string_representation_of_spark_type(data_type: T.DataType):
    if data_type is T.BinaryType:
        return "BINARY"
    elif data_type is T.BooleanType:
        return "BOOLEAN"
    elif data_type is T.ByteType:
        return "BYTE"
    elif data_type is T.DateType:
        return "DATE"
    elif data_type is T.DoubleType:
        return "DOUBLE"
    elif data_type is T.FloatType:
        return "FLOAT"
    elif data_type is T.IntegerType:
        return "INTEGER"
    elif data_type is T.LongType:
        return "LONG"
    elif data_type is T.NullType:
        return "NULL_TYPE"
    elif data_type is T.ShortType:
        return "SHORT"
    elif data_type is T.StringType:
        return "STRING"
    elif data_type is T.TimestampType:
        return "TIMESTAMP"
    elif data_type is T.DecimalType:
        return "DECIMAL"
    elif data_type is T.StructType:
        return "STRUCT"
    elif data_type is T.StructField:
        return "FIELD"
    elif data_type is T.ArrayType:
        return "ARRAY"
    elif data_type is T.MapType:
        return "MAP"

    return None if not data_type else "UNKNOWN"


def get_simple_string_representation_of_polars_type(data_type: pl.DataType):
    if verify_polars_data_type(data_type, pl.Binary):
        return "BINARY"
    elif verify_polars_data_type(data_type, pl.Boolean):
        return "BOOLEAN"
    elif verify_polars_data_type(data_type, pl.Int8):
        return "BYTE"
    elif verify_polars_data_type(data_type, pl.Date):
        return "DATE"
    elif verify_polars_data_type(data_type, pl.Float64):
        return "DOUBLE"
    elif verify_polars_data_type(data_type, pl.Float32):
        return "DOUBLE"
    if verify_polars_data_type(data_type, pl.Int32):
        return "INTEGER"
    elif verify_polars_data_type(data_type, pl.Int64):
        return "LONG"
    elif verify_polars_data_type(data_type, pl.Null):
        return "NULL_TYPE"
    elif verify_polars_data_type(data_type, pl.Int16):
        return "SHORT"
    elif verify_polars_data_type(data_type, pl.String):
        return "STRING"
    elif verify_polars_data_type(data_type, pl.Utf8):
        return "STRING"
    elif verify_polars_data_type(data_type, pl.Datetime):
        return "TIMESTAMP"
    elif verify_polars_data_type(data_type, pl.Struct):
        return "STRUCT"
    elif verify_polars_data_type(data_type, pl.Array):
        return "ARRAY"
    elif verify_polars_data_type(data_type, pl.List):
        return "ARRAY"

    return None if not data_type else "UNKNOWN"
