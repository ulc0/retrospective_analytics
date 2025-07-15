from __future__ import annotations
from typing import Union, OrderedDict, NamedTuple, Dict
from pyspark.sql import types as T
import polars as pl
from transforms.expectations.wrapper._backend import GET_CURRENT_BACKEND, Backend
from ._types import PolarsOrSparkDataType, is_spark_data_type, is_polars_data_type

PolarsSchema = OrderedDict[str, pl.DataType]
SparkSchema = T.StructType


def return_type_depending_on_backend(
    spark_type: T.DataType, polars_type: pl.DataType
) -> PolarsOrSparkDataType:
    if GET_CURRENT_BACKEND() == Backend.SPARK:
        return spark_type
    else:
        return polars_type


class PolarsSchemaField(NamedTuple):
    """
    Spark represents each schema field using a StructType object, which contains name and datatype
    For polars, the whole schema is a dict [str, Datatype]. To replicate behaviour, we wrap each
    polars schema field in a NamedTuple with name and datatype."""

    name: str
    dataType: pl.DataType


PolarsOrSparkSchemaField = Union[T.StructField, PolarsSchemaField]


class PolarsSchemaWrapper:
    def __init__(self, schema: PolarsSchema):
        self.schema = schema

    def __getitem__(self, key) -> PolarsSchemaField:
        name = key
        datatype = self.schema[key]
        return PolarsSchemaField(name, datatype)

    def fieldNames(self) -> list[str]:
        return list(self.schema.keys())

    @property
    def fields(self) -> list[PolarsSchemaField]:
        return [
            PolarsSchemaField(name, datatype) for name, datatype in self.schema.items()
        ]


SparkOrPolarsSchema = Union[SparkSchema, PolarsSchemaWrapper]


def wrap_schema_if_needed(
    schema: Union[T.StringType, PolarsSchema],
) -> SparkOrPolarsSchema:
    if isinstance(schema, T.StructType):
        return schema
    elif isinstance(schema, PolarsSchema):
        return PolarsSchemaWrapper(schema)
    else:
        raise TypeError(f"Schema type {type(schema)} not supported")


def convert_to_schema(
    schema_dict: Union[Dict[str, T.DataType], Dict[str, pl.DataType]],
) -> SparkOrPolarsSchema:
    if not schema_dict:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return T.StructType([])
        else:
            return PolarsSchemaWrapper({})

    first_value = next(iter(schema_dict.values()))
    if is_spark_data_type(first_value):
        return T.StructType(
            [
                T.StructField(name, column_type, nullable=True)
                for name, column_type in schema_dict.items()
            ]
        )
    elif is_polars_data_type(first_value):
        return PolarsSchemaWrapper(schema_dict)

    raise TypeError(f"Schema type {type(first_value)} not supported")
