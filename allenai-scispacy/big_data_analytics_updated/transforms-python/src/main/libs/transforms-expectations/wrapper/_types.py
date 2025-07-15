from typing import Union
from pyspark.sql import types as T
import polars as pl
from datetime import date, datetime

PolarsOrSparkDataType = Union[T.DataType, pl.DataType]
ARRAY_TYPE = [T.ArrayType, pl.List, pl.Array]
ARRAY_OR_MAP_TYPE = [T.ArrayType, T.MapType, pl.List, pl.Array]
TIMESTAMP_TYPE = [T.TimestampType, pl.Datetime]


def is_spark_data_type(value) -> bool:
    # T.ArrayType() -> instance, therefore use the isinstance
    # T.ArrayType -> class, therefore the issubclass method must be used
    if isinstance(value, type):
        return issubclass(value, T.DataType)
    else:
        return isinstance(value, T.DataType)


def is_polars_data_type(value) -> bool:
    # Polars has two different types for data:
    # pl.Int64() -> pl.Datatype
    # pl.Int64 -> pl.datatypes.classes.DataTypeClass
    return isinstance(value, (pl.DataType, pl.datatypes.classes.DataTypeClass))


def is_array_type(data_type: PolarsOrSparkDataType):
    """
    Matches spark ArrayType + polars List and Array
    """
    allowed_types = [T.ArrayType, pl.List, pl.Array]
    for allowed_type in allowed_types:
        if data_type is allowed_type or isinstance(data_type, allowed_type):
            return True

    return False


def serialize_custom_types(obj):
    """JSON serializer for objects not serializable by default json code.
    Intended to mimic the behaviour of spark's to_json serialization"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)
