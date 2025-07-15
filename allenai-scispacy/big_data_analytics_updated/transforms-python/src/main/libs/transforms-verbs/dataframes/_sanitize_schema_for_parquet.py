from pyspark.sql import functions as F, types as T
import re


def _sanitize_field_name(field_name: str):
    parquet_illegal_characters = " ,;{}()\n\t="
    sanitized = re.sub(f"^[{parquet_illegal_characters}]+", "", field_name)
    sanitized = re.sub(f"[{parquet_illegal_characters}]+$", "", sanitized)
    return re.sub(f"[{parquet_illegal_characters}_]+", "_", sanitized)


def _sanitize_type(data_type):
    if isinstance(data_type, T.StructType):
        sanitized_fields = [
            T.StructField(
                _sanitize_field_name(field.name), _sanitize_type(field.dataType)
            )
            for field in data_type.fields
        ]
        return T.StructType(sanitized_fields)
    if isinstance(data_type, T.ArrayType):
        sanitized_field = _sanitize_type(data_type.elementType)
        return T.ArrayType(sanitized_field)
    return data_type


def sanitize_schema_for_parquet(dataframe):
    """
    Returns a new dataframe with renamed fields and struct subschema fields such that they can be written as parquet,
    which is the default output file format in transforms. This is necessary when working with a dataframe
    produced from JSON data that uses parentheses, spaces, etc. in field names.

    Args:
        dataframe: Input :class:`DataFrame`
    Returns:
        A :class:`DataFrame` where each field and struct subschema field has been sanitized to be valid in parquet.
    """
    return dataframe.select(
        *(
            F.col(f"`{field.name}`")
            .alias(_sanitize_field_name(field.name))
            .cast(_sanitize_type(field.dataType))
            for field in dataframe.schema
        )
    )
