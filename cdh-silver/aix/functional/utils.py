from typing import Callable

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col


def name_to_column(name: str) -> Column:
    """
    Converts a string to a column object.
    """
    if isinstance(name, str):
        column = col(name)
    elif isinstance(name, Column):
        column = name
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return column


def apply_func(
    dataframe: DataFrame,
    text: "ColumnOrName", 
    func: Callable[..., Column],
    name: str=None, 
    **kwargs
    ) -> DataFrame:
    """
    Applies a provided function with kwargs to a column in a dataframe.
    """
    column = name_to_column(text)

    # If no name supplied, replace column in dataframe
    # str(column) returns Column<'name'>, we use [8:-2] to get the name only, can't run Java/Scala code
    name = str(column)[8:-2] if name is None else name

    return dataframe.withColumn(name, func(column, **kwargs))
