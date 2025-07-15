#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from inspect import signature
from typing import Callable, Container, Iterable, Optional

from pyspark.sql import functions as F, types as T, DataFrame

from transforms.verbs.columns._utils import typed_empty_array


def sort_array_columns(df) -> DataFrame:
    """Sorts all array columns of the provided :class:`DataFrame` object."""
    return map_columns(
        df,
        lambda col: F.array_sort(col),
        subset=[
            col
            for col in df.columns
            if isinstance(df.schema[col].dataType, T.ArrayType)
        ],
    )


def fill_nulls(
    df: DataFrame, value, subset: Optional[Container[str]] = None
) -> DataFrame:
    """Replaces nulls with the provided value for the columns that exist in the provided subset."""
    subset_columns = df.columns if subset is None else subset
    if len(set([df.schema[col].dataType for col in subset_columns])) > 1:
        raise TypeError(
            "The provided subset of columns had inconsistent datatypes - type coercion is not permitted. "
            "Please specify a subset such that all columns are the same datatype."
        )
    return map_columns(df, lambda col: F.coalesce(col, value), subset=subset)


def map_columns(
    df,
    function: Callable,
    subset: Optional[Container[str]] = None,
    allow_renames: Optional[bool] = False,
) -> DataFrame:
    """Applies the provided function on the columns of the provided :class:`DataFrame` object,
    for the columns that exist in the provided subset.

    By default, this will not allow for column renaming. Use :param:`allow_renames` to rename
    columns.
    """
    function = _normalize_column_map_function(function)
    function = _force_column_names(function) if not allow_renames else function

    _throw_if_ambiguous(
        df.columns,
        "The original dataframe has multiple columns with the same name, resulting in an "
        "ambiguous dataframe: {duplicates}",
    )

    result = df.select(
        [
            function(df[col], df.schema[col].dataType, col)
            if subset is None or col in subset
            else col
            for col in df.columns
        ]
    )

    _throw_if_ambiguous(
        result.columns,
        "The resulting dataframe has multiple columns with the same name, resulting in "
        "an ambiguous dataframe: {duplicates}",
    )

    return result


def _force_column_names(function: Callable):
    return lambda col, datatype, name: function(col, datatype, name).alias(name)


def _normalize_column_map_function(function: Callable):
    arg_count = len(signature(function).parameters)
    if arg_count == 0 or arg_count > 3:
        raise TypeError(
            f"The provided function must have at least one and no more than 3 parameters, "
            f"but was {arg_count}. The first parameter that will be passed in is the column object, "
            f"the second is the datatype of the column, and the third is the name of the column."
        )
    return lambda col, datatype, name: function(*(col, datatype, name)[:arg_count])


def _throw_if_ambiguous(columns: Iterable[str], error_message: str):
    """Throws a RuntimeError if any names are shared among columns.
    :param: columns - columns of a DataFrame
    :param: error_message - the error message to display. Use '{duplicates}' to interpolate the list of duplicate names
    """
    seen = set()
    dupes = set()
    for col_name in columns:
        if col_name in seen:
            dupes.add(col_name)
        else:
            seen.add(col_name)

    if len(dupes) > 0:
        raise RuntimeError(error_message.format(duplicates=dupes))
