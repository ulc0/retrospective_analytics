from math import isclose as _isclose
from functools import total_ordering as _total_ordering

from pyspark.sql import types as _T

_ONE_MILLION_ROWS = 1000000


def compare_datasets(
    expected_df, result_df, check_nullable=False, maximum_dataset_size=_ONE_MILLION_ROWS
):
    """
    Compares two datasets via an assert. This is driver intensive operation, mainly to be used in unit tests. This will
    fail a build if the assertion is false while running within transforms.
    """
    len_expected = expected_df.count()
    len_result = result_df.count()

    assert (
        len_expected == len_result
    ), f"The dataframes have different number of rows (expected={len_expected}, got={len_result})"

    assert len_result <= maximum_dataset_size, (
        f"This dataset is too large ({len_result} rows) and the computation is going to be slow. If you still want to"
        f"compute this, please set the parameter `maximum_dataset_size` to a value bigger than {len_result}"
    )

    compare_schemas(expected_df, result_df, check_nullable)
    expected = _consistent_collect(expected_df)
    result = _consistent_collect(result_df)

    for row_a, row_b in zip(expected, result):
        for col in expected_df.schema:
            name = col.name
            if col.dataType == _T.DoubleType():
                assert _compare_nullable_float(
                    row_a[name], row_b[name]
                ), f"Value difference on column '{name}': Expected '{row_a[name]}', got '{row_b[name]}'"
            else:
                assert (
                    row_a[name] == row_b[name]
                ), f"Value difference on column '{name}': Expected '{row_a[name]}', got '{row_b[name]}'"


def compare_schemas(expected_df, result_df, check_nullable=False):
    """
    Compares the schemas of two datasets via an assert. This will fail a build if the assertion is false while running
    within transforms.
    """
    schema_a = expected_df.schema
    schema_b = result_df.schema
    missing_in_result, extra_in_result = _quick_compare_columns(schema_a, schema_b)
    assert not missing_in_result and not extra_in_result, (
        "The dataframes have different columns.\n"
        f"Missing in result: {_format_iterable(missing_in_result)}.\n"
        f"Extra in result: {_format_iterable(extra_in_result)}."
    )
    for col_a in schema_a:
        assert (
            col_a.name in result_df.columns
        ), f"Column '{col_a.name}' not present in the results DataFrame"
        if check_nullable:
            assert (
                col_a == schema_b[col_a.name]
            ), f"Column '{col_a.name}': Expected type '{col_a}', got '{schema_b[col_a.name]}'"
        else:
            assert (
                col_a.dataType == schema_b[col_a.name].dataType
            ), f"Column '{col_a.name}': Expected type '{col_a.dataType}', got '{schema_b[col_a.name].dataType}'"


def _consistent_collect(df):
    return sorted(
        df.collect(),  # noqa
        key=lambda x: tuple(_convert_nullable(x[col]) for col in sorted(df.columns)),
    )


def _compare_nullable_float(a, b):
    if a is None:
        return b is None
    if b is None:
        return False
    return _isclose(a, b)


def _convert_nullable(v):
    if v is None:
        return MIN
    if isinstance(v, list):
        return [_convert_nullable(x) for x in v]
    return v


def _quick_compare_columns(schema_a, schema_b):
    return _differences(
        [field.name for field in schema_a], [field.name for field in schema_b]
    )


def _differences(a, b):
    set_a = set(a)
    set_b = set(b)
    return (set_a.difference(b), set_b.difference(set_a))


def _format_iterable(it):
    return ", ".join([f"`{x}`" for x in it])


@_total_ordering
class MinType(object):
    """
    A type that always comes first in comparisons.

    >>> MinType() < 0
    True
    >>> MinType() < None
    True
    >>> MinType() < float('-inf')
    True
    >>> MinType() < MinType()
    False
    >>> MinType() == MinType()
    True
    """

    def __le__(self, other):
        if isinstance(other, MinType):
            return False
        return True

    def __eq__(self, other):
        return isinstance(other, MinType)


MIN = MinType()
