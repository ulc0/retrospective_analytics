# pylint: disable=no-self-argument, too-many-public-methods
from __future__ import annotations
from ._wrapper import (
    ExpectationColumn,
    PolarsExpectationColumn,
    unwrap_if_wrapped_column,
)
from datetime import datetime
from typing import Any, Union, List
from pyspark.sql import functions as F, types as T, Window
import polars as pl
import json
import pytz
import operator
from transforms.expectations.wrapper._backend import GET_CURRENT_BACKEND, Backend
from ._wrapper_decorators import delegate_to_backend, delegate_if_in_spark_mode
from ._types import PolarsOrSparkDataType, serialize_custom_types


class ColumnFunctions:
    @delegate_to_backend()
    def lit(value: Any) -> ExpectationColumn:
        return pl.lit(value)

    @delegate_to_backend()
    def expr(str: str) -> PolarsExpectationColumn:
        raise NotImplementedError(
            "Expr operation not implemented yet for Polars backend"
        )

    @delegate_to_backend()
    def count(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return col.count()

    @delegate_to_backend()
    def sum(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return col.sum()

    @delegate_to_backend()
    def when(condition: PolarsExpectationColumn, value: Any) -> PolarsExpectationColumn:
        unwrapped_value = unwrap_if_wrapped_column(value)
        return pl.when(condition).then(unwrapped_value)

    @delegate_to_backend()
    def col(col: str) -> PolarsExpectationColumn:
        return pl.col(col)

    @delegate_to_backend()
    def first(
        col: PolarsExpectationColumn, ignorenulls: bool = False
    ) -> PolarsExpectationColumn:
        return col.first()

    @delegate_to_backend()
    def array(*cols: Union[PolarsExpectationColumn, str]) -> PolarsExpectationColumn:
        columns = [unwrap_if_wrapped_column(col) for col in cols]
        column = pl.concat_list(columns)
        return column

    @delegate_to_backend()
    def min(col: Union[PolarsExpectationColumn, str]) -> PolarsExpectationColumn:
        return col.min()

    @delegate_to_backend()
    def max(col: Union[PolarsExpectationColumn, str]) -> PolarsExpectationColumn:
        return col.max()

    @delegate_to_backend()
    def countDistinct(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        # n_unique considers None a unique value, which spark does not
        return col.filter(col.is_not_null()).n_unique()

    @delegate_to_backend()
    def approx_count_distinct(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        # polars does not contain an approximate method
        # n_unique considers None a unique value, which spark does not
        return col.filter(col.is_not_null()).n_unique()

    @delegate_to_backend()
    def collect_set(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        raise NotImplementedError(
            "Collect_set operation not implemented yet for Polars backend"
        )

    @delegate_to_backend()
    def stddev_samp(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return col.std()

    @delegate_to_backend()
    def stddev_pop(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return col.var(ddof=0) ** 0.5

    @delegate_to_backend()
    def to_json(col: PolarsExpectationColumn) -> PolarsExpectationColumn:
        column = col.map_elements(
            lambda struct_val: json.dumps(struct_val, default=serialize_custom_types),
            skip_nulls=False,
            return_dtype=pl.String,
        )
        return column

    @delegate_to_backend()
    def broadcast(df: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return df

    @delegate_if_in_spark_mode()
    def struct(
        *value: Union[PolarsExpectationColumn, str],
    ) -> PolarsExpectationColumn:
        columns = [unwrap_if_wrapped_column(col) for col in value]
        # polars panics if passed no columns to a struct
        # https://github.com/pola-rs/polars/issues/9216
        if len(columns) > 0:
            column = pl.struct(*columns)
        else:
            column = pl.struct(pl.lit(None))

        return PolarsExpectationColumn(column)

    def first_non_null(
        col: ExpectationColumn, ignorenulls: bool = False
    ) -> ExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.first(col, ignorenulls=ignorenulls)
        else:
            column: pl.Expr = col.unwrap()
            column = column.filter(column.is_not_null()).first()
            return PolarsExpectationColumn(column)

    def isin(col: ExpectationColumn, vals: Any) -> ExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return col.isin(*[F.lit(value) for value in vals])
        else:
            # polars needs the primitives not wrapped in cols
            column = col.unwrap().is_in(vals)
            return PolarsExpectationColumn(column)

    def row_count() -> ExpectationColumn:  # pylint: disable=no-method-argument
        """
        Spark uses the "agg" for row count, while polars can only use select.
        As such, for spark, aggregating on the literal count works, but in polars, the same
        would just result in "1" because pl.lit(1).count() doesn't iterate over rows to count them,
        instead, it seems to evaluate the literal value in a singular context.
        """
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.count(F.lit(1))
        else:
            col = pl.len()
            return PolarsExpectationColumn(col)

    def failed_row_count(predicate: ExpectationColumn) -> ExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            is_failed_col = F.when(predicate, F.lit(0)).otherwise(F.lit(1))
            failed_row_count_col = F.sum(is_failed_col)
            return failed_row_count_col
        else:
            unwrapped_predicate = predicate.unwrap()
            # To count the failed rows we invert the predicate column
            # spark also counts nulls in the sum but polars doesn't, so we
            # replace them by True to count
            failed_count: pl.Expr = (
                (~unwrapped_predicate).fill_null(True).sum().cast(pl.Int32)
            )
            failed_row_count_col = (
                pl.when(unwrapped_predicate.count() == 0)
                .then(None)
                .otherwise(failed_count)
            )
            return PolarsExpectationColumn(failed_row_count_col)

    def array_contains(
        col: ExpectationColumn, value: Any, col_type: PolarsOrSparkDataType
    ) -> ExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.array_contains(col, value)
        else:
            # Polars contains two "collection" types, array and list
            # we need to special case this method to validate both
            unwraped_col = col.unwrap()
            if col_type == pl.List:
                return PolarsExpectationColumn(unwraped_col.list.contains(value))
            elif col_type == pl.Array:
                return PolarsExpectationColumn(unwraped_col.arr.contains(value))
            else:
                raise TypeError("col_type must either be pl.List or pl.Array")

    def size(
        col: Union[str, ExpectationColumn], col_type: PolarsOrSparkDataType = None
    ) -> PolarsExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.size(col)
        else:
            # Polars contains two "collection" types, array and list
            # we need to special case this method to validate both
            if col_type == pl.List:
                return PolarsExpectationColumn(pl.col(col).list.len())
            elif col_type == pl.Array:
                # an array type has a fixed shape, so we can directly extract the shape property
                # and return the length of the first dimension
                return PolarsExpectationColumn(pl.lit(col_type.shape[0]))
            else:
                raise TypeError("col_type must either be pl.List or pl.Array")

    def array_is_in(
        col_name: str, allowed_values: List[any], col_type: PolarsOrSparkDataType
    ) -> ExpectationColumn:
        """
        Returns, for each row, if the values contained in the array col are all within the allowed_values.

        For spark we do this by computing the set difference between the array col and the allowed values
        and then ensuring size is 0.

        For polars we apply a UDF that compares the elements of each row against the list and reduces the
        result to a boolean per row.
        """
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            allowed_values = F.array(*[F.lit(value) for value in allowed_values])
            return F.size(F.array_except(F.col(col_name), allowed_values)) == 0
        else:

            def check_all_values_allowed(col):
                return col.list.eval(pl.element().is_in(allowed_values)).list.all()

            if col_type == pl.List:
                return PolarsExpectationColumn(
                    check_all_values_allowed(pl.col(col_name))
                )
            elif col_type == pl.Array:
                converted_col = pl.col(
                    col_name
                ).arr.to_list()  # array does not have a eval method because the array type is still WIP
                return PolarsExpectationColumn(check_all_values_allowed(converted_col))
            else:
                raise TypeError("col_type must either be pl.List or pl.Array")

    def compare_timestamp(
        col: ExpectationColumn,
        timestamp: Union[datetime, ExpectationColumn],
        op: operator,
        col_type: Union[T.TimestampType, pl.Datetime],
    ) -> ExpectationColumn:
        """
        Compares the given column agains the given timestamp (can be a datetime object or a timestamp col.).

        For Spark: any timestamp for any timezone can be directly compared, since
        spark handles timezone conversion internally.

        For Polars, we first must ensure the timezone of the column data type matches the timestamp one.
        A lack of timezone means it is in UTC.
        """
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return op(col, timestamp)
        else:
            unwrapped_col: pl.Expr = col.unwrap()
            col_timezone = col_type.time_zone
            if not col_timezone:
                col_timezone = "UTC"
                unwrapped_col = unwrapped_col.dt.replace_time_zone(col_timezone)

            localized_ts = localize_timestamp(timestamp, col_timezone)
            return PolarsExpectationColumn(op(unwrapped_col, localized_ts))

    def offset_col_by_seconds(col: str, offset_in_seconds: int) -> ExpectationColumn:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.col(col) + F.expr(f"INTERVAL {offset_in_seconds} SECONDS")
        else:
            return PolarsExpectationColumn(
                (pl.col(col) + pl.duration(seconds=offset_in_seconds)).cast(pl.Datetime)
            )

    def row_count_over_group(cols: List[str]) -> ExpectationColumn:
        """
        Returns a col indicating how many elements belong to each group.
        Ex:
            F.row_count_over_group(["col1", "col2"])
            ┌──────┬──────┬─────────────────┐
            │ col1 ┆ col2 ┆ Count per group │
            │ ---  ┆ ---  ┆ ---             │
            │ str  ┆ i64  ┆ u32             │
            ╞══════╪══════╪═════════════════╡
            │ A    ┆ 1    ┆ 1               │
            │ A    ┆ 2    ┆ 1               │
            │ B    ┆ 4    ┆ 3               │
            │ B    ┆ 4    ┆ 3               │
            │ B    ┆ 4    ┆ 3               │
            └──────┴──────┴─────────────────┘
        """
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return F.sum(F.lit(1)).over(
                Window.partitionBy(*[F.col(col) for col in cols])
            )
        else:
            col = pl.len().over(cols)
            return PolarsExpectationColumn(col)


def localize_timestamp(
    timestamp: Union[datetime, ExpectationColumn], desired_timezone: str
) -> Union[datetime, pl.Expr]:
    if isinstance(timestamp, datetime):
        timezone = pytz.timezone(desired_timezone)
        return timestamp.astimezone(timezone)
    elif isinstance(timestamp, PolarsExpectationColumn):
        unwrapped_col = timestamp.unwrap()
        localized_col = unwrapped_col.dt.convert_time_zone(desired_timezone)
        return localized_col
    else:
        raise TypeError("Can only shift timestamps for datetime or polars objects")
