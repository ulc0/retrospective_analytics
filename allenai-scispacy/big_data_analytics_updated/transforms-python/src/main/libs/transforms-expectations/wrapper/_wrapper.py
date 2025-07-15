from __future__ import annotations
import operator
from datetime import datetime
from typing import Union, Any
from pyspark.sql import Column, Window, WindowSpec
import polars as pl
from polars.dataframe.group_by import GroupBy
from transforms.expectations.wrapper._backend import GET_CURRENT_BACKEND, Backend


class PolarsExpectationColumn:
    """
    A ExpectationColumn is a wrapper around a pl.EXPR object.
    This allows us to have the same api as pyspark Column objects, abstracting the data engine at a higher level
    """

    def __init__(self, column: pl.Expr):
        self.column = column

    def unwrap(self) -> pl.Expr:
        return self.column

    def alias(self, alias: str) -> PolarsExpectationColumn:
        column = self.column.alias(alias)
        return PolarsExpectationColumn(column)

    def isNull(self) -> PolarsExpectationColumn:
        column = self.column.is_null()
        return PolarsExpectationColumn(column)

    def isNotNull(self) -> PolarsExpectationColumn:
        column = self.column.is_not_null()
        return PolarsExpectationColumn(column)

    def otherwise(
        self, condition: Union[int, PolarsExpectationColumn]
    ) -> PolarsExpectationColumn:
        condition = unwrap_if_wrapped_column(condition)
        column = self.column.otherwise(condition)
        return PolarsExpectationColumn(column)

    def rlike(self, expression: str) -> PolarsExpectationColumn:
        # polars can only do the regex check if at least one of the values is not null
        str_col = self.column.cast(pl.Utf8)
        all_null = str_col.is_null().all()

        column = (
            pl.when(all_null)
            .then(pl.lit(True))
            .otherwise(str_col.str.contains(expression))
        )

        return PolarsExpectationColumn(column)

    def over(self, window: WindowWrapper) -> PolarsExpectationColumn:
        raise NotImplementedError(
            "Over operation not implemented yet for Polars backend"
        )

    # Custom behavior for >
    def __gt__(self, other: PolarsExpectationColumn) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.gt, ">")

    # Custom behavior for >=
    def __ge__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.ge, ">=")

    # Custom behavior for <
    def __lt__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.lt, "<")

    # Custom behavior for <=
    def __le__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.le, "<=")

    # Custom behavior for ==
    def __eq__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.eq, "==")

    # Custom behavior for !=
    def __ne__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.ne, "!=")

    # Custom behavior for |
    def __or__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.or_, "|")

    # Custom behaviour for &
    def __and__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.and_, "&")

    # Custom behavior for +
    def __add__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.add, "+")

    # Custom behavior for /
    def __truediv__(self, other) -> PolarsExpectationColumn:
        return self._comparison_helper(other, operator.truediv, "/")

    # Custom behaviour for ~
    def __invert__(self) -> PolarsExpectationColumn:
        col = operator.__invert__(self.unwrap())
        return PolarsExpectationColumn(col)

    def _comparison_helper(
        self, other: Union[PolarsExpectationColumn, datetime], op, op_symbol
    ):
        unwrapped_self, unwrapped_other = self.validate_op_args_and_unwrap(other)

        if GET_CURRENT_BACKEND() == Backend.SPARK:
            col = op(unwrapped_self, unwrapped_other)
        else:
            # Spark ignores nulls when doing checks, but polars actually returns nulls.
            # As such we add a condition that when both sides of the op are null
            # we return True, a.k.a ignore it for evaluation
            if isinstance(unwrapped_other, PolarsExpectationColumn):
                col = (
                    pl.when(unwrapped_self.is_null() & unwrapped_other.is_null())
                    .then(True)
                    .otherwise(op(unwrapped_self, unwrapped_other))
                )
            else:
                col = op(unwrapped_self, unwrapped_other)

        return PolarsExpectationColumn(col)

    # We support operations agains other columns or datetimes
    def validate_op_args_and_unwrap(
        self, other: Union[PolarsExpectationColumn, datetime, int]
    ):
        # ) -> Tuple[ColumnOrExpr, Union[ColumnOrExpr, datetime, int]]:
        if not isinstance(other, (PolarsExpectationColumn, datetime, int)):
            raise TypeError(
                f"Unsupported type for comparison operation: {type(other).__name__}"
            )
        return self.unwrap(), unwrap_if_wrapped_column(other)


ExpectationColumn = Union[Column, PolarsExpectationColumn]


def unwrap_if_wrapped_column(value: Any) -> ExpectationColumn:
    if isinstance(value, PolarsExpectationColumn):
        return value.unwrap()
    return value


"""
A WindowWrapper is a wrapper around either a polars GroupBy object.
This allows us to reuse the same expectations code either for evaluating pyspark datasets
or for lightweight transforms, which will use polars as the data processing engine.
"""
WindowOrGroup = Union[WindowSpec, GroupBy]


class WindowWrapper:
    def __init__(self, aggregation: GroupBy):
        self.aggregation = aggregation

    @staticmethod
    def partitionBy(*cols: Union[str, ExpectationColumn]) -> WindowOrGroup:
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return Window.partitionBy(*cols)
        else:
            raise NotImplementedError(
                "PartitionBy operation not implemented yet for Polars backend"
            )
