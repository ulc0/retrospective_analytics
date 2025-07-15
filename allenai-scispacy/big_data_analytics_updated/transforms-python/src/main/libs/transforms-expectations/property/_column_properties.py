#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import uuid
from abc import ABCMeta
from typing import NamedTuple, List, Union

import pyspark.sql.types as T
import polars as pl
from transforms.expectations.wrapper import (
    ExpectationColumn,
    WindowWrapper as Window,
    PolarsOrSparkDataType,
    return_type_depending_on_backend,
    ColumnFunctions as F,
)


class Aggregate(NamedTuple):
    column: ExpectationColumn
    target_column: Union[str, None]
    name: str


class ColumnProperty(metaclass=ABCMeta):
    def __init__(self):
        self._id = uuid.uuid4()

    def id(self) -> uuid:
        return self._id

    def value(self, column, group_by_columns) -> ExpectationColumn:
        if not group_by_columns:
            return self.from_aggregates(column)

        return self.from_window(column, group_by_columns)

    def aggregate_name(self, column, aggregate_type):
        return f"{aggregate_type}_{column}_{self.id()}"

    @staticmethod
    def window(group_by_cols):
        return Window.partitionBy(*[F.col(col) for col in group_by_cols])

    def aggregates(self, column, group_by_columns) -> List[Aggregate]:
        # We only want to pull in aggregates if we're not grouping
        if not group_by_columns:
            return self.required_aggregates(column)

        return []

    def required_aggregates(self, column) -> List[Aggregate]:
        raise NotImplementedError()

    def name(self) -> str:
        raise NotImplementedError()

    def data_type(self) -> PolarsOrSparkDataType:
        """
        Data type of the resulting aggregation
        """
        raise NotImplementedError()

    def from_aggregates(self, column) -> ExpectationColumn:
        raise NotImplementedError()

    def from_window(self, column, group_by_columns) -> ExpectationColumn:
        raise NotImplementedError()


class SinglePropertyAggregate(ColumnProperty, metaclass=ABCMeta):
    def __init__(self, name: str, data_type: T.DataType):
        super(SinglePropertyAggregate, self).__init__()
        self._name = name
        self._data_type = data_type

    def name(self) -> str:
        return self._name

    def data_type(self) -> T.DataType:
        return self._data_type

    def aggregate_function(self, column) -> ExpectationColumn:
        raise NotImplementedError

    def window_function(self, column, group_by_columns) -> ExpectationColumn:
        """Implement this if the aggregate function does not work in a select"""
        return None

    def required_aggregates(self, column) -> List[Aggregate]:
        return [
            Aggregate(
                column=self.aggregate_function(column),
                target_column=column,
                name=self.aggregate_name(column, self.name()),
            )
        ]

    def from_aggregates(self, column) -> ExpectationColumn:
        return F.col(self.aggregate_name(column, self.name()))

    def from_window(self, column, group_by_columns) -> ExpectationColumn:
        maybe_window_function = self.window_function(column, group_by_columns)
        # Some aggregates are not allowed in a window, so we have to special case them.
        if maybe_window_function is not None:
            return maybe_window_function

        return self.aggregate_function(column).over(
            ColumnProperty.window(group_by_columns)
        )


class SumProperty(SinglePropertyAggregate):
    def __init__(self):
        super(SumProperty, self).__init__(
            name="SUM",
            data_type=T.DoubleType(),
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.sum(F.col(column))


class SampleStandardDeviationProperty(SinglePropertyAggregate):
    def __init__(self):
        super(SampleStandardDeviationProperty, self).__init__(
            name="SAMPLE_STANDARD_DEVIATION",
            data_type=T.DoubleType(),
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.stddev_samp(F.col(column))


class PopulationStandardDeviationProperty(SinglePropertyAggregate):
    def __init__(self):
        super(PopulationStandardDeviationProperty, self).__init__(
            name="POPULATION_STANDARD_DEVIATION",
            data_type=T.DoubleType(),
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.stddev_pop(F.col(column))


class NullCountProperty(SinglePropertyAggregate):
    def __init__(self):
        super(NullCountProperty, self).__init__(
            name="NULL_COUNT", data_type=T.LongType()
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.sum(F.when(F.col(column).isNull(), 1).otherwise(0))


class NonNullCountProperty(SinglePropertyAggregate):
    def __init__(self):
        super(NonNullCountProperty, self).__init__(
            name="NON_NULL_COUNT", data_type=T.LongType()
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.sum(F.when(F.col(column).isNotNull(), 1).otherwise(0))


class DistinctCountProperty(SinglePropertyAggregate):
    def __init__(self):
        super(DistinctCountProperty, self).__init__(
            name="DISTINCT_COUNT", data_type=T.LongType()
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.countDistinct(F.col(column))

    def window_function(self, column, group_by_columns) -> ExpectationColumn:
        # Distinct count is not supported in a window so we have to special case.
        return F.size(
            F.collect_set(F.col(column)).over(ColumnProperty.window(group_by_columns))
        )


class ApproxDistinctCountProperty(SinglePropertyAggregate):
    def __init__(self):
        super(ApproxDistinctCountProperty, self).__init__(
            name="APPROX_DISTINCT_COUNT", data_type=T.DoubleType()
        )

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.approx_count_distinct(F.col(column))


class MaxProperty(SinglePropertyAggregate):
    def __init__(self, expected_type=T.DoubleType()):
        super(MaxProperty, self).__init__(name="MAX", data_type=expected_type)

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.max(F.col(column))


class MinProperty(SinglePropertyAggregate):
    def __init__(self, expected_type=T.DoubleType()):
        super(MinProperty, self).__init__(name="MIN", data_type=expected_type)

    def aggregate_function(self, column) -> ExpectationColumn:
        return F.min(F.col(column))


class NullPercentageProperty(ColumnProperty):
    def __init__(self):
        super(NullPercentageProperty, self).__init__()

    def name(self):
        return "NULL_PERCENTAGE"

    def data_type(self) -> PolarsOrSparkDataType:
        return return_type_depending_on_backend(T.DoubleType(), pl.Float64())

    def _row_count_aggregate(self, column):
        return Aggregate(
            column=F.row_count(),
            target_column=column,
            name=self.aggregate_name(column, "ROW_COUNT"),
        )

    def _null_count_aggregate(self, column):
        return Aggregate(
            column=F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)),
            target_column=column,
            name=self.aggregate_name(column, "NULL_COUNT"),
        )

    def required_aggregates(self, column) -> List[Aggregate]:
        return [
            self._null_count_aggregate(column),
            self._row_count_aggregate(column),
        ]

    def from_aggregates(self, column):
        row_count = F.col(self.aggregate_name(column, "ROW_COUNT"))
        null_count = F.col(self.aggregate_name(column, "NULL_COUNT"))
        return null_count / row_count

    def from_window(self, column, group_by_columns):
        null_count = self._null_count_aggregate(column).column.over(
            ColumnProperty.window(group_by_columns)
        )
        row_count = self._row_count_aggregate(column).column.over(
            ColumnProperty.window(group_by_columns)
        )
        return null_count / row_count
