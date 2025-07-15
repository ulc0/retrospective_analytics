from transforms.expectations.core._numeric_comparison_factory import (
    NumericComparisonFactory,
)
from transforms.expectations.property._column_properties import (
    NullPercentageProperty,
    NullCountProperty,
    NonNullCountProperty,
    DistinctCountProperty,
    ApproxDistinctCountProperty,
    MinProperty,
    MaxProperty,
    SumProperty,
    SampleStandardDeviationProperty,
    PopulationStandardDeviationProperty,
)
from transforms.expectations.foreign._dataset_references import (
    DatasetCount,
    DatasetReference,
)
from transforms.expectations.grouped._group_by_expectation import (
    _UniqueExpectation,
    _DatasetRowCountComparisonExpectation,
    _RowCountComparisonExpectation,
)
from transforms.expectations.property._property_expectation_factory import (
    NumericPropertyExpectationFactory,
    DerivedPropertyExpectationFactory,
)


def _get_row_count_comparison_expectation(cols, op, threshold):
    if isinstance(threshold, DatasetCount):
        if cols:
            raise ValueError(
                "Grouped row count does not support cross dataset expectation."
            )
        return _DatasetRowCountComparisonExpectation(op, threshold)
    elif isinstance(threshold, DatasetReference):
        raise ValueError(
            f"Dataset Reference is not a valid threshold for comparisons. "
            f"Did you mean E.dataset_ref('{threshold._dataset_parameter}').count()?"
        )

    return _RowCountComparisonExpectation(cols, op, threshold)


class GroupByExpectationFactory(object):
    """Create expectations over an aggregation of rows.

    Warning:
        Group-by expectations are in general significantly more expensive to compute than column-based expectations.
    """

    def __init__(self, cols):
        self._cols = cols

    def is_unique(self):
        """Assert that the groupBy columns identify unique rows of the dataset."""
        return _UniqueExpectation(self._cols)

    def count(self):
        """Make a numeric assertion on the number of rows identified by the groupBy."""

        return NumericComparisonFactory(
            lambda op, threshold: _get_row_count_comparison_expectation(
                self._cols, op, threshold
            )
        )

    def col(self, col: str):
        return GroupableColumnExpectationFactory(col, self._cols)


class GroupableColumnExpectationFactory(object):
    def __init__(self, col, group_by_cols=None):
        self._col = col
        self._group_by_cols = group_by_cols

    def null_percentage(self) -> NumericPropertyExpectationFactory:
        """Assert expectations on the null percentage of a column."""
        return NumericPropertyExpectationFactory(
            self._col, NullPercentageProperty(), self._group_by_cols
        )

    def null_count(self) -> NumericPropertyExpectationFactory:
        """Assert expectations on the null count of a column."""
        return NumericPropertyExpectationFactory(
            self._col, NullCountProperty(), self._group_by_cols
        )

    def non_null_count(self) -> NumericPropertyExpectationFactory:
        """Assert expectations on the not null count of a column."""
        return NumericPropertyExpectationFactory(
            self._col, NonNullCountProperty(), self._group_by_cols
        )

    def distinct_count(self) -> NumericPropertyExpectationFactory:
        """Assert expectations on the distinct count of a column.

        Warning: This is an expensive check. Consider using approx_distinct_count instead.
        """
        return NumericPropertyExpectationFactory(
            self._col, DistinctCountProperty(), self._group_by_cols
        )

    def approx_distinct_count(self) -> NumericPropertyExpectationFactory:
        """Assert expectations on the approximate distinct count of a column.

        Note: Uses pyspark.sql.approx_count_distinct which guarantees a relative standard deviation of
        the error of max 5%."""
        return NumericPropertyExpectationFactory(
            self._col, ApproxDistinctCountProperty(), self._group_by_cols
        )

    def min_value(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the min value of a column."""
        return DerivedPropertyExpectationFactory(
            self._col, MinProperty(), self._group_by_cols
        )

    def max_value(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the max value of a column."""
        return DerivedPropertyExpectationFactory(
            self._col, MaxProperty(), self._group_by_cols
        )

    def sum(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the sum of a column.

        Note: This only works for numeric columns."""
        return DerivedPropertyExpectationFactory(
            self._col, SumProperty(), self._group_by_cols
        )

    def standard_deviation_sample(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the sample standard deviation of a column.

        Note: This only works for numeric columns."""
        return DerivedPropertyExpectationFactory(
            self._col, SampleStandardDeviationProperty(), self._group_by_cols
        )

    def standard_deviation_population(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the population standard deviation of a column.

        Note: This only works for numeric columns."""
        return DerivedPropertyExpectationFactory(
            self._col, PopulationStandardDeviationProperty(), self._group_by_cols
        )
