import operator

from transforms.expectations.core._numeric_comparison_factory import (
    NumericComparisonFactory,
)
from transforms.expectations.core._expectation import Expectation
from transforms.expectations.column._column_expectation import (
    _NonNullExpectation,
    _ColComparisonExpectation,
    _ColumnsExistExpectation,
    _ColumnTypeExpectation,
    _ColumnRegexExpectation,
    _ColumnValuesExpectation,
    _ArrayContainsExpectation,
    _SizeExpectation,
    _ComparisonExpectation,
    _NullExpectation,
)
from transforms.expectations.property._column_properties import MinProperty, MaxProperty
from transforms.expectations.foreign._dataset_references import (
    DatasetColumn,
    DatasetComposite,
    JoinedDatasetColumn,
)
from transforms.expectations.grouped._group_by_expectation_factory import (
    GroupableColumnExpectationFactory,
)
from transforms.expectations.property._property_expectation_factory import (
    DerivedPropertyExpectationFactory,
)
from transforms.expectations.foreign._foreign_values_expectation import (
    _ReferentialIntegrityExpectation,
    _ForeignColComparisonExpectation,
)
from transforms.expectations.timestamp._timestamp_comparison_factory import (
    TimestampComparisonFactory,
    TimestampColumnComparisonFactory,
    RelativeTimestampComparisonFactory,
)
from transforms.expectations.timestamp._timestamp_comparison_expectation import (
    _TimestampComparisonExpectation,
    _TimestampColumnComparisonExpectation,
    _TimestampRelativeComparisonExpectation,
)


def _get_numeric_comparison_expectation(col, op, threshold):
    return _ComparisonExpectation(col, op, threshold)


def _get_timestamp_comparison_expectation(col, op, threshold):
    return _TimestampComparisonExpectation(col, op, threshold)


def _get_relative_timestamp_comparison_expectation(col, op, offset):
    return _TimestampRelativeComparisonExpectation(col, op, offset)


def _get_timestamp_column_comparison_expectation(col, op, other_col, offset_in_seconds):
    return _TimestampColumnComparisonExpectation(col, op, other_col, offset_in_seconds)


def _get_col_comparison_expectation(col, op, other_col):
    if isinstance(other_col, str):
        return _ColComparisonExpectation(col, op, other_col)
    elif isinstance(other_col, JoinedDatasetColumn):
        return _ForeignColComparisonExpectation(col, op, other_col)


class ColumnExpectationFactory(
    NumericComparisonFactory,
    TimestampComparisonFactory,
    RelativeTimestampComparisonFactory,
    TimestampColumnComparisonFactory,
    GroupableColumnExpectationFactory,
):
    """Create expectations over a set of columns."""

    def __init__(self, col):
        NumericComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_numeric_comparison_expectation(
                col, op, threshold
            ),
        )
        TimestampComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_timestamp_comparison_expectation(
                col, op, threshold
            ),
        )
        TimestampColumnComparisonFactory.__init__(
            self,
            lambda op, other_col, offset: _get_timestamp_column_comparison_expectation(
                col, op, other_col, offset
            ),
        )
        RelativeTimestampComparisonFactory.__init__(
            self,
            lambda op, offset: _get_relative_timestamp_comparison_expectation(
                col, op, offset
            ),
        )
        GroupableColumnExpectationFactory.__init__(self, col, None)
        self._col = col

    def non_null(self) -> Expectation:
        """Assert the column is not null."""
        return _NonNullExpectation(self._col)

    def is_null(self) -> Expectation:
        """Assert the column is null."""
        return _NullExpectation(self._col)

    def gt_col(self, other_column: str) -> Expectation:
        """Assert the column has a value greater than other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.gt, other_column)

    def gte_col(self, other_column: str) -> Expectation:
        """Assert the column has a value greater than or equal to other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.ge, other_column)

    def lt_col(self, other_column: str) -> Expectation:
        """Assert the column has a value less than other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.lt, other_column)

    def lte_col(self, other_column: str) -> Expectation:
        """Assert the column has a value less than or equal to other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.le, other_column)

    def equals_col(self, other_column: str) -> Expectation:
        """Assert the column has a value equal to other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.eq, other_column)

    def not_equals_col(self, other_column: str) -> Expectation:
        """Assert the column has a value not equal to other_column.

        Null values are ignored."""
        return _get_col_comparison_expectation(self._col, operator.ne, other_column)

    def exists(self) -> Expectation:
        """Assert the column exists in the schema."""
        return _ColumnsExistExpectation([self._col])

    def has_type(self, col_type) -> Expectation:
        """Assert the column is of the specified type."""
        return _ColumnTypeExpectation(self._col, col_type)

    def rlike(self, regex: str) -> Expectation:
        """Assert the column matches a regex pattern.

        This requires only a partial match of the regex.
        Note: Null values are ignored.
        Note: Uses pyspark.sql.functions.rlike."""
        return _ColumnRegexExpectation(self._col, regex)

    def is_in_foreign_col(self, foreign_col: DatasetColumn):
        """Assert the values in the column are all present in the foreign column.

        Warning: This is an expensive expectation to run."""
        return _ReferentialIntegrityExpectation(
            [self._col],
            DatasetComposite(foreign_col.dataset_reference(), [foreign_col.col_name()]),
        )

    def is_in(self, *allowed_values) -> Expectation:
        """Assert the values in the column match the allowed values.

        If None is in the allowed values, null values are also accepted.
        Complex types map/list/structs are not supported for this expectation.
        """
        return _ColumnValuesExpectation(self._col, *allowed_values)

    def min_value(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the min value of a column."""
        return DerivedPropertyExpectationFactory(self._col, MinProperty())

    def max_value(self) -> DerivedPropertyExpectationFactory:
        """Assert expectations on the max value of a column."""
        return DerivedPropertyExpectationFactory(self._col, MaxProperty())

    def array_contains(self, value):
        """Assert an array column contains the specified value."""
        return _ArrayContainsExpectation(self._col, value)

    def size(self):
        """Assert a column has the specified size.

        Note: This works only for array and map type columns."""
        return NumericComparisonFactory(
            lambda op, threshold: _SizeExpectation(self._col, op, threshold)
        )
