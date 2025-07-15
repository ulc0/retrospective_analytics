from typing import List

from transforms.expectations.core._numeric_comparison_factory import (
    NumericComparisonFactory,
)
from transforms.expectations.property._column_properties import MaxProperty, MinProperty
from transforms.expectations.property._property_expectation import (
    _PropertyComparisonExpectation,
    _TimestampPropertyComparisonExpectation,
    _TimestampPropertyRelativeComparison,
)
from transforms.expectations.property._grouped_property_expectation import (
    _GroupedPropertyComparisonExpectation,
    _GroupedPropertyTimestampComparisonExpectation,
    _GroupedPropertyTimestampRelativeComparisonExpectation,
)
from transforms.expectations.timestamp._timestamp_comparison_factory import (
    TimestampComparisonFactory,
    RelativeTimestampComparisonFactory,
)
from pyspark.sql import types as T


class NumericPropertyExpectationFactory(NumericComparisonFactory):
    """Create expectations over the numeric property of a column (count, null count, ...)"""

    def __init__(self, col, property, group_by_cols: List[str] = None):
        NumericComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_property_comparison_expectation(
                col, property, op, threshold, group_by_cols
            ),
        )
        self._col = col
        self._property = property
        self._group_by_cols = group_by_cols


class DerivedPropertyExpectationFactory(
    NumericComparisonFactory,
    TimestampComparisonFactory,
    RelativeTimestampComparisonFactory,
):
    """Create expectations over a value extracted from a column by a property (max_value, min_value, ...)"""

    def __init__(self, col, property, group_by_cols: List[str] = None):
        NumericComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_property_comparison_expectation(
                col, property, op, threshold, group_by_cols
            ),
        )
        TimestampComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_property_timestamp_comparison_expectation(
                col, property, op, threshold, group_by_cols
            ),
        )
        RelativeTimestampComparisonFactory.__init__(
            self,
            lambda op, threshold: _get_property_relative_timestamp_comparison_expectation(
                col, property, op, threshold, group_by_cols
            ),
        )
        self._col = col
        self._property = property
        self._group_by_cols = group_by_cols


def _get_property_comparison_expectation(
    col, property, op, threshold, group_by_cols: List[str] = None
):
    if group_by_cols:
        return _GroupedPropertyComparisonExpectation(
            col, property, op, threshold, group_by_cols
        )
    return _PropertyComparisonExpectation(col, property, op, threshold)


def _get_property_timestamp_comparison_expectation(
    col, property, op, threshold, group_by_cols: List[str] = None
):
    if group_by_cols:
        typed_property = (
            MaxProperty(T.TimestampType())
            if isinstance(property, MaxProperty)
            else MinProperty(T.TimestampType())
        )
        return _GroupedPropertyTimestampComparisonExpectation(
            col, typed_property, op, threshold, group_by_cols
        )
    return _TimestampPropertyComparisonExpectation(col, property, op, threshold)


def _get_property_relative_timestamp_comparison_expectation(
    col, property, op, threshold, group_by_cols: List[str] = None
):
    if group_by_cols:
        typed_property = (
            MaxProperty(T.TimestampType())
            if isinstance(property, MaxProperty)
            else MinProperty(T.TimestampType())
        )
        return _GroupedPropertyTimestampRelativeComparisonExpectation(
            col, typed_property, op, threshold, group_by_cols
        )
    return _TimestampPropertyRelativeComparison(col, property, op, threshold)
