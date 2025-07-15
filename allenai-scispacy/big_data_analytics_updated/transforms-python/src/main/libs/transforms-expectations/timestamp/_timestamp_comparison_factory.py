import operator

from transforms.expectations.core._numeric_comparison_factory import (
    NumericComparisonFactory,
)
from transforms.expectations.timestamp._timestamp_utils import _is_offset_positive
from enum import Enum


class TimestampComparisonFactory(object):
    def __init__(self, timestamp_expectation_factory):
        self.timestamp_expectation_factory = timestamp_expectation_factory

    def is_on_or_after(self, timestamp) -> "Expectation":
        """
        Assert that the values in the column (interpreted as a timestamp) are on, or after the provided timestamp
        Thresholds can be provided as a `datetime.datetime` or as an ISO8601 formatted string

        Warning: This expectation will fail if the column is not a Timestamp column
        Warning: This expectation should be used with STATIC dates only. Comparison with dynamic date should be
        done by using `timestamp_offset_to_current_time()` and `timestamp_offset_from_current_time()`
        """
        return self.timestamp_expectation_factory(operator.ge, timestamp)

    def is_after(self, timestamp) -> "Expectation":
        """Assert that the values in the column (interpreted as a timestamp) are strictly after the provided timestamp
        Thresholds can be provided as a `datetime.datetime` or as an ISO8601 formatted string

        Warning: This expectation will fail if the column is not a Timestamp column
        Warning: This expectation should be used with STATIC dates only. Comparison with dynamic date should be
        done by using `timestamp_offset_to_current_time()` and `timestamp_offset_from_current_time()`
        """
        return self.timestamp_expectation_factory(operator.gt, timestamp)

    def is_on_or_before(self, timestamp) -> "Expectation":
        """Assert that the values in the column (interpreted as a timestamp) are on, or before the provided timestamp
        Thresholds can be provided as a `datetime.datetime` or as an ISO8601 formatted string

        Warning: This expectation will fail if the column is not a Timestamp column
        Warning: This expectation should be used with STATIC dates only. Comparison with dynamic date should be
        done by using `timestamp_offset_to_current_time()` and `timestamp_offset_from_current_time()`
        """
        return self.timestamp_expectation_factory(operator.le, timestamp)

    def is_before(self, timestamp) -> "Expectation":
        """Assert that the values in the column (interpreted as a timestamp) are before the provided timestamp
        Thresholds can be provided as a `datetime.datetime` or as an ISO8601 formatted string

        Warning: This expectation will fail if the column is not a Timestamp column
        Warning: This expectation should be used with STATIC dates only. Comparison with dynamic date should be
        done by using `timestamp_offset_to_current_time()` and `timestamp_offset_from_current_time()`
        """
        return self.timestamp_expectation_factory(operator.lt, timestamp)


class RelativeTimestampComparisonFactory(object):
    class Method(Enum):
        FROM = "timestamp_offset_from_current_time"
        TO = "timestamp_offset_to_current_time"

    def __init__(self, relative_timestamp_expectation_factory):
        self._relative_timestamp_expectation_factory = (
            relative_timestamp_expectation_factory
        )

    def __internal_factory(self, op, offset, method: Method):
        # We provide two helper functions to let users deal with timestamps in the past or in the future
        # We'll need to mirror the comparison in case users are dealing with timestamp in the past.

        if not _is_offset_positive(offset):
            other = (
                "timestamp_offset_from_current_time"
                if method == RelativeTimestampComparisonFactory.Method.TO
                else "timestamp_offset_to_current_time"
            )
            raise ValueError(
                "Negative offset values are not allowed for relative timestamp comparisons."
                f"Please use the opposite corresponding method {other}"
            )

        if method == RelativeTimestampComparisonFactory.Method.TO:
            invert_operator = op
            if op == operator.gt:
                invert_operator = operator.lt
            elif op == operator.ge:
                invert_operator = operator.le
            elif op == operator.lt:
                invert_operator = operator.gt
            elif op == operator.le:
                invert_operator = operator.ge
            return self._relative_timestamp_expectation_factory(
                invert_operator, -offset
            )
        else:
            return self._relative_timestamp_expectation_factory(op, offset)

    def timestamp_offset_from_current_time(self):
        """
        Asserts that the values in the column are (more / less) than a given offset from the current time.
        Use this method for dealing with timestamps in the FUTURE.

        Offsets are expressed either in `seconds` or as `timedelta` objects

        # Check that all dates are less than 1 hour in the future
        E.col("timestamp").timestamp_offset_from_current_time().lt(3600)

        # Check that all dates are more than 2 days in the future
        E.col("timestamp").timestamp_offset_from_current_time().ge(timedelta(days=2))

        Warning: This expectation should be used for RELATIVE dates only. Comparing with a static date should be
        done with `is_after` and related methods.
        """
        return NumericComparisonFactory(
            lambda op, threshold: self.__internal_factory(
                op, threshold, method=RelativeTimestampComparisonFactory.Method.FROM
            )
        )

    def timestamp_offset_to_current_time(self):
        """
        Asserts that the values in the column are (more / less) than a given offset from the current time.
        Use this method for dealing with timestamps in the PAST.

        Offsets are expressed either in `seconds` or as `timedelta` objects

        # Check that all dates are less than 1 hour in the past
        E.col("timestamp").timestamp_offset_to_current_time().lt(3600)

        # Check that all dates are more than 2 days ago
        E.col("timestamp").timestamp_offset_to_current_time().ge(timedelta(days=2))

        Warning: This expectation should be used for RELATIVE dates only. Comparing with a static date should be
        done with `is_after` and related methods.
        """
        return NumericComparisonFactory(
            lambda op, threshold: self.__internal_factory(
                op, threshold, method=RelativeTimestampComparisonFactory.Method.TO
            )
        )


class TimestampColumnComparisonFactory(object):
    def __init__(self, col_comparison_timestamp_factory):
        self.col_comparison_timestamp_factory = col_comparison_timestamp_factory

    def is_after_col(self, other_col: str, offset_in_seconds: float = 0):
        """
        Assert that the values in the column are after the values in other column, plus an optional delay.

        :param other_col:
        :param offset_in_seconds: An optional positive or negative delay (in seconds) that will be added to
            timestamps from other_col before the comparison.

        Warning: This expectation will fail if either columns are not a Timestamp column.
        Warning: This expectation should be used with STATIC dates and delay only. Comparing with `datetime.now()` or
            dates derived from `datetime.now()` will yield unexpected results.
        """

        return self.col_comparison_timestamp_factory(
            operator.gt, other_col, offset_in_seconds
        )

    def is_on_or_after_col(self, other_col: str, offset_in_seconds: float = 0):
        """
        Assert that the values in the column are on, or after the values in other column, plus an optional delay.

        :param other_col:
        :param offset_in_seconds: An optional positive or negative delay (in seconds) that will be added to
            timestamps from other_col before the comparison.

        Warning: This expectation will fail if either columns are not a Timestamp column.
        Warning: This expectation should be used with STATIC dates and delay only. Comparing with `datetime.now()` or
            dates derived from `datetime.now()` will yield unexpected results.
        """

        return self.col_comparison_timestamp_factory(
            operator.ge, other_col, offset_in_seconds
        )

    def is_before_col(self, other_col: str, offset_in_seconds: float = 0):
        """
        Assert that the values in the column are before the values in other column, plus an optional delay.

        :param other_col:
        :param offset_in_seconds: An optional positive or negative delay (in seconds) that will be added to
            timestamps from other_col before the comparison.

        Warning: This expectation will fail if either columns are not a Timestamp column.
        Warning: This expectation should be used with STATIC dates and delay only. Comparing with `datetime.now()` or
            dates derived from `datetime.now()` will yield unexpected results.
        """

        return self.col_comparison_timestamp_factory(
            operator.lt, other_col, offset_in_seconds
        )

    def is_on_or_before_col(self, other_col: str, offset_in_seconds: float = 0):
        """
        Assert that the values in the column are on or before the values in other column, plus an optional delay.

        :param other_col:
        :param offset_in_seconds: An optional positive or negative delay (in seconds) that will be added to
            timestamps from other_col before the comparison.

        Warning: This expectation will fail if either columns are not a Timestamp column.
        Warning: This expectation should be used with STATIC dates and delay only. Comparing with `datetime.now()` or
            dates derived from `datetime.now()` will yield unexpected results.
        """

        return self.col_comparison_timestamp_factory(
            operator.le, other_col, offset_in_seconds
        )
