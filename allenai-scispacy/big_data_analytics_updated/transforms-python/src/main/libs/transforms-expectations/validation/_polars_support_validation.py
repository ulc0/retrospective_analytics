from transforms.expectations.core._expectation import Expectation

from transforms.expectations.composite._composite_expectation import (
    AllExpectation,
    AnyExpectation,
    NotExpectation,
)
from transforms.expectations.core._true_false_expectation import TrueFalseExpectation
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
from transforms.expectations.timestamp._timestamp_comparison_expectation import (
    _TimestampRelativeComparisonExpectation,
    _TimestampComparisonExpectation,
    _TimestampColumnComparisonExpectation,
)
from transforms.expectations.property._property_expectation import (
    _PropertyComparisonExpectation,
)
from transforms.expectations.property._grouped_property_expectation import (
    _GroupedPropertyComparisonExpectation,
    _GroupedPropertyTimestampComparisonExpectation,
)
from transforms.expectations.grouped._group_by_expectation import (
    _UniqueExpectation,
    _RowCountComparisonExpectation,
    _DatasetRowCountComparisonExpectation,
)

from transforms.expectations.core._primary_key_expectation import _PrimaryKeyExpectation
from transforms.expectations.schema._schema_expectation import _SchemaExpectation

POLARS_SUPPORTED_EXPECATIONS = {
    _NonNullExpectation,
    _NullExpectation,
    _ColComparisonExpectation,
    _ColumnsExistExpectation,
    _ColumnRegexExpectation,
    _ColumnValuesExpectation,
    _ComparisonExpectation,
    TrueFalseExpectation,
    AllExpectation,
    AnyExpectation,
    NotExpectation,
    _PropertyComparisonExpectation,
    _SchemaExpectation,
    _ArrayContainsExpectation,
    _SizeExpectation,
    _TimestampComparisonExpectation,
    _TimestampRelativeComparisonExpectation,
    _TimestampColumnComparisonExpectation,
    _ColumnTypeExpectation,
    _PrimaryKeyExpectation,
    _UniqueExpectation,
    _RowCountComparisonExpectation,
}


# Because we have a deep inheritance model where Expectations
# can be subclasses of other instantiable expectations, we must state expectations that we do not support
# that subclass supported expectations
POLARS_EXPLICITLY_UNSUPPORTED_EXPECTATIONS = {
    _GroupedPropertyComparisonExpectation,
    _GroupedPropertyTimestampComparisonExpectation,
    _DatasetRowCountComparisonExpectation,
}


def expectation_is_supported_in_polars_mode(expectation: Expectation) -> bool:
    for unsupported_type in POLARS_EXPLICITLY_UNSUPPORTED_EXPECTATIONS:
        if isinstance(expectation, unsupported_type):
            return False

    if isinstance(expectation, (AllExpectation, AnyExpectation, NotExpectation)):
        for child in expectation.children():
            if not expectation_is_supported_in_polars_mode(child):
                return False

    for supported_type in POLARS_SUPPORTED_EXPECATIONS:
        if isinstance(expectation, supported_type):
            return True

    return False
