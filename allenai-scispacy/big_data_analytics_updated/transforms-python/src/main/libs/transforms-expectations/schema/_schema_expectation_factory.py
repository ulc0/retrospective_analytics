from ._schema_expectation import (
    _SchemaContainsExpectation,
    _SchemaEqualsExpectation,
    _SchemaIsSubsetOfExpectation,
    ColumnMapping,
)


class SchemaExpectationFactory(object):
    """Create expectations on the schema of a dataframe."""

    def contains(self, column_mapping: ColumnMapping) -> "Expectation":
        """Assert the schema of the dataframe contains at least the specified columns.

        When column -> dataType mapping is provided nullability of columns is ignored.
        """
        return _SchemaContainsExpectation(column_mapping)

    def equals(self, column_mapping: ColumnMapping) -> "Expectation":
        """Assert the schema of the dataframe equals the specified schema.

        When column -> dataType mapping is provided nullability of columns is ignored.
        """
        return _SchemaEqualsExpectation(column_mapping)

    def is_subset_of(self, column_mapping: ColumnMapping) -> "Expectation":
        """Assert the schema of the dataframe is a subset of the specified schema.

        When column -> dataType mapping is provided nullability of columns is ignored.
        """
        return _SchemaIsSubsetOfExpectation(column_mapping)
