#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import json
from abc import ABCMeta
from typing import Dict, Union, List, Optional, Any

from pyspark.sql import types as T
import polars as pl

from transforms.expectations.core._expectation import Expectation
from transforms.expectations.utils._expectation_utils import static_expectation
from transforms.expectations._results import (
    ResultValue,
    ResultType,
    SCHEMA_RESULT_TYPE_KEY,
    SCHEMA_ERROR_FIELD_KEY,
    Results,
    get_conjure_result_value,
)
from transforms.expectations._data_types import get_conjure_data_type
from transforms.expectations.evaluator import EvaluationTarget
from transforms.expectations.foreign._dataset_references import DatasetSchema
from transforms.expectations.evaluator import ParamsToDF
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    PolarsOrSparkDataType,
    PolarsOrSparkSchemaField,
    ColumnFunctions as F,
    schema_difference,
    convert_to_schema,
    SparkOrPolarsSchema,
)


ColumnMapping = Union[
    Dict[str, T.DataType], T.StructType, DatasetSchema, Dict[str, pl.DataType]
]


@static_expectation
class _SchemaExpectation(Expectation, metaclass=ABCMeta):
    def __init__(self, column_mapping: ColumnMapping):
        super(_SchemaExpectation, self).__init__()

        self._column_mapping = column_mapping

    def test_against_all_rows(self) -> bool:
        return False

    def columns(self) -> List[str]:
        # No need to return columns for this check.
        return []

    @staticmethod
    def _get_struct_type_from_column_mapping(
        column_mapping: ColumnMapping,
        params_to_df: ParamsToDF,
    ) -> SparkOrPolarsSchema:
        if isinstance(column_mapping, Dict):
            return convert_to_schema(column_mapping)
        elif isinstance(column_mapping, T.StructType):
            return column_mapping
        elif isinstance(column_mapping, DatasetSchema):
            return column_mapping.evaluate(params_to_df)

        raise TypeError(
            "Column mapping must be either a dict of name to type or a StructType"
        )

    def _field_type(self, field: PolarsOrSparkDataType):
        return {"type": get_conjure_data_type(field.dataType), "nullable": None}

    def _field_error(
        self,
        actual_field: Union[PolarsOrSparkSchemaField, None],
        expected_field: Union[PolarsOrSparkSchemaField, None],
    ) -> Dict[str, Optional[Any]]:
        return {
            "columnName": expected_field.name if expected_field else actual_field.name,
            "actual": self._field_type(actual_field) if actual_field else None,
            "expected": self._field_type(expected_field) if expected_field else None,
        }

    def _fields_match(
        self, actual: PolarsOrSparkSchemaField, expected: PolarsOrSparkSchemaField
    ) -> bool:
        return (actual and expected) and (actual.dataType == expected.dataType)

    def _column_definitions(self):
        expected_struct_type = self._get_struct_type_from_column_mapping(
            self._column_mapping, params_to_df=None
        )
        return [
            {
                "columnName": field.name,
                "type": get_conjure_data_type(field.dataType),
                "nullable": None,
            }
            for field in expected_struct_type.fields
        ]

    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            F.lit(json.dumps(self._schema_errors(target.df, target.params_to_df))),
            ResultType.SCHEMA_ERROR,
        )

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.lit(not self._schema_errors(target.df, target.params_to_df))

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def _schema_errors(self, df: ExpectationDataFrame, params_to_df: ParamsToDF):
        """Computes all errors between expected schema and dataframe schema."""
        expected_struct_type = self._get_struct_type_from_column_mapping(
            self._column_mapping, params_to_df
        )
        schema_diff = schema_difference(expected_struct_type, df.schema)
        added_column_errors = [
            self._field_error(None, dropped) for dropped in schema_diff.dropped_columns
        ]
        missing_column_errors = [
            self._field_error(added, None) for added in schema_diff.added_columns
        ]
        changed_column_types_errors = [
            self._field_error(cast_column.new, cast_column.old)
            for cast_column in schema_diff.cast_columns
            if not self._fields_match(cast_column.new, cast_column.old)
        ]

        return self._get_relevant_errors(
            missing_column_errors, changed_column_types_errors, added_column_errors
        )

    def _get_relevant_errors(
        self, missing_column_errors, changed_column_type_errors, added_column_errors
    ):
        """Picks the relevant error types between expected schema and dataframe from the error types given."""
        raise NotImplementedError

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        # We should only pass on the empty dataframe if there are no errors present
        result_value = get_conjure_result_value(results[self.id()])
        if SCHEMA_RESULT_TYPE_KEY not in result_value:
            return True

        schema_result = result_value[SCHEMA_RESULT_TYPE_KEY]
        return (
            not schema_result[SCHEMA_ERROR_FIELD_KEY]
            if SCHEMA_ERROR_FIELD_KEY in schema_result
            else True
        )


@static_expectation
class _SchemaContainsExpectation(_SchemaExpectation):
    def __init__(self, column_mapping: ColumnMapping):
        super(_SchemaContainsExpectation, self).__init__(column_mapping)

    def _get_relevant_errors(
        self, missing_column_errors, changed_column_type_errors, added_column_errors
    ):
        # We ignore missing column errors for this expectations.
        # Missing cols mean that something in the expected schema was not in the actual schema
        # which is allowed in this case.
        return added_column_errors + changed_column_type_errors

    def definition(self):
        if isinstance(self._column_mapping, DatasetSchema):
            return {
                "type": "schema",
                "schema": {
                    "type": "containsCrossDataset",
                    "containsCrossDataset": {
                        "foreignDataset": self._column_mapping.definition(),
                    },
                },
            }
        else:
            return {
                "type": "schema",
                "schema": {
                    "type": "contains",
                    "contains": {"columns": self._column_definitions()},
                },
            }


@static_expectation
class _SchemaEqualsExpectation(_SchemaExpectation):
    def __init__(self, column_mapping: ColumnMapping):
        super(_SchemaEqualsExpectation, self).__init__(column_mapping)

    def _get_relevant_errors(
        self, missing_column_errors, changed_column_type_errors, added_column_errors
    ):
        return missing_column_errors + changed_column_type_errors + added_column_errors

    def definition(self):
        if isinstance(self._column_mapping, DatasetSchema):
            return {
                "type": "schema",
                "schema": {
                    "type": "matchesCrossDataset",
                    "matchesCrossDataset": {
                        "foreignDataset": self._column_mapping.definition(),
                    },
                },
            }
        else:
            return {
                "type": "schema",
                "schema": {
                    "type": "matches",
                    "matches": {"columns": self._column_definitions()},
                },
            }


@static_expectation
class _SchemaIsSubsetOfExpectation(_SchemaExpectation):
    def __init__(self, column_mapping: ColumnMapping):
        super(_SchemaIsSubsetOfExpectation, self).__init__(column_mapping)

    def _get_relevant_errors(
        self, missing_column_errors, changed_column_type_errors, added_column_errors
    ):
        # If there are any added columns to the schema that are not in the dataframe, then the dataframe is a subset
        # of the schema. Due to that we only err if there are columns that changed, or dataframe columns not present
        # on the schema.
        return missing_column_errors + changed_column_type_errors

    def definition(self):
        if isinstance(self._column_mapping, DatasetSchema):
            return {
                "type": "schema",
                "schema": {
                    "type": "isSubsetCrossDataset",
                    "isSubsetCrossDataset": {
                        "foreignDataset": self._column_mapping.definition(),
                    },
                },
            }
        else:
            return {
                "type": "schema",
                "schema": {
                    "type": "isSubset",
                    "isSubset": {"columns": self._column_definitions()},
                },
            }
