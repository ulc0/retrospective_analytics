#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import json
import polars as pl
import inspect

from typing import Callable, List

from transforms.expectations.utils._col_utils import (
    missing_columns,
    get_actual_column_type,
)
from transforms.expectations._results import (
    missing_columns_result,
    ResultValue,
    ErrorType,
)
from transforms.expectations._data_types import (
    get_simple_string_representation_of_type,
)
from transforms.expectations.core._utils import as_list
from transforms.expectations.evaluator import EvaluationTarget
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    ColumnFunctions as F,
)
from transforms.expectations.wrapper._backend import GET_CURRENT_BACKEND, Backend

POLARS_DATATYPES_TO_PYTHON_TYPES = {
    int: {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    },
    float: {pl.Float32, pl.Float64},
    str: {pl.Utf8, pl.String},
    bool: {pl.Boolean},
}


def check_and_inject_column_type(
    required_types, column_extractor: Callable[[object], str], bind_col_type=True
):
    """Protects calls against accessing a column of incorrect type.

    This decorator will make sure the underlying method is not called,
    if the column is not of the required type.

    If bind_col_type == true: If the anotated function contains an argument called 'col_type', this decorator
    will inject the column data type on it."""
    required_types = as_list(required_types)

    def _get_type_if_matches_required(self, target) -> type:
        actual_type_obj = get_actual_column_type(target.df, column_extractor(self))
        actual_type_type = type(actual_type_obj)
        # we return the field obj (which contains more information like inner data type
        # in cases of lists
        return actual_type_obj if actual_type_type in required_types else None

    def _decorator(method):
        if method.__name__ == "predicate":

            def _protected_predicate(
                self, value_column: ExpectationColumn, target: EvaluationTarget
            ):
                matched_type = _get_type_if_matches_required(self, target)
                if not matched_type:
                    return F.lit(False)

                method_signature = inspect.signature(method)
                if "col_type" in method_signature.parameters and bind_col_type:
                    return method(self, value_column, target, col_type=matched_type)
                else:
                    return method(self, value_column, target)

            return _retain_method_signature(_protected_predicate, method)

        elif method.__name__ == "reduce_to_value":

            def _protected_reduce_to_value(
                self,
                value_column: ExpectationColumn,
                predicate_column: ExpectationColumn,
                target: EvaluationTarget,
            ):
                actual_type = type(
                    get_actual_column_type(target.df, column_extractor(self))
                )
                if actual_type not in required_types:
                    return ResultValue(
                        F.lit(
                            json.dumps(
                                {
                                    "actualType": get_simple_string_representation_of_type(
                                        actual_type
                                    ),
                                    "allowedTypes": deduplicate_and_preserve_order(
                                        [
                                            get_simple_string_representation_of_type(
                                                required_type
                                            )
                                            for required_type in required_types
                                        ]
                                    ),
                                }
                            )
                        ),
                        ErrorType.INVALID_COLUMN_TYPE,
                    )

                return method(self, value_column, predicate_column, target)

            return _retain_method_signature(_protected_reduce_to_value, method)

        elif method.__name__ == "value":

            def _protected_value(self, target: EvaluationTarget):
                matched_type = _get_type_if_matches_required(self, target)
                if not matched_type:
                    return F.lit(None)

                method_signature = inspect.signature(method)
                if "col_type" in method_signature.parameters:
                    return method(self, target, col_type=matched_type)
                else:
                    return method(self, target)

            return _retain_method_signature(_protected_value, method)

        raise Exception(  # pylint: disable=broad-exception-raised
            "Require columns is only supported on expectation methods"
        )

    return _decorator


def check_equal_types_compared(method):
    """Spark allows for comparing different data types and just returns False when that's the case.

    However, polars will raise an error when comparing different data types. As such, this decorator
    compares polars dtypes with python types to verify the comparison is valid, otherwise return a False EXPR
    """

    def _protected_predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ):
        if GET_CURRENT_BACKEND() == Backend.SPARK:
            return method(self, value_column, target)

        threshold_type = type(self._threshold)
        if threshold_type not in POLARS_DATATYPES_TO_PYTHON_TYPES:
            return F.lit(False)

        value_column_type = get_col_dtype(value_column, target.get_df_with_value_cols())
        allowed_polars_types_for_treshold_dtype = POLARS_DATATYPES_TO_PYTHON_TYPES[
            threshold_type
        ]

        if value_column_type not in allowed_polars_types_for_treshold_dtype:
            return F.lit(False)

        return method(self, value_column, target)

    return _retain_method_signature(_protected_predicate, method)


def check_columns_exist(method):
    """Protects calls against accessing columns that are not present.

    This decorator will make sure the underlying method is not called if the columns are not present,
    and will instead return an error result.

    Note: Currently only implemented for predicate and reduce_to_value methods."""

    if method.__name__ == "predicate":

        def _protected_predicate(
            self, value_column: ExpectationColumn, target: EvaluationTarget
        ):
            if missing_columns(self.columns(), target.df):
                return F.lit(False)

            return method(self, value_column, target)

        return _retain_method_signature(_protected_predicate, method)

    elif method.__name__ == "reduce_to_value":

        def _protected_reduce_to_value(
            self,
            value_column: ExpectationColumn,
            predicate_column: ExpectationColumn,
            target: EvaluationTarget,
        ):
            missing_cols = missing_columns(self.columns(), target.df)
            if missing_cols:
                return missing_columns_result(missing_cols)

            return method(self, value_column, predicate_column, target)

        return _retain_method_signature(_protected_reduce_to_value, method)

    elif method.__name__ == "value":

        def _protected_value(self, target: EvaluationTarget):
            if missing_columns(self.columns(), target.df):
                return F.lit(None)

            return method(self, target)

        return _retain_method_signature(_protected_value, method)

    elif method.__name__ == "add_foreign_column":

        def _protected_foreign_col(self, target, df, je):
            df = df.withColumn(self._foreign_column, F.lit(None)).withColumn(
                self._error_column, F.lit(None)
            )
            # Do not add an error column here as missing local col will be handled by reduce_to_value
            if missing_columns(self.columns(), target.df):
                return df

            return method(self, target, df, je)

        return _retain_method_signature(_protected_foreign_col, method)

    raise Exception(  # pylint: disable=broad-exception-raised
        "Require columns is only supported on predicate and reduce_to_value methods"
    )


def deduplicate_and_preserve_order(strings: List[str]) -> List[str]:
    return list(dict.fromkeys(strings))  # dict maintains insertion order


def static_expectation(cls):
    setattr(cls, "__is_static_expectation__", True)
    return cls


def _retain_method_signature(new_method, old_method):
    new_method.__name__ = old_method.__name__
    new_method.__doc__ = old_method.__doc__
    return new_method


def get_col_dtype(col: ExpectationColumn, df: ExpectationDataFrame) -> pl.DataType:
    column: pl.Expr = col.unwrap()
    columm_name = column.meta.output_name()

    df: pl.LazyFrame = df.unwrap()
    return df.schema[columm_name]
