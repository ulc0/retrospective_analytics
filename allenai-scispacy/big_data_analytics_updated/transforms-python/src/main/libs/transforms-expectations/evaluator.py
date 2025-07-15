#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from __future__ import annotations
from pyspark.sql import DataFrame
from polars import LazyFrame
import json
import logging
from typing import (
    Dict,
    Callable,
    Union,
    NamedTuple,
    Tuple,
    List,
    Iterator,
    TYPE_CHECKING,
    Optional,
)

from transforms.expectations.property._column_properties import Aggregate

from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    WrappedPolarsLazyFrame,
    SparkOrPolarsSchema,
    ColumnFunctions as F,
    keep_and_add_cols,
    use_new_cols,
)
from transforms.expectations.wrapper._backend import SET_CURRENT_BACKEND, Backend

from ._results import (
    Results,
    build_result_value,
    build_results_struct,
    CollectedResultValue,
    ErrorType,
)

if TYPE_CHECKING:
    from transforms.expectations import Expectation

log = logging.getLogger(__name__)


class JoinPair(NamedTuple):
    pk: str
    fk: str


class JoinKey(NamedTuple):
    dataset: str
    pairs: Tuple[JoinPair]


class JoinValue(NamedTuple):
    column_name: str
    column_alias: str
    fk_alias: str


class JoinEvaluator(object):
    def __init__(self, params_to_df):
        self._column_map = {}  # Maps datasets to columns to be joined from that dataset
        self._params_to_df = params_to_df

    def queue_column_join(
        self, foreign_df, key_pairs, foreign_col, foreign_col_alias, fk_alias
    ):
        key = JoinKey(foreign_df, key_pairs)
        val = self._column_map.get(key, [])
        join_value = JoinValue(foreign_col, foreign_col_alias, fk_alias)
        if join_value not in val:
            val.append(join_value)
            self._column_map[key] = val

    def join_columns(self, df):
        for jk, jv_list in self._column_map.items():
            # We need multiple fk aliases for joins with composite keys
            fk_aliases = []
            fks = []
            join_cond = F.lit(True)
            # join_cond must not be hardcoded to true to avoid a cross join
            if not jk.pairs:
                raise Exception(  # pylint: disable=broad-exception-raised
                    "At least one join pair must be specified"
                )
            for i, jp in enumerate(jk.pairs):
                fk_alias = jv_list[0].fk_alias + "-" + str(i)
                fk_aliases.append(fk_alias)
                fks.append(F.col(jp.fk).alias(fk_alias))
                join_cond = join_cond & (F.col(fk_alias) == F.col(jp.pk))
            cols = list(
                map(lambda jv: F.col(jv.column_name).alias(jv.column_alias), jv_list)
            )
            fk_dataframe = (
                self._params_to_df[jk.dataset]
                .get()
                .select(*cols, *fks)
                .dropDuplicates(fk_aliases)
            )
            df = df.join(fk_dataframe, on=join_cond, how="left")
            df = df.drop(*fk_aliases)
        return df


class AggregateEvaluator(object):
    def __init__(self):
        self._aggregate_map = {}

    def queue_aggregate(self, agg: Aggregate):
        self._aggregate_map[agg.name] = agg

    def add_aggregate_columns(self, df: ExpectationDataFrame) -> ExpectationDataFrame:
        valid_aggregate_columns = [
            aggregate.column.alias(aggregate.name)
            for aggregate in self._aggregate_map.values()
            # Do not run an aggregate if the target column is not present
            if not aggregate.target_column or aggregate.target_column in df.columns
        ]

        if not valid_aggregate_columns:
            return df

        aggregate_df = df.agg(*valid_aggregate_columns)

        return df.crossJoin(F.broadcast(aggregate_df))


class LazyTransformsDataFrame(object):
    def __init__(self, df: Union[DataFrame, Callable[[], DataFrame]]):
        self._df = df
        self._cached_count = None

    def get(self) -> ExpectationDataFrame:
        # Transforms-Python could be lazy and provide us with the dataframe method reference instead.
        if callable(self._df):
            return self._df()
        return self._df

    def count(self) -> int:
        if self._cached_count is None:
            self._cached_count = self.get().count()

        return self._cached_count

    def schema(self) -> SparkOrPolarsSchema:
        return self.get().schema


ParamsToDF = Dict[str, LazyTransformsDataFrame]


class EvaluationTarget:
    def __init__(self, df: ExpectationDataFrame, params_to_df: ParamsToDF):
        self.df = df
        self.params_to_df = params_to_df
        self._df_with_value_cols: Optional[ExpectationDataFrame] = None

    def set_df_with_value_cols(self, df_with_value_cols: ExpectationDataFrame) -> None:
        """
        This method should be used after the value columns have been added to the dataframe.

        We must validate the dtypes of the value columns used in the `predicate` and `reduce_to_value` methods,
        because polars will throw when trying to compare different types.

        As such, we need the schema of the dataframe that contains all the columns that may be referenced
        in these methods.
        """
        self._df_with_value_cols = df_with_value_cols

    def get_df_with_value_cols(self) -> ExpectationDataFrame:
        if self._df_with_value_cols is None:
            raise ValueError(
                "Trying to access the dataframe with appended value columns without it having been provided. "
                "It's likely this method is being used outside of the `predicate` or `reduce_to_value` methods."
            )
        return self._df_with_value_cols


def _reduce_expectation_to_result(
    target: EvaluationTarget, expectation: Expectation
) -> ExpectationColumn:
    """Return a column of StructType(failed_rows, value) that contains number of failed rows and a result value."""
    value_column = F.col(expectation.value_column_name())
    predicate_column = F.col(expectation.predicate_column_name())
    is_failed_column = F.when(predicate_column, F.lit(0)).otherwise(F.lit(1))

    failed_rows = (
        F.failed_row_count(predicate_column)
        if expectation.test_against_all_rows()
        else F.first(is_failed_column)
    )

    result = expectation.reduce_to_value(value_column, predicate_column, target)
    results_struct = build_results_struct(failed_rows, result)

    return results_struct


def _evaluate(
    dataframe: ExpectationDataFrame,
    expectations: List[Expectation],
    params_to_df: ParamsToDF,
) -> Results:
    """Evaluate the given expectations against the dataframe."""
    original_df = dataframe
    target = EvaluationTarget(df=original_df, params_to_df=params_to_df)

    # We can make an optimization if all expectations are static in which case we don't need to read any data
    if all(is_static_expectation(expectation) for expectation in expectations):
        dataframe = dataframe.limit(0)

    je = JoinEvaluator(params_to_df)
    ae = AggregateEvaluator()
    # Queue joins from foreign values expectations
    for exp in expectations:
        # Add foreign_columns returns a new df because foreign col expectations are expected to always add
        # foreign and error columns, even if they are empty
        dataframe = exp.add_foreign_column(target, dataframe, je)
        # Queue aggregate columns to be added to the dataframe
        exp.add_aggregate_columns(ae)

    # Join foreign columns, performing each unique join only once
    dataframe = je.join_columns(dataframe)
    dataframe = ae.add_aggregate_columns(dataframe)

    # Construct a dataframe with predicate and value columns for each expectation
    value_columns = [
        exp.value(target).alias(exp.value_column_name()) for exp in expectations
    ]

    dataframe = keep_and_add_cols(dataframe, *value_columns)

    # Update the dataframe that the target evaluator has access to
    # This will allow us to do type checking on the new value columns
    # with the @check_equal_types_compared decorator when evaluating predicates
    target.set_df_with_value_cols(dataframe)

    predicate_columns = [
        exp.predicate(F.col(exp.value_column_name()), target).alias(
            exp.predicate_column_name()
        )
        for exp in expectations
    ]

    dataframe = keep_and_add_cols(dataframe, *predicate_columns)
    # Reduce columns to results
    # We let pyspark serialize this for us to avoid having to do custom serialization.
    dataframe = use_new_cols(
        dataframe,
        *[
            F.to_json(_reduce_expectation_to_result(target, exp)).alias(exp.id())
            for exp in expectations
        ],
    )

    # Since we've reduced the results we can just take a single row
    results_row = dataframe.first()

    return Results(
        {
            exp.id(): build_result_value(json.loads(results_row[exp.id()]))
            for exp in expectations
        }
    )


def build_params_to_expectation_df(params_to_df) -> ParamsToDF:
    return (
        {name: LazyTransformsDataFrame(df) for name, df in params_to_df.items()}
        if params_to_df
        else {}
    )


def extract_and_evaluate_expectations(
    dataframe: ExpectationDataFrame, expectations, params_to_df
):
    all_expectations = list(_depth_first_traversal(expectations))
    try:
        params_to_expectation_df = build_params_to_expectation_df(params_to_df)
        return _evaluate(dataframe, all_expectations, params_to_expectation_df)

    # We should do ALL we can to make sure expectations don't throw runtime exceptions. However we are not perfect and
    # we might cause some runtime exceptions. In this case let's make sure we catch them correctly.
    except Exception as e:
        log.exception("An error occurred evaluating expectations")
        return Results(
            {
                exp.id(): CollectedResultValue(
                    failed_rows=1, value=None, type=ErrorType.EXCEPTION
                )
                for exp in all_expectations
            },
            exception=e,
        )


def evaluate_all(
    dataframe: DataFrame,
    expectations,
    params_to_df: Dict[str, Union[DataFrame, Callable[[], DataFrame]]] = None,
) -> Results:
    """Evaluate the given expectations, including nested expectations, against the dataframe."""
    SET_CURRENT_BACKEND(Backend.SPARK)
    dataframe_ref = dataframe
    return extract_and_evaluate_expectations(dataframe_ref, expectations, params_to_df)


def evaluate_all_polars(
    dataframe: LazyFrame,
    expectations,
    params_to_df: Dict[str, Union[LazyFrame, Callable[[], LazyFrame]]] = None,
) -> Results:
    SET_CURRENT_BACKEND(Backend.POLARS)
    """Evaluate the given expectations, including nested expectations, against the polars lazyframe."""
    dataframe_ref = dataframe
    wrapped_dataframe = WrappedPolarsLazyFrame(dataframe_ref)
    return extract_and_evaluate_expectations(
        wrapped_dataframe, expectations, params_to_df
    )


def _depth_first_traversal(expectations) -> Iterator[Expectation]:
    for exp in expectations:
        yield exp
        for child_exp in _depth_first_traversal(exp.children()):
            yield child_exp


def is_static_expectation(expectation) -> bool:
    def _is_static(cls):
        return hasattr(cls, "__is_static_expectation__")

    return _is_static(expectation) and all(
        _is_static(child) for child in expectation.children()
    )
