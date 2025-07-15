import json
from abc import ABCMeta
from typing import List, Callable

from transforms.expectations._types import as_comparison_operator
from transforms.expectations.core._expectation import Expectation
from transforms.expectations.foreign._dataset_references import (
    _MissingDatasetException,
    _MissingForeignColumnsException,
    DatasetComposite,
    _MismatchedCompositesException,
    JoinedDatasetColumn,
)
from transforms.expectations.utils._expectation_utils import check_columns_exist
from transforms.expectations._results import ResultValue, ErrorType
from transforms.expectations.utils._col_utils import (
    _first_failed_value,
    _get_cell_struct,
    _get_composite_cell_struct,
)
from transforms.expectations.evaluator import EvaluationTarget, JoinEvaluator, JoinPair
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    ColumnFunctions as F,
)


class _ForeignValuesExpectation(Expectation, metaclass=ABCMeta):
    def __init__(self, pk_cols: List[str], fk_cols: DatasetComposite, to_add: str):
        super(_ForeignValuesExpectation, self).__init__()
        self._pk_cols = pk_cols
        self._to_add = to_add
        self._fk_cols = fk_cols
        self._foreign_column = f"{self.id()}_foreign"
        self._error_column = f"{self.id()}_foreign_val_error"

    def _add_error_column(self, df, err):
        return df.withColumn(
            self._error_column, F.lit(json.dumps(err.as_conjure_definition()))
        )

    def _check_keys(self):
        pks = self._pk_cols
        fks = self._fk_cols.col_names()
        if len(pks) != len(fks):
            raise _MismatchedCompositesException(pks, fks)

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            _first_failed_value(predicate_column, value_column),
            ErrorType.FOREIGN_DATASET_ERROR,
        )

    @check_columns_exist
    def add_foreign_column(
        self, target: EvaluationTarget, df: ExpectationDataFrame, je: JoinEvaluator
    ) -> ExpectationDataFrame:
        """Returns a new dataframe with the specified foreign column added. If foreign columns are missing,
        this method returns the unmodified dataframe. Methods that make use of the foreign column
        should first check that it exists on the target dataframe.

        This function MUST always preserve the row count and columns of the original dataframe. The only
        allowed operation is to add a new column to the dataframe. In the case of 1toN relationships,
        an arbitrary foreign row will be joined to the corresponding target row.
        """
        try:
            foreign_dataframe = self._fk_cols.evaluate(target.params_to_df)
            self._check_keys()
        except (
            _MissingDatasetException,
            _MissingForeignColumnsException,
            _MismatchedCompositesException,
        ) as e:
            return self._add_error_column(df, e)

        if self._to_add not in foreign_dataframe.columns:
            return self._add_error_column(
                df,
                _MissingForeignColumnsException(
                    self._fk_cols._dataset_reference._dataset_parameter, self._to_add
                ),
            )

        fk_alias = f"{self.id()}_fk"
        pks = self._pk_cols
        fks = self._fk_cols.col_names()

        key_pairs = [JoinPair(pks[i], fks[i]) for i in range(len(pks))]

        je.queue_column_join(
            self._fk_cols._dataset_reference._dataset_parameter,
            tuple(key_pairs),
            self._to_add,
            self._foreign_column,
            fk_alias,
        )

        # Drop the foreign column placeholder - at this point we have committed to join a column
        # which will cause a duplicate if we don't drop the placeholder.
        return df.drop(self._foreign_column)


class _ReferentialIntegrityExpectation(_ForeignValuesExpectation):
    def __init__(self, pk_cols: List[str], foreign_key_composite: DatasetComposite):
        # Join in any column - we only need to see if it is null
        super(_ReferentialIntegrityExpectation, self).__init__(
            pk_cols, foreign_key_composite, foreign_key_composite.col_names()[0]
        )

    def columns(self) -> List[str]:
        return self._pk_cols

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        err_col = F.col(self._error_column)
        cols_name = (
            "Composite(" + ",".join([col for col in self._pk_cols]) + ")"
            if len(self._pk_cols) > 1
            else self._pk_cols[0]
        )
        return F.struct(
            err_col.alias("err"),
            F.struct(
                *[
                    (
                        _get_composite_cell_struct(target.df, self._pk_cols)
                        if len(self._pk_cols) > 1
                        else _get_cell_struct(target.df, self._pk_cols[0])
                    ).alias(
                        cols_name
                    )  # Column must exist in target.df
                ]
            ).alias("failed_val"),
        )

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        null_cond = F.col(self._pk_cols[0]).isNull()
        for col in self._pk_cols[1:]:
            null_cond = null_cond & F.col(col).isNull()

        return (
            F.col(self._foreign_column).isNotNull() | null_cond
            if self._foreign_column
            else F.lit(False)
        )

    def definition(self):
        return {
            "type": "refIntegrityV2",
            "refIntegrityV2": {
                "columns": self._pk_cols,
                "foreignDataset": self._fk_cols.dataset_reference().definition(),
                "foreignColumns": self._fk_cols.col_names(),
            },
        }


class _ForeignColComparisonExpectation(_ForeignValuesExpectation):
    def __init__(self, local_col: str, op: Callable, other_col: JoinedDatasetColumn):
        keys = other_col.keys()
        foreign_keys = other_col.foreign_keys()
        foreign_operand = other_col.col_name()
        fks_ref = DatasetComposite(other_col.dataset_reference(), foreign_keys)
        super(_ForeignColComparisonExpectation, self).__init__(
            keys, fks_ref, foreign_operand
        )
        self._op = op
        self._local_operand = local_col
        self._foreign_operand = foreign_operand

    def columns(self) -> List[str]:
        return self._pk_cols + [self._local_operand]

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    def value(self, target: EvaluationTarget):
        return F.struct(
            F.col(self._error_column).alias("err"),
            F.struct(
                _get_cell_struct(target.df, self._local_operand),
                # We can't get the foreign value's type since
                # the schema hasn't been updated at this point
                F.struct(
                    F.lit(None).alias("type"),
                    F.col(self._foreign_column).alias("value"),
                ),
            ).alias("failed_val"),
        )

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        left_operand = F.col(self._local_operand)
        # Foreign column is the column that is joined with randomized id (not foreign_operand)
        right_operand = F.col(self._foreign_column)
        null_cond = left_operand.isNull() | right_operand.isNull()
        return F.col(self._error_column).isNull() & (
            null_cond | self._op(left_operand, right_operand)
        )

    def definition(self):
        return {
            "type": "foreignColComparison",
            "foreignColComparison": {
                "foreignDataset": self._fk_cols.dataset_reference().definition(),
                "localColumn": {"type": "name", "name": self._local_operand},
                "operator": as_comparison_operator(self._op),
                "foreignColumn": {"type": "name", "name": self._foreign_operand},
                "keys": self._pk_cols,
                "foreignKeys": self._fk_cols.col_names(),
            },
        }
