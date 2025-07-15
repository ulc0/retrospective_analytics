from __future__ import annotations
import polars as pl
from pyspark.sql import DataFrame
from transforms.expectations.wrapper._backend import GET_CURRENT_BACKEND, Backend
from transforms.expectations.wrapper._wrapper import (
    PolarsExpectationColumn,
    unwrap_if_wrapped_column,
    ExpectationColumn,
)
from transforms.expectations.wrapper._schema import PolarsSchemaWrapper
from typing import Union, List, Dict, Any


class WrappedPolarsLazyFrame:
    def __init__(self, df: pl.LazyFrame):
        self.df = df

    def unwrap(self) -> pl.LazyFrame:
        return self.df

    @property
    def schema(self) -> PolarsSchemaWrapper:
        return PolarsSchemaWrapper(self.df.schema)

    @property
    def columns(self) -> List[str]:
        return self.df.schema.keys()

    def count(self) -> int:
        raise NotImplementedError(
            "Count operation not implemented yet for Polars backend"
        )

    def limit(self, n: int) -> WrappedPolarsLazyFrame:
        df = self.df.limit(n)
        return WrappedPolarsLazyFrame(df)

    def agg(self, *agg_cols: PolarsExpectationColumn) -> WrappedPolarsLazyFrame:
        agg_cols = [col.unwrap() for col in agg_cols]
        # Polars does not support agg operation, instead should use a normal select
        df = self.df.select(*agg_cols)
        return WrappedPolarsLazyFrame(df)

    def crossJoin(self, other: WrappedPolarsLazyFrame) -> WrappedPolarsLazyFrame:
        other = other.unwrap()
        df = self.df.join(other, how="cross")
        return WrappedPolarsLazyFrame(df)

    def first(self) -> Dict[str, Any]:
        if isinstance(self.df, pl.LazyFrame):
            collected_df: pl.DataFrame = self.df.collect(type_coercion=False)
        else:
            collected_df = self.df

        if len(collected_df) > 0:
            return collected_df.row(0, named=True)
        else:
            """
            if there's no row, we fake it by creating a new DataFrame with the same columns and empty jsons
            """
            data_with_values = {
                column_name: ["{}"] for column_name in collected_df.columns
            }
            # Create a new DataFrame with the specified values
            df_with_values = pl.DataFrame(data_with_values)

            return df_with_values.row(0, named=True)

    def withColumn(
        self, colName: str, col: WrappedPolarsLazyFrame
    ) -> WrappedPolarsLazyFrame:
        col = col.unwrap()
        return WrappedPolarsLazyFrame(self.df.with_columns(col))

    def drop(
        self, *cols: Union[str, PolarsExpectationColumn]
    ) -> WrappedPolarsLazyFrame:
        if not all(isinstance(item, (str, PolarsExpectationColumn)) for item in cols):
            raise TypeError(
                "All items in cols must be of type str or PolarsExpectationColumn"
            )
        cols = [unwrap_if_wrapped_column(col) for col in cols]
        raise NotImplementedError(
            "Drop operation not implemented yet for Polars backend"
        )

    def select(
        self, *cols: Union[str, PolarsExpectationColumn]
    ) -> WrappedPolarsLazyFrame:
        if not all(isinstance(item, (str, PolarsExpectationColumn)) for item in cols):
            raise TypeError(
                "All items in cols must be of type str or PolarsExpectationColumn"
            )

        cols = [unwrap_if_wrapped_column(col) for col in cols]
        df = self.df.select(*cols)
        return WrappedPolarsLazyFrame(df)

    def dropDuplicates(self, subset: List[str]) -> WrappedPolarsLazyFrame:
        raise NotImplementedError(
            "DropDuplicates operation not implemented yet for Polars backend"
        )

    def join(
        self, other: WrappedPolarsLazyFrame, on: PolarsExpectationColumn, how: str
    ) -> WrappedPolarsLazyFrame:
        raise NotImplementedError(
            "Join operation not implemented yet for Polars backend"
        )

    def show(self) -> None:
        print(self.df.collect(type_coercion=False))


ExpectationDataFrame = Union[WrappedPolarsLazyFrame, DataFrame]


def keep_and_add_cols(
    df: ExpectationDataFrame, *new_cols: ExpectationColumn
) -> ExpectationDataFrame:
    if GET_CURRENT_BACKEND() == Backend.SPARK:
        return df.select("*", *new_cols)
    else:
        new_cols = [col.unwrap() for col in new_cols]
        df = df.unwrap()
        df = df.select(pl.all(), *new_cols)
        return WrappedPolarsLazyFrame(df)


def use_new_cols(df, *new_cols: ExpectationColumn) -> ExpectationDataFrame:
    if GET_CURRENT_BACKEND() == Backend.SPARK:
        return df.select(*new_cols)
    else:
        new_cols = [col.unwrap() for col in new_cols]
        df = df.unwrap()
        df = df.select(*new_cols)
        return WrappedPolarsLazyFrame(df)
