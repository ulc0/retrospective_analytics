#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.

from typing import Callable, Collection, Iterable, Optional, Union

from uuid import uuid4
from pyspark.sql import Column, DataFrame, Window, functions as F


class _MinMaxGroupBySpec:
    def __init__(
        self,
        group_by: Union[Collection[str], Collection[Column]],
        order_by: Iterable[Column],
        in_case_of_tie_pick_randomly: bool,
    ):
        if len(group_by) == 0:
            raise ValueError(
                "For performance reasons, the grouped clause must not be empty"
            )
        self._group_by = group_by
        self._order_by = order_by
        self._in_case_of_tie_pick_randomly = in_case_of_tie_pick_randomly

    def apply(self, df: DataFrame) -> DataFrame:
        window = Window.partitionBy(*self._group_by).orderBy(*self._order_by)
        row_num_col_name = "__rn_dummy_col__" + str(uuid4())
        order_func = F.row_number if self._in_case_of_tie_pick_randomly else F.rank

        return (
            df.withColumn(row_num_col_name, order_func().over(window))
            .where(F.col(row_num_col_name) == 1)
            .drop(row_num_col_name)
        )


class _SampleGroupBySpec(object):
    def __init__(
        self, group_by: Union[Collection[str], Collection[Column]], sample_size: int
    ):
        if len(group_by) == 0:
            raise ValueError(
                "For performance reasons, the grouped clause must not be empty"
            )
        if sample_size <= 0:
            raise ValueError("Sample size must be a positive integer")
        self._sample_size = sample_size
        self._group_by = group_by

    def apply(self, df: DataFrame):
        # The Window API requires us to order by a column; we don't care which, so we create one arbitrarily.
        window = Window.partitionBy(*self._group_by).orderBy(F.lit(0))
        row_num_col_name = "__rn_dummy_col__" + str(uuid4())

        return (
            df.withColumn(row_num_col_name, F.row_number().over(window))
            .where(F.col(row_num_col_name) <= self._sample_size)
            .drop(row_num_col_name)
        )


class _GroupBySpec:
    """Description of how columns should be grouped for subsequent operations."""

    def __init__(self, group_by: Union[Collection[str], Collection[Column]]):
        self._group_by = group_by

    def _make_ordered_spec(
        self,
        sort_func: Callable,
        order_by: Union[str, Iterable[str]],
        in_case_of_tie_pick_randomly: Optional[bool] = False,
    ):
        if isinstance(order_by, str):
            order_by = (order_by,)
        elif not isinstance(order_by, Iterable) or not isinstance(
            next(iter(order_by)), str
        ):
            raise TypeError("order_by must be a string or an iterable of strings")

        return _MinMaxGroupBySpec(
            self._group_by,
            order_by=[sort_func(order_col) for order_col in order_by],
            in_case_of_tie_pick_randomly=in_case_of_tie_pick_randomly,
        )

    def min_by(
        self,
        order_by: Union[str, Iterable[str]],
        in_case_of_tie_pick_randomly: Optional[bool] = False,
    ) -> _MinMaxGroupBySpec:
        """For each group, keep the row with the lowest value of `order_by`.

        Args:
            in_case_of_tie_pick_randomly: When set to True, force the deduplication of rows
                and return a unique row per group with the same value of `order_by`.
                The deduplication is not deterministic.
                Defaults to False.
        """
        return self._make_ordered_spec(F.asc, order_by, in_case_of_tie_pick_randomly)

    def max_by(
        self,
        order_by: Union[str, Iterable[str]],
        in_case_of_tie_pick_randomly: Optional[bool] = False,
    ) -> _MinMaxGroupBySpec:
        """For each group, keep the row with the highest value of `order_by`.

        Args:
            in_case_of_tie_pick_randomly: When set to True, force the deduplication of rows
                and return a unique row per group with the same value of `order_by`.
                The deduplication is not deterministic.
                Defaults to False.
        """
        return self._make_ordered_spec(F.desc, order_by, in_case_of_tie_pick_randomly)

    def sample(self, sample_size: int = 1) -> _SampleGroupBySpec:
        """For each group, return a sample of rows

        Args:
            sample_size: The maximum size of the sample for each group.
            If the group doesn't have $sample_size rows, return every row in the group.

            The sample comes without any guarantees whatsoever: it is NOT UNIFORMLY RANDOM,
            nor is it deterministic.
        """
        return _SampleGroupBySpec(self._group_by, sample_size=sample_size)
