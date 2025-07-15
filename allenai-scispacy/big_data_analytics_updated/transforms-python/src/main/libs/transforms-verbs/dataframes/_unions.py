#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from typing import Optional
from pyspark.sql import functions as F, DataFrame, types as T, SparkSession
from itertools import chain
from collections import defaultdict

SEP_STR = "\n  - "


def union_many(
    *dfs: DataFrame,
    how="strict",
    strategy="dataframe",
    spark_session: Optional[SparkSession] = None,
) -> DataFrame:
    """Unions many :class:`DataFrame` objects together.

    Note: This function carefully structures the query to avoid exponential time query analysis.

    :param dfs: Variable length list of dataframes to be unioned.
    :type: dfs: DataFrame
    :param how: Method used to union the dataframes together. One of: `strict`, `wide`, `narrow`. Defaults to `strict`.
                `strict` only unions dataframes that match exactly in column name and type. Throws an error otherwise.
                `wide` unions dataframes and keeps all columns. If the same column name is found with different types,
                an error is thrown.
                `narrow` unions dataframes and keeps columns where the name and type match. Columns with names and
                types that aren't present across all dataframes will be dropped.
    :type how: str
    :param strategy: Method by which the union shall be executed, either `dataframe` or `rdd`. Defaults to `dataframe`.
    :type strategy: str
    :param spark_session: Optional spark session to be provided. Only required if `strategy` is equal to `rdd`.
    :type spark_session: Optional, SparkSession

    :return: Dataframe with all provided dataframes unioned together via the provided `how` method.
    :rtype: DataFrame
    """
    _verify_how(how)
    _verify_strategy(strategy)
    _verify_spark_session(strategy, spark_session)

    # Compute output schemas and check for 'how' correctness
    output_schema_set, output_columns_set = None, None
    for arg_i, df in enumerate(dfs):
        current_schema_set = set(_remove_nullability(field) for field in df.schema)
        current_columns_set = set(df.columns)

        if not output_schema_set and not output_columns_set:
            output_schema_set, output_columns_set = (
                current_schema_set,
                current_columns_set,
            )
            continue

        if how == "strict":
            _verify_union_strict(
                arg_i,
                current_schema_set,
                current_columns_set,
                output_schema_set,
                output_columns_set,
            )
        elif how == "wide":
            output_schema_set, output_columns_set = _combine_wide_columns(
                arg_i,
                current_schema_set,
                current_columns_set,
                output_schema_set,
                output_columns_set,
            )

        elif how == "narrow":
            output_schema_set, output_columns_set = _combine_narrow_columns(
                arg_i,
                current_schema_set,
                current_columns_set,
                output_schema_set,
                output_columns_set,
            )

    # Handle potential periods in column names
    output_columns_set = [add_backticks_if_necessary(col) for col in output_columns_set]

    # Fix and order schemas of each df
    prepped_dfs = []
    for df in dfs:
        if how == "wide":
            #  For the wide case, we must add in any missing columns to each df
            df_schema_set = set(_remove_nullability(field) for field in df.schema)
            missing_column_set = output_schema_set - df_schema_set
            df = df.select(
                "*",
                *[
                    F.lit(None).cast(x.dataType).alias(x.name)
                    for x in missing_column_set
                ],
            )
        # Due to the positional nature of sparkContext.union, we must explicitly
        #  order the columns of the output set.  We choose to alphabetize them.
        prepped_dfs += [df.select(sorted(output_columns_set))]

    _union = _binary_union if strategy == "dataframe" else _rdd_union
    return _union(
        *prepped_dfs, output_schema_set=output_schema_set, spark_session=spark_session
    )


def _binary_union(*dfs, output_schema_set, spark_session):
    q = list(dfs)
    while len(q) > 1:
        left = q.pop(0)
        right = q.pop(0)
        q.append(left.unionByName(right))

    return q[0]


def _rdd_union(*dfs, output_schema_set, spark_session):
    rdd_list = [df.rdd for df in dfs]
    return spark_session.createDataFrame(
        spark_session.sparkContext.union(rdd_list),
        # Due to the positional nature of sparkContext.union, we must explicitly
        #  order the columns of the output set.  We choose to alphabetize them.
        schema=T.StructType(list(sorted(output_schema_set, key=lambda x: x.name))),
    )


def _verify_union_strict(
    current_schema_index,
    current_schema_set,
    current_columns_set,
    output_schema_set,
    output_columns_set,
):
    #  Schemas must be exactly the same
    if current_schema_set != output_schema_set:

        def schema_set_to_dict(schema_set):
            return {field.name: field for field in schema_set}

        output_not_in_current = schema_set_to_dict(
            output_schema_set.difference(current_schema_set)
        )
        current_not_in_output = schema_set_to_dict(
            current_schema_set.difference(output_schema_set)
        )

        mismatched_fields = output_not_in_current.keys() | current_not_in_output.keys()

        mismatch_error_strs = []
        missing_current_strs = []
        missing_output_strs = []
        for field_name in mismatched_fields:
            if (
                field_name in output_not_in_current
                and field_name in current_not_in_output
            ):
                # mismatched type
                mismatch_error_strs.append(
                    f"Mismatched column type for `{field_name}` (union<>actual): "
                    f"{output_not_in_current[field_name]}<>{current_not_in_output[field_name]}"
                )
            elif field_name in output_not_in_current:
                # missing from current
                missing_current_strs.append(
                    f"UnionDF column `{field_name}` with type `{output_not_in_current[field_name]}` missing from ArgDF"
                )
            elif field_name in current_not_in_output:
                # missing from output
                missing_output_strs.append(
                    f"ArgDF column `{field_name}` with type `{current_not_in_output[field_name]}` missing from UnionDF"
                )
            else:
                raise RuntimeError(field_name)
        raise ValueError(
            f"Union operation with how = 'strict' found differing schemas for UnionDF and "
            f"ArgDF#{current_schema_index}:"
            + SEP_STR
            + SEP_STR.join(
                chain(mismatch_error_strs, missing_current_strs, missing_output_strs)
            )
        )


def _raise_bad_schema_columns(current_schema_index, name_to_fields):
    base_str = (
        f"Schema found with differing types for the same column name in ArgDF#{current_schema_index}:"
        + SEP_STR
    )
    raise ValueError(
        base_str
        + SEP_STR.join(
            [f"`{n}`: {' & '.join(map(str, f))}" for n, f in name_to_fields.items()]
        )
    )


def _combine_wide_columns(
    current_schema_index,
    current_schema_set,
    current_columns_set,
    prev_output_schema_set,
    prev_output_columns_set,
):
    #  When the same column name that has different type is added to each set,
    #    the set length will be different between the columns and schema.  This
    #    will be caught outside the loop.
    output_schema_set = prev_output_schema_set.union(current_schema_set)
    output_columns_set = prev_output_columns_set.union(current_columns_set)
    schema_len = len(output_schema_set)
    columns_len = len(output_columns_set)
    if schema_len > columns_len:
        #  Group all fields by name and only keep items with more than 1 type associated
        all_name_to_fields = defaultdict(list)
        for field in output_schema_set:
            all_name_to_fields[field.name].append(field)

        name_to_fields = {
            name: fields
            for name, fields in all_name_to_fields.items()
            if len(fields) > 1
        }

        _raise_bad_schema_columns(current_schema_index, name_to_fields)
    return output_schema_set, output_columns_set


def _combine_narrow_columns(
    current_schema_index,
    current_schema_set,
    current_columns_set,
    prev_output_schema_set,
    prev_output_columns_set,
):
    #  When columns with the same name have different types,
    #    they will be silently dropped since they are not the same
    output_schema_set = prev_output_schema_set.intersection(current_schema_set)
    output_columns_set = prev_output_columns_set.intersection(current_columns_set)
    schema_len = len(output_schema_set)
    columns_len = len(output_columns_set)

    if schema_len < columns_len:
        #  Get the struct fields dropped in the intersection
        missing_schemas = prev_output_schema_set.symmetric_difference(
            current_schema_set
        )
        name_to_fields = defaultdict(list)
        for field in missing_schemas:
            name_to_fields[field.name].append(field)
        _raise_bad_schema_columns(current_schema_index, name_to_fields)

    return output_schema_set, output_columns_set


def _verify_how(how):
    if how not in ["strict", "wide", "narrow"]:
        raise ValueError(
            f"Invalid 'how' value '{how}' passed.  Must be one of \
['strict', 'wide', 'narrow']."
        )


def _verify_strategy(strategy):
    if strategy not in ["rdd", "dataframe"]:
        raise ValueError(
            f"Invalid 'strategy' value '{strategy}' passed.  Must be one of \
['rdd', 'dataframe']."
        )


def _verify_spark_session(strategy, spark_session):
    if strategy == "rdd" and spark_session is None:
        raise ValueError(
            "non-empty spark_session value must be passed as argument if using 'rdd' strategy.  This \
is required for certain low-level Spark interactions to succeed."
        )


def _remove_nullability(data_type: T.DataType):
    """Remove the nullability part of the struct field that can result in spurious schema inconsistency"""
    if isinstance(data_type, T.StructType):
        return T.StructType(
            fields=[_remove_nullability(field) for field in data_type.fields]
        )
    elif isinstance(data_type, T.StructField):
        return T.StructField(
            data_type.name, _remove_nullability(data_type.dataType), True
        )
    elif isinstance(data_type, T.ArrayType):
        return T.ArrayType(_remove_nullability(data_type.elementType), True)
    elif isinstance(data_type, T.MapType):
        return T.MapType(
            data_type.keyType, _remove_nullability(data_type.valueType), True
        )
    else:
        return data_type


def add_backticks_if_necessary(col_name):
    if "." in col_name:
        return "`" + col_name + "`"
    else:
        return col_name
