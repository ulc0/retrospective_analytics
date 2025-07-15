from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def get_source_priority(df_priority, df_case):
    df = df_case.join(df_priority, ["jurisdiction_name", "source"], "left")
    return df


# Backfills null values in case records based on priority ordering
# See notes on orderBy above
def run_case_augmentation(df):
    df = df.filter(F.col("case_id") != "Missing State-Assigned ID")

    win_augment = Window.partitionBy(
        "jurisdiction_name",
        "case_id"
    ).orderBy(
        "priority"
    ).rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    exclude_cols = set(["priority", "source", "created_at"])
    df = df.select(
        "priority",
        "created_at",
        F.collect_list("source").over(win_augment).alias("source"),
        # F.min("created_at").over(win_augment).alias("created_at"),
        *[F.first(F.col(x), ignorenulls=True).over(win_augment).alias(x) for x in df.columns if x not in exclude_cols]
    )
    df = df.withColumn('source', F.array_distinct(F.col('source')))
    return df


# Filters to the augmented case with the highest priority based on settings in mpx_source_priority_edited
def get_cases_by_priority(df):
    win_rank = Window.partitionBy("jurisdiction_name", "case_id").orderBy("priority")
    df = df.withColumn("rank", F.row_number().over(win_rank))
    df = df.filter(F.col("rank") == 1)
    df = df.select([F.col(col).alias(col.replace("_mpx-augmented", "")) for col in df.columns])
    df = df.drop("rank")
    return df


def get_array_columns(df):
    array_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, T.ArrayType)]
    return array_columns


# Converts array columns to string to prepare for merging non null values in priority order
# Needed because by default blank array columns are not null
def convert_array_to_string(df):
    for c in [f.name for f in df.schema.fields if isinstance(f.dataType, T.ArrayType)]:
        df = df.withColumn(
            c,
            F.when(
                F.size(c) > 0,
                F.concat_ws(";", F.col(c))
            ).otherwise(
                F.lit(None)
            )
        )
    return df


# Converts array columns back to array
def convert_string_to_array(df, array_columns):
    for c in array_columns:
        df = df.withColumn(
            c,
            F.split(F.col(c), ";")
        )
    return df


@transform_df(
    Output("ri.foundry.main.dataset.379ea965-7b43-4328-b3d4-017c378fe034"),
    df_case=Input("ri.foundry.main.dataset.1f4ff9e6-9c7d-4049-b103-ce25c54025cf"),
    df_priority=Input("ri.foundry.main.dataset.ff01748b-9ad3-448a-9d75-493af9a06f36"),
)
def compute(df_case, df_priority):
    # Adapted from https://dcipher.cdc.gov/workspace/data-integration/code/repos/ri.stemma.main.repository.68b3d405-29fe-4fd2-9f1e-fdd7baff0b77/contents/refs%2Fheads%2Fmaster/transforms-python/src/myproject/datasets/mpox_case_augmentation.py?fallbackCommit=8e74e6d3c7e428cf5d43eebc033f8d84275a9b8d
    # removed id_mapping from compute statement
    df_priority = df_priority.withColumnRenamed("jurisdiction", "jurisdiction_name")
    array_columns = get_array_columns(df_case)
    df_case = get_source_priority(df_priority, df_case)
    df = convert_array_to_string(df_case)
    df = run_case_augmentation(df)
    df = get_cases_by_priority(df)
    df = convert_string_to_array(df, array_columns)
    return df
