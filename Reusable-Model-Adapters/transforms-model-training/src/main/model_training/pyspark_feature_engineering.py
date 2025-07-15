from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transforms.api import transform, Input, Output


@transform(
    raw=Input("ri.foundry.main.dataset.1014fb29-bce7-4041-8db2-0d231d3362d0"),
    train=Output("ri.foundry.main.dataset.903680ab-8838-4e9e-b4e4-7894ee6908cc"),
    test=Output("ri.foundry.main.dataset.9d8753df-a1d5-4745-904a-7bc367a7cdf3"),
)
def compute(ctx, raw, train, test):
    W = Window.partitionBy("policy_type").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # Load the training DataFrame
    df = (
        raw
        .dataframe()
        .filter(F.col("policy_type") != "['none']")
        .dropna(subset=['text_clean'])
        .withColumn("idx", F.hash('text').cast('string'))
        .select('idx', 'text', 'policy_type')
        .withColumn(
            'target_label',
            F.explode(F.split(F.regexp_replace("policy_type", '[^A-Za-z,]+', ' '), ',')))
        .withColumn('text', F.regexp_replace("text", '[^A-Za-z0-9 -]+', ''))
        .withColumn('weight', F.lit(1)/F.count('policy_type').over(W))
    )

    # Load the test DataFrame
    test_df = (
        raw
        .dataframe()
        .filter(F.col("policy_type") == "['none']")
        .dropna(subset=['text_clean'])
        .withColumn("idx", F.hash('text_clean'))
        .select('idx', 'text', 'text_clean', 'policy_type')
        .withColumn('text', F.regexp_replace("text", '[^A-Za-z0-9 -]+', ''))
    )

    train.write_dataframe(df)
    test.write_dataframe(test_df)
