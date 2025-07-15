# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/1CDP-Training-736bf7/1CDP Training Playground/ulc0/Cluster images for comprehensive feature analysis (2025-04-03 10:28:26)/repositories/adapter"),
    source_df=Input("SOURCE_DATASET_PATH"),
)
def compute(source_df):
    return source_df
