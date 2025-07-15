# from pyspark.sql import DataFrame
#
# # from pyspark.sql import functions as F
# from transforms.api import Input, Output, transform_df
#
# from myproject.datasets import utils
#
#
# @transform_df(
#     Output("/1CDP-Training-736bf7/1CDP Training Playground/TARGET_DATASET_PATH"),
#     source_df=Input("/1CDP-Training-736bf7/1CDP Training Playground/SOURCE_DATASET_PATH"),
# )
# def compute(source_df: DataFrame) -> DataFrame:
#     return utils.identity(source_df)
