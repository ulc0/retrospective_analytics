from transforms.api import transform, Input, Output, configure

# Spark Profiles
# EXECUTOR_MEMORY_EXTRA_EXTRA_SMALL
# EXECUTOR_MEMORY_EXTRA_SMALL
# EXECUTOR_MEMORY_SMALL
# DRIVER_MEMORY_OVERHEAD_MEDIUM
# EXECUTOR_MEMORY_OFFHEAP_FRACTION_HIGH
# DRIVER_MEMORY_MEDIUM


def transform_generator(sources):
    transforms = []
    for source in sources:

        @configure(
            profile=["EXECUTOR_MEMORY_EXTRA_EXTRA_SMALL", "DRIVER_MEMORY_MEDIUM"]
        )
        @transform(
            output=Output(
                f"/1CDP-Training-736bf7/1CDP Training Playground/twl4/target/synthea_v1/{source}"
            ),
            my_input=Input(
                f"/1CDP-Training-736bf7/1CDP Training Playground/twl4/source/synthea/{source}"
            ),
        )
        def compute_function(my_input, output, source=source):
            df = my_input.dataframe()
            output.write_dataframe(df)
            # LIMITrecords 
            # df_input = my_input.dataframe().limit(5)
            # filtered_df = df_input.select("dataset_name", "raw_delivery_folder").filter(F.col("dataset_name") == 'hv_full_comparator')
            # filtered_df = df_input.select("dataset_name", "raw_delivery_folder").filter((F.col("dataset_name") == 'hv_full_comparator') 
            # & (F.col("archive_delivery_order") ==2))
            # # Rename a column
            # filtered_df = df_input.withColumnRenamed('dataset_name', 'dataset_name_new')
            # # Drop duplicate rows in a dataset (same as distinct())
            # filtered_df = df_input.dropDuplicates()
            # filtered_df = df_input.filter(df_input.raw_delivery_folder.contains('abfss'))
            # filtered_df = df_input.groupBy(F.col('dataset_name')).agg(count('raw_delivery_folder'))

        transforms.append(compute_function)
    return transforms


TRANSFORMS = transform_generator(["allergies", "careplans"])
