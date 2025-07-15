# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.functions as W
import pyspark.sql.types as T
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Bucketizer
import pandas as pd
import numpy as np
import pyspark


# COMMAND ----------

lung = pd.read_csv("../test-data/lung.csv")

# These steps would need to be converted to a robust pyspark function for the pcc
# configure analytic dataset
lung["karno"] = lung["ph.karno"].fillna(lung["pat.karno"])
# adjust time to months
lung["time2"] = np.ceil(lung["time"]/30)
lung["sex2"] = lung["sex"]-1
time = lung.time2
delta = lung.status - 1

# karno try categorical and continuous
lung2 = pd.concat([delta, time, lung[["age","sex2","karno"]]], axis=1)

# sklearn set-up
# y_sk = ssf.get_y_sklearn(delta, time)
# x_sk = lung[["age","sex2","karno"]]




# COMMAND ----------

lungs = spark.createDataFrame(lung2)
# lungs = lungs.select(["tim])
lungs.display()

# COMMAND ----------

time_min = (
    lungs
    .select(F.min("time2").alias("t_min"))
).collect()[0]["t_min"]
print(time_min)

def to_zero(x):
    return x*0

@F.udf(T.ArrayType(T.IntegerType()))
def repeat_sizeof(col1, col2):
    return [col1] * len(col2)

(lungs
    .withColumn("time", F.col("time2").cast("int"))
    .withColumn("tte_seq", F.sequence(F.lit(int(time_min)), F.col("time")))
    # .withColumn("ev_array", F.transform("tte_seq", lambda x: x*0))
    .withColumn("ev_array", repeat_sizeof(F.lit(0), F.col("tte_seq")))
    # .withColumn("ev_array", F.array([F.lit(0)]*F.col("tte_seq")))
    # .withColumn("tte_val", 
    #         F.concat(
    #             F.slice(F.col("ev_array"), 1, (F.col("tte_ccsr"))),
    #             F.transform(
    #                 F.slice(F.col("ev_array"), F.col("tte_ccsr"), 1),
    #                 lambda x: F.col("p3_ccsr_first")
    #                 )
    #         )
    # )
).display()

# COMMAND ----------

def get_tte_long(dtst, time, delta):
    t_min = (
        lungs
        .select(F.min(time).alias("t_min"))
    ).collect()[0]["t_min"]


    out = (
            dtst
            .withColumn("tte_seq", F.sequence(F.lit(t_min), F.col(time)))
            .withColumn("ev_array", F.transform("tte_seq", lambda x: x*0))
            .withColumn("tte_val", 
                        F.concat(
                            F.slice(F.col("ev_array"), 1, (F.col("tte_ccsr"))),
                            F.transform(
                                F.slice(F.col("ev_array"), F.col("tte_ccsr"), 1),
                                lambda x: F.col("p3_ccsr_first")
                                )
                        )
            )
            .drop("ev_array")
            .withColumn("tmp", F.arrays_zip(F.col("tte_seq"), F.col("tte_val")))
            .withColumn("tmp", F.explode(F.col("tmp")))
            .drop("tte_seq", "tte_val")
            .select("*", 
                    F.col("tmp.tte_seq").cast("short").alias("tte_seq"), 
                    F.col("tmp.tte_val").cast("short").alias("tte_val"))
            .drop("tmp")
        )
    return out
