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

# load in lung
lung = spark.read.csv("test-data/lung.csv").drop("Unnamed: 0", "inst")
        .withColumn("karno", F.when(
            F.isnan(F.col("`ph.karno`")),
            F.col("`pat.karno`")
            ).otherwise(F.col("`ph.karno`"))
            )
        .withColumn("month", F.ceil(F.col("time")/30))
        .withColumn("week", F.ceil(F.col("time")/7))
        .withColumn("female", F.col("sex") - 1)
        .withColumn("event", F.col("status")-1)
lung.display()

# COMMAND ----------

# transform lung dataset
def transform_lung(dtst, time_adj = 30):
    lungs = spark.createDataFrame(dtst)
    # lungs = lungs.select(["tim])
    # lungs.display()

    out = (lungs
        .drop("Unnamed: 0", "inst")
        .withColumn("karno", F.when(
            F.isnan(F.col("`ph.karno`")),
            F.col("`pat.karno`")
            ).otherwise(F.col("`ph.karno`"))
            )
        .withColumn("time", F.ceil(F.col("time")/time_adj))
        .withColumn("sex", F.col("sex") - 1)
        .withColumn("status", F.col("status")-1)
        .select("status", "time", "sex","karno","age")
        )
    return out

# COMMAND ----------

lungs = transform_lung(lung)
lungs.display()

# COMMAND ----------

def train_survival_long(dtst, time, event):

    time_arr = (
        dtst
        .select(time)
        .distinct()
    ).collect()
    time_arr = [t[time] for t in time_arr]
    time_arr.sort()
    time_min = time_arr[0]
    # print(time_min)
    # print(time_arr)

    out = (dtst
        .withColumn("time_tmp", F.col(time).cast("int"))
        .withColumn("tte_seq", F.array([F.lit(i) for i in time_arr]))
        .withColumn("tte_ind", F.expr("array_position(tte_seq, time_tmp)").cast("integer"))
        .withColumn("tte_seq", F.slice(F.col("tte_seq"), F.lit(time_min), F.col("tte_ind")))
        .withColumn("tte_val", F.array_repeat(F.lit(0), F.col("tte_ind")-1))
        .withColumn("tte_val", F.concat(F.col("tte_val"), F.array(F.col(event))))
        .withColumn("tmp", F.arrays_zip(F.col("tte_seq"), F.col("tte_val")))
        .withColumn("tmp", F.explode(F.col("tmp")))
        .drop("tte_seq", "tte_val")
        .select(
                F.col("tmp.tte_val").cast("short").alias("tte_val"),
                F.col("tmp.tte_seq").cast("short").alias("tte_seq"), 
                "*"
                )
        .drop("tmp", "time_tmp", "tte_ind", time, event)
    )
    return out

# COMMAND ----------

train = train_survival_long(lungs, time="time", event="status")
train.display()

# COMMAND ----------

def test_survival_long(dtst, time, event):
    time_arr = (
        dtst
        .select(time)
        .distinct()
    ).collect()
    time_arr = [t[time] for t in time_arr]
    time_arr.sort()
    time_min = time_arr[0]

    out = (dtst
        .withColumn("time_tmp", F.col(time).cast("int"))
        .withColumn("tte_seq_tmp", F.array([F.lit(i) for i in time_arr]))
        .withColumn("tte_seq_tmp", F.explode(F.col("tte_seq_tmp")))
        .select(F.col("tte_seq_tmp").alias("tte_seq"), "*")
        .drop("tmp", "time_tmp", "tte_ind", "tte_seq_tmp", time, event)
    )
    return out

# COMMAND ----------

test = test_survival_long(lungs, "time", "status")
test.display()

# COMMAND ----------


