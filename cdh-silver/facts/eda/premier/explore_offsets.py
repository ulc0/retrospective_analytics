# Databricks notebook source
from schema.premier_cdm import patdemo
from pyspark.sql import DataFrame


def transform(self, f):
    return f(self)


DataFrame.transform = transform


def renameColumns(df, rename_dict):
    from pyspark import col
    return df.select([col(c).alias(rename_dict.get(c, c)) for c in df.columns])


df_renamed = spark.sql("select * from edav_prd_cdh.cdh_premier_v2.patdemo limit 10000").transform(renameColumns(patdemo.t_dict))
#df_renamed=spark.sql("select * from edav_prd_cdh.cdh_premier_v2.patdemo limit 10000").select([col(c).alias(rename_dict.get(c, c)) for c in df_initial.columns])
print(df_renamed)
