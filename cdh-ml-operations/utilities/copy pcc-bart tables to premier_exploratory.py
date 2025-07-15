# Databricks notebook source
tbls=spark.sql("show tables in bart_pcc; ").toPandas()
tbls["tableName"]

# COMMAND ----------

for t in tbls.tableName:
    sqlt=f"create or replace table cdh_premier_exploratory.ml_bart_pcc_{t} as select * from bart_pcc.{t};"
    print(sqlt)
    spark.sql(sqlt)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bart_pcc.subset_ccsrs

# COMMAND ----------

create table cdh_premier_exploratory.ml_
