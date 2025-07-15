# Databricks notebook source
import pandas as pd

# COMMAND ----------

s1_fin2 = s1_fin.toPandas()

# COMMAND ----------

pd.crosstab(s1_fin2["covid_patient"], s1_fin2["p3_ccsr_first"])
