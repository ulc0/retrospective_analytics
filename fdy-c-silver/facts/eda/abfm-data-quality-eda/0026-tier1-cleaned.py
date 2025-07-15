# Databricks notebook source
# MAGIC %md
# MAGIC # Tier1 (Table) Cleaned

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-19

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

sc.version

# COMMAND ----------

# # Maximize Pandas output text width.
# import pandas as pd
# pd.set_option('display.max_colwidth', 200)

# #ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np

# #import pyspark sql functions with alias
# import pyspark.sql.functions as F
# import pyspark.sql.window as W

# #import specific functions from pyspark sql types
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-utils-high

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

display(
    load_patient_table()
)

# COMMAND ----------


