# Databricks notebook source
# MAGIC %pip install tableone                                                     
# MAGIC %pip install fsspec
# MAGIC dbutils.library.restartPython()

# COMMAND ----------



import pandas as pd
import numpy as np

# Read data from Delta table
data_df = spark.read.table("cdh_hv_race_ethnicity_covid_exploratory.ujr9_stroke_table_re_sept2023_strokeflag_subset")

# Define columns and categorical variables
columns = ['pos', 'sex', 'age_group', 'stroke_flag']
categorical = ['pos', 'sex', 'age_group']
groupby = 'stroke_flag'
#columns+categorical+[groupby]
# Convert Spark DataFrame to Pandas DataFrame
data_pdf = data_df.toPandas()




# COMMAND ----------

from tableone import TableOne

# Create TableOne object
mytable = TableOne(data_pdf, columns, categorical, groupby)

# Print the table
print(mytable.tabulate(tablefmt="fancy_grid"))

# Export table to Excel
mytable.to_excel('/Workspace/Users/ulc0@cdc.gov/mytable.xlsx')
