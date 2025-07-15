# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('task_name',defaultValue='feature_code_table_CCSR_Category')
TASK_NAME=dbutils.widgets.get('task_name')


dbutils.widgets.text("SRC_CATALOG",defaultValue="edav_prd_cdh")
SRC_CATALOG=dbutils.widgets.get("SRC_CATALOG")
dbutils.widgets.text('SRC_SCHEMA',defaultValue='cdh_reference_data')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text("CATALOG",defaultValue="edav_prd_cdh")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text('SCHEMA',defaultValue='cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')


dbutils.widgets.text('SRC_TABLE',defaultValue='ccsr_dx_codelist')
SRC_TABLE=dbutils.widgets.get('SRC_TABLE')
dbutils.widgets.text('VOCABULARY_ID',defaultValue='ICD')
VOCABULARY_ID=dbutils.widgets.get('VOCABULARY_ID')
dbutils.widgets.text('code',defaultValue='icd_10_cm_code')
code=dbutils.widgets.get("code")
dbutils.widgets.text('FEATURE',defaultValue='CCSR_Category')
FEATURE=dbutils.widgets.get('FEATURE')


# COMMAND ----------

print(code)
FEATURE='_'.join(TASK_NAME.split('_')[3:])
print(FEATURE)

keepcols=['code','vocabulary_id','feature_name']

# COMMAND ----------

src_table=f"{SRC_CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}"

#newcols=['code']#,'feature']
##src_data = spark.table(src_table).select([code]).toDF(*newcols).withColumn('feature_name',F.lit(FEATURE)).withColumn('vocabulary_id',F.lit(VOCABULARY_ID))
src_data = spark.table(src_table).withColumn('feature_name',F.lit(FEATURE)).withColumn('vocabulary_id',F.lit(VOCABULARY_ID))
print(src_data.columns)
src_data=src_data.withColumn('code', src_data[code].cast(StringType())).select(*keepcols)

# COMMAND ----------

src_data.display()

# COMMAND ----------

ft_name=f"{CATALOG}.{SCHEMA}.concept_sets"
write_mode = "append"
schema_option = "mergeSchema"

src_data.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable( ft_name)
