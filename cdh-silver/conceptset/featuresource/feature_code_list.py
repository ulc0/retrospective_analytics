# Databricks notebook source
import pyspark.sql.functions as F


# COMMAND ----------

dbutils.widgets.text("CATALOG",defaultValue="HIVE_METASTORE")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text("SRC_CATALOG",defaultValue="HIVE_METASTORE")
SRC_CATALOG=dbutils.widgets.get("SRC_CATALOG")

dbutils.widgets.text('SRC_SCHEMA',defaultValue='edav_prd_cdh.cdh_sandbox')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text('SCHEMA',defaultValue='edav_prd_cdh.cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')


dbutils.widgets.text('SRC_TABLE',defaultValue='icd10cm_diagnosis_codes_to_ccsr_categories_map')
SRC_TABLE=dbutils.widgets.get('SRC_TABLE')
dbutils.widgets.text('VOCABULARY_ID',defaultValue='ICD')
VOCABULARY_ID=dbutils.widgets.get('VOCABULARY_ID')
dbutils.widgets.text('code',defaultValue='`ICD-10-CM_Code`')
code=dbutils.widgets.get("code")
dbutils.widgets.text('FEATURE',defaultValue='CCSR_Category')
FEATURE=dbutils.widgets.get('FEATURE')


# COMMAND ----------

print(code)
print(FEATURE)

# COMMAND ----------

src_table=f"{SRC_CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}"
newcols=['code','feature_name']
src_data = spark.table(src_table).select([code,FEATURE]).toDF(*newcols).withColumn('vocabulary_id',F.lit(VOCABULARY_ID))


# COMMAND ----------

src_data.display()

# COMMAND ----------

ft_name=f"{CATALOG}.{SCHEMA}.concept_sets"
#drugfeatures.write.format("delta").saveAsTable(ft_name)
ft=spark.table(ft_name)
fp=ft.unionByName(src_data).dropDuplicates()
fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)
