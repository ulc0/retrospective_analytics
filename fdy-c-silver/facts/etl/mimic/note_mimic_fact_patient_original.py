# Databricks notebook source
dbutils.widgets.text("CATALOG",'edav_dev_cdh')
dbutils.widgets.text("SCHEMA",'cdh_mimic')


# COMMAND ----------

CATALOG=dbutils.widgets.get("CATALOG")
SCHEMA=dbutils.widgets.get("SCHEMA")


# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
import json

col_map={
"subject_id":"person_id",
"charttime":"observation_period_datetime",
"chartdate":"observation_period_datetime",
"text":"note_text",
"hadm_id":"observation_period_id",
"field_value":"concept_code",
"icd_code":"concept_code",
"hcpcs_cd":"concept_code",
#"field_name":"vocabulary_id",
#"note_id":"mimic_note_id",
#"new_note_id":"note_id",
}

fact_tables=['procedures_icd','hcpcsevents']
v={"procedures_icd":"ICD", "hcpcsevents":"HCPCS"}


# COMMAND ----------

m={}
keep_cols=["person_id","observation_period_id","observation_period_datetime","concept_code","vocabulary_id"]
for t in fact_tables:
    vocab=v[t]
    m[t]=spark.table(f"{CATALOG}.{SCHEMA}.{t}").dropDuplicates().withColumnsRenamed(col_map).withColumn("vocabulary_id",F.lit(vocab)).select(*keep_cols)
    display(m[t])



# COMMAND ----------

s=m['procedures_icd']
print(s.schema)
fact = spark.createDataFrame([], s.schema)
for t in fact_tables:
    fact=fact.union(m[t])
display(fact)

# COMMAND ----------

fact.write.mode("overwrite").option('mergeSchema',"true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}_ra.fact_patient")

