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



# COMMAND ----------

mimic_detail_rad=spark.table(f"{CATALOG}.{SCHEMA}.radiology_detail")
mimic_detail_dsc=spark.table(f"{CATALOG}.{SCHEMA}.discharge_detail")
mimic_detail_j=(mimic_detail_rad.union(mimic_detail_dsc)).withColumnRenamed('note_id','mimic_note_id').join(mimic_index,'mimic_note_id','left')
display(mimic_detail_j)

# COMMAND ----------

mimic_df = mimic_detail_j.drop('mimic_note_id','field_ordinal') #.join(mimic_index,'mimic_note_id', how='left')

display(mimic_df)

# COMMAND ----------

display(mimic_df.select('field_name').distinct())
#display(mimic_df.select('field_ordinal').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC #### initial review
# MAGIC #### radiology detail drop field_name, use field_value as HCPCS concept code 
# MAGIC #### discharge detail 
# MAGIC csv_fn=['radiology','discharge']
# MAGIC for csv in csv_fn:
# MAGIC     for suffix in ['','_detail']:
# MAGIC         raw_notes=spark.read.options(header=True,multiLine=True,schema=mimic_schema,).csv(f"{INPUT_DIR}{csv}{suffix}.csv")
# MAGIC         display(raw_notes)

# COMMAND ----------
#TODO map vocabulary based on exam_code,
mimic_facts=mimic_df.filter(mimic_df.field_name.isin(['exam_code','cpt_code',]))
display(mimic_facts)

# COMMAND ----------

col_map={
"subject_id":"person_id",
"charttime":"note_datetime",
"text":"note_text",
"hadm_id":"visit_occurrence_number",
"field_value":"code",
#"field_name":"vocabulary_id",
#"note_id":"mimic_note_id",
#"new_note_id":"note_id",
}
drop_cols=["storetime","note_seq","mimic_note_id",'field_name','field_ordinal']
# TODO, break out feature table from field_name, field_value
mimic_fact=mimic_df.dropDuplicates().withColumnsRenamed(col_map).filter(~F.col("field_name").isin(["exam_name","author"])).drop(*drop_cols).withColumn('vocabulary_id',F.lit('CPT_CODE'))
display(mimic_fact)

# COMMAND ----------

mimic_fact.write.mode("overwrite").option('mergeSchema',"true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}_ra.fact_patient")

