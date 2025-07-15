# Databricks notebook source
dbutils.widgets.text("INPUT_DIR",'/Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/')
dbutils.widgets.text("CATALOG",'edav_dev_cdh')
dbutils.widgets.text("SCHEMA",'cdh_mimic')
dbutils.widgets.text("INPUT_CSVS",'radiology,discharge')

# COMMAND ----------

INPUT_DIR=dbutils.widgets.get("INPUT_DIR")
CATALOG=dbutils.widgets.get("CATALOG")
SCHEMA=dbutils.widgets.get("SCHEMA")
#INPUT_CSVS=dbutils.widgets.get("INPUT_CSVS")
#INPUT_CSV_LIST=INPUT_CSVS.split(',')		
#['radiology','discharge']
#print(INPUT_CSV_LIST)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------


INPUT_CSV='radiology'
mimic_rad=spark.table(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}").withColumn("note_source",F.lit(f"{INPUT_CSV}"))
INPUT_CSV='discharge'
mimic_dsc=spark.table(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}").withColumn("note_source",F.lit(f"{INPUT_CSV}"))
mimic_note_df=mimic_dsc.union(mimic_rad).withColumn("new_note_id", F.monotonically_increasing_id())
display(mimic_note_df)

# COMMAND ----------

from pyspark.sql.window import Window as W
windowSpec = W.orderBy("new_note_id")
mimic_note_df = mimic_note_df.withColumn("new_note_id", F.row_number().over(windowSpec))

# COMMAND ----------

mimic_index=mimic_note_df.select(['note_id','new_note_id','hadm_id','charttime'])
display(mimic_index)
mimic_index.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.note_fact_xref")

# COMMAND ----------

col_map={
"subject_id":"person_id",
"charttime":"note_datetime",
"text":"note_text",
"hadm_id":"visit_occurrence_number",
"note_id":"mimic_note_id",
"new_note_id":"note_id",
"type":"note_type",
}
drop_cols=["storetime","note_seq","mimic_note_id","m_note_id"]

# COMMAND ----------

mimic_note=mimic_note_df.drop(*drop_cols).withColumnsRenamed(col_map)
display(mimic_note)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###[MIMIC-IV 2.2](https://physionet.org/content/mimic-iv-note/2.2/)
# MAGIC The advent of large, open access text databases has driven advances in state-of-the-art model performance in natural language processing (NLP). The relatively limited amount of clinical data available for NLP has been cited as a significant barrier to the field's progress. Here we describe MIMIC-IV-Note: a concept_set of deidentified free-text clinical notes for patients included in the MIMIC-IV clinical database.  
# MAGIC
# MAGIC MIMIC-IV-Note contains **331,794** deidentified discharge summaries from **145,915** patients admitted to the hospital and emergency department at the Beth Israel Deaconess Medical Center in Boston, MA, USA. 
# MAGIC
# MAGIC The database also contains **2,321,355** deidentified radiology reports for **237,427** patients. 
# MAGIC
# MAGIC All notes have had protected health information removed in accordance with the Health Insurance Portability and Accountability Act (HIPAA) Safe Harbor provision.  
# MAGIC
# MAGIC All notes are linkable to MIMIC-IV providing important context to the clinical data therein. The database is intended to stimulate research in clinical natural language processing and associated areas.
# MAGIC

# COMMAND ----------

mimic_note.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}_ra.note")

"""
alter table note alter column note_id set not null;
alter table note add constraint note_id  primary key (note_id)
"""
