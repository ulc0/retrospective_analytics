# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/parameter-value-references

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#job/run level parameters
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_abfm_phi")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_abfm_phi_ra")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")


# COMMAND ----------

#task level parameters
#prefix for dictionary, name for file
#key to the dictionary
##dbutils.widgets.text("conceptset",defaultValue="medication")
#dbutils.widgets.text("taskname",defaultValue="this_conceptset")

# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------

EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------


import shared.etl_utils as utils

# COMMAND ----------

# MAGIC %md
# MAGIC import json
# MAGIC
# MAGIC with open('./vocab_rules.json', 'r') as file:
# MAGIC     vocab = json.load(file)
# MAGIC with open('./premier_mapping.json', 'r') as file:
# MAGIC     FACT_MAPPING = json.load(file)
# MAGIC

# COMMAND ----------
concept_tbl="edav_dev_cdh.cdh_global_reference_etl.bronze_athena_concept"
fact_tbl=f"{DBCATALOG}.{DEST_SCHEMA}.fact_person"
print(fact_tbl)
fact_df=spark.table(fact_tbl)
concept_df=spark.table(concept_tbl)

# COMMAND ----------

xref_vocabs=spark.sql(f"select distinct vocabulary_id from {fact_tbl} as v outer join select distinct vocabulary_id from {concept_tbl} as c on v.vocabulary_id=c.vocabulary_id")

