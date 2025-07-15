# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table edav_prd_cdh.cdh_ml.truveta_concept
# MAGIC select * --distinct conceptid
# MAGIC from edav_prd_cdh.cdh_truveta.concept 
# MAGIC   WHERE domain='Condition'
# MAGIC   order by conceptCode,codesystem
# MAGIC   

# COMMAND ----------


