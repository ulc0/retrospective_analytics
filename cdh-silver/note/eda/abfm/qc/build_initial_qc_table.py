# Databricks notebook source
dbutils.widgets.text("draft_silver_table","edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze")


# COMMAND ----------

draft_silver_table=dbutils.widgets.get("draft_silver_table")

# COMMAND ----------

draft_tbl=spark.sql(f"select note_id,encoding,note_text,clean_text as expected_text,clean_text from {draft_silver_table}")

# COMMAND ----------

display(draft_tbl)

# COMMAND ----------

draft_tbl.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("edav_prd_cdh.cdh_abfm_phi_ra.note_silver_qc")

