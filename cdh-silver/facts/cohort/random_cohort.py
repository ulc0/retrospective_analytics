# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/parameter-value-references

# COMMAND ----------

#job/run level parameters
dbutils.widgets.text("FACT_TABLE",defaultValue="edav_dev_cdh.cdh_mimic_ra.note")
dbutils.widgets.text("COHORT_TABLE",defaultValue="edav_dev_cdh.cdh_mimic_exploratory.note_random")
dbutils.widgets.text("SAMPLE_PROPORTION",defaultValue="0.005")
dbutils.widgets.text("SEED",defaultValue="9832402085837")
dbutils.widgets.text("ID_COL",defaultValue="person_id")


# COMMAND ----------

fact_table=dbutils.widgets.get("FACT_TABLE")
cohort_table=dbutils.widgets.get("COHORT_TABLE")
sample_proportion=dbutils.widgets.get("SAMPLE_PROPORTION")
SEED=dbutils.widgets.get("SEED")
ID_COL=dbutils.widgets.get("ID_COL")

# COMMAND ----------

percent=float(sample_proportion)
print(percent)


# COMMAND ----------

sample_df=spark.sql(f"Select distinct {ID_COL} from {fact_table}").sample(withReplacement=None, fraction=percent, seed=SEED)
display(sample_df)

# COMMAND ----------

sample_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{cohort_table}")
