# Databricks notebook source
# MAGIC %md ## All text table

# COMMAND ----------

donotrun


pxNote = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnote")
pxNote_large = (
    pxNote.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

pxResObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation")

pxNote_large = (
    pxResObs.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)
#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

notes = pxNote_large.unionByName(pxNote_large)
db = "edav_prd_cdh.cdh_abfm_phi_exploratory"
version_run2 = "large"

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory_run9_{}""".format(version_run2))
notes.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run2}")

# COMMAND ----------

spark.table(f"{db}.run9_{version_run2}").display()
spark.table(f"{db}.run9_{version_run2}").count()


# COMMAND ----------

f"{db}.run9_{version_run2}"

# COMMAND ----------

# MAGIC %md single word array

# COMMAND ----------

db = "edav_prd_cdh.cdh_abfm_phi_exploratory"
version_run3 = "short_notes"

# COMMAND ----------

single_word = (
    spark.table(f"{db}.run9_{version_run2}")
    .withColumn("size", F.size("notes"))
    .where("size > 10")
)
    
display(
    single_word
)

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory_run9_{}""".format(version_run3))
single_word.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run3}")

# COMMAND ----------

f"{db}.run9_{version_run3}"

# COMMAND ----------

single_word.count()
