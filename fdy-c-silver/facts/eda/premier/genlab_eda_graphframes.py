# Databricks notebook source
# MAGIC %sql
# MAGIC use edav_prd_cdh.cdh_premier_exploratory

# COMMAND ----------

# MAGIC %md
# MAGIC select lab_test_loinc_code, min(lab_test_loinc_desc), max(lab_test_loinc_desc)
# MAGIC from genlab
# MAGIC group by lab_test_loinc_code

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table lab_loinc as
# MAGIC (select distinct lab_test_loinc_code, lab_test_loinc_desc, specimen_source_snomed_code
# MAGIC from edav_prd_cdh.cdh_premier_v2.genlab )
# MAGIC UNION (
# MAGIC     select distinct lab_test_loinc_code, lab_test as lab_test_loinc_desc, NULL as specimen_source_snomed_code
# MAGIC from edav_prd_cdh.cdh_premier_v2.vitals
# MAGIC )
# MAGIC --where SPECIMEN_SOURCE_SNOMED_CODE is not null
# MAGIC order by lab_test_loinc_code, SPECIMEN_SOURCE_SNOMED_CODE desc
# MAGIC ---limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct lab_test_loinc_code) from lab_loinc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct specimen_source_snomed_code)
# MAGIC from lab_loinc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct specimen_source_snomed_code, SPECIMEN_SOURCE_SNOMED_DESC )
# MAGIC from edav_prd_cdh.cdh_premier_v2.genlab

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table snomed_codes as
# MAGIC select *
# MAGIC from edav_prd_cdh.cdh_premier_v2.genlab

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from edav_prd_cdh.cdh_premier_v2.genlab

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC  
# MAGIC import org.graphframes._

# COMMAND ----------

# MAGIC %scala
# MAGIC val stationVertices = spark.sql("SELECT  lab_test_loinc_code as id FROM lab_loinc")
# MAGIC .distinct()
# MAGIC
# MAGIC val edgeData = spark.sql("SELECT lab_test_loinc_code as src, specimen_source_snomed_code as dst FROM lab_loinc").withColumnRenamed("lab_test_loinc_coe", "src")
# MAGIC   .withColumnRenamed("specimen_source_snomed_code", "dst")

# COMMAND ----------

# MAGIC %scala
# MAGIC val stationGraph = GraphFrame(stationVertices, edgeData)
# MAGIC  
# MAGIC edgeData.cache()
# MAGIC stationVertices.cache()

# COMMAND ----------

# MAGIC %scala
# MAGIC val inDeg = stationGraph.inDegrees
# MAGIC display(inDeg.orderBy(desc("inDegree")).limit(5000))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lab_loinc
# MAGIC where lab_test_loinc_code='3255-7'
