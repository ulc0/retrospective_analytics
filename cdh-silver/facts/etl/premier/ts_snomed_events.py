# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("table_name",defaultValue="ts_snomed_events")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_premier_ra")
DBCATALOG=dbutils.widgets.get('DBCATALOG')
SCHEMA=dbutils.widgets.get('SRC_SCHEMA')
DEST_SCHEMA=dbutils.widgets.get('DEST_SCHEMA')
table_name=dbutils.widgets.get('table_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE ${dest.schema}.ts_premier_snomed_events AS
# MAGIC select 
# MAGIC person_id,
# MAGIC --CAST(order_key as INT) as episode_number,
# MAGIC --hospital_time_index,
# MAGIC visit_occurrence_number,
# MAGIC concept_set_datetime as visit_start_date,
# MAGIC "SNOMED" as vocabulary_id,
# MAGIC specimen_source_code as code,
# MAGIC numeric_value,
# MAGIC lab_test_result as test_result
# MAGIC FROM ${ml.schema}.genlab t
# MAGIC join ${dest.schema}.ts_encounter_index i
# MAGIC on t.pat_key=i.visit_occurrence_number
# MAGIC where specimen_source_code is not NULL
# MAGIC and concept_set_datetime is not NULL
# MAGIC order by person_id,visit_occurrence_number

# COMMAND ----------

genlab=spark.table(f"{DBCATALOG}.{SCHEMA}.genlab")

mlab=genlab.melt(['pat_key','concept_set_datetime'],['SPECIMEN_SOURCE_CODE'],'SOURCE','code').drop('SOURCE').where(F.col("code").isNotNull()).where(F.col("concept_set_datetime").isNotNull()).withColumnRenamed('pat_key','visit_occurrence_number')
display(mlab)

# COMMAND ----------

patlabres=spark.read.table(f"{DBCATALOG}.{SCHEMA}.lab_res")

mres=patlabres.melt(['pat_key','concept_set_datetime'],['SPECIMEN_SOURCE_CODE','BODY_SITE_CATEGORY_CODE'],'SOURCE','code').drop('SOURCE').where(F.col("code").isNotNull()).where(F.col("concept_set_datetime").isNotNull()).withColumnRenamed('pat_key','visit_occurrence_number')
display(mres)



# COMMAND ----------

encidx=spark.table(f"{DBCATALOG}.{DEST_SCHEMA}.ts_encounter_index").select(['person_id','visit_occurrence_number','visit_occurrence_number'])

snomed=mlab.union(mres).dropDuplicates().join(encidx,'visit_occurrence_number').withColumn("vocabulary_id",F.lit("SNOMED")).withColumnRenamed('concept_set_datetime','visit_start_date').drop('visit_occurrence_number')
display(snomed)

# COMMAND ----------

snomed.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC genlab=spark.table(f"{DBCATALOG}.{SCHEMA}.genlab")
# MAGIC patlabres=spark.read.table(f"{DBCATALOG}.{SCHEMA}.lab_res")
# MAGIC encidx=spark.table(f"{DBCATALOG}.{DEST_SCHEMA}.ts_encounter_index")
# MAGIC meltedgenlab=patlabres.melt(['pat_key','concept_set_datetime'],['SPECIMEN_SOURCE_CODE','BODY_SITE_CATEGORY_CODE'],'SOURCE','code').drop('SOURCE').withColumn("vocabulary_id",F.lit("SNOMED")).where(F.col("code").isNotNull())
# MAGIC display(meltedgenlab)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TABLE ${dest.schema}.ts_premier_snomed_events AS
# MAGIC select 
# MAGIC person_id,
# MAGIC --CAST(order_key as INT) as episode_number,
# MAGIC --hospital_time_index,
# MAGIC i.visit_occurrence_number,
# MAGIC concept_set_datetime as visit_start_date,
# MAGIC "SNOMED" as vocabulary_id,
# MAGIC specimen_source_code as code,
# MAGIC numeric_value,
# MAGIC lab_test_result as test_result
# MAGIC FROM ${ml.schema}.genlab t
# MAGIC join ${dest.schema}.ts_encounter_index i
# MAGIC on t.pat_key=i.visit_occurrence_number
# MAGIC where specimen_source_code is not NULL
# MAGIC and concept_set_datetime is not NULL
# MAGIC order by person_id,visit_occurrence_number,visit_start_datetime

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TABLE ${dest.schema}.ts_premier_vitals_events AS
# MAGIC (SELECT 
# MAGIC person_id,
# MAGIC episode_number,
# MAGIC visit_occurrence_number,
# MAGIC hospital_time_index,
# MAGIC --date_add(visit_start_datetime,cast(concept_set_day_number as integer))  as event_date,
# MAGIC timestamp(date_add(visit_start_datetime,cast(episode_day_number as integer))||' '|| concept_set_time_of_day) as visit_start_datetime,
# MAGIC --CAST(order_key as INT) as episode_number,
# MAGIC 1 as episode_number,
# MAGIC lab_test_loinc_code as code,
# MAGIC test_result_numeric_value as numeric_value,
# MAGIC timestamp(date_add(visit_start_datetime,cast(result_day_number as integer))||' '|| result_time_of_day) as result_datetime,
# MAGIC lab_test_result_unit as result_unit,
# MAGIC lab_test_result
# MAGIC FROM edav_prd_cdh.cdh_premier_v2.vitals 
# MAGIC join ${ml.schema}.ts_encounter_index
# MAGIC on pat_key=visit_occurrence_id
# MAGIC order by person_id,visit_occurrence_number, visit_start_datetime, episode_number
# MAGIC )

# COMMAND ----------


sqls=f"select distinct code from {DBCATALOG}.{DEST_SCHEMA}.{table_name} "\
    "where code not in (select distinct code from "\
        f"{DBCATALOG}.cdh_abfm_omop_did.concept "\
            "where vocabulary_id like 'SNOMED' and standard_concept='S')"

# COMMAND ----------

bad=spark.sql(sqls)
display(bad)
