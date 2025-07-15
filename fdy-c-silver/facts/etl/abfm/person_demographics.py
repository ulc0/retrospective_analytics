# Databricks notebook source
dbutils.widgets.text("SCHEMA",defaultValue="cdh_abfm_phi")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_abfm_phi_ra")

SCHEMA=dbutils.widgets.get('SCHEMA')


DEST_SCHEMA=dbutils.widgets.get('DEST_SCHEMA')
DBCATALOG=dbutils.widgets.get('DBCATALOG')
#experiment_id=dbutils.widgets.get('experiment_id')


# COMMAND ----------

ts_df=f"{DBCATALOG}.{DEST_SCHEMA}.fact_person"
#need an if thing spark.sql(f"drop table {ts_df}")

# COMMAND ----------

dem_wide=spark.table(f"{DBCATALOG}.{SCHEMA}.generatedpatientbaseline").withColumnRenamed('patientuid','person_id')
#dem_wide=spark.table("full_demographics")
dem_wide.display()

# COMMAND ----------

concept_list=['gender','hispanic','race',] #'dob','practiceid']
dem=dem_wide.melt(['person_id'],concept_list,'vocabulary_id','concept_code')
dem.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.ts_demographics")


# COMMAND ----------

#ts_concept_list=['discharged_to_code',]
#demts_discharge=dem_wide.melt(['person_id','discharge_datetime',],ts_concept_list,'vocabulary_id','concept_code').withColumnRenamed('discharge_datetime','observation_datetime')
#display(demts_discharge)


# COMMAND ----------

#ts_concept_list=['payor_code','age_at_admission','marital_status','admitted_from_code','admission_type','hispanic','race','race_ethnicity','patient_type_code']
#demts=dem_wide.melt(['person_id','admission_datetime'],ts_concept_list,'vocabulary_id','concept_code').withColumnRenamed('admission_datetime','observation_datetime')
#display(demts)


# COMMAND ----------

#demts.union(demts_discharge).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.ts_premier_facts")

