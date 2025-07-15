# Databricks notebook source
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_premier_ra")

SCHEMA=dbutils.widgets.get('SRC_SCHEMA')


DEST_SCHEMA=dbutils.widgets.get('DEST_SCHEMA')
DBCATALOG=dbutils.widgets.get('DBCATALOG')
#experiment_id=dbutils.widgets.get('experiment_id')

# COMMAND ----------

idxname=f"{DBCATALOG}.{DEST_SCHEMA}.fact_encounter_index"
print(idxname)
enc_idx=spark.table(idxname)

# COMMAND ----------


keep_list=['person_id',
  'marital_status',
  'gender',
  'is_hispanic',
  'race',
  'race_ethnicity',
  'year_of_birth']

# COMMAND ----------


demo_slim=enc_idx.select(keep_list)

# COMMAND ----------

dem_wide=spark.table(f"{DBCATALOG}.{DEST_SCHEMA}.fact_encounter_index")
#dem_wide=spark.table("full_demographics")
dem_wide.display()

# COMMAND ----------

dem_wide.printSchema()

# COMMAND ----------

fact_concept_list=['discharged_to_code',]
demfact_discharge=dem_wide.melt(['person_id','discharge_datetime',],fact_concept_list,'vocabulary_id','code').withColumnRenamed('discharge_datetime','visit_start_date')
display(demfact_discharge)

# COMMAND ----------

fact_concept_list=['payor_code','age_at_admission','race','marital_status','admitted_from_code','admission_type','is_hispanic','race','race_ethnicity','patient_type_code','provider_id','payor_code']
demts=dem_wide.melt(['person_id','visit_start_date'],fact_concept_list,'vocabulary_id','code')
display(demts)

# COMMAND ----------

demts.union(demfact_discharge).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.fact_premier_facts")

# COMMAND ----------

import pyspark.sql.functions as F
death_discharge_codes=['20','40','41','42'] #
#death_discharge_codes=[20,40,41,42]

deaths=demts.filter( (demts.vocabulary_id=='discharged_to_code') & (demts.code.isin(death_discharge_codes))).drop('admission_datetime').drop('vocabulary_id').withColumnRenamed('discharge_datetime','visit_start_date').withColumn('vocabulary_id',F.lit('UB04PATIENTSTATUS'))
#.drop(['vocabulary_id','admission_datetime']).select(F.lit('DEATH').alias('vocabulary_id')).select('discharge_datetime').alias('visit_start_date').drop('discharge_date')
display(deaths)

# COMMAND ----------

deaths.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.fact_deaths")
