# Databricks notebook source
import pyspark.sql.functions as F


# COMMAND ----------


dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
DBCATALOG=dbutils.widgets.get("DBCATALOG")

dbutils.widgets.text('SRC_SCHEMA',defaultValue='cdh_premier_v2')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text('SCHEMA',defaultValue='cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')


dbutils.widgets.text('LAVASCHEMA',defaultValue='cdh_premier_exploratory')
LAVASCHEMA=dbutils.widgets.get('LAVASCHEMA')


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table edav_prd_cdh.cdh_abfm_phi_ra.resp_features as
# MAGIC SELECT
# MAGIC   distinct feature_name,
# MAGIC   code,
# MAGIC   CASE
# MAGIC     substring(feature_name, 1, 3)
# MAGIC     WHEN 'icd' then 'ICD10CM'
# MAGIC     WHEN 'rac' then 'RACE_ETHNICITY'
# MAGIC     WHEN 'sex' then 'GENDER'
# MAGIC     WHEN 'dis' then 'DISCHARGED_TO_CODE'
# MAGIC     WHEN 'pro' then 'PROV_ID'
# MAGIC     ELSE 'STDCHG'
# MAGIC   END as vocabulary_id
# MAGIC FROM
# MAGIC   edav_prd_cdh.cdh_premier_exploratory.code_local_valuesets
# MAGIC where
# MAGIC   feature_name not like "viz_%"
# MAGIC   and feature_name not like "acs_%"
# MAGIC   and feature_name not like "agecat_%"
# MAGIC   and feature_name not like "jira_%"
# MAGIC   and feature_name not like "icd_pri_%"
# MAGIC   and feature_name not like "range_%"
# MAGIC   and feature_name not like "sv_%"
# MAGIC   and feature_name not like "vent_flag%"
# MAGIC   and feature_name not like "clin_sum_%"
# MAGIC   and feature_name not like "virus_%"
# MAGIC   and feature_name not like "admit_%"
# MAGIC   and feature_name not like "std_payor_%"
# MAGIC   and feature_name not like "race_no_%"
# MAGIC order by
# MAGIC   feature_name

# COMMAND ----------

icd_valueset_code =[ 
'code_icd_covid_vw' ,
'code_icd_flu_vw' ,
'code_icd_rsv_vw',
]
chgmstr_valueset_code =[ 
'code_clin_dtl_icu_vent',
]

resp_feature_codes = {
'icu_vent_flag':'STDCHG' ,
'icd_flu':'ICD10CM' ,
'icd_rsv':'ICD10CM',
'icd_covid':'ICD10CM',
'prov_region':'PROV',
'prov_division':'PROV',
'std_payor':'std_payor_type',
'disc_status_expired_flag':'discharged_to_code',

}
valueset_code=list(resp_feature_codes.keys())
print(valueset_code)


# COMMAND ----------

# MAGIC %md
# MAGIC valuesets=spark.table(f"{DBCATALOG}.{LAVASCHEMA}.code_local_valuesets").where(F.col("valueset_code").isin(valueset_code))#keep(['feature_name','code'])
# MAGIC #.withColumn(F.lit('ICD'),'vocabulary_id')

# COMMAND ----------

valuesets=spark.table(f"{DBCATALOG}.cdh_abfm_phi_ra.resp_features")

# COMMAND ----------

display(valuesets)


# COMMAND ----------

ft_name=f"{DBCATALOG}.{SCHEMA}.concept_sets"

write_mode = "append"
schema_option = "mergeSchema"
valuesets.write.format("delta").mode(write_mode).option(
                schema_option, "true"
            ).saveAsTable( ft_name)
 
