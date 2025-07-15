# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
dbutils.widgets.text("omop_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE ${omop_catalog}.${omop_schema}.person
# MAGIC   (
# MAGIC     person_id,
# MAGIC     gender_concept_id,
# MAGIC     year_of_birth,
# MAGIC     month_of_birth,
# MAGIC     day_of_birth,
# MAGIC     race_concept_id,
# MAGIC     ethnicity_concept_id,
# MAGIC     person_source_value,
# MAGIC     gender_source_value,
# MAGIC --         gender_source_concept_id,
# MAGIC     race_source_value,
# MAGIC --       race_source_concept_id,
# MAGIC     ethnicity_source_value,
# MAGIC --      ethnicity_source_concept_id,
# MAGIC     x_srcid,
# MAGIC     x_srcloadid,
# MAGIC     x_srcfile
# MAGIC )
# MAGIC
# MAGIC   select
# MAGIC     pat.id as person_id,
# MAGIC     case pat.gender
# MAGIC         when 'M' then 8507
# MAGIC         when 'F' then 8532
# MAGIC         when 'A' then 8570
# MAGIC         when 'U' then 8551
# MAGIC         when 'O' then 8521
# MAGIC         when null then null
# MAGIC         else 8551
# MAGIC     end as gender_concept_id,
# MAGIC     coalesce(pat.year_of_birth,1885) as year_of_birth,
# MAGIC     pat.month_of_birth as month_of_birth,
# MAGIC     pat.day_of_birth as day_of_birth,
# MAGIC     case pat.race
# MAGIC         when 'White' then 8527
# MAGIC         when 'Black' then 8516
# MAGIC         when 'Asian' then 8515
# MAGIC         when 'American Indian or Alaska Native' then 8657
# MAGIC         when 'Native Hawaiian or Other Pacific Islander' then 8557
# MAGIC         when 'Other' then 8522
# MAGIC         else 8552
# MAGIC     end as race_concept_id,
# MAGIC     case pat.ethnicity
# MAGIC         when   'Hispanic' then 38003563
# MAGIC         when  'Not' then 38003564
# MAGIC         else 0
# MAGIC     end as ethnicity_concept_id,
# MAGIC --     loc.location_id as location_id,
# MAGIC --     prov.provider_id as provider_id,
# MAGIC     -- coalesce( cs.care_site_id, prov.care_site_id ) as care_site_id,
# MAGIC     pat.person_source_value as person_source_value,
# MAGIC     pat.gender_source_value  as gender_source_value,
# MAGIC --      0 as gender_source_concept_id,
# MAGIC     pat.race_source_value as race_source_value,
# MAGIC --     0 as race_source_concept_id,
# MAGIC     pat.ethnicity_source_value as ethnicity_source_value,
# MAGIC --     0 as ethnicity_source_concept_id
# MAGIC     pat.id as x_srcid,
# MAGIC     pat.load_id as x_srcloadid,
# MAGIC     'STAGE_PERSON' as x_srcfile
# MAGIC   from ${etl_catalog}.${etl_schema}.stage_person pat
# MAGIC 	--	left join ${omop_catalog}.${omop_schema}.person pers on cast(pat.person_source_value as bigint) = pers.person_id
# MAGIC 	--	where pers.person_id is null
# MAGIC 		where pat.load_id = 1
# MAGIC         ;
