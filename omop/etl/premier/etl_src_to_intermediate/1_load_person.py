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
# MAGIC -- load person
# MAGIC   INSERT INTO ${etl_catalog}.${etl_schema}.stage_person
# MAGIC   (
# MAGIC     id,
# MAGIC     person_source_value,
# MAGIC     gender,
# MAGIC     year_of_birth,
# MAGIC     death_datetime,
# MAGIC     race,
# MAGIC     ethnicity,
# MAGIC     ethnicity_source_value,
# MAGIC     gender_source_value,
# MAGIC     race_source_value,
# MAGIC     load_id
# MAGIC   )
# MAGIC   select distinct
# MAGIC     id,
# MAGIC     person_source_value,
# MAGIC     gender, -- F = Female, M = Male, U = Unknown,
# MAGIC     year_of_birth, --  age in years
# MAGIC     null as death_datetime,
# MAGIC     case race
# MAGIC         when 'W' then 'White'
# MAGIC         when 'B' then 'Black'
# MAGIC         when 'A' then 'Asian'
# MAGIC         when 'O' then 'Other'
# MAGIC         else null
# MAGIC     end as race,
# MAGIC     case hispanic_ind
# MAGIC         when 'Y' then 'Hispanic'
# MAGIC         when 'N' then 'Not'
# MAGIC         else null
# MAGIC     end as ethnicity,
# MAGIC     hispanic_ind as ethnicity_source_value,
# MAGIC     gender as gender_source_value,
# MAGIC     race as race_source_value,
# MAGIC     1 as load_id
# MAGIC   from
# MAGIC   (
# MAGIC       select distinct
# MAGIC         id,
# MAGIC         person_source_value,
# MAGIC         first_value( case when gender is not null then gender end) over (partition by person_source_value order by admit_date desc  ) as gender,
# MAGIC         first_value( case when race is not null then race end) over (partition by person_source_value order by admit_date desc  ) as race,
# MAGIC         first_value( case when hispanic_ind is not null then hispanic_ind end) over (partition by person_source_value order by admit_date desc  ) as hispanic_ind,
# MAGIC         first_value( case when year_of_birth is not null then year_of_birth end) over (partition by person_source_value order by admit_date desc  )  as year_of_birth
# MAGIC       from 
# MAGIC       (
# MAGIC         select distinct
# MAGIC             el.medrec_key as id,
# MAGIC             el.medrec_key as person_source_value,
# MAGIC             el.gender as gender, -- F = Female, M = Male, U = Unknown,
# MAGIC             left(el.admit_date,4) - el.age as year_of_birth, 
# MAGIC             el.age,
# MAGIC             el.race,
# MAGIC             el.hispanic_ind,
# MAGIC             el.admit_date
# MAGIC       from ${source_catalog}.${source_schema}.patdemo el
# MAGIC       )
# MAGIC   )
# MAGIC   ;
