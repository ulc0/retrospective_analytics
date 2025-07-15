# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_dev_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="edav_dev_cdh")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_omop_etl")
dbutils.widgets.text("omop_catalog",defaultValue="edav_dev_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create visits
# MAGIC   insert overwrite ${omop_catalog}.${omop_schema}.visit_occurrence
# MAGIC             (
# MAGIC                 visit_occurrence_id,
# MAGIC                 visit_concept_id,
# MAGIC                 visit_source_value,
# MAGIC                 visit_source_concept_id,
# MAGIC                 admitted_from_concept_id,
# MAGIC                 discharged_to_concept_id,
# MAGIC                 visit_type_concept_id,
# MAGIC                 visit_start_date,
# MAGIC                 visit_start_timestamp,
# MAGIC                 visit_end_date,
# MAGIC                 visit_end_timestamp,
# MAGIC             --    care_site_id,
# MAGIC                 person_id,
# MAGIC             --    provider_id,
# MAGIC                 x_srcid,
# MAGIC                 x_srcloadid,
# MAGIC                 x_srcfile
# MAGIC             )
# MAGIC             select
# MAGIC                 vis.id as visit_occurrence_id
# MAGIC                 , case vis.visit_type
# MAGIC                 --    when ~ '^\d*$' then cast(vis.visit_type as int
# MAGIC                     when 'IN' then 9201
# MAGIC                     when 'OUT' then 9202
# MAGIC                     when 'ER' then 9203
# MAGIC                     when 'LONGTERM' then 42898160
# MAGIC                     else coalesce(cast(vis.visit_type as int),0) 
# MAGIC                 end as visit_concept_id
# MAGIC                 , vis.visit_source_value as visit_source_value
# MAGIC             		,0 as visit_source_concept_id
# MAGIC             		,0 as admitted_from_concept_id
# MAGIC             		,0 as discharged_to_concept_id
# MAGIC                 ,case vis.visit_source_type_value
# MAGIC                --     when  ~ '^\d*$' then cast(vis.visit_source_type_value as int)
# MAGIC                     when 'CLAIM' then 44818517
# MAGIC                     when 'EHR' then 44818518
# MAGIC                     when 'STUDY' then 44818519
# MAGIC                     else coalesce(cast(vis.visit_source_type_value as int),0)
# MAGIC                 end as visit_type_concept_id
# MAGIC                 , vis.visit_start_date as visit_start_date
# MAGIC                 , vis.visit_start_date as visit_start_timestamp
# MAGIC                 , coalesce(vis.visit_end_date, vis.visit_start_date) as visit_end_date
# MAGIC                 , coalesce(vis.visit_end_date, vis.visit_start_date)  as visit_end_timestamp
# MAGIC               --  , pr.care_site_id as care_site_id
# MAGIC               -- , null as care_site
# MAGIC                 , p.person_id as person_id
# MAGIC               --  , pr.provider_id as provider_id
# MAGIC                 , vis.id as x_srcid
# MAGIC                 , vis.load_id as x_srcloadid
# MAGIC                 , 'STAGE_VISIT' as x_srcfile
# MAGIC             from ${etl_catalog}.${etl_schema}.stage_visit vis
# MAGIC             join ${omop_catalog}.${omop_schema}.person p on p.person_source_value = vis.person_source_value
# MAGIC          --   left join ${omop_catalog}.${omop_schema}.provider_source_value pr on vis.provider_source_value = pr.provider_source_value
# MAGIC             -- left join ${omop_catalog}.${omop_schema}.care_site cs on vis.care_site_source_value = cs.care_site_source_value -- getting caresite info from provider record
# MAGIC       	    --left join ${omop_catalog}.${omop_schema}.visit_occurrence vo on vis.visit_source_value = vo.visit_source_value and vo.person_id = p.person_id
# MAGIC       	    --where vo.visit_occurrence_id is null
# MAGIC             where vis.load_id = 1
# MAGIC             ;
# MAGIC

