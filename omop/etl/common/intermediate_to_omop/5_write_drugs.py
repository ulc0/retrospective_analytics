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
# MAGIC -- procedure 4
# MAGIC
# MAGIC 	INSERT INTO ${omop_catalog}.${omop_schema}.drug_exposure_temp
# MAGIC 	(
# MAGIC 		person_id,
# MAGIC 		drug_concept_id,
# MAGIC 		drug_exposure_start_date,
# MAGIC 		drug_exposure_start_datetime,
# MAGIC 		drug_exposure_end_date,
# MAGIC 		drug_exposure_end_datetime,
# MAGIC 		drug_type_concept_id,
# MAGIC 		quantity,
# MAGIC 	--	route_concept_id,
# MAGIC 	--	provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 		--       visit_detail_id,
# MAGIC 		drug_source_value,
# MAGIC 		drug_source_concept_id,
# MAGIC 		x_srcid,
# MAGIC 		x_srcloadid,
# MAGIC 		x_srcfile
# MAGIC 	)
# MAGIC 	select
# MAGIC 		person_id,
# MAGIC 		drug_concept_id,
# MAGIC 		drug_exposure_start_date,
# MAGIC 		drug_exposure_start_datetime,
# MAGIC 		drug_exposure_end_date,
# MAGIC 		drug_exposure_end_datetime,
# MAGIC 		drug_type_concept_id,
# MAGIC 		quantity,
# MAGIC 		--	route_concept_id,
# MAGIC 		-- provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 		--      visit_detail_id,
# MAGIC 		drug_source_value,
# MAGIC 		drug_source_concept_id,
# MAGIC 		x_srcid,
# MAGIC 		x_srcloadid,
# MAGIC 		x_srcfile
# MAGIC 	from
# MAGIC 	(
# MAGIC 		select
# MAGIC 			p.person_id as person_id,
# MAGIC 			coalesce(src.tar_concept_id, 0 ) as drug_concept_id,
# MAGIC 			coalesce( s.procedure_date, v.visit_start_date ) as drug_exposure_start_date,
# MAGIC 			coalesce( s.procedure_date, v.visit_start_date ) as drug_exposure_start_datetime,
# MAGIC 			coalesce( s.procedure_date, v.visit_start_date ) as drug_exposure_end_date,
# MAGIC 			coalesce( s.procedure_date, v.visit_start_date ) as drug_exposure_end_datetime,
# MAGIC 			38000179 as drug_type_concept_id,   -- physician administered as procedure code
# MAGIC 			s.quantity as quantity,
# MAGIC 		--	0 as route_concept_id,
# MAGIC 			-- pr.provider_id  as provider_id,
# MAGIC 			v.visit_occurrence_id as visit_occurrence_id,
# MAGIC 		--  		vd.visit_detail_id as visit_detail_id,
# MAGIC 			s.procedure_source_value as drug_source_value,
# MAGIC 			src.src_concept_id as drug_source_concept_id,
# MAGIC 			s.id as x_srcid,
# MAGIC 			s.load_id as x_srcloadid,
# MAGIC 			'STAGE_PROCEDURE' as x_srcfile
# MAGIC 		from ${etl_catalog}.${etl_schema}.stage_procedure_temp s
# MAGIC 		join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC 		left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC 		--  	  left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC 		join ${omop_catalog}.${omop_schema}.concept_drug src on s.procedure_source_value = src.clean_concept_code
# MAGIC 		and s.procedure_code_source_type = src.src_vocabulary_id 
# MAGIC 		--left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC 		where s.procedure_date is not null
# MAGIC 		and s.load_id = 1
# MAGIC 	) a
# MAGIC 	;
