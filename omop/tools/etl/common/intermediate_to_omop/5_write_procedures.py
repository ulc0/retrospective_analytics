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
# MAGIC -- procedue -> procedure
# MAGIC
# MAGIC 	INSERT INTO ${omop_catalog}.${omop_schema}.procedure_occurrence_temp
# MAGIC 	(
# MAGIC 		person_id,
# MAGIC 		procedure_concept_id,
# MAGIC 		procedure_date,
# MAGIC 		procedure_datetime,
# MAGIC 		procedure_type_concept_id,
# MAGIC 		modifier_concept_id,
# MAGIC 		quantity,
# MAGIC 		-- provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 	--	visit_detail_id,
# MAGIC 		procedure_source_value,
# MAGIC 		procedure_source_concept_id,
# MAGIC 	  modifier_source_value,
# MAGIC 	  x_srcid,
# MAGIC 	  x_srcloadid,
# MAGIC 	  x_srcfile
# MAGIC 	)
# MAGIC 	select
# MAGIC 	  person_id,
# MAGIC 		procedure_concept_id,
# MAGIC 		procedure_date,
# MAGIC 		procedure_datetime,
# MAGIC 		procedure_type_concept_id,
# MAGIC 		modifier_concept_id,
# MAGIC 		quantity,
# MAGIC 		-- provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 	--	visit_detail_id,
# MAGIC 		procedure_source_value,
# MAGIC 		procedure_source_concept_id,
# MAGIC 	  modifier_source_value,
# MAGIC 	  x_srcid,
# MAGIC 	  x_srcloadid,
# MAGIC 	  x_srcfile
# MAGIC 	from
# MAGIC 	(
# MAGIC 		select
# MAGIC 			p.person_id as person_id,
# MAGIC 			coalesce( src.tar_concept_id, 0 )  as procedure_concept_id,
# MAGIC 			 s.procedure_date as procedure_date,
# MAGIC 			cast(s.procedure_date as timestamp) as procedure_datetime,
# MAGIC 			coalesce(s.procedure_source_type_value ,0) as procedure_type_concept_id,
# MAGIC 			mod.src_concept_id as modifier_concept_id,
# MAGIC 			s.quantity  as quantity,
# MAGIC 			-- v.provider_id as provider_id,
# MAGIC 			v.visit_occurrence_id as visit_occurrence_id,
# MAGIC 	--    	vd.visit_detail_id as visit_detail_id,
# MAGIC 			s.procedure_source_value as procedure_source_value,
# MAGIC 			src.src_concept_id as procedure_source_concept_id,
# MAGIC 			s.code_modifier as modifier_source_value,
# MAGIC 			s.id as x_srcid,
# MAGIC 			s.load_id as x_srcloadid,
# MAGIC 			'STAGE_PROCEDURE' as x_srcfile
# MAGIC 		from ${etl_catalog}.${etl_schema}.stage_procedure_temp s
# MAGIC 		join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC 		left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC 	--    left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC 		left join ${omop_catalog}.${omop_schema}.concept_procedure src on s.procedure_source_value = src.clean_concept_code
# MAGIC 			and s.procedure_code_source_type = src.src_vocabulary_id 
# MAGIC 		left join ${omop_catalog}.${omop_schema}.concept_modifier mod on s.code_modifier = mod.concept_code
# MAGIC 			and mod.vocabulary_id = src.src_vocabulary_id
# MAGIC 		--left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC 		where s.procedure_date is not null
# MAGIC 		and s.load_id = 1
# MAGIC 	) a
# MAGIC 	;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- labs -> procedures
# MAGIC INSERT INTO ${omop_catalog}.${omop_schema}.procedure_occurrence_temp
# MAGIC 	(
# MAGIC 		person_id,
# MAGIC 		procedure_concept_id,
# MAGIC 		procedure_date,
# MAGIC 		procedure_datetime,
# MAGIC 		procedure_type_concept_id,
# MAGIC 	--	modifier_concept_id,
# MAGIC 	--	quantity,
# MAGIC 		-- provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 	--	visit_detail_id,
# MAGIC 		procedure_source_value,
# MAGIC 		procedure_source_concept_id,
# MAGIC --	  modifier_source_value,
# MAGIC 	  x_srcid,
# MAGIC 	  x_srcloadid,
# MAGIC 	  x_srcfile
# MAGIC 	----  x_med_clm_serv_linenum
# MAGIC 	)
# MAGIC 	select
# MAGIC 	  person_id,
# MAGIC 		procedure_concept_id,
# MAGIC 		procedure_date,
# MAGIC 		procedure_datetime,
# MAGIC 		procedure_type_concept_id,
# MAGIC --		modifier_concept_id,
# MAGIC --		quantity,
# MAGIC 		-- provider_id,
# MAGIC 		visit_occurrence_id,
# MAGIC 	--	visit_detail_id,
# MAGIC 		procedure_source_value,
# MAGIC 		procedure_source_concept_id,
# MAGIC --	  modifier_source_value,
# MAGIC 	  x_srcid,
# MAGIC 	  x_srcloadid,
# MAGIC 	  x_srcfile
# MAGIC --	  x_med_clm_serv_linenum
# MAGIC 	from
# MAGIC 	(
# MAGIC 		select
# MAGIC 			p.person_id as person_id,
# MAGIC 			coalesce( src.tar_concept_id, 0 )  as procedure_concept_id,
# MAGIC 			s.measurement_date as procedure_date,
# MAGIC 			cast(s.measurement_date as timestamp)  as procedure_datetime,
# MAGIC 			coalesce(cast(s.measurement_source_type_value as int ),0) as procedure_type_concept_id,
# MAGIC --			mod.concept_id as modifier_concept_id,
# MAGIC --			s.quantity  as quantity,
# MAGIC 			-- v.provider_id as provider_id,
# MAGIC 			v.visit_occurrence_id as visit_occurrence_id,
# MAGIC 	--    	vd.visit_detail_id as visit_detail_id,
# MAGIC 			s.measurement_source_value as procedure_source_value,
# MAGIC 			src.src_concept_id as procedure_source_concept_id,
# MAGIC --			s.code_modifier as modifier_source_value,
# MAGIC 			s.id as x_srcid,
# MAGIC 			s.load_id as x_srcloadid,
# MAGIC 			'STAGE_LAB' as x_srcfile
# MAGIC --			cast(right(s.visit_detail_source_value, -4) as bigint) as x_med_clm_serv_linenum
# MAGIC 		from ${etl_catalog}.${etl_schema}.stage_lab_temp s
# MAGIC 		join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC 		left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC 	--    left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC 	 join ${omop_catalog}.${omop_schema}.concept_procedure src on s.measurement_source_value = src.clean_concept_code
# MAGIC 			and s.measurement_source_type = src.src_vocabulary_id 
# MAGIC 	--	left join ${omop_catalog}.${omop_schema}.concept_modifier mod on s.code_modifier = mod.concept_code	and mod.vocabulary_id = src.src_vocabulary_id
# MAGIC 		--left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC 		where s.measurement_date is not null
# MAGIC 		and s.load_id = 1
# MAGIC 	) a
# MAGIC 	;
