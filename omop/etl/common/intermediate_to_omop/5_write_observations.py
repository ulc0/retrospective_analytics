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
# MAGIC -- labs -> observation
# MAGIC
# MAGIC    insert into ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC     (
# MAGIC       person_id,
# MAGIC       observation_concept_id,
# MAGIC       observation_date,
# MAGIC       observation_datetime,
# MAGIC       observation_type_concept_id,
# MAGIC       value_as_number,
# MAGIC       value_as_string,
# MAGIC       value_as_concept_id,
# MAGIC       unit_concept_id,
# MAGIC       -- provider_id,
# MAGIC       visit_occurrence_id,
# MAGIC       observation_source_value,
# MAGIC       observation_source_concept_id,
# MAGIC       unit_source_value,
# MAGIC       qualifier_source_value,
# MAGIC       x_srcid,
# MAGIC       x_srcloadid,
# MAGIC       x_srcfile
# MAGIC     )     
# MAGIC     select 
# MAGIC       person_id,
# MAGIC       observation_concept_id,
# MAGIC       observation_date,
# MAGIC       observation_datetime,
# MAGIC       observation_type_concept_id,
# MAGIC       value_as_number,
# MAGIC       value_as_string,
# MAGIC       value_as_concept_id,
# MAGIC       unit_concept_id,
# MAGIC       -- provider_id,
# MAGIC       visit_occurrence_id,
# MAGIC       observation_source_value,
# MAGIC       observation_source_concept_id,
# MAGIC       unit_source_value,
# MAGIC       qualifier_source_value,
# MAGIC       x_srcid,
# MAGIC       x_srcloadid,
# MAGIC       x_srcfile
# MAGIC       x_updatedate
# MAGIC     from
# MAGIC     (
# MAGIC       select 
# MAGIC           p.person_id as person_id,
# MAGIC           coalesce( src.tar_concept_id, 0 ) as observation_concept_id,
# MAGIC           s.measurement_date as observation_date,
# MAGIC           cast( s.measurement_date as timestamp ) as observation_datetime,
# MAGIC           coalesce( cast(s.measurement_source_type_value as int), 38000280 )  as observation_type_concept_id,
# MAGIC           cast( s.value_as_number as numeric ) as value_as_number,
# MAGIC           s.value_as_string as value_as_string,
# MAGIC           src.val_concept_id as value_as_concept_id,
# MAGIC           tarunit.concept_id as unit_concept_id,
# MAGIC           -- pr.provider_id as provider_id,
# MAGIC           v.visit_occurrence_id as visit_occurrence_id,
# MAGIC           s.measurement_source_value as observation_source_value,
# MAGIC           src.src_concept_id as observation_source_concept_id,
# MAGIC           s.unit_source_value as unit_source_value,
# MAGIC           null as qualifier_source_value,
# MAGIC           s.id as x_srcid,
# MAGIC           s.load_id as x_srcloadid,
# MAGIC           'STAGE_LAB' as x_srcfile
# MAGIC       from ${etl_catalog}.${etl_schema}.stage_lab_temp s
# MAGIC       join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC       left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC       join ${omop_catalog}.${omop_schema}.concept_observation src on s.measurement_source_value = src.raw_concept_code   -- loinc has dashes
# MAGIC             and s.measurement_source_type = src.src_vocabulary_id
# MAGIC       left join ${omop_catalog}.${omop_schema}.concept srcunit on s.unit_source_value = srcunit.concept_code
# MAGIC           and srcunit.domain_id = 'Unit'
# MAGIC           and srcunit.invalid_reason is null
# MAGIC       left join ${omop_catalog}.${omop_schema}.concept_relationship crunit on srcunit.concept_id = crunit.concept_id_1
# MAGIC           and crunit.relationship_id = 'Maps to'
# MAGIC           and crunit.invalid_reason is null
# MAGIC       left join ${omop_catalog}.${omop_schema}.concept tarunit on crunit.concept_id_2 = tarunit.concept_id
# MAGIC           and tarunit.standard_concept = 'S'
# MAGIC           and tarunit.invalid_reason is null
# MAGIC      -- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC       where s.load_id = 1
# MAGIC       and s.measurement_date is not null
# MAGIC       ) a
# MAGIC     ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- procedure pt 2
# MAGIC
# MAGIC
# MAGIC 	INSERT INTO ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC 	(
# MAGIC 		 person_id
# MAGIC 		, observation_concept_id
# MAGIC 		, observation_date
# MAGIC 		, observation_datetime
# MAGIC 		, observation_type_concept_id
# MAGIC 		, qualifier_concept_id
# MAGIC 		, value_as_concept_id
# MAGIC 		, visit_occurrence_id
# MAGIC --          , visit_detail_id
# MAGIC 		, observation_source_value
# MAGIC 		, observation_source_concept_id
# MAGIC 		-- , provider_id
# MAGIC 		, qualifier_source_value
# MAGIC --	    , obs_event_field_concept_id
# MAGIC 		, x_srcid
# MAGIC 		, x_srcloadid
# MAGIC 		, x_srcfile
# MAGIC 	)
# MAGIC 	select
# MAGIC 		person_id
# MAGIC 		, observation_concept_id
# MAGIC 		, observation_date
# MAGIC 		, observation_datetime
# MAGIC 		, observation_type_concept_id
# MAGIC 		, qualifier_concept_id
# MAGIC 		, value_as_concept_id
# MAGIC 		, visit_occurrence_id
# MAGIC --           , visit_detail_id
# MAGIC 		, observation_source_value
# MAGIC 		, observation_source_concept_id
# MAGIC 		-- , provider_id
# MAGIC 		, qualifier_source_value
# MAGIC --    , obs_event_field_concept_id
# MAGIC 		, x_srcid
# MAGIC 		, x_srcloadid
# MAGIC 		, x_srcfile		
# MAGIC 	from
# MAGIC 	(
# MAGIC 		select
# MAGIC 			p.person_id as person_id
# MAGIC 			, coalesce(src.tar_concept_id, 0 )  as observation_concept_id
# MAGIC 			, coalesce(s.procedure_date, v.visit_start_date) as observation_date
# MAGIC 			, cast(coalesce(s.procedure_date, v.visit_start_date) as timestamp)  as observation_datetime
# MAGIC 			, 38000280 as observation_type_concept_id  -- 'Observation recorded from EHR'  -- TODO: may need to be changed
# MAGIC 			, mod.src_concept_id as qualifier_concept_id
# MAGIC 			, src.val_concept_id as value_as_concept_id
# MAGIC 			, v.visit_occurrence_id as visit_occurrence_id
# MAGIC  --   		, vd.visit_detail_id as visit_detail_id
# MAGIC 			, s.procedure_source_value as observation_source_value
# MAGIC 			, src.src_concept_id as observation_source_concept_id
# MAGIC 			-- , pr.provider_id
# MAGIC 			, s.code_modifier as qualifier_source_value
# MAGIC 	--    , 0 as obs_event_field_concept_id
# MAGIC 			, s.id as x_srcid
# MAGIC 			, s.load_id as x_srcloadid
# MAGIC 			, 'STAGE_PROCEDURE' as x_srcfile
# MAGIC 		from ${etl_catalog}.${etl_schema}.stage_procedure_temp s
# MAGIC 		join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC 		left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC --    	left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC 		join ${omop_catalog}.${omop_schema}.concept_observation src on s.procedure_source_value = src.clean_concept_code
# MAGIC 		and s.procedure_code_source_type = src.src_vocabulary_id 
# MAGIC 		left join ${omop_catalog}.${omop_schema}.concept_modifier mod on s.code_modifier = mod.concept_code
# MAGIC 			and mod.vocabulary_id = src.src_vocabulary_id
# MAGIC 		-- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC 		where s.procedure_date is not null
# MAGIC 		and s.load_id = 1
# MAGIC
# MAGIC      ) a  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- conditions -> observations
# MAGIC
# MAGIC insert into ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC     (
# MAGIC         person_id,
# MAGIC         observation_concept_id,
# MAGIC         observation_date,
# MAGIC         observation_datetime,
# MAGIC         observation_type_concept_id,
# MAGIC         value_as_concept_id,
# MAGIC         visit_occurrence_id,
# MAGIC         observation_source_value,
# MAGIC         observation_source_concept_id,
# MAGIC --  obs_event_field_concept_id,
# MAGIC    --     provider_id,
# MAGIC         x_srcid,
# MAGIC         x_srcloadid,
# MAGIC         x_srcfile
# MAGIC     )
# MAGIC     select
# MAGIC         person_id
# MAGIC         , observation_concept_id
# MAGIC         , observation_date
# MAGIC         , observation_datetime
# MAGIC         , observation_type_concept_id
# MAGIC         , value_as_concept_id
# MAGIC         , visit_occurrence_id
# MAGIC         , observation_source_value
# MAGIC         , observation_source_concept_id
# MAGIC  --  , obs_event_field_concept_id
# MAGIC    --     , provider_id
# MAGIC         , x_srcid
# MAGIC         , x_srcloadid
# MAGIC         , x_srcfile
# MAGIC     from
# MAGIC     (
# MAGIC         select 
# MAGIC             p.person_id as person_id
# MAGIC             , coalesce(src.tar_concept_id, 0 )  as observation_concept_id
# MAGIC             , s.start_date as observation_date
# MAGIC             , s.start_date as observation_datetime
# MAGIC             , 38000280 as observation_type_concept_id  -- 'Observation recorded from EHR'  -- TODO: may need to be changed
# MAGIC             , src.val_concept_id as value_as_concept_id
# MAGIC             , v.visit_occurrence_id as visit_occurrence_id
# MAGIC             , s.condition_source_value as observation_source_value
# MAGIC             , coalesce(src.src_concept_id, 0) as observation_source_concept_id
# MAGIC       -- , 0 as obs_event_field_concept_id
# MAGIC  --           , pr.provider_id
# MAGIC             , s.id as x_srcid
# MAGIC             , s.load_id as x_srcloadid
# MAGIC             , 'STAGE_CONDITION' as x_srcfile
# MAGIC         from ${etl_catalog}.${etl_schema}.stage_condition_temp s
# MAGIC         join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC         left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC         join ${omop_catalog}.${omop_schema}.concept_observation src on s.condition_source_value = src.clean_concept_code
# MAGIC             and s.condition_code_source_type = src.src_vocabulary_id
# MAGIC   --      left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC where s.start_date is not null
# MAGIC and s.load_id = 1
# MAGIC     ) a
# MAGIC     ;
