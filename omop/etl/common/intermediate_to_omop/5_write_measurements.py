# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
dbutils.widgets.text("omop_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_ra")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- labs -> measurement
# MAGIC
# MAGIC INSERT INTO ${omop_catalog}.${omop_schema}.measurement_temp
# MAGIC (
# MAGIC     person_id,
# MAGIC     measurement_concept_id,
# MAGIC     measurement_date,
# MAGIC     measurement_datetime,
# MAGIC     measurement_time,
# MAGIC     measurement_type_concept_id,
# MAGIC     operator_concept_id,
# MAGIC     value_as_number,
# MAGIC     value_as_concept_id,
# MAGIC     unit_concept_id,
# MAGIC     range_low,
# MAGIC     range_high,
# MAGIC    -- provider_id,
# MAGIC     visit_occurrence_id,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_concept_id,
# MAGIC     unit_source_value,
# MAGIC     value_source_value,
# MAGIC     x_srcid,
# MAGIC     x_srcloadid,
# MAGIC     x_srcfile
# MAGIC  )
# MAGIC  select
# MAGIC     person_id,
# MAGIC     measurement_concept_id,
# MAGIC     measurement_date,
# MAGIC     measurement_datetime,
# MAGIC     measurement_time,
# MAGIC     measurement_type_concept_id,
# MAGIC     operator_concept_id,
# MAGIC     value_as_number,
# MAGIC     value_as_concept_id,
# MAGIC     unit_concept_id,
# MAGIC     range_low,
# MAGIC     range_high,
# MAGIC     -- provider_id,
# MAGIC     visit_occurrence_id,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_concept_id,
# MAGIC     unit_source_value,
# MAGIC     value_source_value,
# MAGIC     x_srcid,
# MAGIC     x_srcloadid,
# MAGIC     x_srcfile
# MAGIC   from
# MAGIC  (
# MAGIC     select
# MAGIC        p.person_id as person_id,
# MAGIC        coalesce( src.tar_concept_id, 0 ) as measurement_concept_id,
# MAGIC        s.measurement_date as measurement_date,
# MAGIC        s.measurement_date as measurement_datetime,
# MAGIC         trim( to_char(extract( hour from s.measurement_date ), '09') ) || ':' ||
# MAGIC         trim( to_char(extract( minute from s.measurement_date ), '09') ) || ':' ||
# MAGIC         trim( to_char(extract( second from s.measurement_date ), '09') )
# MAGIC        as measurement_time,
# MAGIC        cast( coalesce( s.measurement_source_type_value ,'44818702') as int)  as measurement_type_concept_id,  -- default "from lab result"
# MAGIC        oper.concept_id as operator_concept_id,
# MAGIC        cast(s.value_as_number as numeric ) as value_as_number,
# MAGIC        src.val_concept_id as value_as_concept_id,
# MAGIC        tarunit.concept_id as  unit_concept_id,
# MAGIC        s.range_low as range_low,
# MAGIC        s.range_high as range_high,
# MAGIC        -- pr.provider_id as provider_id,
# MAGIC        v.visit_occurrence_id as visit_occurrence_id,
# MAGIC        s.measurement_source_value as measurement_source_value,
# MAGIC        src.src_concept_id  as measurement_source_concept_id,
# MAGIC        s.unit_source_value as unit_source_value,
# MAGIC        s.value_source_value as value_source_value
# MAGIC        , s.id as x_srcid
# MAGIC        , s.load_id as x_srcloadid
# MAGIC        , 'STAGE_LAB' as x_srcfile
# MAGIC     from ${etl_catalog}.${etl_schema}.stage_lab_temp s
# MAGIC     join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC     left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept_measurement src on s.measurement_source_value = src.raw_concept_code   -- loinc has dashes
# MAGIC         and s.measurement_source_type = src.src_vocabulary_id
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept oper on s.operator_source_value = oper.concept_code
# MAGIC         and oper.domain_id = 'Meas Value Operator'
# MAGIC         and oper.standard_concept = 'S'
# MAGIC         and oper.invalid_reason is null
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept srcunit on s.unit_source_value = srcunit.concept_code
# MAGIC         and srcunit.domain_id = 'Unit'
# MAGIC         and srcunit.invalid_reason is null
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept_relationship crunit on srcunit.concept_id = crunit.concept_id_1
# MAGIC         and crunit.relationship_id = 'Maps to'
# MAGIC         and crunit.invalid_reason is null
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept tarunit on crunit.concept_id_2 = tarunit.concept_id
# MAGIC         and tarunit.standard_concept = 'S'
# MAGIC         and tarunit.invalid_reason is null
# MAGIC    -- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC     where s.load_id = 1
# MAGIC     and  s.measurement_date is not null
# MAGIC   ) a
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- condition to measurement
# MAGIC insert into ${omop_catalog}.${omop_schema}.measurement_temp
# MAGIC (
# MAGIC     person_id,
# MAGIC     measurement_concept_id,
# MAGIC     measurement_date,
# MAGIC     measurement_datetime,
# MAGIC     measurement_type_concept_id,
# MAGIC     value_as_concept_id,
# MAGIC     visit_occurrence_id,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_concept_id,
# MAGIC --     provider_id,
# MAGIC     x_srcid,
# MAGIC     x_srcloadid,
# MAGIC     x_srcfile
# MAGIC )
# MAGIC select
# MAGIC     person_id
# MAGIC     , measurement_concept_id
# MAGIC     , measurement_date
# MAGIC     , measurement_datetime
# MAGIC     , measurement_type_concept_id
# MAGIC     , value_as_concept_id
# MAGIC     , visit_occurrence_id
# MAGIC     , measurement_source_value
# MAGIC     , measurement_source_concept_id
# MAGIC --     , provider_id
# MAGIC     , x_srcid
# MAGIC     , x_srcloadid
# MAGIC     , x_srcfile
# MAGIC from
# MAGIC (
# MAGIC     select 
# MAGIC --        p.person_id as person_id
# MAGIC         person_id,
# MAGIC         , coalesce(src.tar_concept_id, 0 )  as measurement_concept_id
# MAGIC         , s.start_date as measurement_date
# MAGIC         , s.start_date as measurement_datetime
# MAGIC         , 44818701 as measurement_type_concept_id  -- 'From physical examination' -- TODO: may need to be changed
# MAGIC         , src.val_concept_id as value_as_concept_id
# MAGIC --        , v.visit_occurrence_id as visit_occurrence_id
# MAGIC         , visit_occurrence_id,
# MAGIC         , s.condition_source_value as measurement_source_value
# MAGIC         , coalesce(src.src_concept_id, 0) as measurement_source_concept_id
# MAGIC   --      , pr.provider_id
# MAGIC         , s.id as x_srcid
# MAGIC         , s.load_id as x_srcloadid
# MAGIC         , 'STAGE_CONDITION' as x_srcfile
# MAGIC     from ${etl_catalog}.${etl_schema}.stage_condition_temp s
# MAGIC --    join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC --    left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value  and p.person_id = v.person_id
# MAGIC     join ${omop_catalog}.${omop_schema}.concept_measurement src on s.condition_source_value = src.clean_concept_code
# MAGIC         and s.condition_code_source_type = src.src_vocabulary_id
# MAGIC --       left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC     where s.start_date is not null
# MAGIC     and s.load_id = 1
# MAGIC ) a
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- procedure 3
# MAGIC
# MAGIC
# MAGIC 	insert into ${omop_catalog}.${omop_schema}.measurement_temp
# MAGIC 	(
# MAGIC 		person_id,
# MAGIC 		measurement_concept_id,
# MAGIC 		measurement_date,
# MAGIC 		measurement_datetime,
# MAGIC 		measurement_time,
# MAGIC 		measurement_type_concept_id,
# MAGIC 		value_as_concept_id,
# MAGIC 		visit_occurrence_id,
# MAGIC --           visit_detail_id,
# MAGIC 		measurement_source_value,
# MAGIC 		measurement_source_concept_id,
# MAGIC 		-- provider_id,
# MAGIC 		x_srcid,
# MAGIC 		x_srcloadid,
# MAGIC 		x_srcfile
# MAGIC 	)
# MAGIC 	select
# MAGIC 		 person_id
# MAGIC 		, measurement_concept_id
# MAGIC 		, measurement_date
# MAGIC 		, measurement_datetime
# MAGIC 		, measurement_time
# MAGIC 		, measurement_type_concept_id
# MAGIC 		, value_as_concept_id
# MAGIC 		, visit_occurrence_id
# MAGIC --   		, visit_detail_id
# MAGIC 		, measurement_source_value
# MAGIC 		, measurement_source_concept_id
# MAGIC 		-- , provider_id
# MAGIC 		, x_srcid
# MAGIC 		, x_srcloadid
# MAGIC 		, x_srcfile
# MAGIC 	from
# MAGIC 	(
# MAGIC 		select distinct
# MAGIC 			p.person_id as person_id
# MAGIC 			, coalesce(src.tar_concept_id, 0 )  as measurement_concept_id
# MAGIC 			, coalesce( s.procedure_date, v.visit_start_date ) as measurement_date
# MAGIC 			, coalesce( s.procedure_date, v.visit_start_date ) as measurement_datetime
# MAGIC 			, trim( to_char(extract( hour from s.procedure_date ), '09' ) ) || ':' ||
# MAGIC 				  trim( to_char(extract( minute from s.procedure_date ), '09' ) ) || ':' ||
# MAGIC 				  trim( to_char(extract( second from s.procedure_date ), '09' ) )
# MAGIC 			  as measurement_time
# MAGIC 			, 44818701 as measurement_type_concept_id  -- 'From physical examination' -- TODO: may need to be changed
# MAGIC 			, src.val_concept_id as value_as_concept_id
# MAGIC 			, v.visit_occurrence_id as visit_occurrence_id
# MAGIC --   		    , vd.visit_detail_id as visit_detail_id
# MAGIC 			, s.procedure_source_value as measurement_source_value
# MAGIC 			, src.src_concept_id as measurement_source_concept_id
# MAGIC 			-- , pr.provider_id
# MAGIC 			, s.id as x_srcid
# MAGIC 			, s.load_id as x_srcloadid
# MAGIC 			, 'STAGE_PROCEDURE' as x_srcfile
# MAGIC 		from ${etl_catalog}.${etl_schema}.stage_procedure_temp s
# MAGIC 		join ${omop_catalog}.${omop_schema}.person p on  p.person_source_value = s.person_source_value
# MAGIC 		left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC --   		left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC 		join ${omop_catalog}.${omop_schema}.concept_measurement src on s.procedure_source_value = src.clean_concept_code
# MAGIC 			  and s.procedure_code_source_type = src.src_vocabulary_id 
# MAGIC 		-- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC 		where s.procedure_date is not null
# MAGIC 		and s.load_id = 1
# MAGIC 	) a
# MAGIC         ;
