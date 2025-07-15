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
# MAGIC --sql server CDM DDL Specification for OMOP Common Data Model 5.4 
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.CARE_SITE;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.CARE_SITE (
# MAGIC 			care_site_id bigint NOT NULL,
# MAGIC 			care_site_name varchar(255),
# MAGIC 			place_of_service_concept_id integer,
# MAGIC 			location_id bigint,
# MAGIC 			care_site_source_value varchar(50),
# MAGIC 			place_of_service_source_value varchar(50),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp default current_timestamp(),
# MAGIC 			x_updatedate timestamp default current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if exists ${omop_catalog}.${omop_schema}.PERSON;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.PERSON (
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			gender_concept_id integer NOT NULL,
# MAGIC 			year_of_birth integer NOT NULL,
# MAGIC 			month_of_birth integer,
# MAGIC 			day_of_birth integer,
# MAGIC 			birth_timestamp timestamp,
# MAGIC 			race_concept_id integer NOT NULL,
# MAGIC 			ethnicity_concept_id integer NOT NULL,
# MAGIC 			location_id bigint,
# MAGIC 			provider_id bigint,
# MAGIC 			care_site_id bigint,
# MAGIC 			person_source_value varchar(50),
# MAGIC 			gender_source_value varchar(50),
# MAGIC 			gender_source_concept_id integer,
# MAGIC 			race_source_value varchar(50),
# MAGIC 			race_source_concept_id integer,
# MAGIC 			ethnicity_source_value varchar(50),
# MAGIC 			ethnicity_source_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD (
# MAGIC 			observation_period_id bigint generated always as identity NOT NULL,
# MAGIC 			--observation_period_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			observation_period_start_date date NOT NULL,
# MAGIC 			observation_period_end_date date NOT NULL,
# MAGIC 			period_type_concept_id integer NOT NULL,      
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE (
# MAGIC 			visit_occurrence_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			visit_concept_id integer NOT NULL,
# MAGIC 			visit_start_date date NOT NULL,
# MAGIC 			visit_start_timestamp timestamp,
# MAGIC 			visit_end_date date NOT NULL,
# MAGIC 			visit_end_timestamp timestamp,
# MAGIC 			visit_type_concept_id Integer NOT NULL,
# MAGIC 			provider_id bigint,
# MAGIC 			care_site_id bigint,
# MAGIC 			visit_source_value varchar(50),
# MAGIC 			visit_source_concept_id integer,
# MAGIC 			admitted_from_concept_id integer,
# MAGIC 			admitted_from_source_value varchar(50),
# MAGIC 			discharged_to_concept_id integer,
# MAGIC 			discharged_to_source_value varchar(50),
# MAGIC 			preceding_visit_occurrence_id bigint,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if exists ${omop_catalog}.${omop_schema}.VISIT_DETAIL;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.VISIT_DETAIL (
# MAGIC 			visit_detail_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			visit_detail_concept_id integer NOT NULL,
# MAGIC 			visit_detail_start_date date NOT NULL,
# MAGIC 			visit_detail_start_timestamp timestamp,
# MAGIC 			visit_detail_end_date date NOT NULL,
# MAGIC 			visit_detail_end_timestamp timestamp,
# MAGIC 			visit_detail_type_concept_id integer NOT NULL,
# MAGIC 			provider_id bigint,
# MAGIC 			care_site_id bigint,
# MAGIC 			visit_detail_source_value varchar(50),
# MAGIC 			visit_detail_source_concept_id Integer,
# MAGIC 			admitted_from_concept_id Integer,
# MAGIC 			admitted_from_source_value varchar(50),
# MAGIC 			discharged_to_source_value varchar(50),
# MAGIC 			discharged_to_concept_id integer,
# MAGIC 			preceding_visit_detail_id bigint,
# MAGIC 			parent_visit_detail_id bigint,
# MAGIC 			visit_occurrence_id bigint NOT NULL,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE (
# MAGIC 			condition_occurrence_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			condition_concept_id integer NOT NULL,
# MAGIC 			condition_start_date date NOT NULL,
# MAGIC 			condition_start_datetime timestamp,
# MAGIC 			condition_end_date date,
# MAGIC 			condition_end_datetime timestamp,
# MAGIC 			condition_type_concept_id integer NOT NULL,
# MAGIC 			condition_status_concept_id integer,
# MAGIC 			stop_reason varchar(20),
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			condition_source_value varchar(50),
# MAGIC 			condition_source_concept_id integer,
# MAGIC 			condition_status_source_value varchar(50),  
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC ;
# MAGIC
# MAGIC -- drop TABLE if exists ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE ;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE (
# MAGIC 			drug_exposure_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			drug_concept_id integer NOT NULL,
# MAGIC 			drug_exposure_start_date date NOT NULL,
# MAGIC 			drug_exposure_start_datetime timestamp,
# MAGIC 			drug_exposure_end_date date NOT NULL,
# MAGIC 			drug_exposure_end_datetime timestamp,
# MAGIC 			verbatim_end_date date,
# MAGIC 			drug_type_concept_id integer NOT NULL,
# MAGIC 			stop_reason varchar(20),
# MAGIC 			refills integer,
# MAGIC 			quantity float,
# MAGIC 			days_supply integer,
# MAGIC 			sig varchar(4000),
# MAGIC 			route_concept_id integer,
# MAGIC 			lot_number varchar(50),
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			drug_source_value varchar(50),
# MAGIC 			drug_source_concept_id integer,
# MAGIC 			route_source_value varchar(50),
# MAGIC 			dose_unit_source_value varchar(50),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if exists ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE (
# MAGIC 			procedure_occurrence_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			procedure_concept_id integer NOT NULL,
# MAGIC 			procedure_date date NOT NULL,
# MAGIC 			procedure_datetime timestamp,
# MAGIC 			procedure_end_date date,
# MAGIC 			procedure_end_datetime timestamp,
# MAGIC 			procedure_type_concept_id integer NOT NULL,
# MAGIC 			modifier_concept_id integer,
# MAGIC 			quantity integer,
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			procedure_source_value varchar(50),
# MAGIC 			procedure_source_concept_id integer,
# MAGIC 			modifier_source_value varchar(50),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE (
# MAGIC 			device_exposure_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			device_concept_id integer NOT NULL,
# MAGIC 			device_exposure_start_date date NOT NULL,
# MAGIC 			device_exposure_start_datetime timestamp,
# MAGIC 			device_exposure_end_date date,
# MAGIC 			device_exposure_end_datetime timestamp,
# MAGIC 			device_type_concept_id integer NOT NULL,
# MAGIC 			unique_device_id varchar(255),
# MAGIC 			production_id varchar(255),
# MAGIC 			quantity integer,
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			device_source_value varchar(50),
# MAGIC 			device_source_concept_id integer,
# MAGIC 			unit_concept_id integer,
# MAGIC 			unit_source_value varchar(50),
# MAGIC 			unit_source_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.MEASUREMENT;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.MEASUREMENT (
# MAGIC 			measurement_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			measurement_concept_id integer NOT NULL,
# MAGIC 			measurement_date date NOT NULL,
# MAGIC 			measurement_datetime timestamp,
# MAGIC 			measurement_time varchar(10),
# MAGIC 			measurement_type_concept_id integer NOT NULL,
# MAGIC 			operator_concept_id integer,
# MAGIC 			value_as_number float,
# MAGIC 			value_as_concept_id integer,
# MAGIC 			unit_concept_id integer,
# MAGIC 			range_low float,
# MAGIC 			range_high float,
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			measurement_source_value varchar(50),
# MAGIC 			measurement_source_concept_id integer,
# MAGIC 			unit_source_value varchar(50),
# MAGIC 			unit_source_concept_id integer,
# MAGIC 			value_source_value varchar(50),
# MAGIC 			measurement_event_id bigint,
# MAGIC 			meas_event_field_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.OBSERVATION;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.OBSERVATION (
# MAGIC 			observation_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			observation_concept_id integer NOT NULL,
# MAGIC 			observation_date date NOT NULL,
# MAGIC 			observation_datetime timestamp,
# MAGIC 			observation_type_concept_id integer NOT NULL,
# MAGIC 			value_as_number float,
# MAGIC 			value_as_string varchar(60),
# MAGIC 			value_as_concept_id Integer,
# MAGIC 			qualifier_concept_id integer,
# MAGIC 			unit_concept_id integer,
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			observation_source_value varchar(50),
# MAGIC 			observation_source_concept_id integer,
# MAGIC 			unit_source_value varchar(50),
# MAGIC 			qualifier_source_value varchar(50),
# MAGIC 			value_source_value varchar(50),
# MAGIC 			observation_event_id bigint,
# MAGIC 			obs_event_field_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.DEATH;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.DEATH (
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			death_date date NOT NULL,
# MAGIC 			death_timestamp timestamp,
# MAGIC 			death_type_concept_id integer,
# MAGIC 			cause_concept_id integer,
# MAGIC 			cause_source_value varchar(50),
# MAGIC 			cause_source_concept_id integer ,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.NOTE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.NOTE (
# MAGIC 			note_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			note_date date NOT NULL,
# MAGIC 			note_timestamp timestamp,
# MAGIC 			note_type_concept_id integer NOT NULL,
# MAGIC 			note_class_concept_id integer NOT NULL,
# MAGIC 			note_title varchar(250),
# MAGIC 			note_text varchar(4000) NOT NULL,
# MAGIC 			encoding_concept_id integer NOT NULL,
# MAGIC 			language_concept_id integer NOT NULL,
# MAGIC 			provider_id bigint,
# MAGIC 			visit_occurrence_id bigint,
# MAGIC 			visit_detail_id bigint,
# MAGIC 			note_source_value varchar(50),
# MAGIC 			note_event_id bigint,
# MAGIC 			note_event_field_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.NOTE_NLP;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.NOTE_NLP (
# MAGIC 			note_nlp_id bigint NOT NULL,
# MAGIC 			note_id bigint NOT NULL,
# MAGIC 			section_concept_id integer,
# MAGIC 			snippet varchar(250),
# MAGIC 			offset varchar(50),
# MAGIC 			lexical_variant varchar(250) NOT NULL,
# MAGIC 			note_nlp_concept_id integer,
# MAGIC 			note_nlp_source_concept_id integer,
# MAGIC 			nlp_system varchar(250),
# MAGIC 			nlp_date date NOT NULL,
# MAGIC 			nlp_timestamp timestamp,
# MAGIC 			term_exists varchar(1),
# MAGIC 			term_temporal varchar(50),
# MAGIC 			term_modifiers varchar(2000),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.SPECIMEN;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.SPECIMEN (
# MAGIC 			specimen_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			specimen_concept_id integer NOT NULL,
# MAGIC 			specimen_type_concept_id integer NOT NULL,
# MAGIC 			specimen_date date NOT NULL,
# MAGIC 			specimen_timestamp timestamp,
# MAGIC 			quantity float,
# MAGIC 			unit_concept_id integer,
# MAGIC 			anatomic_site_concept_id integer,
# MAGIC 			disease_status_concept_id integer,
# MAGIC 			specimen_source_id varchar(50),
# MAGIC 			specimen_source_value varchar(50),
# MAGIC 			unit_source_value varchar(50),
# MAGIC 			anatomic_site_source_value varchar(50),
# MAGIC 			disease_status_source_value varchar(50),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if exists ${omop_catalog}.${omop_schema}.LOCATION;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.LOCATION (
# MAGIC 			location_id bigint NOT NULL,
# MAGIC 			address_1 varchar(50),
# MAGIC 			address_2 varchar(50),
# MAGIC 			city varchar(50),
# MAGIC 			state varchar(2),
# MAGIC 			zip varchar(9),
# MAGIC 			county varchar(20),
# MAGIC 			location_source_value varchar(50),
# MAGIC 			country_concept_id integer,
# MAGIC 			country_source_value varchar(80),
# MAGIC 			latitude float,
# MAGIC 			longitude float,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.PROVIDER ;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.PROVIDER (
# MAGIC 			provider_id bigint NOT NULL,
# MAGIC 			provider_name varchar(255),
# MAGIC 			npi varchar(20),
# MAGIC 			dea varchar(20),
# MAGIC 			specialty_concept_id integer,
# MAGIC 			care_site_id bigint,
# MAGIC 			year_of_birth integer,
# MAGIC 			gender_concept_id integer,
# MAGIC 			provider_source_value varchar(50),
# MAGIC 			specialty_source_value varchar(50),
# MAGIC 			specialty_source_concept_id integer,
# MAGIC 			gender_source_value varchar(50),
# MAGIC 			gender_source_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD (
# MAGIC 			payer_plan_period_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			payer_plan_period_start_date date NOT NULL,
# MAGIC 			payer_plan_period_end_date date NOT NULL,
# MAGIC 			payer_concept_id integer,
# MAGIC 			payer_source_value varchar(50),
# MAGIC 			payer_source_concept_id integer,
# MAGIC 			plan_concept_id integer,
# MAGIC 			plan_source_value varchar(50),
# MAGIC 			plan_source_concept_id integer,
# MAGIC 			sponsor_concept_id integer,
# MAGIC 			sponsor_source_value varchar(50),
# MAGIC 			sponsor_source_concept_id integer,
# MAGIC 			family_source_value varchar(50),
# MAGIC 			stop_reason_concept_id integer,
# MAGIC 			stop_reason_source_value varchar(50),
# MAGIC 			stop_reason_source_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.COST;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.COST (
# MAGIC 			cost_id bigint NOT NULL,
# MAGIC 			cost_event_id integer NOT NULL,
# MAGIC 			cost_domain_id varchar(20) NOT NULL,
# MAGIC 			cost_type_concept_id integer NOT NULL,
# MAGIC 			currency_concept_id integer,
# MAGIC 			total_charge float,
# MAGIC 			total_cost float,
# MAGIC 			total_paid float,
# MAGIC 			paid_by_payer float,
# MAGIC 			paid_by_patient float,
# MAGIC 			paid_patient_copay float,
# MAGIC 			paid_patient_coinsurance float,
# MAGIC 			paid_patient_deductible float,
# MAGIC 			paid_by_primary float,
# MAGIC 			paid_ingredient_cost float,
# MAGIC 			paid_dispensing_fee float,
# MAGIC 			payer_plan_period_id bigint,
# MAGIC 			amount_allowed float,
# MAGIC 			revenue_code_concept_id integer,
# MAGIC 			revenue_code_source_value varchar(50),
# MAGIC 			drg_concept_id integer,
# MAGIC 			drg_source_value varchar(3),
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.DRUG_ERA;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.DRUG_ERA (
# MAGIC 			drug_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			drug_concept_id integer NOT NULL,
# MAGIC 			drug_era_start_date timestamp NOT NULL,
# MAGIC 			drug_era_end_date timestamp NOT NULL,
# MAGIC 			drug_exposure_count integer,
# MAGIC 			gap_days integer )
# MAGIC 			 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.DOSE_ERA;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.DOSE_ERA (
# MAGIC 			dose_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			drug_concept_id integer NOT NULL,
# MAGIC 			unit_concept_id integer NOT NULL,
# MAGIC 			dose_value float NOT NULL,
# MAGIC 			dose_era_start_date timestamp NOT NULL,
# MAGIC 			dose_era_end_date timestamp NOT NULL )
# MAGIC 			 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.CONDITION_ERA;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.CONDITION_ERA (
# MAGIC 			condition_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			condition_concept_id integer NOT NULL,
# MAGIC 			condition_era_start_date timestamp NOT NULL,
# MAGIC 			condition_era_end_date timestamp NOT NULL,
# MAGIC 			condition_occurrence_count integer )
# MAGIC 			 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.EPISODE;
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.EPISODE (
# MAGIC 			episode_id bigint NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			episode_concept_id integer NOT NULL,
# MAGIC 			episode_start_date date NOT NULL,
# MAGIC 			episode_start_timestamp timestamp,
# MAGIC 			episode_end_date date,
# MAGIC 			episode_end_timestamp timestamp,
# MAGIC 			episode_parent_id bigint,
# MAGIC 			episode_number integer,
# MAGIC 			episode_object_concept_id integer NOT NULL,
# MAGIC 			episode_type_concept_id integer NOT NULL,
# MAGIC 			episode_source_value varchar(50),
# MAGIC 			episode_source_concept_id integer,
# MAGIC 			x_srcid bigint,   
# MAGIC 			x_srcloadid integer,
# MAGIC 			x_srcfile varchar(20),
# MAGIC 			x_createdate timestamp  default current_timestamp(),
# MAGIC 			x_updatedate timestamp  default current_timestamp()
# MAGIC  ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported') ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter tables to add columns and defaults
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.condition_occurrence_temp;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.condition_occurrence_temp (
# MAGIC     condition_occurrence_id bigint generated always as identity NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     condition_concept_id integer NOT NULL,
# MAGIC     condition_start_date date NOT NULL,
# MAGIC     condition_start_datetime timestamp,
# MAGIC     condition_end_date date,
# MAGIC     condition_end_datetime timestamp,
# MAGIC     condition_type_concept_id integer NOT NULL,
# MAGIC     condition_status_concept_id integer,
# MAGIC     stop_reason varchar(20),
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     condition_source_value varchar(50),
# MAGIC     condition_source_concept_id integer,
# MAGIC     condition_status_source_value varchar(50),
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.device_exposure_temp;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.device_exposure_temp (
# MAGIC     device_exposure_id bigint generated always as identity NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     device_concept_id integer NOT NULL,
# MAGIC     device_exposure_start_date date NOT NULL,
# MAGIC     device_exposure_start_datetime timestamp,
# MAGIC     device_exposure_end_date date,
# MAGIC     device_exposure_end_datetime timestamp,
# MAGIC     device_type_concept_id integer NOT NULL,
# MAGIC     unique_device_id varchar(255),
# MAGIC     production_id varchar(255),
# MAGIC     quantity integer,
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     device_source_value varchar(50),
# MAGIC     device_source_concept_id integer,
# MAGIC     unit_concept_id integer,
# MAGIC     unit_source_value varchar(50),
# MAGIC     unit_source_concept_id integer,
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.drug_exposure_temp ;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.drug_exposure_temp (
# MAGIC     drug_exposure_id bigint generated always as identity NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     drug_concept_id integer NOT NULL,
# MAGIC     drug_exposure_start_date date NOT NULL,
# MAGIC     drug_exposure_start_datetime timestamp,
# MAGIC     drug_exposure_end_date date NOT NULL,
# MAGIC     drug_exposure_end_datetime timestamp,
# MAGIC     verbatim_end_date date,
# MAGIC     drug_type_concept_id integer NOT NULL,
# MAGIC     stop_reason varchar(20),
# MAGIC     refills integer,
# MAGIC     quantity numeric,
# MAGIC     days_supply integer,
# MAGIC     sig varchar(4000),
# MAGIC     route_concept_id integer,
# MAGIC     lot_number varchar(50),
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     drug_source_value varchar(50),
# MAGIC     drug_source_concept_id integer,
# MAGIC     route_source_value varchar(50),
# MAGIC     dose_unit_source_value varchar(50),
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.location_source_value;
# MAGIC
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.location_source_value (
# MAGIC     location_source_value varchar(50) NOT NULL,
# MAGIC     location_id bigint
# MAGIC );
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.measurement_temp;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.measurement_temp (
# MAGIC     measurement_id bigint generated always as identity  NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     measurement_concept_id integer NOT NULL,
# MAGIC     measurement_date date NOT NULL,
# MAGIC     measurement_datetime timestamp,
# MAGIC     measurement_time varchar(10),
# MAGIC     measurement_type_concept_id integer NOT NULL,
# MAGIC     operator_concept_id integer,
# MAGIC     value_as_number numeric,
# MAGIC     value_as_concept_id integer,
# MAGIC     unit_concept_id integer,
# MAGIC     range_low numeric,
# MAGIC     range_high numeric,
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     measurement_source_value varchar(50),
# MAGIC     measurement_source_concept_id integer,
# MAGIC     unit_source_value varchar(50),
# MAGIC     unit_source_concept_id integer,
# MAGIC     value_source_value varchar(50),
# MAGIC     measurement_event_id integer,
# MAGIC     meas_event_field_concept_id integer,
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.observation_temp;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.observation_temp (
# MAGIC     observation_id bigint generated always as identity  NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     observation_concept_id integer NOT NULL,
# MAGIC     observation_date date NOT NULL,
# MAGIC     observation_datetime timestamp,
# MAGIC     observation_type_concept_id integer NOT NULL,
# MAGIC     value_as_number numeric,
# MAGIC     value_as_string varchar(60),
# MAGIC     value_as_concept_id integer,
# MAGIC     qualifier_concept_id integer,
# MAGIC     unit_concept_id integer,
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     observation_source_value varchar(50),
# MAGIC     observation_source_concept_id integer,
# MAGIC     unit_source_value varchar(50),
# MAGIC     qualifier_source_value varchar(50),
# MAGIC     value_source_value varchar(50),
# MAGIC     observation_event_id integer,
# MAGIC     obs_event_field_concept_id integer,
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.person_source_value;
# MAGIC
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.person_source_value (
# MAGIC     person_source_value varchar(50) NOT NULL,
# MAGIC     person_id bigint
# MAGIC );
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.procedure_occurrence_temp;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.procedure_occurrence_temp (
# MAGIC     procedure_occurrence_id bigint generated always as identity  NOT NULL,
# MAGIC     person_id bigint NOT NULL,
# MAGIC     procedure_concept_id integer NOT NULL,
# MAGIC     procedure_date date NOT NULL,
# MAGIC     procedure_datetime timestamp,
# MAGIC     procedure_end_date date,
# MAGIC     procedure_end_datetime timestamp,
# MAGIC     procedure_type_concept_id integer NOT NULL,
# MAGIC     modifier_concept_id integer,
# MAGIC     quantity integer,
# MAGIC     provider_id bigint,
# MAGIC     visit_occurrence_id bigint,
# MAGIC     visit_detail_id bigint,
# MAGIC     procedure_source_value varchar(50),
# MAGIC     procedure_source_concept_id integer,
# MAGIC     modifier_source_value varchar(50),
# MAGIC     x_srcid bigint,
# MAGIC     x_srcloadid integer,
# MAGIC     x_srcfile varchar(20),
# MAGIC     x_createdate timestamp DEFAULT current_timestamp(),
# MAGIC     x_updatedate timestamp DEFAULT current_timestamp()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC drop TABLE if exists ${omop_catalog}.${omop_schema}.provider_source_value ;
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.provider_source_value (
# MAGIC     provider_source_value varchar(50) NOT NULL,
# MAGIC     provider_id bigint
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- create OMOP CDM tables
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.METADATA;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.METADATA (
# MAGIC 			metadata_id integer NOT NULL,
# MAGIC 			metadata_concept_id integer NOT NULL,
# MAGIC 			metadata_type_concept_id integer NOT NULL,
# MAGIC 			name varchar(250) NOT NULL,
# MAGIC 			value_as_string varchar(250),
# MAGIC 			value_as_concept_id integer,
# MAGIC 			value_as_number float,
# MAGIC 			metadata_date date,
# MAGIC 			metadata_timestamp timestamp );
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.CDM_SOURCE;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.CDM_SOURCE (
# MAGIC 			cdm_source_name varchar(255) NOT NULL,
# MAGIC 			cdm_source_abbreviation varchar(25) NOT NULL,
# MAGIC 			cdm_holder varchar(255) NOT NULL,
# MAGIC 			source_description varchar(4000),
# MAGIC 			source_documentation_reference varchar(255),
# MAGIC 			cdm_etl_reference varchar(255),
# MAGIC 			source_release_date date NOT NULL,
# MAGIC 			cdm_release_date date NOT NULL,
# MAGIC 			cdm_version varchar(10),
# MAGIC 			cdm_version_concept_id integer NOT NULL,
# MAGIC 			vocabulary_version varchar(20) NOT NULL );
# MAGIC
# MAGIC drop TABLE if  exists ${omop_catalog}.${omop_schema}.SOURCE_TO_CONCEPT_MAP ;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.SOURCE_TO_CONCEPT_MAP (
# MAGIC 			source_code varchar(50) NOT NULL,
# MAGIC 			source_concept_id integer NOT NULL,
# MAGIC 			source_vocabulary_id varchar(20) NOT NULL,
# MAGIC 			source_code_description varchar(255),
# MAGIC 			target_concept_id integer NOT NULL,
# MAGIC 			target_vocabulary_id varchar(20) NOT NULL,
# MAGIC 			valid_start_date date NOT NULL,
# MAGIC 			valid_end_date date NOT NULL,
# MAGIC 			invalid_reason varchar(1) );
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.COHORT;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.COHORT (
# MAGIC 			cohort_definition_id integer NOT NULL,
# MAGIC 			subject_id integer NOT NULL,
# MAGIC 			cohort_start_date date NOT NULL,
# MAGIC 			cohort_end_date date NOT NULL );
# MAGIC
# MAGIC -- drop TABLE if  exists ${omop_catalog}.${omop_schema}.COHORT_DEFINITION;
# MAGIC --HINT DISTRIBUTE ON RANDOM
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.COHORT_DEFINITION (
# MAGIC 			cohort_definition_id integer NOT NULL,
# MAGIC 			cohort_definition_name varchar(255) NOT NULL,
# MAGIC 			cohort_definition_description varchar(4000),
# MAGIC 			definition_type_concept_id integer NOT NULL,
# MAGIC 			cohort_definition_syntax varchar(4000),
# MAGIC 			subject_concept_id integer NOT NULL,
# MAGIC 			cohort_initiation_date date );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create and populate the OMOP tables and default OHDSI datasets
# MAGIC drop table if exists  ${omop_catalog}.${omop_schema}.concept;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept
# MAGIC as select concept_id,
# MAGIC concept_name,
# MAGIC domain_id,
# MAGIC vocabulary_id,
# MAGIC concept_class_id,
# MAGIC standard_concept,
# MAGIC concept_code,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason
# MAGIC  from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept;
# MAGIC
# MAGIC  /*INSERT OVERWRITE ${omop_catalog}.${omop_schema}.concept
# MAGIC  (
# MAGIC   concept_id, concept_name, domain_id, vocabulary_id,
# MAGIC   concept_class_id, standard_concept, concept_code,
# MAGIC   valid_start_date, valid_end_date, invalid_reason
# MAGIC  )
# MAGIC  select concept_id, concept_name, domain_id, vocabulary_id,
# MAGIC  concept_class_id, standard_concept, concept_code,
# MAGIC  valid_start_date, valid_end_date, invalid_reason
# MAGIC  from ${omop_catalog}.cdh_ml.concept;*/
# MAGIC
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_ancestor;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_ancestor
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_ancestor;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_class;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_class
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_class;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_relationship;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_relationship
# MAGIC as select 
# MAGIC concept_id_1,
# MAGIC concept_id_2,
# MAGIC relationship_id,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason 
# MAGIC from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_relationship;
# MAGIC
# MAGIC
# MAGIC drop table if exists  ${omop_catalog}.${omop_schema}.concept_synonym;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_synonym
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_synonym;
# MAGIC
# MAGIC
# MAGIC drop  table if exists ${omop_catalog}.${omop_schema}.domain;
# MAGIC create table ${omop_catalog}.${omop_schema}.domain
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_domain;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.drug_strength;
# MAGIC create table ${omop_catalog}.${omop_schema}.drug_strength
# MAGIC as select 
# MAGIC drug_concept_id,
# MAGIC ingredient_concept_id,
# MAGIC amount_value,
# MAGIC amount_unit_concept_id,
# MAGIC numerator_value,
# MAGIC numerator_unit_concept_id,
# MAGIC denominator_value,
# MAGIC denominator_unit_concept_id,
# MAGIC box_size,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason
# MAGIC  from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_drug_strength;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.vocabulary;
# MAGIC create table ${omop_catalog}.${omop_schema}.vocabulary
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_vocabulary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create concept source specific tables
# MAGIC
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_condition;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_condition as
# MAGIC select * from
# MAGIC (
# MAGIC   select
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last 
# MAGIC                       )  as rn
# MAGIC   from ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC       and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC       and tar.standard_concept = 'S'
# MAGIC   where src.domain_id like '%Cond%'
# MAGIC )
# MAGIC a 
# MAGIC where rn =1
# MAGIC  ;
# MAGIC
# MAGIC /*
# MAGIC create index idx_concept_condition_raw on ${omop_catalog}.${omop_schema}.concept_condition( raw_concept_code, src_vocabulary_id );
# MAGIC create index idx_concept_condition on ${omop_catalog}.${omop_schema}.concept_condition( clean_concept_code, src_vocabulary_id );
# MAGIC */
# MAGIC
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_measurement;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_measurement as
# MAGIC select * from 
# MAGIC (
# MAGIC   select
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     val.concept_id as val_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last , val.concept_id nulls last
# MAGIC                       )  as rn
# MAGIC   from ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC           and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC           and tar.standard_concept = 'S'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
# MAGIC           and crv.relationship_id = 'Maps to value'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept val on crv.concept_id_2 = val.concept_id
# MAGIC           and val.standard_concept = 'S'
# MAGIC           and val.domain_id = 'Meas Value'
# MAGIC   where src.domain_id like '%Meas%'
# MAGIC ) a
# MAGIC where rn=1
# MAGIC ;
# MAGIC
# MAGIC /*
# MAGIC create index idx_concept_meas_raw on ${omop_catalog}.${omop_schema}.concept_measurement( raw_concept_code, src_vocabulary_id );
# MAGIC create index idx_concept_meas on ${omop_catalog}.${omop_schema}.concept_measurement( clean_concept_code, src_vocabulary_id );
# MAGIC */
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_observation;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_observation as
# MAGIC select * from
# MAGIC (
# MAGIC   select
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     val.concept_id as val_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last , val.concept_id nulls last
# MAGIC                       )  as rn
# MAGIC   from ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC               and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC               and tar.standard_concept = 'S'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
# MAGIC               and crv.relationship_id = 'Maps to value'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept val on crv.concept_id_2 = val.concept_id
# MAGIC               and val.standard_concept = 'S'
# MAGIC   where src.domain_id like '%Obs%'
# MAGIC ) a
# MAGIC where rn = 1
# MAGIC ;
# MAGIC
# MAGIC /*
# MAGIC create index idx_concept_obs_raw on ${omop_catalog}.${omop_schema}.concept_observation( raw_concept_code, src_vocabulary_id );
# MAGIC create index idx_concept_obs on ${omop_catalog}.${omop_schema}.concept_observation( clean_concept_code, src_vocabulary_id );
# MAGIC */
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_procedure;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_procedure as
# MAGIC select * from 
# MAGIC (
# MAGIC   select
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last 
# MAGIC                       )  as rn
# MAGIC     from ${omop_catalog}.${omop_schema}.concept src
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC         and cr.relationship_id = 'Maps to'
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC         and tar.standard_concept = 'S'
# MAGIC     where src.domain_id like '%Proc%'
# MAGIC ) a
# MAGIC where rn=1
# MAGIC ;
# MAGIC
# MAGIC /*
# MAGIC create index idx_concept_proc_raw on ${omop_catalog}.${omop_schema}.concept_procedure( raw_concept_code, src_vocabulary_id );
# MAGIC create index idx_concept_proc on ${omop_catalog}.${omop_schema}.concept_procedure( clean_concept_code, src_vocabulary_id );
# MAGIC */
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_drug;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_drug as
# MAGIC select * from 
# MAGIC (
# MAGIC   select 
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last 
# MAGIC                       )  as rn
# MAGIC   from ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC       and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC       and tar.standard_concept = 'S'
# MAGIC   where src.domain_id like '%Drug%'
# MAGIC ) a
# MAGIC where rn = 1
# MAGIC ;
# MAGIC
# MAGIC /*
# MAGIC create index idx_concept_drug_raw on ${omop_catalog}.${omop_schema}.concept_drug( raw_concept_code, src_vocabulary_id );
# MAGIC create index idx_concept_drug on ${omop_catalog}.${omop_schema}.concept_drug( clean_concept_code, src_vocabulary_id );
# MAGIC */
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_device;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_device as
# MAGIC select * from 
# MAGIC (
# MAGIC   select
# MAGIC     src.concept_code as raw_concept_code,
# MAGIC     replace(src.concept_code, '.', '' ) as clean_concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC     src.domain_id as src_domain_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     tar.concept_id as tar_concept_id,
# MAGIC     src.valid_end_date as src_end_date,
# MAGIC     cr.valid_end_date as cr_end_date,
# MAGIC     tar.valid_end_date as tar_end_date,
# MAGIC     src.invalid_reason as src_invalid_reason,
# MAGIC     cr.invalid_reason as cr_invalid_reason,
# MAGIC     tar.invalid_reason as tar_invalid_reason,
# MAGIC     coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
# MAGIC     row_number() over ( partition by src.concept_code  , src.vocabulary_id, src.domain_id 
# MAGIC                         order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
# MAGIC                                  tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
# MAGIC                                  src.valid_end_date desc nulls last 
# MAGIC                       )  as rn
# MAGIC   from ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC   and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC   and tar.standard_concept = 'S'
# MAGIC   where src.domain_id like '%Dev%'
# MAGIC ) a
# MAGIC where rn=1
# MAGIC ;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.concept_modifier;
# MAGIC create table ${omop_catalog}.${omop_schema}.concept_modifier as
# MAGIC select distinct concept_code, vocabulary_id, src_concept_id from 
# MAGIC (
# MAGIC   select distinct
# MAGIC     src.concept_code as concept_code,
# MAGIC     replace(src.vocabulary_id, 'CPT4', 'HCPCS') as vocabulary_id,
# MAGIC     src.concept_id as src_concept_id,
# MAGIC     row_number() over ( partition by src.concept_code  
# MAGIC                         order by  src.invalid_reason  nulls first, 
# MAGIC                                  src.valid_end_date desc nulls last ,
# MAGIC                                  src.vocabulary_id desc 
# MAGIC                       )  as rn
# MAGIC     from ${omop_catalog}.${omop_schema}.concept src
# MAGIC     where src.concept_class_id like '%Modifier%'
# MAGIC ) a
# MAGIC where rn=1
# MAGIC ;
# MAGIC
# MAGIC
