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
# MAGIC -- Databricks notebook source
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE if not exists ${etl_catalog}.${etl_schema}.logmessage (
# MAGIC     msg_id bigint generated always as identity NOT NULL,
# MAGIC     logtime timestamp  DEFAULT current_timestamp(),
# MAGIC     process varchar(50),
# MAGIC     step varchar(200),
# MAGIC     details varchar(200)
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE if not exists ${etl_catalog}.${etl_schema}.load_info (
# MAGIC     load_id bigint generated always as identity NOT NULL,
# MAGIC     load_name varchar(100),
# MAGIC     load_description varchar(1000),
# MAGIC     status integer,
# MAGIC     create_date timestamp  DEFAULT current_timestamp()
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_care_site (
# MAGIC     id bigint generated always as identity NOT NULL,
# MAGIC     care_site_name varchar(255),
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     location_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     place_of_service_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_care_site_archive (
# MAGIC     id bigint,
# MAGIC     care_site_name varchar(255),
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     location_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     place_of_service_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC drop table if exists 	${etl_catalog}.${etl_schema}.stage_condition	;
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_condition (
# MAGIC     id bigint ,
# MAGIC     condition_code_source_type varchar(20),
# MAGIC     condition_source_value varchar(20),
# MAGIC     condition_source_type_value varchar(20),
# MAGIC     start_date timestamp ,
# MAGIC     end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_condition_archive (
# MAGIC     id bigint ,
# MAGIC     condition_code_source_type varchar(20),
# MAGIC     condition_source_value varchar(20),
# MAGIC     condition_source_type_value varchar(20),
# MAGIC     start_date timestamp ,
# MAGIC     end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_condition_error (
# MAGIC     id bigint NOT NULL,
# MAGIC     condition_code_source_type varchar(20),
# MAGIC     condition_source_value varchar(20),
# MAGIC     condition_source_type_value varchar(20),
# MAGIC     start_date timestamp ,
# MAGIC     end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_condition_temp (
# MAGIC     id bigint  generated always as identity,
# MAGIC     condition_code_source_type varchar(20),
# MAGIC     condition_source_value varchar(20),
# MAGIC     condition_source_type_value varchar(20),
# MAGIC     start_date timestamp ,
# MAGIC     end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_death (
# MAGIC     id bigint  generated always as identity,
# MAGIC     person_source_value varchar(50),
# MAGIC     dod timestamp ,
# MAGIC     death_type_source_value varchar(50),
# MAGIC     cause_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_death_archive (
# MAGIC     id bigint,
# MAGIC     person_source_value varchar(50),
# MAGIC     dod timestamp ,
# MAGIC     death_type_source_value varchar(50),
# MAGIC     cause_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_lab (
# MAGIC     id bigint,
# MAGIC     measurement_source_type varchar(20),
# MAGIC     measurement_source_value varchar(50),
# MAGIC     measurement_source_type_value varchar(20),
# MAGIC     measurement_date timestamp ,
# MAGIC     operator_source_value varchar(2),
# MAGIC     unit_source_value varchar(50),
# MAGIC     value_source_value varchar(50),
# MAGIC     value_as_number varchar(50),
# MAGIC     value_as_string varchar(50),
# MAGIC     range_low numeric,
# MAGIC     range_high numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_lab_archive (
# MAGIC     id bigint,
# MAGIC     measurement_source_type varchar(20),
# MAGIC     measurement_source_value varchar(50),
# MAGIC     measurement_source_type_value varchar(20),
# MAGIC     measurement_date timestamp ,
# MAGIC     operator_source_value varchar(2),
# MAGIC     unit_source_value varchar(50),
# MAGIC     value_source_value varchar(50),
# MAGIC     value_as_number varchar(50),
# MAGIC     value_as_string varchar(50),
# MAGIC     range_low numeric,
# MAGIC     range_high numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_lab_error (
# MAGIC     id bigint NOT NULL,
# MAGIC     measurement_source_type varchar(20),
# MAGIC     measurement_source_value varchar(50),
# MAGIC     measurement_source_type_value varchar(20),
# MAGIC     measurement_date timestamp ,
# MAGIC     operator_source_value varchar(2),
# MAGIC     unit_source_value varchar(50),
# MAGIC     value_source_value varchar(50),
# MAGIC     value_as_number varchar(50),
# MAGIC     value_as_string varchar(50),
# MAGIC     range_low numeric,
# MAGIC     range_high numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_lab_temp (
# MAGIC     id bigint  generated always as identity,
# MAGIC     measurement_source_type varchar(20),
# MAGIC     measurement_source_value varchar(50),
# MAGIC     measurement_source_type_value varchar(20),
# MAGIC     measurement_date timestamp ,
# MAGIC     operator_source_value varchar(2),
# MAGIC     unit_source_value varchar(50),
# MAGIC     value_source_value varchar(50),
# MAGIC     value_as_number varchar(50),
# MAGIC     value_as_string varchar(50),
# MAGIC     range_low numeric,
# MAGIC     range_high numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_person (
# MAGIC     id bigint,
# MAGIC     person_source_value varchar(50) NOT NULL,
# MAGIC     gender varchar(1),
# MAGIC     year_of_birth integer,
# MAGIC     month_of_birth integer,
# MAGIC     day_of_birth integer,
# MAGIC     time_of_birth integer,
# MAGIC     race varchar(50),
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     ethnicity varchar(8),
# MAGIC     ethnicity_source_value varchar(50),
# MAGIC     gender_source_value varchar(50),
# MAGIC     race_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     location_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0,
# MAGIC     death_datetime timestamp 
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_person_archive (
# MAGIC     id bigint,
# MAGIC     person_source_value varchar(50),
# MAGIC     gender varchar(1),
# MAGIC     year_of_birth integer,
# MAGIC     month_of_birth integer,
# MAGIC     day_of_birth integer,
# MAGIC     time_of_birth integer,
# MAGIC     race varchar(50),
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     ethnicity varchar(8),
# MAGIC     ethnicity_source_value varchar(50),
# MAGIC     gender_source_value varchar(50),
# MAGIC     race_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     location_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer,
# MAGIC     death_datetime timestamp 
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_procedure (
# MAGIC     id bigint NOT NULL,
# MAGIC     procedure_code_source_type varchar(20),
# MAGIC     procedure_source_value varchar(50),
# MAGIC     procedure_source_type_value varchar(20),
# MAGIC     code_modifier varchar(50),
# MAGIC     procedure_date timestamp ,
# MAGIC     quantity integer,
# MAGIC     stop_reason varchar(20),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_alowed numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_procedure_archive (
# MAGIC     id bigint,
# MAGIC     procedure_code_source_type varchar(20),
# MAGIC     procedure_source_value varchar(50),
# MAGIC     procedure_source_type_value varchar(20),
# MAGIC     code_modifier varchar(50),
# MAGIC     procedure_date timestamp ,
# MAGIC     quantity integer,
# MAGIC     stop_reason varchar(20),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_procedure_error (
# MAGIC     id bigint NOT NULL,
# MAGIC     procedure_code_source_type varchar(20),
# MAGIC     procedure_source_value varchar(50),
# MAGIC     procedure_source_type_value varchar(20),
# MAGIC     code_modifier varchar(50),
# MAGIC     procedure_date timestamp ,
# MAGIC     quantity integer,
# MAGIC     stop_reason varchar(20),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_procedure_temp (
# MAGIC     id bigint  generated always as identity,
# MAGIC     procedure_code_source_type varchar(20),
# MAGIC     procedure_source_value varchar(50),
# MAGIC     procedure_source_type_value varchar(20),
# MAGIC     code_modifier varchar(50),
# MAGIC     procedure_date timestamp ,
# MAGIC     quantity integer,
# MAGIC     stop_reason varchar(20),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC ;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_provider (
# MAGIC     id bigint  generated always as identity,
# MAGIC     provider_name varchar(50),
# MAGIC     npi varchar(20),
# MAGIC     dea varchar(20),
# MAGIC     specialty_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     location_source_value varchar(50),
# MAGIC     gender varchar(1),
# MAGIC     year_of_birth integer,
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     gender_source_value varchar(50),
# MAGIC     provider_source_value varchar(50) NOT NULL,
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_provider_archive (
# MAGIC     id bigint,
# MAGIC     provider_name varchar(50),
# MAGIC     npi varchar(20),
# MAGIC     dea varchar(20),
# MAGIC     specialty_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     location_source_value varchar(50),
# MAGIC     gender varchar(1),
# MAGIC     year_of_birth integer,
# MAGIC     address_1 varchar(50),
# MAGIC     address_2 varchar(50),
# MAGIC     city varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip varchar(9),
# MAGIC     county varchar(20),
# MAGIC     gender_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_rx (
# MAGIC     id bigint NOT NULL,
# MAGIC     drug_source_type varchar(20),
# MAGIC     drug_source_value varchar(50),
# MAGIC     drug_source_type_value varchar(50),
# MAGIC     drug_start_date timestamp ,
# MAGIC     drug_end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     refills integer,
# MAGIC     quantity numeric,
# MAGIC     days_supply integer,
# MAGIC     dose_unit_source_value varchar(50),
# MAGIC     effective_drug_dose numeric,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     paid_ingredient_cost numeric,
# MAGIC     pait_dispensing_fee numeric,
# MAGIC     route_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_rx_archive (
# MAGIC     id bigint,
# MAGIC     drug_source_type varchar(20),
# MAGIC     drug_source_value varchar(50),
# MAGIC     drug_source_type_value varchar(50),
# MAGIC     drug_start_date timestamp ,
# MAGIC     drug_end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     refills integer,
# MAGIC     quantity numeric,
# MAGIC     days_supply integer,
# MAGIC     dose_unit_source_value varchar(50),
# MAGIC     effective_drug_dose numeric,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     paid_ingredient_cost numeric,
# MAGIC     pait_dispensing_fee numeric,
# MAGIC     route_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_rx_error (
# MAGIC     id bigint NOT NULL,
# MAGIC     drug_source_type varchar(20),
# MAGIC     drug_source_value varchar(50),
# MAGIC     drug_source_type_value varchar(50),
# MAGIC     drug_start_date timestamp ,
# MAGIC     drug_end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     refills integer,
# MAGIC     quantity numeric,
# MAGIC     days_supply integer,
# MAGIC     dose_unit_source_value varchar(50),
# MAGIC     effective_drug_dose numeric,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     paid_ingredient_cost numeric,
# MAGIC     pait_dispensing_fee numeric,
# MAGIC     route_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_rx_temp (
# MAGIC     id bigint  generated always as identity,
# MAGIC     drug_source_type varchar(20),
# MAGIC     drug_source_value varchar(50),
# MAGIC     drug_source_type_value varchar(50),
# MAGIC     drug_start_date timestamp ,
# MAGIC     drug_end_date timestamp ,
# MAGIC     stop_reason varchar(20),
# MAGIC     refills integer,
# MAGIC     quantity numeric,
# MAGIC     days_supply integer,
# MAGIC     dose_unit_source_value varchar(50),
# MAGIC     effective_drug_dose numeric,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     paid_ingredient_cost numeric,
# MAGIC     pait_dispensing_fee numeric,
# MAGIC     route_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_detail_source_value varchar(50),
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_visit (
# MAGIC     id bigint   generated always as identity,
# MAGIC     visit_source_value varchar(50) NOT NULL,
# MAGIC     visit_type varchar(8),
# MAGIC     visit_source_type_value varchar(5),
# MAGIC     visit_start_date timestamp ,
# MAGIC     visit_end_date timestamp ,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_visit_archive (
# MAGIC     id bigint,
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_type varchar(8),
# MAGIC     visit_source_type_value varchar(5),
# MAGIC     visit_start_date timestamp ,
# MAGIC     visit_end_date timestamp ,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_visit_detail (
# MAGIC     id bigint  generated always as identity, --sequence
# MAGIC     visit_detail_source_value varchar(50) NOT NULL, -- visit detail id source value
# MAGIC     visit_detail_concept_source_value varchar(50), -- visit_detail_concept_id
# MAGIC     visit_detail_start_datetime timestamp ,
# MAGIC     visit_detail_end_datetime timestamp ,
# MAGIC     visit_detail_type_source_value  varchar(50), --visit_detail_type_concept_id
# MAGIC     admitted_from_source_value varchar(50),
# MAGIC     discharged_to_source_value varchar(50),
# MAGIC     preceding_visit_detail_source_value varchar(50),
# MAGIC     parent_visit_detail_source_value varchar(50),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_visit_detail_archive (
# MAGIC     id bigint NOT NULL, --sequence
# MAGIC     visit_detail_source_value varchar(50) NOT NULL, -- visit detail id source value
# MAGIC     visit_detail_concept_source_value varchar(50), -- visit_detail_concept_id
# MAGIC     visit_detail_start_datetime timestamp ,
# MAGIC     visit_detail_end_datetime timestamp ,
# MAGIC     visit_detail_type_source_value  varchar(50), --visit_detail_type_concept_id
# MAGIC     admitted_from_source_value varchar(50),
# MAGIC     discharged_to_source_value varchar(50),
# MAGIC     preceding_visit_detail_source_value varchar(50),
# MAGIC     parent_visit_detail_source_value varchar(50),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.stage_visit_detail_error (
# MAGIC     id bigint NOT NULL, --sequence
# MAGIC     visit_detail_source_value varchar(50) NOT NULL, -- visit detail id source value
# MAGIC     visit_detail_concept_source_value varchar(50), -- visit_detail_concept_id
# MAGIC     visit_detail_start_datetime timestamp ,
# MAGIC     visit_detail_end_datetime timestamp ,
# MAGIC     visit_detail_type_source_value  varchar(50), --visit_detail_type_concept_id
# MAGIC     admitted_from_source_value varchar(50),
# MAGIC     discharged_to_source_value varchar(50),
# MAGIC     preceding_visit_detail_source_value varchar(50),
# MAGIC     parent_visit_detail_source_value varchar(50),
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_by_patient numeric,
# MAGIC     paid_patient_copay numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(50),
# MAGIC     provider_source_value varchar(50),
# MAGIC     care_site_source_value varchar(50),
# MAGIC     visit_source_value varchar(50),
# MAGIC     load_id integer,
# MAGIC     loaded integer DEFAULT 0
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.temp_stage_person (
# MAGIC     person_source_value varchar(40),
# MAGIC     gender varchar(50),
# MAGIC     year_of_birth integer,
# MAGIC     month_of_birth integer,
# MAGIC     day_of_birth integer,
# MAGIC     race varchar(50),
# MAGIC     state varchar(2),
# MAGIC     zip numeric,
# MAGIC     county varchar(50),
# MAGIC     ethnicity varchar(50),
# MAGIC     ethnicity_source_value varchar(50),
# MAGIC     gender_source_value varchar(50),
# MAGIC     race_source_value varchar(50),
# MAGIC     location_source_value varchar(50),
# MAGIC     load_id integer
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE or REPLACE TABLE ${etl_catalog}.${etl_schema}.temp_stage_visit (
# MAGIC     visit_source_value varchar(50),
# MAGIC     visit_type varchar(50),
# MAGIC     visit_source_type_value varchar(50),
# MAGIC     visit_start_date date,
# MAGIC     visit_end_date date,
# MAGIC     total_charge numeric,
# MAGIC     total_cost numeric,
# MAGIC     total_paid numeric,
# MAGIC     paid_by_payer numeric,
# MAGIC     paid_patient_coinsurance numeric,
# MAGIC     paid_patient_deductible numeric,
# MAGIC     paid_by_primary numeric,
# MAGIC     amount_allowed numeric,
# MAGIC     person_source_value varchar(40),
# MAGIC     provider_source_value varchar(12),
# MAGIC     load_id integer
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


