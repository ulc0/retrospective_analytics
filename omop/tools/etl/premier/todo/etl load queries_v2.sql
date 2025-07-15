-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
-- MAGIC dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
-- MAGIC #mandatory parameters and names. The orchestrator will always pass these
-- MAGIC dbutils.widgets.text("etl_catalog",defaultValue="HIVE_METASTORE")
-- MAGIC dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
-- MAGIC dbutils.widgets.text("omop_catalog",defaultValue="HIVE_METASTORE")
-- MAGIC dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop_v2")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ${source_catalog}.${source_schema}
-- MAGIC ${etl_catalog}.${etl__schema}
-- MAGIC ${omop_catalog}.${omop_schema}

-- COMMAND ----------

-- create load_id
insert into ${etl_catalog}.${etl__schema}.load_info(  load_name, load_description, status )
values ('premier1_v2', 'first premier_v2 etl load', 0);

-- COMMAND ----------

select * from ${etl_catalog}.${etl__schema}.load_info;

-- COMMAND ----------

-- load person
  INSERT INTO ${etl_catalog}.${etl__schema}.stage_person
  (
    person_source_value,
    gender,
    year_of_birth,
    death_datetime,
    race,
    ethnicity,
    ethnicity_source_value,
    gender_source_value,
    race_source_value,
    load_id
  )
  select distinct
      person_source_value,
      gender, -- F = Female, M = Male, U = Unknown,
      year_of_birth, --  age in years
      null as death_datetime,
      case race
          when 'W' then 'White'
          when 'B' then 'Black'
          when 'A' then 'Asian'
          when 'O' then 'Other'
          else null
      end as race,
      case hispanic_ind
          when 'Y' then 'Hispanic'
          when 'N' then 'Not'
          else null
      end as ethnicity,
      hispanic_ind as ethnicity_source_value,
      gender as gender_source_value,
      race as race_source_value,
      1 as load_id
  from
  (
      select distinct
        person_source_value,
        first_value( case when gender is not null then gender end) over (partition by person_source_value order by admit_date desc  ) as gender,
        first_value( case when race is not null then race end) over (partition by person_source_value order by admit_date desc  ) as race,
        first_value( case when hispanic_ind is not null then hispanic_ind end) over (partition by person_source_value order by admit_date desc  ) as hispanic_ind,
        first_value( case when year_of_birth is not null then year_of_birth end) over (partition by person_source_value order by admit_date desc  )  as year_of_birth
      from 
      (
        select distinct
            el.medrec_key as person_source_value,
            el.gender as gender, -- F = Female, M = Male, U = Unknown,
            left(el.admit_date,4) - el.age as year_of_birth, 
            el.age,
            el.race,
            el.hispanic_ind,
            el.admit_date
      from ${source_catalog}.${source_schema}.patdemo el
      )
  )
  ;

-- COMMAND ----------

-- load provider
INSERT INTO ${etl_catalog}.${etl__schema}.stage_provider
(
 
    care_site_source_value,
    location_source_value,
    provider_source_value,
    load_id,
    loaded
)
SELECT distinct
    providers.prov_region || '_' || providers.prov_division AS care_site_source_value,
    providers.prov_region || '_' || providers.prov_division AS location_source_value,
    providers.prov_id AS provider_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.providers
;

-- COMMAND ----------

-- load visit for v1
/*
INSERT INTO cdh_premier_omop_etl.stage_visit
  (
    visit_source_value,
    visit_type,
  --  visit_source_type_value,
    visit_start_date,
    visit_end_date,
    person_source_value,
    -- provider_source_value,
    load_id,
    loaded
  )
select 
    a2.pat_key as visit_source_value,
    case 
      when a2.pat_type = 28
        then 'ER'
      when a2.i_o_ind = 'I'
        then 'IN'
      when a2.i_o_ind = 'O'
        then 'OUT'
      else null 
    end as visit_type,
    case 
        when same_month = 1 -- a2.disc_mon = a2.adm_mon
            then dateadd(day, coalesce(los_admit, 0) +los_count, disc_date)
        else
            dateadd(day, -1*(length_of_stay), disc_date)
    end as visit_start_date,
    least(
            case 
                when same_month = 1 -- a2.disc_mon = a2.adm_mon
                    then dateadd(day, coalesce(los_admit, 0) +los_count +length_of_stay, disc_date)
                else
                    dateadd(day, -1*(length_of_stay) +length_of_stay, disc_date)
            end,
            last_day(disc_date)  -- get last day of month
    ) as visit_end_date,
    a2.medrec_key as person_source_value, 
  -- a2.att_phy as provider_source_value,
    1 as load_id,
    0 as loaded
from
(
  select
        a1.medrec_key, a1.pat_key, a1.pat_type, a1.i_o_ind,  disc_mon_seq, disc_date, length_of_stay,
        case
            when  disc_mon=adm_mon
                then 1
            else 0
        end as same_month,  
        sum(a1.length_of_stay) over (   
                partition by medrec_key, disc_mon, adm_mon                
                order by medrec_key, disc_mon, disc_mon_seq
                rows between unbounded preceding and 1 preceding
        ) as los_admit,
        count(nullif(a1.length_of_stay,0)) over (    -- count of non-zero length stays
                partition by medrec_key, disc_mon                
                order by medrec_key, disc_mon, disc_mon_seq
                rows between unbounded preceding  and 1 preceding
            )  as los_count
    from
    (
        select distinct
            pd.medrec_key, pd.pat_key,  pd.pat_type, pd.i_o_ind, pd.disc_mon, pd.adm_mon, pd.disc_mon_seq,
            max(pb.serv_day) over (partition by pd.medrec_key, pd.pat_key, pd.pat_type, pd.i_o_ind, pd.disc_mon, pd.disc_mon_seq) as length_of_stay ,
            to_date(concat(left(pd.disc_mon, 4),right(pd.disc_mon,2), '01'),'yyyyMMdd') as disc_date
        from cdh_premier_v2.patdemo pd
        join cdh_premier_v2.patbill pb on pd.pat_key = pb.pat_key
    ) a1
) a2
order by a2.medrec_key, a2.disc_date, a2.disc_mon_seq
*/


-- COMMAND ----------

-- load visit 2

-- load visit
INSERT INTO ${etl_catalog}.${etl__schema}.stage_visit
  (
    visit_source_value,
    visit_type,
    visit_start_date,
    visit_end_date,
    person_source_value,
    load_id,
    loaded
  )
select 
    a2.pat_key as visit_source_value,
    case 
      when a2.pat_type = 28
        then 'ER'
      when a2.i_o_ind = 'I'
        then 'IN'
      when a2.i_o_ind = 'O'
        then 'OUT'
      else null 
    end as visit_type,
    admit_date as visit_start_date,
    discharge_date as visit_end_date,
    a2.medrec_key as person_source_value, 
    1 as load_id,
    0 as loaded
from ${source_catalog}.${source_schema}.patdemo a2
;




-- COMMAND ----------

-- load conditions


INSERT INTO ${etl_catalog}.${etl__schema}.stage_condition_temp
(
    condition_code_source_type,
    condition_source_value,
    condition_source_type_value,
    start_date,
    visit_source_value,
    person_source_value,
    load_id,
    loaded
)
SELECT
    case d.icd_version 
      when '10' then 'ICD10CM'
      else 'ICD9CM'
    end AS condition_code_source_type,
    replace(d.icd_code, '.','') AS condition_source_value,
    case d.icd_pri_sec 
      when 'A' then 42894222  
      when 'P' then 44786627
      when 'S' then 44786629
    end AS condition_source_type_value,
    p.admit_date AS start_date,
    d.pat_key AS visit_source_value,
    p.medrec_key AS person_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.paticd_diag d
join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
;

-- 2827366210

-- COMMAND ----------

-- MAGIC %md
-- MAGIC num_affected_rows	num_inserted_rows
-- MAGIC 2827366210	2827366210

-- COMMAND ----------

-- load procedures from icd


INSERT INTO ${etl_catalog}.${etl__schema}.stage_procedure_temp
(
    procedure_code_source_type,
    procedure_source_value,
    procedure_source_type_value,
    procedure_date,
    visit_source_value,
    person_source_value,
    load_id,
    loaded
)
SELECT
    'ICD10PCS' AS procedure_code_source_type,
    d.icd_code AS procedure_source_value,
    case d.icd_pri_sec 
      when 'P' then 44786630
      when 'S' then 44786631
    end AS procedure_source_type_value,
    d.proc_date AS procedure_date,
    d.pat_key AS visit_source_value,
    p.medrec_key AS person_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.paticd_proc d
join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC num_affected_rows	num_inserted_rows
-- MAGIC 88869168	88869168

-- COMMAND ----------

-- load procedures from cpt

INSERT INTO ${etl_catalog}.${etl__schema}.stage_procedure_temp
(
    procedure_code_source_type,
    procedure_source_value,
    procedure_source_type_value,
    code_modifier,
    procedure_date,
    visit_source_value,
    person_source_value,
    load_id,
    loaded
)
SELECT
    'HCPCS' AS procedure_code_source_type,
    d.cpt_code AS procedure_source_value,
    38000275 AS procedure_source_type_value,
    d.cpt_mod_code_1 as code_midifier,
    d.proc_date AS procedure_date,
    d.pat_key AS visit_source_value,
    p.medrec_key AS person_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.patcpt d
join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC num_affected_rows	num_inserted_rows
-- MAGIC 3243006914	3243006914

-- COMMAND ----------

-- load rx


-- COMMAND ----------

-- load general labs

INSERT INTO ${etl_catalog}.${etl__schema}.stage_lab_temp
(
    measurement_source_type,
    measurement_source_value,
    measurement_source_type_value,
    measurement_date,
    operator_source_value,
    unit_source_value,
    value_source_value,
    value_as_number,
    value_as_string,
    range_low,
    range_high,
    visit_source_value,
    person_source_value,
    load_id,
    loaded
)
SELECT
    d.lab_test_code_type  AS measurement_source_type, -- LOINC, null, or nonstandard
    d.lab_test_code AS measurement_source_value,
    '44818702' AS measurement_source_type_value,
    d.collection_datetime AS measurement_date,
    d.numeric_value_operator AS operator_source_value,
    left(d.lab_test_result_unit, 50) AS unit_source_value,
    coalesce(left(d.lab_test_result, 50), 'no result') AS value_source_value,
    d.numeric_value AS value_as_number,
    left(d.lab_test_result, 50) AS value_as_string,
    NULL AS range_low,
    NULL AS range_high,
    d.pat_key AS visit_source_value,
    p.medrec_key AS person_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.genlab d
join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC num_affected_rows	num_inserted_rows
-- MAGIC 2882175483	2882175483

-- COMMAND ----------

-- load labs from vitals

INSERT INTO ${etl_catalog}.${etl__schema}.stage_lab_temp
(

    measurement_source_type,
    measurement_source_value,
    measurement_source_type_value,
    measurement_date,
    operator_source_value,
    unit_source_value,
    value_source_value,
    value_as_number,
    value_as_string,
    range_low,
    range_high,
    visit_source_value,
    person_source_value,
    load_id,
    loaded
)
SELECT
    replace( lab_test_code_type, 'SNOMED-CT', 'SNOMED') AS measurement_source_type,
    left(d.lab_test_code, 50) AS measurement_source_value,
    '44818702' AS measurement_source_type_value,
    d.observation_datetime AS measurement_date,
    d.numeric_value_operator AS operator_source_value,
    left(d.lab_test_result_unit, 50) AS unit_source_value,
    coalesce(left(d.lab_test_result, 50), 'no result') AS value_source_value,
    d.numeric_value AS value_as_number,
    left(d.lab_test_result, 50) AS value_as_string,
    NULL AS range_low,
    NULL AS range_high,
    d.pat_key AS visit_source_value,
    p.medrec_key AS person_source_value,
    1 AS load_id,
    0 AS loaded
FROM ${source_catalog}.${source_schema}.vitals d
join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC num_affected_rows	num_inserted_rows
-- MAGIC 3121618797	3121618797
