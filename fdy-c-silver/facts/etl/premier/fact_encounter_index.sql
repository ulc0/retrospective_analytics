-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("DBCATALOG","edav_prd_cdh")
-- MAGIC dbutils.widgets.text("SRC_SCHEMA","cdh_premier_v2")
-- MAGIC dbutils.widgets.text("DEST_SCHEMA","cdh_premier_exploratory")

-- COMMAND ----------

-- DBTITLE 1,Create Encounter fact Table By `patient_id`
CREATE OR REPLACE TABLE edav_prd_cdh.cdh_premier_ra.fact_encounter_index AS
  SELECT CAST(pat_key as LONG)  as observation_period_number,
  medrec_key as person_id,
  pat_type as visit_type_code,
  point_of_origin,
  disc_status,
  --admit_date as observation_period_datetime,
  discharge_date as observation_period_end_date,
  LOS as length_of_stay,
  date_add(ADMIT_DATE,LOS) as discharge_datetime,
  ADMIT_DATE as observation_datetime,
  datediff(discharge_date,admit_date) as days_of_visit,
  (i_o_ind=="I") as is_inpatient_visit,
    case 
    when pat_type = 28
      then 'ER'
    when i_o_ind = 'I'
      then 'IN'
    when i_o_ind = 'O'
      then 'OUT'
    else null 
  end as visit_type,
  POINT_OF_ORIGIN as admitted_from_concept_code,
  DISC_STATUS as discharged_to_concept_code,
  ms_drg, 
  MART_STATUS as marital_status,
  gender,
  CASE WHEN HISPANIC_IND == "Y" THEN "Y" ELSE "N" END as is_hispanic,
  race,
  case
    when hispanic_ind = "U" then "Unknown"
    when hispanic_ind = "Y" then "Hispanic"
    when race = 'B' and hispanic_ind = 'N' then "NH Black"
    when race = "W" and hispanic_ind = "N" then "NH White"
    when race = "A" and hispanic_ind = "N" then "NH Asian"
    when race = "O" and hispanic_ind = "N" then "Other"
    when race = "U" and hispanic_ind = "N" then "Unknown"
  end as race_ethnicity,
  CAST(age as String) as age_at_admission,
  lpad(CAST(pat_type as String),2,'0') as patient_type_code,
  lpad(CAST(point_of_origin as String),2,'0') as admitted_from_code,
  lpad(CAST(DISC_STATUS as String),2,'0') as discharged_to_code,
  lpad(CAST(ADM_TYPE as String),2,'0') as admission_type,
  lpad(CAST(PROV_ID as String),3,'0') as provider_id,   
  --p.URBAN_RURAL as prov_urban_rural,
  --p.TEACHING as prov_teaching,
  --p.BEDS_GRP as prov_beds,
  --p.PROV_REGION as prov_region,
  --p.PROV_DIVISION as prov_division,
  --p.COST_TYPE as prov_cost_type,
  CAST(STD_PAYOR AS String) as payor_code,
 (CAST(year(admit_date) as INTEGER) - CAST(age as INTEGER))  as year_of_birth,
DENSE_RANK() OVER(PARTITION BY MEDREC_KEY ORDER BY pat_key, admit_date) AS observation_period_index,
DENSE_RANK() OVER(PARTITION BY MEDREC_KEY,I_O_IND ORDER BY pat_key, admit_date) AS hospital_time_index
from edav_prd_cdh.cdh_premier_v2.patdemo 
-- FUTURE for fact_providers
--join $${DBCATALOG}.${SCHEMA}.providers p
--where medrec_key in (select DISTINCT person_id from $${DBCATALOG}.$${DEST_SCHEMA}.cohort_list )
order by person_id,observation_datetime,observation_period_number;
