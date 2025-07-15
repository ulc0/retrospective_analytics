-- load conditions
INSERT INTO cdh_premier_omop_etl.stage_condition_temp
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
FROM cdh_premier_v2.paticd_diag d
join cdh_premier_v2.patdemo p on d.pat_key = p.pat_key
;
