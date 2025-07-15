-- load procedures from icd


INSERT INTO cdh_premier_omop_etl.stage_procedure_temp
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
FROM cdh_premier_v2.paticd_proc d
join cdh_premier_v2.patdemo p on d.pat_key = p.pat_key
;
