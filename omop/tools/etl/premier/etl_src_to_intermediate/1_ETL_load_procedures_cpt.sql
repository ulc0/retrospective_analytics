-- load procedures from cpt

INSERT INTO cdh_premier_omop_etl.stage_procedure_temp
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
FROM cdh_premier_v2.patcpt d
join cdh_premier_v2.patdemo p on d.pat_key = p.pat_key
;
