-- load labs from vitals

INSERT INTO cdh_premier_omop_etl.stage_lab_temp
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
FROM cdh_premier_v2.vitals d
join cdh_premier_v2.patdemo p on d.pat_key = p.pat_key
;
