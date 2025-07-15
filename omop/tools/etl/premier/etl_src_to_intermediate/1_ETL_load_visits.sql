
-- load visit
INSERT INTO cdh_premier_omop_etl.stage_visit
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
from cdh_premier_v2.patdemo a2
;
