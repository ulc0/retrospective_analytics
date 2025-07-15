### Tables read from premier

* common\intermediate\5_write_{domain}.py
* [Athena links](https://athena.ohdsi.org/search-terms/terms/38000275)

```mermaid
graph 
   subgraph patdemo
    patkey
    medrec_key
   end

   subgraph occurrence
    i_o_ind
    point_of_origin
    admit_date
    discharge_date
    disc_status
    msdrg
    ms_drg_mdc
    adm_phy
    admphy_spec
    prov_id
   end   

   subgraph demographics
    age
    gender
    race
    mart_status
    hispanic_ind
   end

```
   
```python
    case pat.gender
        when 'M' then 8507
        when 'F' then 8532
        when 'A' then 8570
        when 'U' then 8551
        when 'O' then 8521
        when null then null
        else 8551
    end as gender_concept_id,
```

```python
# visit_type
      when a2.pat_type = 28 then 'ER' when 'ER' then 9203
      when a2.i_o_ind = 'I' then 'IN' when 'IN' then 9201
      when a2.i_o_ind = 'O' then 'OUT' when 'OUT' then 9202
      else null  when 'LONGTERM' then 42898160
```
```python
# race
        when 'W' then 'White'
        when 'B' then 'Black'
        when 'A' then 'Asian'
        when 'O' then 'Other'
```

=======
```mermaid
graph LR
  subgraph patdemo
    pat_key-->stage_visit_source_value
    pat_type-->stage_visit_type
    i_o_ind-->stage_visit_type
    admit_date-->stage_visit_start_date
    discharge_date-->stage_visit_end_date
    medrec_key-->stage_person_source_value
    gender-->stage_person_gender_source_value
    year_of_birth-->stage_person_year_of_birth
    race-->stage_person_race
    hispanic_ind-->stage_person_ethnicity_source_value
    1-->stage_load_id
    0-->stage_loaded 
  end
  subgraph stage_visit
    stage_visit_source_value
    stage_visit_type
    stage_visit_start_date
    stage_visit_end_date
    stage_person_source_value
    stage_load_id
    stage_loaded
  end
  subgraph stage_person
    stage_person_source_value
    stage_gender
  end
  subgraph visit_occurrence
    visit_occurrence_id
    visit_concept_id
    visit_source_value
    visit_source_concept_id
    admitted_from_concept_id
    discharged_to_concept_id
    visit_type_concept_id
    visit_start_date
    visit_start_timestamp
    visit_end_date
    visit_end_timestamp
    care_site_id
    person_id
    provider_id
  end
```

```mermaid
graph LR
    subgraph Premier
        patcpt
        paticd_proc
        paticd_diag
        genlab
        vitals
        labsens
        patbill    
    end
    subgraph OMOP_Domains_Stage
        stage_procedures_temp
        stage_lab_temp
        stage_condition_temp
    end
    subgraph Procedure_Domain
        procedure_occurrence_temp
    end
    subgraph Measurement_Domain
        measurement_temp
    end
    subgraph Observation_Domain
        observation_temp
    end
    subgraph Condition_Domain
        condition_occurrence_temp
    end

    patcpt--HCPCS 38000275-->stage_procedures_temp 
    paticd_proc--ICDxPCS P 44786630 S 44786631-->stage_procedures_temp 
     
    genlab--LOINC 44818702-->stage_lab_temp  
    vitals--LOINC/SNOMED 44818702-->stage_lab_temp

    paticd_diag--ICDxCM A 42894222 P 44786627 S 44786629 -->stage_condition_temp

    stage_lab_temp--athena-->condition_occurrence_temp 
    stage_condition_temp--athena-->condition_occurrence_temp

    stage_lab_temp--athena-->measurement_temp
    stage_condition_temp--athena-->measurement_temp
    stage_procedures_temp--athena-->measurement_temp
    
    stage_lab_temp--athena-->procedure_occurrence_temp
    stage_procedures_temp--athena-->procedure_occurrence_temp

    stage_condition_temp--athena-->observation_temp
    stage_lab_temp--athena-->observation_temp
```

```mermaid
graph LR
    subgraph Premier
        patcpt
        paticd_proc
        paticd_diag
        genlab
        vitals
        labsens
        patbill    
    end
    subgraph OMOP_Domains_Stage
        stage_procedures_temp
        stage_lab_temp
        stage_condition_temp
    end
    subgraph Procedure_Domain
        procedure_occurrence_temp
    end
    subgraph Measurement_Domain
        measurement_temp
    end
    subgraph Observation_Domain
        observation_temp
    end
    subgraph Condition_Domain
        condition_occurrence_temp
    end

    patcpt--HCPCS 38000275 ORder Entry List-->procedure_occurrence_temp 
    paticd_proc--ICDxPCS P 44786630 primary procedure S 44786631 secondary procedure -->procedure_occurrence_temp 
     
    genlab--LOINC 44818702 Lab Result -->procedure_occurrence_temp  
    vitals--LOINC/SNOMED 44818702 Lab Result-->procedure_occurrence_temp
    genlab--LOINC 44818702 Lab Result -->condition_occurrence_temp  
    vitals--LOINC/SNOMED 44818702 Lab Result-->condition_occurrence_temp
    paticd_diag--ICDxCM A 42894222 Chielf Complaint P 44786627 Primary Condition S 44786629 secondary condition-->condition_occurrence_temp
    
    genlab--LOINC-->measurement_temp  
    vitals--LOINC/SNOMED-->measurement_temp
    paticd_diag--ICDxCM A 42894222  P44786627 S 44786629-->measurement_temp

    genlab--LOINC-->observation_temp  
    vitals--LOINC/SNOMED-->observation_temp
    paticd_diag--ICDxCM A 42894222 P 44786627 S 44786629-->observation_temp
    
    
    
```


* premier\todo\4b3_create_labs_meas
* premier\todo\4c3_create_procedures_meas

#### Current ODHSI

* Conditions
    - paticd_diag
* Labs
    - genlab
* Vitals
    - vitals
* Procedures
    - paticd_proc
* Visits
    - patdemo



#### Current CDH Featurization by Vocabulary

* CPT/HCPCS
    - patcpt
* ICD
    - paticd_diag
    - paticd_proc
* LOINC
    - genlab
    - **lab_sens**
    - vitals
* SNOMED
    - genlab
    - **labres**
* STDCODE
    - **pharmacy**
    - lab
    - allother
* HOSPCHG
    - **pharmacy**
    - lab
    - all other    

