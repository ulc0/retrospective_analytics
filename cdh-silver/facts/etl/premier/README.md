# Premier Times Series Feature Store

Using [Standardized Vocabularies](https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html) and custom CDH vocabularies e.g. edav_prd_cdh.cdh_PREMIER.CHGMSTR

For all healthdata 

#### Data Challenges with Premier

* Premier Fact Tables do *NOT* contain 


#### Patient Fact Table  

**OMOP Mappings**  

Note: _source_type_value --> _type_concept_id  


patcpt:
  - 38000275 AS procedure_source_type_value,  

paticd_diag:  
  -    case d.icd_pri_sec 
  -    when 'A' then 42894222   
      when 'P' then 44786627  
      when 'S' then 44786629  
    end AS condition_source_type_value,  
#TODO Change FACTS vocabulary  
    case d.icd_version   
      when '10' then 'ICD10CM'  
      else 'ICD9CM'  
    end AS condition_code_source_type,  

paticd_proc:  
    case d.icd_pri_sec   
      when 'P' then 44786630  
      when 'S' then 44786631  

genlab:  
    '44818702' AS measurement_source_type_value,  

vitals:  
    '44818702' AS measurement_source_type_value,      

```mermaid
flowchart LR
subgraph Facts
    ICD
    CPT
    SNOMED
    LOINC
    NDC
    CUSTOM_VOCABULARY
end
subgraph Premier_Fact
genlab
paticd_diag
paticd_proc
patcpt
vitals
labsense
patbill
end
subgraph omop
  stage_lab_temp
  stage_procedure_temp
  stage_condition_temp
  visit_temp
  observation
end
genlab-->stage_lab_temp
paticd_diag-->stage_condition_temp
paticd_proc-->stage_procedure_temp
patcpt-->stage_procedure_temp
vitals-->stage_lab_temp
stage_lab_temp-->visit_temp
stage_condition_temp-->visit_temp
stage_procedure_temp-->visit_temp
visit_temp-->observation
paticd_diag-->ICD
paticd_proc-->ICD
patcpt-->CPT
genlab-->LOINC
labsense-->LOINC
vitals-->LOINC
genlab-->SNOMED
patbill-->|std_chg_dept==300|lab
patbill-->|std_chg_dept==250|pharmacy
pharmacy-->NDC
lab-->LOINC
patbill-->|std_chg_dept not 250,300|genbill
genbill-->CUSTOM_VOCABULARY
patbill-->hospchg
hospchg-->CUSTOM_VOCABULARY

```

For Premier:
  - PATBILL+CHGMSTR with custom vocabulary_id's
  - ICDDIAG, ICDPROC  (ICD* vocabulary)
  - GENLAB, LAB_SENS (SNOMED vocabulary)
  - GENLAB, LAB_SENS  (LOINC vocabulary)
  - CPT (CPT* vocabulary)
  - Demographics (TBD)
  - Lab/Vitals results (TBD)

Feature Table:
  - code
  - vocabulary_id
  - FEATURE

Cohort Table/List **defined by Principal Investigator*
  - PATIENT_ID

FEATURE_STORE is inner join of Cohort, Feature, and Patient Fact tables.

#### OMOP to FACT
```mermaid
graph LR
    subgraph vocabularies TD
        HCPCS
        ICD
        LOINC
        SNOMED
        STDRX
        STDLAB
        STDCHG
    end
    subgraph Premier TD
      patcpt
      paticd_proc
      genlab
      paticd_diag
      labsens
      vitals
      patbill
    end
    subgraph OMOP TD
      measurements
      observations
      conditions
      procedures
     end
 
    patcpt-->HCPCS
    patcpt-->procedures
    paticd_diag-->ICD
    paticd_diag-->conditions
    paticd_proc-->ICD
    paticd_proc-->procedures
    genlab-->LOINC
    genlab-->measurements
    vitals-->SNOMED
    vitals-->measurements
    labsens-->LOINC
    labsens-->SNOMED
    patbill-->STDCHG
    patbill-->STDLAB
    patbill-->STDRX


  
```

```mermaid
graph LR
    subgraph HCPCS
      patcpt
    end
       subgraph ICD
        paticd_diag
        paticd_proc
       end 
        subgraph LOINC
        genlab
        end
        subgraph SNOMED
          vitals
        end
        subgraph stdchg_codes
            STDRX
         STDLAB
        STDCHG
        
    end
    subgraph Premier TD
      patcpt
      paticd_proc
      genlab
      paticd_diag
      labsens
      vitals
      patbill
    end
    subgraph OMOP TD
      measurements
      observations
      conditions
      procedures
     end
 
    patcpt-->HCPCS
    patcpt-->procedures
    paticd_diag-->ICD
    paticd_diag-->conditions
    paticd_proc-->ICD
    paticd_proc-->procedures
    genlab-->LOINC
    genlab-->measurements
    vitals-->SNOMED
    vitals-->measurements
    labsens-->LOINC
    labsens-->SNOMED
    patbill-->STDCHG
    patbill-->STDLAB
    patbill-->STDRX

```
  