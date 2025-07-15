
###  Premier Bronze Tables

* **Encounters/Demographics** *patdemo* [person_id=medrec_key, encounter_id=patkey]
    * Encounter details
        - I_O_IND,PAT_TYPE
        - Dates DISCHARGE_DATE-ADMIT_DATE=LOS
            - ADMIT_DATE
            - DISCHARGE_DATE
            - LOS
        - ADM_TYPE
        - MS_DRG,MS_DRG_MDC
        - POINT_OF_ORIGIN
        - DISC_STATUS
    * Demographics
        - MART_STATUS
        - AGE
        - GENDER
        - RACE
        - HISPANIC_IND
    * Provider/Payor
        - PROV_ID
        - ADMPHY_SPEC
        - ADM_PHY
        - ATTPHY_SPEC
        - ATT_PHY
        - STD_PAYOR
        - PAT_CHARGES
        - PAT_COST
        - PAT_FIX_COST
       - PAT_VAR_COST

* **HCPCS Procedure Table** *patcpt* [encounter_id=patkey, encounter_date= PROC_DATE]
    * CPT_CODE - HCPCS Concept Code
    * NOT USED
        - CPT_POS - Place of Service
        - CPT_ORDER_PHY
        - CPT_ORDER_PHY_SPEC
        - CPT_PROC_PHY
        - CPT_PROC_PHY_SPEC
        - CPT_MOD_CODE_1
        - CPT_MOD_CODE_2
        - CPT_MOD_CODE_3
        - CPT_MOD_CODE_4

* **ICD9/10 Procedure Table** *paticd_proc* [encounter_id=patkey, encounter_date=PROC_DATE]
    * ICD_CODE - ICD Concept COde
    * ICD_VERSION - ICD Vocabulary Modifier
        - NOT USED
            - ICD_PRI_SEC - Secondary Code
            - PROC_PHY
            - PROCPHY_SPEC

* **ICD10 Diagnosis Table** *paticd_diag* [encounter_id=patkey]
    * ICD_CODE - ICD Concept COde
    * ICD_VERSION - ICD Vocabulary Modifier
    * ICD_POA - Present on Admission, determines whether date is admit date or discharge date
        - NOT USED
            - ICD_PRI_SEC - Secondary Code

* **Billing Records Table** *patbill* [encounter_id=patkey, encounter_date=serv_date]
    * STD_CHG_CODE - Custom Vocabulary Concept Code, maps to STDCHGCODE Table, StdChgDept='250'=Drugs, '300'=Lab, then Others
    * HOSP_CHG_ID - Custom Vocabulary Concept Code, maps to CHGMSTR
    - Not Used
        - HOSP_QTY
        - STD_QTY
        - BILL_CHARGES
        - BILL_COST
        - BILL_VAR_COST
        - BILL_FIX_COST

* **LOINC Tables** 
- genlab, lab_sens, vitals

* **SNOMED Table**
- genlab, labres


### Tables read from premier

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



```

#### Source Specific code
subdirectories for source specific pre-processing under *etl* and *eda*



