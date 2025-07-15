-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Truveta Start Guide
-- MAGIC <br>
-- MAGIC <b>Primary analysis</b><br>
-- MAGIC Become familiar with Truveta data through examples using SQL<br>
-- MAGIC
-- MAGIC <b>Primary objective in this notebook</b><br>
-- MAGIC - [Part 1](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615281?o=5189663741904016): How can we find patients who had COVID-19 using ICD10 = 'U07.1' 
-- MAGIC - <b>Part 2: Number of patients hospitalized with covid by year and mean paid amount </b>
-- MAGIC - [Part 3](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615479?o=5189663741904016): Patients with A1C1 test > 6.5 who take metformin. Stratify by education and income levels
-- MAGIC
-- MAGIC <b> Contact information</b><br>
-- MAGIC Data Hub Support [datahubsupport@cdc.gov](datahubsupport@cdc.gov) <!--- enter email address in both the square brackets and the parens --> 

-- COMMAND ----------

-- By default, notebooks will point to the most recent data release. If you want to specify a version use'@v' 
-- E.g., cdh_truveta.concept@v0
DESCRIBE HISTORY cdh_truveta.concept

-- COMMAND ----------

-- MAGIC %md # 2. Number of patients hospitalized with covid by year and mean paid amount
-- MAGIC
-- MAGIC You can find hospitalization counts in two places: 
-- MAGIC
-- MAGIC 1. EHR Data, this is our most reliable data with best coverage. 
-- MAGIC 2. Claims data, this is supplementary data and has cost information associated with visit information. 

-- COMMAND ----------

-- MAGIC %md ### 2a. Finding COVID hospitalizations in EHR data by year

-- COMMAND ----------

-- MAGIC %md #### Inpatient encounter codes
-- MAGIC
-- MAGIC Truveta provides two definitions for inpatient encounters. 
-- MAGIC
-- MAGIC The <b> loose</b> definition increases sensitivity. If interested in events occuring during hospitalization, look for procedures that occur on any of these encounters. Use the codes defined by <br>
-- MAGIC - loose_inpatient_class_codes
-- MAGIC - loose_inpatient_clinical_type_codes
-- MAGIC <br>
-- MAGIC
-- MAGIC The example below will use the <b> strict</b> definition that filters primarily for parent inpatient encounters. Truveta recommends using this definition but it is left to the analyst to modify the definition depending on each analysis. Using this definition may result in incomplete linkages to events if using direct linkage to events by EncounterId instead of using a datetime linkage. If interested in events occuring during hospitalization, join to the procedure table looking for procedures in general occurring between a strict inpatient encounter StartDateTime and EndDateTime. Use the codes defined by <br>
-- MAGIC - strict_inpatient_class_codes
-- MAGIC - strict_inpatient_clinical_type_codes

-- COMMAND ----------

-- DBTITLE 1,Loose definition encounter codes
CREATE OR REPLACE TEMP VIEW loose_inpatient_class_codes AS
SELECT *
FROM cdh_truveta.concept
WHERE ConceptCode IN 
("1065297", "1065603", "1065302", "1065215", "1065223", "1065220", "1065223", "1065304", "1065305", "1065306", "1065307");

CREATE OR REPLACE TEMP VIEW loose_inpatient_clinical_type_codes AS
SELECT *
FROM cdh_truveta.concept
WHERE ConceptCode IN 
("1065302", "1065215", "1065223", "1065220", "1065223", "1065304", "1065305", "1065306", "1065307")

-- COMMAND ----------

-- DBTITLE 1,Strict definition encounter codes (recommended)
CREATE OR REPLACE TEMP VIEW strict_inpatient_class_codes AS
SELECT *
FROM cdh_truveta.concept
WHERE ConceptCode IN 
("1065220",
"1065215",
"1065223");

CREATE OR REPLACE TEMP VIEW strict_inpatient_clinical_type_codes AS
SELECT *
FROM cdh_truveta.concept
WHERE ConceptCode IN 
("3059272",
"1065297")

-- COMMAND ----------

SELECT *
FROM strict_inpatient_class_codes

-- COMMAND ----------

SELECT *
FROM strict_inpatient_clinical_type_codes

-- COMMAND ----------

-- MAGIC %md #### Find Inpatient Encounters using PersonIds from the covid_conditions table. 
-- MAGIC We're reusing the covid_conditions dataframe from Q1 which contains COVID-19 conditions

-- COMMAND ----------

--view COVID encounters associated with both strict inpatient class codes and clinical type codes.
--we do this by performing a join (inner by default) between the encounter table and our covid encounter table from Start Guide Part 1. 
--Keep in mind, the current recommendation is to avoid joining by EncounterID, as some COVID encounters may be missed in this approach.
CREATE OR REPLACE TEMP VIEW covid_encounter AS
SELECT e.*
FROM cdh_truveta.Encounter e 
JOIN cdh_truveta_exploratory.QSGcovid_condition c ON e.PersonId = c.PersonId
WHERE e.ClassConceptId IN (SELECT ConceptId FROM strict_inpatient_class_codes)
AND e.TypeConceptId IN (SELECT ConceptId FROM strict_inpatient_clinical_type_codes)


-- COMMAND ----------

SELECT COUNT(*) FROM covid_encounter

-- COMMAND ----------

-- DBTITLE 1,Preview the COVID Encounter table

SELECT *
FROM covid_encounter
LIMIT 10

-- COMMAND ----------

-- MAGIC %md #### Calculating absolute time
-- MAGIC Load the output table 'SearchResult_dob'.  You can see the rows in this table include a few key columns:
-- MAGIC   - absolute_year_month_dob:  absolute year month of DOB
-- MAGIC   - shifted_dob: shifted DOB
-- MAGIC   - PersonId: Patient Identifier 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW absolute_time_index AS 
SELECT PersonId, absolute_year_month_dob, shifted_dob
FROM cdh_truveta.SearchResult_dob

-- COMMAND ----------

SELECT * 
FROM absolute_time_index
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ### 2b.  Finding COVID hospitalizations in claims and calculating mean paid amount per year
-- MAGIC
-- MAGIC The workflow is similar to the EHR data, first create your codeset table. 
-- MAGIC Use codes to find COVID claims. <br>
-- MAGIC There are two ways you can go about this. 
-- MAGIC
-- MAGIC I. Find claims where COVID is the Primary Diagnosis. 
-- MAGIC II. Find any claim that is associated with a COVID diagnosis. 

-- COMMAND ----------

-- MAGIC %md #### I.  Find claims where COVID is the Primary Diagnosis

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW covid_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("U07.1")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW covid_claims_pd AS
SELECT *   
FROM cdh_truveta.claim
WHERE claim.PrimaryDiagnosisCodeConceptId
    IN (
        SELECT ConceptId 
        FROM covid_codes
        )

-- COMMAND ----------

SELECT *
FROM covid_claims_pd
LIMIT 5

-- COMMAND ----------

SELECT COUNT(*) AS row_count
FROM covid_claims_pd

-- COMMAND ----------

-- MAGIC %md #### 2. Find claims where COVID is associated with the claim
-- MAGIC
-- MAGIC This approach is a little more complex.  You must use the ClaimLineCodes table as the look-up table for ClaimLine rows of data, and then use the ClaimId in the ClaimLine table to find the rows of data in the claims table. 
-- MAGIC
-- MAGIC Below is the first step, finding the ClaimLine rows using the ClaimLineCodes table. 

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW covid_claim_lines AS
SELECT claimline.*, ClaimLineId, CodeConceptId, CategoryConceptId
FROM cdh_truveta.claimline
JOIN cdh_truveta.claimlinecodes ON claimline.Id = claimlinecodes.ClaimLineId
WHERE CodeConceptId IN (
         SELECT ConceptId 
         FROM covid_codes
     )

-- COMMAND ----------

SELECT *
FROM covid_claim_lines
LIMIT 5

-- COMMAND ----------

SELECT COUNT(*) AS row_count
FROM covid_claim_lines

-- COMMAND ----------

-- MAGIC %md ###### Now you can use this table's ClaimId as a lookup for the claim table
-- MAGIC Notice that you can include the SettingOfCareConceptid from the ClaimLine table as a variable as well.  

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW covid_claims_all AS
SELECT claim.*, cc.SettingOfCareConceptId  
FROM cdh_truveta.claim claim
JOIN covid_claim_lines cc ON Claim.Id = cc.ClaimId

-- COMMAND ----------

SELECT *
FROM covid_claims_all
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ##### This code looks at the breakdown of SettingOfCareConceptId for the dataframe
-- MAGIC You can see they are all NULL for the covid claims, though when we look at the larger table there are many values present.

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM covid_claims_all cc
JOIN cdh_truveta.concept c ON cc.SettingOfCareConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM cdh_truveta.ClaimLine cc
JOIN cdh_truveta.concept c ON cc.SettingOfCareConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

SELECT COUNT(*) AS row_count
FROM covid_claims_all

-- COMMAND ----------

-- MAGIC %md ##### Now you can filter to those rows that were inpatient visits associated with any COVID claim. 
-- MAGIC
-- MAGIC You can find these codes here: https://learn.truveta.com/studio/docs/tcodes#settingofcare 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW inpatient_claim_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("3058547", "3058561", "3058577", "3058578", "3058581", "3058582", "3058580", "3058583", "3058585", "3058584", "3058605", "3058586", "3058587")

-- COMMAND ----------

SELECT * 
FROM inpatient_claim_codes

-- COMMAND ----------

-- MAGIC %md ##### You can use this list of codes to find the inpatient claims visits. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW covid_claims_inpatient AS
SELECT *  
FROM covid_claims_all c
WHERE c.SettingOfCareConceptId IN (SELECT ConceptId FROM inpatient_claim_codes)

-- COMMAND ----------


SELECT *
FROM covid_claims_inpatient
LIMIT 5
