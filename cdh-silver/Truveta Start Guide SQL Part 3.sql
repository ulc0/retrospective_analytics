-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Truveta Start Guide
-- MAGIC <br>
-- MAGIC <b>Primary analysis</b><br>
-- MAGIC Become familiar with Truveta data through examples using SQL<br>
-- MAGIC
-- MAGIC <b>Primary objective in this notebook</b><br>
-- MAGIC - [Part 1](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615281?o=5189663741904016): How can we find patients who had COVID-19 using ICD10 = 'U07.1' 
-- MAGIC - [Part 2](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615353?o=5189663741904016): Number of patients hospitalized with covid by year and mean paid amount
-- MAGIC - <b>Part 3: Patients with A1C1 test > 6.5 who take metformin. Stratify by education and income levels</b>
-- MAGIC
-- MAGIC <b> Contact information</b><br>
-- MAGIC Data Hub Support [datahubsupport@cdc.gov](datahubsupport@cdc.gov) <!--- enter email address in both the square brackets and the parens -->

-- COMMAND ----------

-- By default, notebooks will point to the most recent data release. If you want to specify a version use'@v' 
-- E.g., cdh_truveta.concept@v0

DESCRIBE HISTORY cdh_truveta.concept

-- COMMAND ----------

-- MAGIC %md ## 3. Patients with A1C1 test > 6.5 who take metformin. Stratify by education and income levels.

-- COMMAND ----------

-- MAGIC %md ##Create codeset table for A1C codes

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW A1C_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("4548-4")

-- COMMAND ----------


SELECT *
FROM A1C_codes

-- COMMAND ----------

-- MAGIC %md ##Use this to filter to lab results and find values > 6.5

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW A1C_results AS
SELECT lr.*   
FROM cdh_truveta.LabResult lr
JOIN cdh_truveta.LabResultCodes lrc ON lr.Id=lrc.LabResultId
WHERE CodeConceptId IN (SELECT ConceptId FROM A1C_codes)
AND NormalizedValueNumeric > 6.5

-- COMMAND ----------

SELECT *
FROM A1C_results
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ##Find Patients who take Metformin
-- MAGIC
-- MAGIC First create your codeset table. Only a few RXNorm codes were included in this example, but additional codes such as NDC can be added. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW treatment_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("6809", "861007", "860975", "861004" );

SELECT *
FROM treatment_codes

-- COMMAND ----------

-- MAGIC %md ## Pull in MedicationRequest and MedicationAdministration and MedicationDispense rows of data

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW med_request AS
SELECT mr.*   
FROM cdh_truveta.MedicationRequest mr
JOIN cdh_truveta.MedicationCodeConceptMap mc ON mr.CodeConceptMapId = mc.Id
WHERE CodeConceptId IN (SELECT ConceptId FROM treatment_codes)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW med_admin AS
SELECT ma.*   
FROM cdh_truveta.MedicationAdministration ma
JOIN cdh_truveta.MedicationCodeConceptMap mc ON ma.CodeConceptMapId = mc.Id
WHERE CodeConceptId IN (SELECT ConceptId FROM treatment_codes)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW med_dispense AS
SELECT md.*   
FROM cdh_truveta.MedicationDispense md
JOIN cdh_truveta.MedicationCodeConceptMap mc ON md.CodeConceptMapId = mc.Id
WHERE CodeConceptId IN (SELECT ConceptId FROM treatment_codes)


-- COMMAND ----------

-- MAGIC %md ## Sometimes medication orders are entered in error by physician or are cancelled. These codes represent the values below. 
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW non_admin_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("1065593", "1065595", "1065627", "1065629");

SELECT * FROM non_admin_codes

-- COMMAND ----------

-- MAGIC %md ## Combine the MedAdmin and MedRequest tables, selecting and renaming columns while filtering out the erroroneous data

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW treatment AS
SELECT PersonId, StartDateTime AS treatment_StartDateTime, EncounterId AS treatment_EncounterId, Id
FROM med_request
WHERE StatusConceptId NOT IN (SELECT ConceptId FROM non_admin_codes)
UNION ALL
SELECT PersonId, StartDateTime AS treatment_StartDateTime, EncounterId AS treatment_EncounterId, Id
FROM med_admin
 WHERE StatusConceptId NOT IN (SELECT ConceptId FROM non_admin_codes)
UNION ALL
SELECT PersonId, DispenseDateTime AS treatment_StartDateTime, "NONE" AS treatment_EncounterId, Id
FROM med_dispense
WHERE StatusConceptId NOT IN (SELECT ConceptId FROM non_admin_codes)

-- COMMAND ----------


SELECT *
FROM treatment
LIMIT 5

-- COMMAND ----------

SELECT COUNT(*)
FROM treatment

-- COMMAND ----------

-- MAGIC %md ## Only keep lab result values for PersonIds that also have Metformin RX

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW A1C_results_metformin AS
SELECT r.*
FROM A1C_results r
JOIN treatment t ON r.PersonId=t.PersonId

-- COMMAND ----------


SELECT *
FROM A1C_results_metformin
LIMIT 5

-- COMMAND ----------

SELECT COUNT(*)
FROM A1C_results

-- COMMAND ----------


SELECT COUNT(*)
FROM A1C_results_metformin

-- COMMAND ----------

-- MAGIC %md ## Get SDOH Data
-- MAGIC First let's take a look at the SDOH data and see what is there.

-- COMMAND ----------

SELECT *
FROM cdh_truveta.SocialDeterminantsOfHealth
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ## The data is represented in Concept Ids 
-- MAGIC The table is in a "long" format.  There is one row per PersonId per SDOH variable.   
-- MAGIC
-- MAGIC 1. AttributeConceptId
-- MAGIC 2. SourceConceptId
-- MAGIC 3. NormalizedValueConceptId
-- MAGIC
-- MAGIC You can use the Concept table to learn what information is represented in these various ConceptIds. You achieve this by pulling in the Concept table and making some joins on the various concept Id columns to learn what is present. 

-- COMMAND ----------

-- MAGIC %md ### AttributeConceptId gives you the variable name

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM cdh_truveta.SocialDeterminantsOfHealth SDOH
JOIN cdh_truveta.concept c ON SDOH.AttributeConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

-- MAGIC %md ### SourceConceptId gives you the source

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM cdh_truveta.SocialDeterminantsOfHealth SDOH
JOIN cdh_truveta.concept c ON SDOH.SourceConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

-- MAGIC %md ### NormalizedValueConceptId gives you a string to represent a numeric value, typically on a scale. 
-- MAGIC The values can also have a normalized value numeric, which is handy if you want to filter based on a numeric value rather than a set of strings. 

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count, SDOH.NormalizedValueNumeric
FROM cdh_truveta.SocialDeterminantsOfHealth SDOH
JOIN cdh_truveta.concept c ON SDOH.NormalizedValueConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName, SDOH.NormalizedValueNumeric
ORDER BY row_count DESC

-- COMMAND ----------

-- MAGIC %md ## Filter to just SDOH variables for Education and Income
-- MAGIC You can look at the AttributeConceptId table we joined to see the options available, or use our Learn page: https://learn.truveta.com/studio/docs/tcodes#sdohattribute.
-- MAGIC
-- MAGIC This example uses Truveta code '3058444' for education and '2506713' for income. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW education_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("3058444");

SELECT *
FROM education_codes
LIMIT 5

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW income_codes AS
SELECT *
FROM cdh_truveta.Concept
WHERE ConceptCode IN ("2506713");

SELECT *
FROM income_codes
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ## Find rows of data where PersonId is in the A1C_results_metformin_SDOH table and Attribute ConceptId is related to our Education code

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW A1C_results_metformin_education AS
SELECT sdoh.*
FROM cdh_truveta.SocialDeterminantsOfHealth sdoh
JOIN A1C_results_metformin am ON sdoh.PersonId = am.PersonId
WHERE AttributeConceptId IN (SELECT ConceptId FROM education_codes) 

-- COMMAND ----------


SELECT *
FROM A1C_results_metformin_education
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ## Get counts

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM A1C_results_metformin_education SDOH
JOIN cdh_truveta.concept c ON SDOH.NormalizedValueConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

-- This table has already been written to the exploratory database. As standard practice, do not overwrite tables you didn't create.

CREATE OR REPLACE TEMP VIEW A1C_results_metformin_income AS
SELECT sdoh.*
FROM cdh_truveta.SocialDeterminantsOfHealth sdoh
JOIN A1C_results_metformin am ON sdoh.PersonId=am.PersonId
WHERE AttributeConceptId IN (SELECT ConceptId FROM income_codes)

-- COMMAND ----------

SELECT *
FROM A1C_results_metformin_income
LIMIT 5

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM A1C_results_metformin_income SDOH
JOIN cdh_truveta.concept c ON SDOH.NormalizedValueConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC

-- COMMAND ----------

SELECT c.ConceptId, c.ConceptName, COUNT(*) AS row_count
FROM cdh_truveta.SocialDeterminantsOfHealth SDOH
JOIN cdh_truveta.concept c ON SDOH.AttributeConceptId = c.ConceptId
GROUP BY c.ConceptId, c.ConceptName
ORDER BY row_count DESC
