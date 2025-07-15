-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Truveta Start Guide
-- MAGIC <br>
-- MAGIC <b>Primary analysis</b><br>
-- MAGIC Become familiar with Truveta data through examples<br>
-- MAGIC <i>By default, notebooks will point to the most recent data release. specify version in the FROM statement, after the table name using</i> <b>'VERSION AS OF 5' </b> </i>
-- MAGIC or <b>'@v5'</b> where appropriate
-- MAGIC
-- MAGIC <b>Primary objective in this notebook<br></b>
-- MAGIC - <b>Part 1: How can we find patients who had COVID-19 using ICD10 = 'U07.1'</b>
-- MAGIC - [Part 2](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615353?o=5189663741904016): Number of patients hospitalized with covid by year and mean paid amount
-- MAGIC - [Part 3](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4489091775615479?o=5189663741904016): Patients with A1C1 test > 6.5 who take metformin. Stratify by education and income levels
-- MAGIC
-- MAGIC <b> Contact information</b><br>
-- MAGIC Data Hub Support [datahubsupport@cdc.gov](datahubsupport@cdc.gov) <!--- enter email address in both the square brackets and the parens -->

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Get info from an existing database
-- MAGIC
-- MAGIC Many tutorials demonstrate use the Databricks File System (DBFS) for creating tables, but DBFS is disabled here for security purposes. Instead we will work with Database Tables. Available databases can be explored from the 'Catalog' tab on the lefthand sidebar, or within the notebook as demonstrated below.
-- MAGIC
-- MAGIC If additional data is needed for analysis (ex. a table of FIPS codes), send a request to CDHSupport@cdc.gov and attach the table as a .csv so it can be added to the `cdh_reference_data` database.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For additional information on the truveta data model, see this page: https://learn.truveta.com/studio/docs/truveta-data-model. The Entity Relationship Diagram (ERD) use the diagram as a guide to relationships between the tables in TDM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Tables in the Truveta Data Model are available on cdh_truveta on DataBricks. Use the cell below to list tables in the cdh_truveta database.

-- COMMAND ----------

-- DBTITLE 1,Show tables in a database
SHOW TABLES IN cdh_truveta 

-- COMMAND ----------

-- DBTITLE 1,View table metadata
DESCRIBE cdh_truveta.condition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set a database as default if mainly working from a single database to avoid writing `database_name.table_name` when reading each table and instead just refrence `table_name`. Tables in other databases can still be referenced with `database_name.table_name`.

-- COMMAND ----------

-- DBTITLE 1,Set default database
USE cdh_truveta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using DateTime variables
-- MAGIC Each table in the Truveta Data Model has at least one date time variable representing the timing of an event.

-- COMMAND ----------

-- MAGIC %md ### Preferred date time fields by table
-- MAGIC
-- MAGIC <table>
-- MAGIC <thead>
-- MAGIC <tr>
-- MAGIC <th> Table Name </th>
-- MAGIC <th> Preferred Date Time Field </th>
-- MAGIC </tr>
-- MAGIC </thead>
-- MAGIC <tbody>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Condition</td>
-- MAGIC <td>1. OnsetDateTime 
-- MAGIC </br>2. RecordedDateTime
-- MAGIC </td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Immunization</td>
-- MAGIC <td>1. AdministeredDateTime
-- MAGIC <br>2. RecordedDateTime
-- MAGIC </td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Observation </td>
-- MAGIC <td>1. EffectiveDateTime
-- MAGIC <br>2. RecordedDateTime</td>
-- MAGIC </tr> 
-- MAGIC <tr>
-- MAGIC
-- MAGIC <td>Encounter</td>
-- MAGIC <td>StartDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>MedicationAdministration</td>
-- MAGIC <td>1. StartDateTime
-- MAGIC <br>2. RecordedDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>MedicationRequest</td>
-- MAGIC <td>1. StartDateTime
-- MAGIC <br>2. AuthoredOnDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>MedicationDispense</td>
-- MAGIC <td>DispenseDatetime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Procedure</td>
-- MAGIC <td>1. StartDateTime 
-- MAGIC <br>2. RecordedDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Claim</td>
-- MAGIC <td>ServiceBeginDate</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>DeviceUse</td>
-- MAGIC <td>1. ActionDateTime
-- MAGIC <br>2. RecordedDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>LabResults</td>
-- MAGIC <td>1. EffectiveDateTime
-- MAGIC <br>2. SpecimenCollectionDateTime</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>Person</td>
-- MAGIC <td>BirthDateTime</td>
-- MAGIC <tr>
-- MAGIC </tr>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sample the data  
-- MAGIC 1. Run the following cell to view the first 1000 rows where the recorded datetime is on or after March 1, 2020.
-- MAGIC 1. When the table appears, click the 'Table' dropdown for options to download the data or add the table to a dashboard.
-- MAGIC 1. Click the + icon to the right of the 'Table' dropdown to create a visualization or data profile.
-- MAGIC <br><br>
-- MAGIC
-- MAGIC For more information, see [Azure Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/query/)

-- COMMAND ----------

-- DBTITLE 1,Display a sample of the data
-- Note: The visualization is for demonstration purposes only and the variables plotted don't make any analytic sense.
SELECT *
FROM condition  
WHERE RecordedDateTime >= "2020-03-01"
LIMIT 1000

-- COMMAND ----------

-- MAGIC %md ### Concept Code sources by type
-- MAGIC
-- MAGIC Patient information is organized by Concepts.
-- MAGIC
-- MAGIC <br>Concepts are declared in Truveta tables using Concept Codes that correspond to a common ontology (for example, ICD10, SNOMED). </br>
-- MAGIC <br> Concept Codes are also assigned a unique Truveta identifier (Concept ID). When no common ontology exists for a Concept, the Concept ID and Concept Code are equivalent.</br>
-- MAGIC <br>Concept Codes can be interpreted by mapping to the Concept table by the corresponding Code Concept Map table.</br>
-- MAGIC
-- MAGIC Below are the common ontologies/code systems used in the Truveta data model and the tables in which they are used:
-- MAGIC
-- MAGIC <table>
-- MAGIC <thead>
-- MAGIC <tr>
-- MAGIC <th> Type </th>
-- MAGIC <th> Code Sources </th>
-- MAGIC </tr>
-- MAGIC </thead>
-- MAGIC <tbody>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>ICD10</td>
-- MAGIC <td>ConditionCodes
-- MAGIC </br>Procedure Codes
-- MAGIC </td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>SNOMED CT</td>
-- MAGIC <td>ConditionCodes
-- MAGIC <br>LabResultCodes
-- MAGIC <br> Observation
-- MAGIC </td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>LOINC </td>
-- MAGIC <td>LabResultCodes
-- MAGIC <br>Observation</td>
-- MAGIC </tr> 
-- MAGIC <tr>
-- MAGIC
-- MAGIC <td>CPT/HCPCS</td>
-- MAGIC <td>ProcedureCodes</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>CVX</td>
-- MAGIC <td>ImmunizationCodes</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>RxNorm</td>
-- MAGIC <td>MedicationCodes</td>
-- MAGIC </tr>
-- MAGIC
-- MAGIC <tr>
-- MAGIC <td>NDC <br>(TDM generally relies on RxNorm)</td>
-- MAGIC <td>MedicationCodes</td>
-- MAGIC </tr>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Get data types for fields in table of interest 
-- MAGIC
-- MAGIC It is important to know data types when joining tables. Data types must be equivalent when joining.
-- MAGIC

-- COMMAND ----------

-- The default database is set so you can eliminate cdh_truveta. before all table names in your code
DESCRIBE condition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Combine Tables
-- MAGIC Use `JOIN` to combine two or more tables on join criteria. Note: the default join type is `INNER`. <br>
-- MAGIC See [Azure Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-join) for additional options. 

-- COMMAND ----------

-- DBTITLE 1,Format for joins
-- MAGIC %md
-- MAGIC ```sql
-- MAGIC -- Replace 'core' with the Truveta table name to pull relevant codes from the Concept table
-- MAGIC SELECT Core.*, codes.*
-- MAGIC FROM Concept c
-- MAGIC JOIN CoreCodes codes ON codes.CodeConceptId = c.ConceptId
-- MAGIC JOIN Core ON Core.Id = codes.CoreId
-- MAGIC WHERE c.ConceptCode IN ('', '')
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```sql
-- MAGIC -- [filter all conditions to only include ones where there is at least one mapping that maps to a code in my tbl_codes view ](https://learn.truveta.com/studio/docs/code-sets)
-- MAGIC SELECT  
-- MAGIC   *
-- MAGIC FROM Condition 
-- MAGIC WHERE CodeConceptMapId IN ( 
-- MAGIC   SELECT Id FROM ConditionCodeConceptMap WHERE CodeConceptId IN (SELECT ConceptId FROM tbl_codes) 
-- MAGIC )
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md #### Define ICD10 and SNOMED codes of interest
-- MAGIC In this example we've included the ICD10 code U07.1 and SNOMED-CT 840539006 <br>
-- MAGIC ICD10 codes and LOINC codes are found in the Concept table. Within the Concept table, the ConceptId field can be used to join other tables of interest, such as the Conditions table (as described above ICD10 and SNOMEDCT can be found in the conditions table)

-- COMMAND ----------

--This command selects all rows from the concept table where the concept code is equal to our codes  of interest, U07.1 (ICD10) or (SNOMED-CT) 840539006, diagnosis of COVID-19 
CREATE OR REPLACE TEMP VIEW  covid_codes AS
SELECT *
FROM Concept@v13
WHERE ConceptCode IN ('U07.1', '840539006');

SELECT * 
FROM covid_codes 
LIMIT 1000;



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  covid_codes_omop AS
SELECT *
FROM edav_prd_cdh.cdh_global_reference_etl.bronze_athena_concept
WHERE concept_code IN ('U07.1', '840539006');

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  covid_codes_omop_standard AS
SELECT r.concept_id_2, c.*
FROM edav_prd_cdh.cdh_global_reference_etl.bronze_athena_concept_relationship r
join covid_codes_omop c
on concept_id_1=concept_id
WHERE relationship_id='Maps to'
and standard_concept='S';
select * FROM covid_codes_omop_standard;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####CodeConceptMap Tables 
-- MAGIC <br>
-- MAGIC The Truveta data structure maps concepts to their values by  CodeConceptMapId. Each Truveta core table (with the exception of Medication tables) has a corresponding CodeConceptMapTable that must be linked in order to access additonal information for the concept.   </br> For the Condition table,  CodeConceptMapId must be joined with the Id in the ConditionCodeConceptMap as a primary key

-- COMMAND ----------

-- DBTITLE 1,Example table linkage
--The query requires linking 3 tables. 
--Here we select from the Condition, Concept and ConditionCodeConceptMap tables where: 
--1) the CodeConceptMapId from the Condition table matches the Id in the ConditionCodeConceptMapId 
--2) the CodeConceptId from the ConditionCodeConceptMap table matches the Concept Id from the Concept Table and 
--3) Where ConceptCode from the Concept table is equal to our ICD('s) of interest - in this example, U07.1
CREATE OR REPLACE TEMP VIEW covid_condition  AS
SELECT *   
FROM Condition c 
WHERE c.CodeConceptMapId IN (SELECT Id FROM ConditionCodeConceptMap 
WHERE CodeConceptId IN (SELECT ConceptId FROM Concept 
WHERE ConceptCode IN ("U07.1", '840539006')))


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```sql
-- MAGIC -- [filter all conditions to only include ones where there is at least one mapping that maps to a code in my tbl_codes view ](https://learn.truveta.com/studio/docs/code-sets)
-- MAGIC SELECT  
-- MAGIC   *
-- MAGIC FROM Condition 
-- MAGIC WHERE CodeConceptMapId IN ( 
-- MAGIC   SELECT Id FROM ConditionCodeConceptMap WHERE CodeConceptId IN (SELECT ConceptId FROM tbl_codes) 
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Count all instances of COVID-19 by ICD10 andL LOINC
-- When joining large tables in, it is best practice to generate row counts in a separate cell to avoid a full scan of the resulting joined dataframe. This command can be expensive and uses an excessive amount of resources.
SELECT COUNT(*) FROM covid_condition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####The covid_conditions  above contains all instances of COVID-19, but you may want to choose the first instance of a condition for a patient  <br>
-- MAGIC
-- MAGIC The following command selects the first instance of the condition for each PersonId. <br>
-- MAGIC <br>
-- MAGIC Each Patient has 1 PersonId, but may have separate PatientId's for each visit.<br>
-- MAGIC <br> 
-- MAGIC We will select for the index event by determining the first instance of the ICD10code for each person by RecordedDateTime. <br>
-- MAGIC
-- MAGIC RecordedDateTime represents the date the condition was recorded. OnsetDateTime represents the onset of the condition and is not as populated as RecordedDateTime.
-- MAGIC
-- MAGIC *Note: Typically the RecordedDateTime column is populated more than the OnsetDateTime column.  Keep this in mind when choosing what to sort by.*

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  index_covid_conditions AS 
SELECT * FROM (SELECT *,
    ROW_NUMBER() OVER (PARTITION BY PersonId ORDER BY RecordedDateTime, Id)
      AS row_number
    FROM covid_condition) t
WHERE row_number = 1

-- COMMAND ----------

-- DBTITLE 1,Count of index covid conditions
SELECT COUNT(*) FROM index_covid_conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Check for duplicates. 
-- MAGIC
-- MAGIC There should be one row per PersonId now.  You can check this by comparing the output from the cell above with the result of the cell below. 

-- COMMAND ----------

SELECT COUNT(DISTINCT PersonId)
FROM index_covid_conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Save DataFrame to the exploratory database  
-- MAGIC
-- MAGIC In previous cells, we have created temporary tables that are only accessible within this notebook. A DataFrame can be saved for future use in the __exploratory_ database for the relevant dataset. <br>
-- MAGIC <br> The cell below is commented out. In general, do not overwrite or delete tables you did not create. 

-- COMMAND ----------

-- DBTITLE 1,Save permanent table
--The table below was previously created in the exploratory database. In practice, do not overwrite tables you did not create. 

--CREATE OR REPLACE TABLE cdh_truveta_exploratory.QSGcovid_condition
--SELECT *   
--FROM Condition c 
--WHERE c.CodeConceptMapId IN (SELECT Id FROM ConditionCodeConceptMap 
--WHERE CodeConceptId IN (SELECT ConceptId FROM Concept 
--WHERE ConceptCode IN ("U07.1")))

-- COMMAND ----------

-- MAGIC %md # 1b. Identify COVID-19 by condition and lab

-- COMMAND ----------

-- MAGIC %md #### Define LOINC codes of interest
-- MAGIC In section 1a, we identified patients with ICD10 or SNOMED codes indicating COVID-19 diagnosis or condition.
-- MAGIC
-- MAGIC In the following cells, we will define covid by positive lab using LOINC using a similar approach.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW covid_lab  AS
SELECT *   
FROM labresult l
WHERE l.CodeConceptMapId IN (SELECT Id FROM labresultcodeconceptmap 
WHERE CodeConceptId IN (SELECT ConceptId FROM Concept 
WHERE ConceptCode IN ("95521-1", "94510-5", "94311-8", "94312-6", "95522-9", "94644-2", "94511-3", "94646-7", "94645-9", "94745-7",  "94746-5", "94819-0", "94642-6","94643-4", "94313-4", "94509-7", "95423-0", "94760-6", "95409-9", "94533-7", "94756-4", "94757-2", "95425-5",  "94766-3", "94316-7", "94307-6","94308-4", "94559-2", "95824-9", "94639-2", "94534-5", "94314-2", "94565-9", "94759-8", "95406-5", "95608-6",  "94500-6", "95424-8", "94845-5","94822-4", "94660-8", "94309-2", "94640-0", "95609-4", "94767-1", "94641-8", "94764-8", "94310-0", "94758-0",  "95823-1", "94765-5","94315-9", "94502-2", "94647-5", "94532-9", "95970-0", "96091-4", "94306-8", "96120-1", "96121-9", "96122-7",  "96123-5", "96448-6", "96741-4","96751-3", "96752-1", "96763-8", "96764-6", "96765-3", "96766-1", "96797-6", "96829-7", "96894-1", "96895-8",   "96896-6", "96897-4","96898-2", "96899-0", "96900-6", "96957-6", "96958-4", "96986-5", "95826-4", "96094-8", "97098-8",  "98132-4",  "98494-8", "97104-4", "98131-6","98493-0", "98062-3", "96756-2", "96757-0",	"98080-5", "94531-1", "99314-7", "100156-9", "95209-3", "94558-4",	"96119-3", "97097-0", "94661-6", "95825-6", "94762-2", "94769-7", "94562-6", "94768-9", "95427-1", "94720-0",  "95125-1",	"94761-4",	"94563-4", "94507-1",	"95429-7", "94505-5", "94547-7", "95542-7",	"95416-4", "94564-2", "94508-9", "95428-9",	 "94506-3",	"95411-5",	"95410-7",	"96603-6",	"96742-2", "96118-5", "98733-9", "98069-8",	"98732-1", "98734-7", "94504-8", "94503-0",	 "99596-9", "99597-7",	"95971-8",	"95972-6", "95973-4", "96755-4", "98846-9", "98847-7",	"95974-2", "94763-0", "99771-8", "99774-2",  "99773-4", "99772-6")));

-- COMMAND ----------

SELECT COUNT(DISTINCT personid) 
FROM covid_lab

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  index_covid_labs AS 
SELECT * FROM (SELECT *,
    ROW_NUMBER() OVER (PARTITION BY PersonId ORDER BY RecordedDateTime, Id)
      AS row_number
    FROM covid_lab) t
WHERE row_number = 1

-- COMMAND ----------

SELECT COUNT(*) FROM index_covid_labs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic Joins
-- MAGIC Similar to the example in 1a, we want to join the LabResult, Concept and LabResultCodeConceptMap tables where: 
-- MAGIC 1) The CodeConceptMapId from the LabResult table matches the Id in the LabResultCodeConceptMap table 
-- MAGIC 2) The CodeConceptId from the LabResultCodeConceptMap table matches the Concept Id from the Concept Table and 
-- MAGIC 3) ConceptCode from the Concept table is equal to our LOINC codes of interest.****

-- COMMAND ----------

--The query requires linking 3 tables. 
--Here we select from the LabResult, Concept and LabResultCodeConceptMapables where: 
--1) the CodeConceptMapId from the LabResult table matches the Id in the LabResultCodeConceptMap 
--2) the CodeConceptId from the LabResultCodeConceptMapable matches the Concept Id from the Concept Table and 
--3) Where ConceptCode from the Concept table is equal to our LOINC codes of interest
CREATE OR REPLACE TEMP VIEW covid_lab  AS
SELECT *   
FROM labresult l 
WHERE l.CodeConceptMapId IN (SELECT Id FROM LabResultCodeConceptMap WHERE CodeConceptId IN (SELECT ConceptId FROM Concept 
WHERE ConceptCode IN ("95521-1", "94510-5", "94311-8", "94312-6", "95522-9", "94644-2", "94511-3", "94646-7", "94645-9", "94745-7",  "94746-5", "94819-0", "94642-6","94643-4", "94313-4", "94509-7", "95423-0", "94760-6", "95409-9", "94533-7", "94756-4", "94757-2", "95425-5",  "94766-3", "94316-7", "94307-6","94308-4", "94559-2", "95824-9", "94639-2", "94534-5", "94314-2", "94565-9", "94759-8", "95406-5", "95608-6",  "94500-6", "95424-8", "94845-5","94822-4", "94660-8", "94309-2", "94640-0", "95609-4", "94767-1", "94641-8", "94764-8", "94310-0", "94758-0",  "95823-1", "94765-5","94315-9", "94502-2", "94647-5", "94532-9", "95970-0", "96091-4", "94306-8", "96120-1", "96121-9", "96122-7",  "96123-5", "96448-6", "96741-4","96751-3", "96752-1", "96763-8", "96764-6", "96765-3", "96766-1", "96797-6", "96829-7", "96894-1", "96895-8",   "96896-6", "96897-4","96898-2", "96899-0", "96900-6", "96957-6", "96958-4", "96986-5", "95826-4", "96094-8", "97098-8",  "98132-4",  "98494-8", "97104-4", "98131-6","98493-0", "98062-3", "96756-2", "96757-0",	"98080-5", "94531-1", "99314-7", "100156-9", "95209-3", "94558-4",	"96119-3", "97097-0", "94661-6", "95825-6", "94762-2", "94769-7", "94562-6", "94768-9", "95427-1", "94720-0",  "95125-1",	"94761-4",	"94563-4", "94507-1",	"95429-7", "94505-5", "94547-7", "95542-7",	"95416-4", "94564-2", "94508-9", "95428-9",	 "94506-3",	"95411-5",	"95410-7",	"96603-6",	"96742-2", "96118-5", "98733-9", "98069-8",	"98732-1", "98734-7", "94504-8", "94503-0",	 "99596-9", "99597-7",	"95971-8",	"95972-6", "95973-4", "96755-4", "98846-9", "98847-7",	"95974-2", "94763-0", "99771-8", "99774-2",  "99773-4", "99772-6")))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Normalized Value Concept ID's
-- MAGIC An additional step is taken to identify positive covid labs. The table above identifies all covid labs in TDM including positive, negative, and written in error. We want to include positive labs and exclude negative and invalid labs. 
-- MAGIC
-- MAGIC Let's identify the meaning of the codes in the Concept table

-- COMMAND ----------

SELECT * 
FROM concept
WHERE ConceptCode IN ("1065667","1065692","260373001","720735008","10828004","52101004")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This information is stored in the Normalized Value Concept ID columns.
-- MAGIC
-- MAGIC Now let's filter the covid_bylab table for only positive labs, excluding labs that were negative or invalid

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW f_covid_lab  AS
SELECT * 
FROM covid_lab
WHERE NormalizedValueConceptId IN ("241618", "10125", "1065667","1065692", "407410", "49078" )
AND NormalizedValueConceptId NOT IN ("1065712","1065714","1065719","1065717");


-- COMMAND ----------

SELECT COUNT(DISTINCT personid)
FROM f_covid_lab

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  f_index_covid_labs AS 
SELECT * FROM (SELECT *,
    ROW_NUMBER() OVER (PARTITION BY PersonId ORDER BY RecordedDateTime, Id)
      AS row_number
    FROM f_covid_lab) t
WHERE row_number = 1

-- COMMAND ----------

SELECT COUNT(*)
FROM f_index_covid_labs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Combine COVID-19 events by lab and condition
-- MAGIC Now that we have two data frames containing index COVID-19 events (conditions and lab). We can combine these data frames using union and deduplicate to create a dataframe with COVID-19 events by labs and conditions
-- MAGIC
-- MAGIC There are a few important additional steps we make in the table below:
-- MAGIC 1. We are selecting only 3 existing columns from each table: EncounterId, PersonId, and the preferred Date Time field
-- MAGIC 2. We are adding an additional column (SourceTable) and populating this field with the name of the table the row comes from
-- MAGIC 3. The union method collects all rows from both tables (conditions and labs). The caviat to using this method is that it matches columns in the order they are presented. with EncounterId, PersonId, and the DateTime fields as the first second third fields respectively. Keep this in mind when using this method. Here we will rename the field to just "DateTime"

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  covid_conditions_labs AS
SELECT DISTINCT
c.EncounterId,
c.PersonId,
c.RecordedDateTime,
'Conditions' AS SourceTable
FROM index_covid_conditions c
UNION ALL
SELECT DISTINCT
l.EncounterId,
l.PersonId,
l.RecordedDateTime,
'Labs' AS SourceTable
FROM f_index_covid_labs l;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  index_covid_conditions_labs AS 
SELECT * FROM (SELECT *,
    ROW_NUMBER() OVER (PARTITION BY PersonId ORDER BY RecordedDateTime)
      AS row_number
    FROM covid_conditions_labs) t
WHERE row_number = 1
