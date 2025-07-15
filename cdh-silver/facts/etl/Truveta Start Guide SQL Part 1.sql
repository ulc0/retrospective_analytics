-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks QuickStart
-- MAGIC
-- MAGIC This guide is a brief introduction to working with Truveta in Databricks notebooks.
-- MAGIC
-- MAGIC Intended for users with the following subject familiarity:
-- MAGIC - Databricks - Novice
-- MAGIC - Data Source (Truveta) - Novice
-- MAGIC - SQL - Intermediate
-- MAGIC <br><br>
-- MAGIC
-- MAGIC <b> Contact information</b><br>
-- MAGIC Data Hub Support [datahubsupport@cdc.gov](datahubsupport@cdc.gov) <!--- enter email address in both the square brackets and the parens --> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Workspaces  
-- MAGIC Access workspaces from the menu in the lefthand sidebar. <br>
-- MAGIC Each user has a workspace located at `/Users/<id>@cdc.gov` for personal use. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Connect to a cluster
-- MAGIC
-- MAGIC To query the Truveta datasets you must connect notebooks to the `CDH_CommonDatasets` cluster.
-- MAGIC Clusters are set to remain running, as signified by the green dot next to their names in the menu.
-- MAGIC
-- MAGIC 1. In the upper right, click the 'Connect' dropdown menu to view available clusters. 
-- MAGIC 2. Select CDH_CommonDatasets to connect. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run commands
-- MAGIC
-- MAGIC 1. Click **<img src="http://docs.databricks.com/_static/images/notebooks/run-all.png"/></a> Run All** button in the upper right (next to the cluster dropdown) to run all commands.
-- MAGIC 1. Alternately, run cells individually by clicking <img src="http://docs.databricks.com/_static/images/notebooks/run-cell.png"/></a> in the upper left of a code cell, or by selecting a code cell and using the shortcut ctrl + Enter. There are options to run all below, all above, and others in the dropdown <img src="http://docs.databricks.com/_static/images/notebooks/run-cell.png"/></a> as well.
-- MAGIC
-- MAGIC A list of keyboard shortucts can be found under the 'Help' menu in the upper left.

-- COMMAND ----------

-- DBTITLE 1,Code cell
-- Empty code cell to reference Run Commands notes above

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Get info from an existing database
-- MAGIC
-- MAGIC Many tutorials demonstrate use the Databricks File System (DBFS) for creating tables, but DBFS is disabled here for security purposes. Instead we will work with Database Tables. Available databases can be explored from the 'Catalog' tab on the lefthand sidebar, or within the notebook as demonstrated below.
-- MAGIC
-- MAGIC If additional data is needed for analysis (ex. a table of FIPS codes), send a request to CDHSupport@cdc.gov and attach the table as a .csv so it can be added to the `cdh_reference_data` database.

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

-- The default database is set so you can eliminate cdh_truveta. before all table names in your code
DESCRIBE condition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Where to find codes and which date variable to use

-- COMMAND ----------

-- MAGIC %md ### Datetime fields by table
-- MAGIC
-- MAGIC <b>*RecordedDateTime*</b><br>
-- MAGIC Condition <br>
-- MAGIC Immunization <br>
-- MAGIC Observation <br>
-- MAGIC
-- MAGIC <b>*StartDateTime*</b><br>
-- MAGIC Encounter <br>
-- MAGIC MedicationAdministration <br>
-- MAGIC MedicationRequest <br>
-- MAGIC Procedure<br>
-- MAGIC
-- MAGIC <b> Claim ></b> ServiceBeginDate <br>
-- MAGIC <b> DeviceUse ></b> ActionDateTime <br>
-- MAGIC <b> LabResults ></b> EffectiveDateTime <br>
-- MAGIC <b> MedicationDispense ></b> DispenseDatetime <br>
-- MAGIC <b> Person ></b> BirthDateTime <br>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Code sources by type
-- MAGIC <b>ICD10 </b> <br>
-- MAGIC ConditionCodes <br>
-- MAGIC ProcedureCodes <br>
-- MAGIC
-- MAGIC <b>SNOMED CT</b> <br>
-- MAGIC ConditionCodes <br>
-- MAGIC LabResultCodes <br>
-- MAGIC Observation <br>
-- MAGIC
-- MAGIC <b>LOINC </b> <br>
-- MAGIC LabResultCodes <br>
-- MAGIC Observation <br>
-- MAGIC
-- MAGIC <b>CPT/HCPCS </b> <br>
-- MAGIC ProcedureCodes <br>
-- MAGIC
-- MAGIC <b>CVX </b> <br>
-- MAGIC ImmunizationCodes <br>
-- MAGIC
-- MAGIC <b>RXNorm </b> <br>
-- MAGIC MedicationCodes <br>
-- MAGIC
-- MAGIC <b>NDC (Majority of medications are recorded as RXNorm) </b> <br>
-- MAGIC MedicationCodes <br>

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

-- MAGIC %md
-- MAGIC ## Combine Tables
-- MAGIC Use `JOIN` to combine two or more tables on join criteria. Note: the default join type is `INNER`. <br>
-- MAGIC See [Azure Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-join) for additional options. 

-- COMMAND ----------

-- DBTITLE 1,Format for joins
-- Replace 'core' with the Truveta table name to pull relevant codes from the Concept table
SELECT Core.*
FROM Concept c
JOIN CoreCodes codes ON codes.CodeConceptId = c.ConceptId
JOIN Core ON Core.Id = codes.CoreId
WHERE c.ConceptCode IN ('', '')

-- COMMAND ----------

-- DBTITLE 1,Example of join
SELECT Condition.*
FROM Concept c
JOIN ConditionCodes codes ON codes.CodeConceptId = c.ConceptId
JOIN Condition ON Condition.Id = codes.ConditionId
WHERE c.ConceptCode IN ("U07","U07.1")
LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Save DataFrame to the exploratory database  
-- MAGIC
-- MAGIC A DataFrame can be saved for future use in the __exploratory_ database for the relevant dataset.

-- COMMAND ----------

-- DBTITLE 1,Save permanent table
CREATE OR REPLACE TABLE cdh_truveta_exploratory.strict_inpatient_class_codes 
SELECT *
FROM Concept
WHERE ConceptCode IN ("1065220", "1065215", "1065223")
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table comments and additional details can be viewed in the Catalog tab in the lefthand sidebar, or by running `DESCRIBE EXTENDED`

-- COMMAND ----------

-- DBTITLE 1,Describe user created table
DESCRIBE EXTENDED cdh_truveta_exploratory.strict_inpatient_class_codes 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Table History
-- MAGIC Delta format preserves version history. Past versions of a table can be queried by specifying the version number.

-- COMMAND ----------

-- DBTITLE 1,View table version history
DESCRIBE HISTORY cdh_truveta.concept

-- COMMAND ----------

-- DBTITLE 1,Query specific version
SELECT *
-- specify version in the FROM statement
FROM cdh_truveta.concept VERSION AS OF 1
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Drop Unused Tables  
-- MAGIC Tables that are no longer in use for analysis should be deleted to reduce clutter in the exploratory database. You may only delete tables for which you are an owner.

-- COMMAND ----------

-- DBTITLE 1,Drop existing table
DROP TABLE cdh_truveta_exploratory.strict_inpatient_class_codes 
