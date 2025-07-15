-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Patient Problem Cleaning
-- MAGIC - Data has 162,753,995 rows with 16 variables
-- MAGIC - Primary variables to clean consist of: problemcode, problemtext, problemcategory, documentationdate, problemresolutiondate, problemonsetdate 

-- COMMAND ----------

DESCRIBE cdh_abfm_phi.patientproblem

-- COMMAND ----------

SELECT COUNT(*) AS RowCount FROM cdh_abfm_phi.patientproblem

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC PP = spark.table("cdh_abfm_phi.patientproblem")
-- MAGIC from pyspark.sql.functions import isnan, when, count, col
-- MAGIC num_nulls = PP.select([count(when(col(c).isNull(), c)).alias(c) for c in PP.columns])
-- MAGIC num_nulls.show()
-- MAGIC
-- MAGIC # Null values
-- MAGIC #     patientuid                 100.0% full
-- MAGIC #     problemcode                99.87% full *key variable
-- MAGIC #     problemtext                92.29% full *key variable
-- MAGIC #     problemcategory            98.87% full *key variable
-- MAGIC #     documentationdate          99.99% full *key variable
-- MAGIC #     problemresolutiondate      17.51% full *key variable
-- MAGIC #     problemonsetdate           02.11% full *key variable
-- MAGIC #     problemtypecode            92.49% full
-- MAGIC #     problemtypetext            86.76% full
-- MAGIC #     problemstatuscode          05.54% full
-- MAGIC #     problemstatustext          06.58% full
-- MAGIC #     targetsitecode             00.38% full
-- MAGIC #     targetsitetext             31.49% full
-- MAGIC #     problemhealthstatustext    00.17% full
-- MAGIC #     problemcomment             01.58% full
-- MAGIC #     practiceid                 99.97% full

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC total = 162753995
-- MAGIC
-- MAGIC print(1-round(0/total,4),
-- MAGIC      1-round(214746/total,4),
-- MAGIC      1-round(12554814/total,4),
-- MAGIC      1-round(1835053/total,4),
-- MAGIC      1-round(19034/total,4),
-- MAGIC      1-round(134256772/total,4),
-- MAGIC      1-round(159319815/total,4),
-- MAGIC      1-round(12215757/total,4),
-- MAGIC      1-round(21552663/total,4),
-- MAGIC      1-round(153739349/total,4),
-- MAGIC      1-round(152048929/total,4),
-- MAGIC      1-round(162133795/total,4),
-- MAGIC      1-round(111509353/total,4),
-- MAGIC      1-round(162482907/total,4),
-- MAGIC      1-round(160186506/total,4),
-- MAGIC      1-round(47371/total,4))

-- COMMAND ----------

SELECT * FROM cdh_abfm_phi.patientproblem

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Patientuid

-- COMMAND ----------

SELECT patientuid FROM cdh_abfm_phi.patientproblem WHERE LENGTH(patientuid) < 36

-- A significant number of suspicious-looking ids
-- Consider enforcing a consistant format

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Code and Text

-- COMMAND ----------

SELECT
  COUNT(DISTINCT problemcode) AS Problem_Codes,
  COUNT(DISTINCT LOWER(problemtext)) AS Problem_Texts
FROM cdh_abfm_phi.patientproblem

-- far more text discriptions than codes: not a 1-to-1 relationship
-- filter and group based only on the code variable

-- COMMAND ----------

SELECT DISTINCT problemcode FROM cdh_abfm_phi.patientproblem

-- Seem to be primarily ICD codes: some SNOMED and other codes may be in the mix as well

-- COMMAND ----------

SELECT 
  problemcode, 
  COUNT(problemcode) AS Count,
  collect_set(problemtext)
FROM cdh_abfm_phi.patientproblem
GROUP BY problemcode 
SORT BY Count DESC

-- E11.65 is the most common code but has <1% of the codes, distribution isn't wildly uneven

-- COMMAND ----------

SELECT 
  LOWER(problemtext), 
  COUNT(LOWER(problemtext)) AS Count 
FROM cdh_abfm_phi.patientproblem
GROUP BY LOWER(problemtext)
SORT BY Count DESC

-- No major discrepincy among descriptions either

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Category

-- COMMAND ----------

SELECT COUNT(DISTINCT problemcategory) AS Categories FROM cdh_abfm_phi.patientproblem

-- COMMAND ----------

SELECT problemcategory, count(*) AS Count FROM cdh_abfm_phi.patientproblem GROUP BY problemcategory SORT BY Count DESC

-- Very inconsistant format, uneven distribution
-- >99.3% of values are 1, 2, 3, and null: consider excluding the rest 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW PP as
SELECT *,
CASE 
  WHEN problemcategory IN ('1') THEN "One"
  WHEN problemcategory IN ('2') THEN "Two"
  WHEN problemcategory IN ('3') THEN "Three"
  WHEN problemcategory IS NULL THEN "Empty"
  ELSE "Other"
END AS Category
FROM cdh_abfm_phi.patientproblem

-- COMMAND ----------

SELECT category FROM PP

-- COMMAND ----------

SELECT * FROM cdh_abfm_phi.patientproblem WHERE problemcode = '191627008'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Documentation Date

-- COMMAND ----------

SELECT 
  MAX(documentationdate) AS Latest, 
  MIN(documentationdate) AS Earliest
FROM cdh_abfm_phi.patientproblem
--WHERE SUBSTR(documentationdate, -3, 3) == "UTC"
--AND documentationdate < CURRENT_DATE
--AND documentationdate IS NOT NULL

-- COMMAND ----------

SELECT 
  *
FROM 
  cdh_abfm_phi.patientproblem
WHERE
  documentationdate like '9999%'

-- COMMAND ----------

SELECT 
  cast(substring_index(documentationdate, "-", 1) AS int) AS vYear,
  count(documentationdate)
FROM 
  cdh_abfm_phi.patientproblem
GROUP BY
  substring_index(documentationdate, "-", 1)

-- COMMAND ----------

SELECT 
  count(documentationdate) AS count
FROM 
  cdh_abfm_phi.patientproblem
WHERE
  substring_index(documentationdate, "-", 1) > 2022

-- COMMAND ----------

SELECT 
  MAX(documentationdate) AS Latest, 
  MIN(documentationdate) AS Earliest,
  COUNT(documentationdate) AS ValidDates
FROM cdh_abfm_phi.patientproblem
WHERE SUBSTR(documentationdate, -3, 3) == "UTC" -- Only dates in UTC
AND documentationdate < CURRENT_DATE -- No dates past today
AND documentationdate IS NOT NULL -- No null values

-- All dates are within reasonable range
-- 99.97% of data preserved with these changes; would recommend

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Resolution and Onset Dates

-- COMMAND ----------

SELECT 
  MAX(problemresolutiondate) AS Latest, 
  MIN(problemresolutiondate) AS Earliest,
  COUNT(problemresolutiondate) AS ValidDates
FROM cdh_abfm_phi.patientproblem
-- WHERE SUBSTR(problemresolutiondate, -3, 3) == "UTC" -- Only dates in UTC
-- AND problemresolutiondate < CURRENT_DATE -- No dates past today
-- AND problemresolutiondate IS NOT NULL -- No null values
-- AND problemonsetdate < problemresolutiondate  -- Problem resolution takes place after onset

-- COMMAND ----------

SELECT 
  MAX(problemresolutiondate) AS Latest, 
  MIN(problemresolutiondate) AS Earliest,
  COUNT(problemresolutiondate) AS ValidDates
FROM cdh_abfm_phi.patientproblem
WHERE SUBSTR(problemresolutiondate, -3, 3) == "UTC" -- Only dates in UTC
AND problemresolutiondate < CURRENT_DATE -- No dates past today
AND problemresolutiondate IS NOT NULL -- No null values

-- AND problemonsetdate <= problemresolutiondate  -- Problem resolution takes place after onset
-- Prior condition leads to massive data loss, cannot recommend

-- Previous conditions will cost us about 4.5% of the data

-- COMMAND ----------

SELECT 
  MAX(problemonsetdate) AS Latest, 
  MIN(problemonsetdate) AS Earliest,
  COUNT(problemonsetdate) AS ValidDates
FROM cdh_abfm_phi.patientproblem
WHERE SUBSTR(problemonsetdate, -3, 3) == "UTC" -- Only dates in UTC
AND problemonsetdate < CURRENT_DATE -- No dates past today
AND problemonsetdate IS NOT NULL -- No null values

-- AND problemonsetdate <= problemresolutiondate  -- Problem resolution takes place after onset
-- Prior condition leads to massive data loss, cannot recommend

-- Earliest date seems too early, but am not aware of any cutoff point

-- Data is mostly empty to begin with, but almost no data is lost from previous conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Conclusion
-- MAGIC - Patientuid has many suspicious-looking values
-- MAGIC - Inquire about Patientuid case-sensitivity
-- MAGIC - Problem codes are primarily ICD
-- MAGIC - Distribution of Problem code and text isn't skewed to any extreme degree
-- MAGIC - The only significant problem category values are 1, 2, and 3
-- MAGIC - Recommend conversion to UTC and no dates past today for all date variables
-- MAGIC - Problem resolution and onset date variables are not relyable due to high proprtion of missing data
