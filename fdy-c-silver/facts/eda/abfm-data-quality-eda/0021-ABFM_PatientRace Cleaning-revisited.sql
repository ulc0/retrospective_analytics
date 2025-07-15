-- Databricks notebook source
-- MAGIC %md
-- MAGIC #PatientRace
-- MAGIC - 4 variables, 6,867,607 observations
-- MAGIC - All variables will be analyzed

-- COMMAND ----------

DESCRIBE cdh_abfm_phi.patientrace

-- COMMAND ----------

SELECT COUNT(*) AS Obs FROM cdh_abfm_phi.patientrace

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC PR = spark.table("cdh_abfm_phi.patientRace")
-- MAGIC from pyspark.sql.functions import isnan, when, count, col
-- MAGIC num_nulls = PR.select([count(when(col(c).isNull(), c)).alias(c) for c in PR.columns])
-- MAGIC num_nulls.show()
-- MAGIC
-- MAGIC # Null values
-- MAGIC #     patientuid          100%  full
-- MAGIC #     patientracecode     78%   full
-- MAGIC #     patientracetext     99.5% full
-- MAGIC #     practiceid          ~100% full

-- COMMAND ----------

-- MAGIC %run ./0000-utils

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC display(
-- MAGIC   percent_full(PR)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Patient ID

-- COMMAND ----------

SELECT COUNT(DISTINCT patientuid) AS IDs FROM cdh_abfm_phi.patientrace

-- Just under 5% of IDs are redundant, not a major problem; may fall out naturally once other variables are addressed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Patient Race Code

-- COMMAND ----------

SELECT DISTINCT patientracecode FROM cdh_abfm_phi.patientrace

-- 147 distinct values
-- 0000-0 seems to be the most consistant format

-- COMMAND ----------

SELECT
  patientracecode,
  COUNT(*) AS code_count
FROM
  cdh_abfm_phi.patientrace
GROUP BY
  patientracecode SORT BY code_count DESC
  
-- 2106-3 is the most common code by far  
-- Some significant codes are not formatted as 0000-0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Patient Race Text

-- COMMAND ----------

SELECT
  COUNT(DISTINCT patientracetext) AS Texts,
  COUNT(DISTINCT LOWER(patientracetext)) AS Lower_Texts
FROM
  cdh_abfm_phi.patientrace

-- Over 1,000 distinct values
-- Converting to lowercase removes 52 values

-- COMMAND ----------

SELECT
  LOWER(patientracetext) AS text,
  COUNT(*) AS text_count
FROM
  cdh_abfm_phi.patientrace
GROUP BY
  text 
SORT BY 
  text_count DESC
LIMIT
  25
  
-- Data is overwhelmingly-white
-- Many text descriptions are redundant and uninterporatable
-- Use the code variable in place of the text variable whenever possible

-- COMMAND ----------

SELECT 
  DISTINCT patientracetext
FROM
  cdh_abfm_phi.patientrace
WHERE
  patientracecode IS NULL
AND
  patientracetext IS NOT NULL
  
-- If we could find the correlated codes for the discriptions below, we could fill in much of the missing data and remove our need for the text variable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Practice ID

-- COMMAND ----------

SELECT
  practiceid,
  COUNT(*) AS ID_count
FROM
  cdh_abfm_phi.patientrace
GROUP BY
  practiceid SORT BY ID_count DESC
  
-- About 10 IDs are significantly more numerous, the rest are pretty consistant

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Conclusion
-- MAGIC - Slight redundancy among patient IDs, may fall out when the other variables are formatted
-- MAGIC - White is the most common text value by far, 2106-3 is the most common code
-- MAGIC    - note: not all white patients have the text value 'white'. Some are bi-racial or have different formats for the value (WHITE, caucasian, ect)
-- MAGIC - Gain insight into which code correlates to which race, and use it to sync the text and code variables with eachother
-- MAGIC    - Not only will this fill in missing data, I suspect it will filter out some redundant patient IDs
-- MAGIC - 190916 is the most common practice ID, but the distribution is not wildly-uneven
