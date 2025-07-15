# Databricks notebook source
# MAGIC %md
# MAGIC # Visit Diagnosis Cleaning
# MAGIC
# MAGIC Key points from Cameron's Analysis:
# MAGIC - Data has 51,458,151 rows with 14 variables
# MAGIC - High proportion of missing data
# MAGIC - patientuid will not be unique given the nature of the data
# MAGIC
# MAGIC Primary varibales to clean consist of: encounterdate, serviceprovidernpi, encounterdiagnosiscode, encounterdiagnosistext, encounterproblemtypecode, and encounterproblemtypetext

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# Saving data to python dataframe
VD = spark.table("cdh_abfm_phi.visitdiagnosis")
VD.count() # 51,458,151 observations present

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Data snapshot

# COMMAND ----------

# # change to isNull, get percent missing
# from pyspark.sql.functions import isnan, when, count, col
# num_nulls = VD.select([count(when(col(c).isNull(), c)).alias(c) for c in VD.columns])
# num_nulls.show()

# # Null values
# #     patientuid                100.0% full
# #     enocunterdate             100.0% full *key variable
# #     serviceprovidernpi        99.81% full *key variable
# #     encounterdiagnosiscode    99.94% full *key variable
# #     encounterdiagnosistext    62.88% full *key variable
# #     encounterproblemtypecode  52.92% full *key variable
# #     encounterproblemtypetext  53.72% full *key variable
# #     documentationdate         98.65% full *key variable
# #     problemstatustext         03.94% full
# #     problemcomment            00.91% full
# #     problemonsetdate          03.22% full
# #     targetsitetext            00.62% full
# #     listorder                 65.86% full
# #     practiceid                99.99% full

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Date
# MAGIC
# MAGIC Sorting dates ascending and descending to check for dates either formatted incorrectly or outside the accepted range

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT encounterdate FROM cdh_abfm_phi.visitdiagnosis sort by encounterdate desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT encounterdate FROM cdh_abfm_phi.visitdiagnosis sort by encounterdate
# MAGIC
# MAGIC -- Dates are formatted correctly and within the expected range, no cleaning necessary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(encounterdate) FROM cdh_abfm_phi.visitdiagnosis WHERE SUBSTR(encounterdate, -3, 3) == "UTC"
# MAGIC
# MAGIC -- All dates are in UTC

# COMMAND ----------

# MAGIC %md
# MAGIC # Serviceprovidernpi
# MAGIC Checking if values are consistant in format

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT serviceprovidernpi FROM cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT serviceprovidernpi) FROM cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT serviceprovidernpi) FROM cdh_abfm_phi.visitdiagnosis WHERE LENGTH(serviceprovidernpi) == 10
# MAGIC
# MAGIC -- Formatting is consistant, no obvious problems

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Diagnosis Code and Text
# MAGIC Checking diagnosis cade and text to see how they could be sorted or grouped

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT encounterdiagnosiscode) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- > 80,000 distinct values for code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT encounterdiagnosiscode FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Format of codes is very inconsistant

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT SUBSTR(encounterdiagnosiscode, 1, 1) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- There seem to be come consistantcies among codes with the first character. Perhaps something could be infered from that

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW VD as
# MAGIC SELECT *,
# MAGIC CASE 
# MAGIC   WHEN encounterdiagnosiscode IN ('J45') THEN "Asthma" 
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 1) IN ('C') OR SUBSTR(encounterdiagnosiscode, 1, 3) IN ('D00','D01','D02','D03','D04','D05','D06','D07','D08','D09') THEN "Cancer"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') THEN "Cerebro"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('E84') THEN "CF"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 1) IN ('N') OR SUBSTR(encounterdiagnosiscode, 1, 3) IN ('O26','Q61') THEN "CKD"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('J44') THEN "COPD"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('E10','E11') THEN "Diabetes"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('Q90') THEN "Down"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('J84') THEN "Fibrosis"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('I21','I22','I23','I24','I25','I42','I43','I50') THEN "Heart"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('I10','I11','I12','I13','I14','I15','I16') THEN "HTN"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('B20','D80','D81','D82','D83','D84','D85','D86','D87','D88','D89','Z79') THEN "Immuno"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('B18','K70','K71','K72','K73','K74','K75','K76','K77') THEN "Liver"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('F01','F02','F03','G30') THEN "Neuro"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('Z33','Z34','O09') THEN "Obesity"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('D57') THEN "SCD"
# MAGIC   WHEN SUBSTR(encounterdiagnosiscode, 1, 3) IN ('D56') THEN "Thalassemia"
# MAGIC   ELSE "Unknown"
# MAGIC END AS Diagnosis
# MAGIC FROM cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM VD
# MAGIC
# MAGIC -- My best efforts at labeling diagnoses based on the codes I have, not much success

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT encounterdiagnosistext) FROM cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT LOWER(encounterdiagnosistext)) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Even with consistant cases, there are more distinct values for text than code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT LOWER(encounterdiagnosistext) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Manual cleaning for this variable not recommended, only useful with respect to the code variable

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Problem Type Code and Text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT encounterproblemtypecode) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Only 6 distinct problem type codes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT encounterproblemtypecode FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- 282291009 and 55607006 are the only valid values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT LOWER(encounterproblemtypetext)) FROM cdh_abfm_phi.visitdiagnosis
# MAGIC
# MAGIC -- Over 8,000 distinct values with consistant cases, manual cleaning not recommended

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT encounterproblemtypetext FROM cdh_abfm_phi.visitdiagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   MAX(documentationdate) AS Latest, 
# MAGIC   MIN(documentationdate) AS Earliest
# MAGIC FROM cdh_abfm_phi.visitdiagnosis
# MAGIC WHERE SUBSTR(documentationdate, -3, 3) == "UTC"
# MAGIC AND documentationdate < CURRENT_DATE
# MAGIC AND documentationdate IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(documentationdate)
# MAGIC FROM cdh_abfm_phi.visitdiagnosis
# MAGIC WHERE SUBSTR(documentationdate, -3, 3) == "UTC"
# MAGIC AND documentationdate < CURRENT_DATE
# MAGIC AND documentationdate IS NOT NULL
# MAGIC
# MAGIC -- 50,764,859 values preserved: 98.65% of data

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC - enocunterdate: No issues found with this variable, dates were formatted correctly and fell within an appropriate range. NOTE: the column cleaning document did flag this variable for 'invalid dates', so I should get a second opinion. I'd like to meet the person who made that note and find out what they meant.
# MAGIC - serviceprovidernpi: No obvious problems with this variable. Almost all values held a consistant format of ten integers.
# MAGIC - encounterdiagnosiscode: Over 80,000 distinct values. There are some odd values among the codes(most start which a character followed by integers but some are just integers, some have decimals and some don't, ect) There may be similarities in encounters based on which character a code starts with, and we could group based on that. There are thousands of condition codes on record, so manually decerning which condition based on which code would take a very long time without subsetting the code. Proposed solutions would either be to somehow upload a file of diagnosis codes and use that for cleaning, or categorize based on code substrings
# MAGIC - encounterdiagnosistext is only useful in context with the code variable. Even with consistant cases, there are over 90,000 distinct values; manual cleaning not recommended
# MAGIC - encounterproblemtypecode is about half-empty. The only meaningful values are 282291009 and 55607006, which I'm guessing mean Diagnosis and Procedure
# MAGIC - encounterproblemtypetext is only useful in context with the code variable. even with consistant cases, there are over 8,000 distinct values; manual cleaning not recommended
# MAGIC - documentationdate: convert to UTC, no nulls, no dates past today

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------

display(percent_full(VD))

# COMMAND ----------


