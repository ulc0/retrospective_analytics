# Databricks notebook source
# MAGIC %md
# MAGIC # Visit Diagnosis Cleaning
# MAGIC
# MAGIC Key points from Ivy's Analysis:
# MAGIC - Data has 43,555,719 rows with 16 variables
# MAGIC - High proportion of missing data
# MAGIC - patientuid will not be unique given the nature of the data
# MAGIC
# MAGIC Primary variables to clean consist of: encountertypecode, encountertypetext, encounterstartdate, and encounterenddate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE cdh_abfm_phi.visit

# COMMAND ----------

# Saving data to python dataframe
V = spark.table("cdh_abfm_phi.visit")
V.count() # 43,555,719 observations present

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------

display(
 percent_full(V)   
)

# COMMAND ----------

# change to isNull, get percent missing
from pyspark.sql.functions import isnan, when, count, col
num_nulls = V.select([count(when(col(c).isNull(), c)).alias(c) for c in V.columns])
num_nulls.show()

# Null values
#     patientuid                  100.0% full
#     encountertypecode           89.11% full *key variable
#     encountertypetext           57.69% full *key variable
#     encounterstartdate          100.0% full *key variable
#     encounterenddate            15.12% full *key variable
#     serviceprovidernpi          99.99% full 
#     serviceproviderrolecode     77.65% full 
#     serviceproviderroletext     01.61% full
#     reasonforvisit              78.80% full
#     service_location_city       98.78% full
#     service_location_state      98.37% full
#     service_location_postalcode 90.03% full
#     encounter_status            89.79% full
#     note                        78.80% full
#     chiefcomplaint              00.01% full
#     practiceid                  99.99% full

# 43,555,719

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdh_abfm_phi.visit

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Type Code and Text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT encountertypecode) FROM cdh_abfm_phi.visit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT LOWER(encountertypecode)) FROM cdh_abfm_phi.visit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT encountertypecode FROM cdh_abfm_phi.visit
# MAGIC
# MAGIC -- Great deal of inconsistency among encounter type codes
# MAGIC -- 239,785 distinct values and no proper format is obvious. Would need more information from vendor to pass judgement

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT encountertypetext), count(DISTINCT LOWER(encountertypetext)) FROM cdh_abfm_phi.visit
# MAGIC
# MAGIC -- There are fewer distinct texts than distinct codes; I've never seen that before.
# MAGIC -- This is a good sign that the encountertypecode variable will be unhelpful, and will likely require extensive cleaning.
# MAGIC -- Fewer distinct values may be due to this variable missing more values than the code variable

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Start and End Date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT encounterstartdate FROM cdh_abfm_phi.visit 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   max(encounterstartdate) AS Latest, 
# MAGIC   min(encounterstartdate) AS Earliest 
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC
# MAGIC -- Erroneous date data and inconsistant formatting detected

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    substr(encounterstartdate,1,4), count(encounterstartdate)
# MAGIC FROM 
# MAGIC   cdh_abfm_phi.visit 
# MAGIC WHERE
# MAGIC   CAST(substr(encounterstartdate,1,4) AS int) > 2022
# MAGIC GROUP BY
# MAGIC   substr(encounterstartdate,1,4)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    encounterstartdate
# MAGIC FROM 
# MAGIC   cdh_abfm_phi.visit 
# MAGIC WHERE
# MAGIC   CAST(substr(encounterstartdate,1,4) AS int) > 2022

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   max(encounterstartdate) AS Latest, 
# MAGIC   min(encounterstartdate) AS Earliest 
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC WHERE SUBSTR(encounterstartdate, -3, 3) == "UTC" -- Only consider dates in UTC
# MAGIC AND encounterstartdate < CURRENT_DATE -- No dates beyond today
# MAGIC
# MAGIC -- All dates fall within reasonable window

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(encounterstartdate)
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC WHERE SUBSTR(encounterstartdate, -3, 3) == "UTC"
# MAGIC AND encounterstartdate < CURRENT_DATE
# MAGIC
# MAGIC -- 99.995% of values are preserved with the previous constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(encounterenddate)
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC WHERE CAST(SUBSTR(encounterenddate,1,4) as int) < 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT encounterenddate, count(encounterenddate)
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC WHERE CAST(SUBSTR(encounterenddate,1,4) as int) < 2018
# MAGIC GROUP BY encounterenddate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   max(encounterenddate) AS Latest, 
# MAGIC   min(encounterenddate) AS Earliest 
# MAGIC FROM cdh_abfm_phi.visit
# MAGIC WHERE SUBSTR(encounterenddate, -3, 3) == "UTC"
# MAGIC AND encounterenddate < CURRENT_DATE
# MAGIC AND encounterenddate IS NOT NULL
# MAGIC
# MAGIC -- Finding range while imposing all of the filters from the previous variable
# MAGIC -- Checking that the variable is not null since this variable is only 15% full
# MAGIC -- ealiest end date is earlier than earliest start date, likely and error

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(encounterenddate)
# MAGIC FROM cdh_abfm_phi.visit 
# MAGIC WHERE SUBSTR(encounterenddate, -3, 3) == "UTC"
# MAGIC AND encounterenddate < CURRENT_DATE
# MAGIC AND '2018-01-01 00:00:00 UTC' <= encounterenddate -- no end dates before the earliest start date
# MAGIC AND encounterenddate IS NOT NULL
# MAGIC
# MAGIC -- 14.65% of values are preserved with the previous constraints (15.12% were null to begin with)

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC - Over 200,000 distinct encounter type codes with no obvious consistant format. Would need more infomation on these codes to decern anything useful
# MAGIC - Encounter text has fewer distinct values than code, though that may be because it has more missing values, recommend converting it to lower case for consistancy
# MAGIC - Recommend removing any dates past the current date and only considering those in UTC, less than 0.5% of data is lost for either date variable
# MAGIC - Encounter end date is mostly empty, which may or may not indicate that most of these encounters are ongoing. End date is expected to be the same as start date for outpatient visits, but ABFM is mostly impatient data, so that is not particularly helpful
# MAGIC - Consider removing an end dates earlier than the earliest start date

# COMMAND ----------

display(
    V
    .groupBy(_clean_input('encountertypecode'))
    .agg(
        F.collect_set(_clean_input('encountertypetext',remove_space_char=False)),
        F.count('encountertypecode').alias('count')
    )
    .orderBy(F.col('count').desc())
)

# COMMAND ----------

display(
    V.filter('encountertypecode = "0004A"')
)

# COMMAND ----------

display(
    V.filter('encountertypecode = "0012A"')
)

# COMMAND ----------

display(
    V
    .groupBy(_clean_input('encountertypecode'), _clean_input('encountertypetext',remove_space_char=False))
    .count()
    .withColumn("count_", F.array("count", "encountertypetext"))
    .groupby("encountertypecode")
    .agg(
        F.max("count_").getItem(1).alias("most_common_text"),
        F.max("count_").getItem(0).cast('int').alias("f")
    )
    .orderBy(F.col('f').desc())
)

# COMMAND ----------


