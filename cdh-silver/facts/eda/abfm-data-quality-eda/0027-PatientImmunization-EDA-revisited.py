# Databricks notebook source
immun_table = spark.table("cdh_abfm_phi.patientimmunization")

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-utils

# COMMAND ----------

display(
  percent_not_empty(immun_table)
)

# COMMAND ----------

display(
  column_profile(immun_table)
)

# COMMAND ----------

display(immun_table.groupBy('maxdosequantity').count())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select B.Year, count(B.Year) from 
# MAGIC (
# MAGIC SELECT immunizationstartdate, CAST(SUBSTR(immunizationstartdate,1,4) as int) as Year
# MAGIC FROM cdh_abfm_phi.patientimmunization 
# MAGIC WHERE CAST(SUBSTR(immunizationstartdate,1,4) as int) > 2021
# MAGIC ) as B
# MAGIC GROUP BY B.Year; 

# COMMAND ----------

display(
  immun_table.select('immunizationcategory')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Schema Analysis
# MAGIC
# MAGIC Current table schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE cdh_abfm_phi.patientimmunization

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Schema reported in the data dictionary:
# MAGIC
# MAGIC patientuid, immunizationname, immunizationcode, immunizationcategory, immunizationstartdate, immunizationstopdate, administeredeffectivedate, repeatnumber, medicationroutecode, medicationroutetext, strength, strengthunitcode, strengthunittext, medicationproductformtext, procedurecode, proceduretext, negationind, medicationbrandname, medicationgenericname, maxdosequantity, dispensingtimeofsupply, dispensingdosequantity, servicelocationname, medicationindicationcriteria, medicationindicationproblemcode, medicationindicationproblemtext, medicationseriesnumber, medicationreactionproblemcode, medicationreactionproblemtext, medicationstatuscode, medicationstatustext, medicinemanufacturer, medicineid, fillerordernumber, enteringorganization, substancelotnumber, substanceexpirationdate, administrationsite, placerordernumber, visitkey, sourceidentifier, maxdate, sourcetype, practiceideidentifier, labaccessionnumber, patientimmunizationkey, patientimmunizationname, patientimmunizationcode, mastermedicationroutetext, mastermedicationindicationproblemtext, mastermedicationstatustext, createddate, modifieddate, practiceid 

# COMMAND ----------

data_dict = {"patientuid", "immunizationname", "immunizationcode", "immunizationcategory", "immunizationstartdate", "immunizationstopdate", "administeredeffectivedate", "repeatnumber", "medicationroutecode", "medicationroutetext", "strength", "strengthunitcode", "strengthunittext", "medicationproductformtext", "procedurecode", "proceduretext", "negationind", "medicationbrandname", "medicationgenericname", "maxdosequantity", "dispensingtimeofsupply", "dispensingdosequantity", "servicelocationname", "medicationindicationcriteria", "medicationindicationproblemcode", "medicationindicationproblemtext", "medicationseriesnumber", "medicationreactionproblemcode", "medicationreactionproblemtext", "medicationstatuscode", "medicationstatustext", "medicinemanufacturer", "medicineid", "fillerordernumber", "enteringorganization", "substancelotnumber", "substanceexpirationdate", "administrationsite", "placerordernumber", "visitkey", "sourceidentifier", "maxdate", "sourcetype", "practiceideidentifier", "labaccessionnumber", "patientimmunizationkey", "patientimmunizationname", "patientimmunizationcode", "mastermedicationroutetext", "mastermedicationindicationproblemtext", "mastermedicationstatustext", "createddate", "modifieddate", "practiceid"}

missing_columns = data_dict.difference(set(immun_table.columns))
new_columns = set(immun_table.columns).difference(data_dict)

print(f"Missing Columns ({len(missing_columns)}): {missing_columns}")
print(f"New Columns ({len(new_columns)}): {new_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Record Counts
# MAGIC
# MAGIC Total number of records in the table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(patientuid)
# MAGIC FROM
# MAGIC   cdh_abfm_phi.patientimmunization

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of unique patientuid present in this table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT patientuid)
# MAGIC FROM
# MAGIC   cdh_abfm_phi.patientimmunization

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A couple of observations:
# MAGIC - Many patients appear multiple times
# MAGIC - There aren't that many patients within this table.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Immunization Names
# MAGIC
# MAGIC Inspecting the immunizationname column to determine the various medicine/vaccine values present. Below, I calculate the counts of immunizationname - converted to uppercase.

# COMMAND ----------

name_count = immun_table.groupBy(F.upper(F.col("immunizationname")).alias('immunizationname')).count().sort("count", ascending=False)

display(name_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are 735 different values present within the data.
# MAGIC
# MAGIC Visualizing the immunizationname frequencies in a wordcloud. The larger the word, the more frequent the term in the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Immunization Categories
# MAGIC
# MAGIC Next we analyze the various immunization categories present in the data.

# COMMAND ----------

category_count = immun_table.groupBy(F.upper(F.col("immunizationcategory"))).count().sort("count", ascending=False)

display(category_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Time Series of Immunization
# MAGIC
# MAGIC Here I create a function to perform a regex string match on immunization name and present the time series counts (by month).
# MAGIC
# MAGIC NOTE: There are invalid datetime stored in the dateset. I have found the prescence of years greater than 2090. So in the following code, I filter by dates before the current_date.

# COMMAND ----------

def immunization_timeseries(immunization_name="FLUZONE"):
  immunization_subset = (
    immun_table
      .withColumn(
        "startdate_month",
        F.trunc(F.col("immunizationstartdate").astype("timestamp"), "month")
      )
      .filter(
        F.upper(F.col("immunizationname")).rlike(immunization_name)
      )
      .filter(
        F.col("startdate_month") < F.current_date()
      )
      .select(F.col("patientuid"), F.col("startdate_month"))
  )

  immunization_counts = (
    immunization_subset
      .groupBy(F.col("startdate_month"))
      .count()
      .orderBy(F.col("startdate_month"), ascending=True)
  )
  
  return immunization_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here I use the databricks widget tool to create a combobox field to make the following chart interactive.
# MAGIC
# MAGIC I prepopulate the widget with all unique immunization names (computed previously). However, the combo box allows you to search for any value.

# COMMAND ----------

name_count.select('immunizationname').toPandas().values

# COMMAND ----------

dbutils.widgets.combobox("ImmunizationName", "FLUZONE", ['FLUZONE', 'FLUZONE HIGH DOSE', 'COVID-19, MRNA, LNP-S, PF, 100 MCG/ 0.5 ML DOSE'])

# COMMAND ----------

display(immunization_timeseries(dbutils.widgets.get("ImmunizationName")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Procedure Code

# COMMAND ----------

procedurecode_count = immun_table.groupBy(F.upper(F.col("procedurecode"))).count().sort("count", ascending=False)

display(procedurecode_count)

# COMMAND ----------

display(
  immun_table
  .groupBy(F.upper(F.col("procedurecode")))
  .agg(
  F.collect_set('proceduretext'),
  F.collect_set('immunizationname'),
  F.count('procedurecode').alias('count')
)
  .sort("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Procedure Text

# COMMAND ----------

proceduretext_count = immun_table.groupBy(F.upper(F.col("proceduretext"))).count().sort("count", ascending=False)

display(proceduretext_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medicine ID

# COMMAND ----------

medicineid_count = immun_table.groupBy(F.upper(F.col("medicineid"))).count().sort("count", ascending=False)

display(medicineid_count)

# COMMAND ----------

display(
  immun_table
  .select(
    F.max('immunizationstartdate'),
    F.min('immunizationstartdate'),
    F.max('immunizationstopdate'),
    F.min('immunizationstopdate'),
    F.max('administeredeffectivedate'),
    F.min('administeredeffectivedate'),
  )
)

# COMMAND ----------

display(
  immun_table.filter(F.col('immunizationstartdate').startswith('9999'))
)

# COMMAND ----------

display(
immun_table.filter(F.upper(F.col("medicineid"))=='LOTNUMBER')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## administeredeffectivedate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select B.Year, count(B.Year) from 
# MAGIC (
# MAGIC SELECT administeredeffectivedate, CAST(SUBSTR(administeredeffectivedate,1,4) as int) as Year
# MAGIC FROM cdh_abfm_phi.patientimmunization 
# MAGIC WHERE CAST(SUBSTR(administeredeffectivedate,1,4) as int) > 2021
# MAGIC ) as B
# MAGIC GROUP BY B.Year; 
# MAGIC
# MAGIC select count(B.Year) from 
# MAGIC (
# MAGIC SELECT administeredeffectivedate, CAST(SUBSTR(administeredeffectivedate,1,4) as int) as Year
# MAGIC FROM cdh_abfm_phi.patientimmunization 
# MAGIC WHERE CAST(SUBSTR(administeredeffectivedate,1,4) as int) < 2018
# MAGIC ) as B

# COMMAND ----------

display(
    immun_table.filter(immun_table.immunizationstartdate.isNotNull() & immun_table.administeredeffectivedate.isNotNull())
)

# COMMAND ----------

display(
    immun_table
    .filter(immun_table.immunizationstartdate.isNotNull() & immun_table.administeredeffectivedate.isNotNull())
    .filter(immun_table.immunizationstartdate != immun_table.administeredeffectivedate)
)

# COMMAND ----------

display(
    immun_table
    .filter(immun_table.immunizationname.rlike('Moderna'))
)

# COMMAND ----------

df = immun_table

display(
  df
#   .withColumn('immunizationname', _clean_input(F.col('immunizationname'),remove_space_char=False))
#   .filter(F.col('immunizationname').rlike("covid|sars"))
#   .transform(clean_date('documentationdate'))
#   .transform(clean_date('problemonsetdate'))
#   .transform(clean_date('problemresolutiondate'))
         )

# COMMAND ----------

df = immun_table

display(
  df
#   .withColumn('immunizationname', _clean_input(F.col('immunizationname'),remove_space_char=False))
  .transform(clean_date('immunizationstartdate'))
  .transform(clean_date('immunizationstopdate'))
  .transform(clean_date('administeredeffectivedate'))
  .transform(clean_date('substanceexpirationdate'))
         )

# COMMAND ----------

display(
  immun_table.groupBy('medicinemanufacturer').count()  
)


# COMMAND ----------

(
  (y=1),
  x+1
)

# COMMAND ----------


