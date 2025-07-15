# Databricks notebook source
# MAGIC %md
# MAGIC # Proposed Cleaning for the ABFM Patient Table
# MAGIC
# MAGIC As Cameron identified in his analysis, the main problem with this dataset is high redundancy with the patientuid variable mostly caused by inconsistant formatting of other variables.

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------

patient_table = spark.table('cdh_abfm_phi.patient')

# COMMAND ----------

col_names = patient_table.columns
col_names

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## How full is the data

# COMMAND ----------

display(
    percent_full(patient_table)
)

# COMMAND ----------



# COMMAND ----------

display(
    patient_table
    .groupBy(F.lower(F.col('statecode')))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Do any patients have more than 1 state listed in the table?

# COMMAND ----------

display(
    patient_table
    .transform(clean_state('statecode'))
    .groupBy('patientuid')
    .agg(
        F.collect_set('statecode').alias('state_set'),
    )
    .select('*',F.size('state_set').alias('num_states'))
    .filter('num_states > 1')
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Do any patients have more than one date of birth?

# COMMAND ----------

display(
    patient_table
    .groupBy('patientuid')
    .agg(
        *[F.collect_set(x) for x in col_names[1:]]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect patient that has more than two dobs listed

# COMMAND ----------

display(
    patient_table.filter('patientuid == "0110f8f0-b375-4a16-bb30-24de67667324"')
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Patientuid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT patientuid) FROM cdh_abfm_phi.patient
# MAGIC
# MAGIC -- About 28.6% of the data is reduendant
# MAGIC -- Consistant formatting my help to filter out some redundant values

# COMMAND ----------

# MAGIC %md
# MAGIC # DoB

# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F

# The following code was written by Cameron

patient_table = (patient_table
  .withColumn(
    "age",
    (F.datediff(
      F.current_date(),
      F.to_date(F.col("dob"))
    )/365).cast("integer")
  )
)

# converting DoB variable to age for formatting consistantcy

# COMMAND ----------

import pyspark.sql.window as W

win = W.Window.partitionBy('age')

display(
patient_table.groupBy('age').count().orderBy('age')
)

# COMMAND ----------

display(
    patient_table.filter('age == 222')
)

# COMMAND ----------

#Removing patients born after the current date
patient_table = patient_table[patient_table["age"] >= 0]
patient_table.count()

# Less than 200 observations lost

# COMMAND ----------

# MAGIC %md
# MAGIC # Gender

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT gender FROM cdh_abfm_phi.patient
# MAGIC
# MAGIC -- Many nonsensical genders recorded

# COMMAND ----------

display(
    patient_table
    .transform(clean_gender())
    .groupBy('gender')
    .count()
)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# The following code was written by Cameron

patient_table = (patient_table
  .withColumn(
    "gender__clean",
    F.upper(F.col("gender"))
  )
  .withColumn(
    "gender__clean",
    F.when((F.col("gender__clean") == "FEMALE") | (F.col("gender__clean") == "F"), "F")
      .when((F.col("gender__clean") == "MALE") | (F.col("gender__clean") == "M"), "M")
      .otherwise("U")
  )
)

# COMMAND ----------

gender_counts = patient_table.groupBy(F.col("gender__clean")).count().sort("count", ascending=False)
 
display(gender_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC # isDeceased

# COMMAND ----------

patient_table.filter(patient_table.isdeceased.isNull()).count()

# COMMAND ----------

death_counts = patient_table.groupBy(F.col("isDeceased")).count().sort("count", ascending=False)
 
display(death_counts)

# isDeceased variable looks good, no cleaning needed

# COMMAND ----------

# Saving isDeceased as a new variable for the sake of keeping the original data and clean data stored separately
patient_table = (patient_table
  .withColumn("isDeceased__clean",
    F.col("isDeceased")
))

# COMMAND ----------

# MAGIC %md
# MAGIC # State and State code

# COMMAND ----------

state_counts = patient_table.groupBy(F.upper(F.col("state"))).count().sort("count", ascending=True)
display(state_counts)

# A lot of inconsistant formatting among state names

# COMMAND ----------

state_counts = patient_table.groupBy(F.upper(F.col("statecode"))).count().sort("count", ascending=True)
display(state_counts)

# State abbriviation have inconsistencies as well

# COMMAND ----------

display(patient_table.filter('statecode == "O;"'))

# COMMAND ----------

# Reformatting the state code

patient_table = (patient_table
  .withColumn(
    "statecode__clean",
    F.upper(F.col("statecode"))
    )
  .withColumn(
    "statecode__clean",
    F.when((F.col("statecode__clean") == "ALABAMA") | (F.col("statecode__clean") == "AL"), "AL")
      .when((F.col("statecode__clean") == "ALASKA") | (F.col("statecode__clean") == "AK"), "AK")
      .when((F.col("statecode__clean") == "ARIZONA") | (F.col("statecode__clean") == "AZ"), "AZ")
      .when((F.col("statecode__clean") == "ARKANSAS") | (F.col("statecode__clean") == "AR"), "AR")
      .when((F.col("statecode__clean") == "CALIFORNIA") | (F.col("statecode__clean") == "CA"), "CA")
      .when((F.col("statecode__clean") == "COLORADO") | (F.col("statecode__clean") == "CO"), "CO")
      .when((F.col("statecode__clean") == "CONNECTICUT") | (F.col("statecode__clean") == "CT"), "CT")
      .when((F.col("statecode__clean") == "DELAWARE") | (F.col("statecode__clean") == "DE"), "DE")
      .when((F.col("statecode__clean") == "FLORIDA") | (F.col("statecode__clean") == "FL"), "FL")
      .when((F.col("statecode__clean") == "GEORGIA") | (F.col("statecode__clean") == "GA"), "GA")
      .when((F.col("statecode__clean") == "HAWAII") | (F.col("statecode__clean") == "HI"), "HI")
      .when((F.col("statecode__clean") == "IDAHO") | (F.col("statecode__clean") == "ID"), "ID")
      .when((F.col("statecode__clean") == "ILLINOIS") | (F.col("statecode__clean") == "IL"), "IL")
      .when((F.col("statecode__clean") == "INDIANA") | (F.col("statecode__clean") == "IN"), "IN")
      .when((F.col("statecode__clean") == "IOWA") | (F.col("statecode__clean") == "IA"), "IA")
      .when((F.col("statecode__clean") == "KANSAS") | (F.col("statecode__clean") == "KS"), "KS")
      .when((F.col("statecode__clean") == "KENTUCKY") | (F.col("statecode__clean") == "KY"), "KY")
      .when((F.col("statecode__clean") == "LOUISIANA") | (F.col("statecode__clean") == "LA"), "LA")
      .when((F.col("statecode__clean") == "MAINE") | (F.col("statecode__clean") == "ME"), "ME")
      .when((F.col("statecode__clean") == "MARYLAND") | (F.col("statecode__clean") == "MD"), "MD")
      .when((F.col("statecode__clean") == "MASSACHUSETTS") | (F.col("statecode__clean") == "MA"), "MA")
      .when((F.col("statecode__clean") == "MICHIGAN") | (F.col("statecode__clean") == "MI"), "MI")
      .when((F.col("statecode__clean") == "MINNESOTA") | (F.col("statecode__clean") == "MN"), "MN")
      .when((F.col("statecode__clean") == "MISSISSIPPI") | (F.col("statecode__clean") == "MS"), "MS")
      .when((F.col("statecode__clean") == "MISSOURI") | (F.col("statecode__clean") == "MO"), "MO")
      .when((F.col("statecode__clean") == "MONTANA") | (F.col("statecode__clean") == "MT"), "MT")
      .when((F.col("statecode__clean") == "NEBRASKA") | (F.col("statecode__clean") == "NE"), "NE")
      .when((F.col("statecode__clean") == "NEVADA") | (F.col("statecode__clean") == "NV"), "NV")
      .when((F.col("statecode__clean") == "NEW HAMPSHIRE") | (F.col("statecode__clean") == "NH"), "NH")
      .when((F.col("statecode__clean") == "NEW JERSEY") | (F.col("statecode__clean") == "NJ"), "NJ")
      .when((F.col("statecode__clean") == "NEW MEXICO") | (F.col("statecode__clean") == "NM"), "NM")
      .when((F.col("statecode__clean") == "NEW YORK") | (F.col("statecode__clean") == "NY"), "NY")
      .when((F.col("statecode__clean") == "NORTH CAROLINA") | (F.col("statecode__clean") == "NC"), "NC")
      .when((F.col("statecode__clean") == "NORTH DAKOTA") | (F.col("statecode__clean") == "ND"), "ND")
      .when((F.col("statecode__clean") == "OHIO") | (F.col("statecode__clean") == "OH"), "OH")
      .when((F.col("statecode__clean") == "OKLAHOMA") | (F.col("statecode__clean") == "OK"), "OK")
      .when((F.col("statecode__clean") == "OREGON") | (F.col("statecode__clean") == "OR"), "OR")
      .when((F.col("statecode__clean") == "PENNSYLVANIA") | (F.col("statecode__clean") == "PA"), "PA")
      .when((F.col("statecode__clean") == "RHODE ISLAND") | (F.col("statecode__clean") == "RI"), "RI")
      .when((F.col("statecode__clean") == "SOUTH CAROLINA") | (F.col("statecode__clean") == "SC"), "SC")
      .when((F.col("statecode__clean") == "SOUTH DAKOTA") | (F.col("statecode__clean") == "SD"), "SD")
      .when((F.col("statecode__clean") == "TENNESSEE") | (F.col("statecode__clean") == "TN"), "TN")
      .when((F.col("statecode__clean") == "TEXAS") | (F.col("statecode__clean") == "TX"), "TX")
      .when((F.col("statecode__clean") == "UTAH") | (F.col("statecode__clean") == "UT"), "UT")
      .when((F.col("statecode__clean") == "VERMONT") | (F.col("statecode__clean") == "VT"), "VT")
      .when((F.col("statecode__clean") == "VIRGINIA") | (F.col("statecode__clean") == "VA"), "VA")
      .when((F.col("statecode__clean") == "WASHINGTON") | (F.col("statecode__clean") == "WA"), "WA")
      .when((F.col("statecode__clean") == "WEST VIRGINIA") | (F.col("statecode__clean") == "WV"), "WV")
      .when((F.col("statecode__clean") == "WISCONSIN") | (F.col("statecode__clean") == "WI"), "WI")
      .when((F.col("statecode__clean") == "WYOMING") | (F.col("statecode__clean") == "WY"), "WY")
      .when(F.col("statecode__clean").isNull() == True, None)
      .otherwise("OTHER")
  )
)

# COMMAND ----------

# Reformatting the state name
# Using formatted state code to fill in missing state names

patient_table = (patient_table
  .withColumn(
    "state__clean",
    F.upper(F.col("state"))
  )
  .withColumn(
    "state__clean",
    F.when((F.col("state__clean") == "ALABAMA") | (F.col("state__clean") == "AL") | (F.col("statecode__clean") == "AL"), "Alabama")
      .when((F.col("state__clean") == "ALASKA") | (F.col("state__clean") == "AK") | (F.col("statecode__clean") == "AK"), "Alaska")
      .when((F.col("state__clean") == "ARIZONA") | (F.col("state__clean") == "AZ") | (F.col("statecode__clean") == "AZ"), "Arizona")
      .when((F.col("state__clean") == "ARKANSAS") | (F.col("state__clean") == "AR") | (F.col("statecode__clean") == "AR"), "Arizona")
      .when((F.col("state__clean") == "CALIFORNIA") | (F.col("state__clean") == "CA") | (F.col("statecode__clean") == "CA"), "California")
      .when((F.col("state__clean") == "COLORADO") | (F.col("state__clean") == "CO") | (F.col("statecode__clean") == "CO"), "Colorado")
      .when((F.col("state__clean") == "CONNECTICUT") | (F.col("state__clean") == "CT") | (F.col("statecode__clean") == "CT"), "Connecticut")
      .when((F.col("state__clean") == "DELAWARE") | (F.col("state__clean") == "DE") | (F.col("statecode__clean") == "DE"), "Delaware")
      .when((F.col("state__clean") == "FLORIDA") | (F.col("state__clean") == "FL") | (F.col("statecode__clean") == "FL"), "Florida")
      .when((F.col("state__clean") == "GEORGIA") | (F.col("state__clean") == "GA") | (F.col("statecode__clean") == "GA"), "Georgia")
      .when((F.col("state__clean") == "HAWAII") | (F.col("state__clean") == "HI") | (F.col("statecode__clean") == "HI"), "Hawaii")
      .when((F.col("state__clean") == "IDAHO") | (F.col("state__clean") == "ID") | (F.col("statecode__clean") == "ID"), "Idaho")
      .when((F.col("state__clean") == "ILLINOIS") | (F.col("state__clean") == "IL") | (F.col("statecode__clean") == "IL"), "Illinois")
      .when((F.col("state__clean") == "INDIANA") | (F.col("state__clean") == "IN") | (F.col("statecode__clean") == "IN"), "Indiana")
      .when((F.col("state__clean") == "IOWA") | (F.col("state__clean") == "IA") | (F.col("statecode__clean") == "IA"), "Iowa")
      .when((F.col("state__clean") == "KANSAS") | (F.col("state__clean") == "KS") | (F.col("statecode__clean") == "KS"), "Kansas")
      .when((F.col("state__clean") == "KENTUCKY") | (F.col("state__clean") == "KY") | (F.col("statecode__clean") == "KY"), "Kentucky")
      .when((F.col("state__clean") == "LOUISIANA") | (F.col("state__clean") == "LA") | (F.col("statecode__clean") == "LA"), "Louisiana")
      .when((F.col("state__clean") == "MAINE") | (F.col("state__clean") == "ME") | (F.col("statecode__clean") == "ME"), "Maine")
      .when((F.col("state__clean") == "MARYLAND") | (F.col("state__clean") == "MD") | (F.col("statecode__clean") == "MD"), "Maryland")
      .when((F.col("state__clean") == "MASSACHUSETTS") | (F.col("state__clean") == "MA") | (F.col("statecode__clean") == "MA"), "Massachusetts")
      .when((F.col("state__clean") == "MICHIGAN") | (F.col("state__clean") == "MI") | (F.col("statecode__clean") == "MI"), "Michigan")
      .when((F.col("state__clean") == "MINNESOTA") | (F.col("state__clean") == "MN") | (F.col("statecode__clean") == "MN"), "Minnesota")
      .when((F.col("state__clean") == "MISSISSIPPI") | (F.col("state__clean") == "MS") | (F.col("statecode__clean") == "MS"), "Mississippi")
      .when((F.col("state__clean") == "MISSOURI") | (F.col("state__clean") == "MO") | (F.col("statecode__clean") == "MO"), "Missouri")
      .when((F.col("state__clean") == "MONTANA") | (F.col("state__clean") == "MT") | (F.col("statecode__clean") == "MT"), "Montana")
      .when((F.col("state__clean") == "NEBRASKA") | (F.col("state__clean") == "NE") | (F.col("statecode__clean") == "NE"), "Nebraska")
      .when((F.col("state__clean") == "NEVADA") | (F.col("state__clean") == "NV") | (F.col("statecode__clean") == "NV"), "Nevada")
      .when((F.col("state__clean") == "NEW HAMPSHIRE") | (F.col("state__clean") == "NH") | (F.col("statecode__clean") == "NH"), "New Hampshire")
      .when((F.col("state__clean") == "NEW JERSEY") | (F.col("state__clean") == "NJ") | (F.col("statecode__clean") == "NJ"), "New Jersey")
      .when((F.col("state__clean") == "NEW MEXICO") | (F.col("state__clean") == "NM") | (F.col("statecode__clean") == "NM"), "New Mexico")
      .when((F.col("state__clean") == "NEW YORK") | (F.col("state__clean") == "NY") | (F.col("statecode__clean") == "NY"), "New York")
      .when((F.col("state__clean") == "NORTH CAROLINA") | (F.col("state__clean") == "NC") | (F.col("statecode__clean") == "NC"), "North Carolina")
      .when((F.col("state__clean") == "NORTH DAKOTA") | (F.col("state__clean") == "ND") | (F.col("statecode__clean") == "ND"), "North Dakota")
      .when((F.col("state__clean") == "OHIO") | (F.col("state__clean") == "OH") | (F.col("statecode__clean") == "OH"), "Ohio")
      .when((F.col("state__clean") == "OKLAHOMA") | (F.col("state__clean") == "OK") | (F.col("statecode__clean") == "OK"), "Oklahoma")
      .when((F.col("state__clean") == "OREGON") | (F.col("state__clean") == "OR") | (F.col("statecode__clean") == "OR"), "Oregon")
      .when((F.col("state__clean") == "PENNSYLVANIA") | (F.col("state__clean") == "PA") | (F.col("statecode__clean") == "PA"), "Pennsylvania")
      .when((F.col("state__clean") == "RHODE ISLAND") | (F.col("state__clean") == "RI") | (F.col("statecode__clean") == "RI"), "Rhode Island")
      .when((F.col("state__clean") == "SOUTH CAROLINA") | (F.col("state__clean") == "SC") | (F.col("statecode__clean") == "SC"), "South Carolina")
      .when((F.col("state__clean") == "SOUTH DAKOTA") | (F.col("state__clean") == "SD") | (F.col("statecode__clean") == "SD"), "South Dakota")
      .when((F.col("state__clean") == "TENNESSEE") | (F.col("state__clean") == "TN") | (F.col("statecode__clean") == "TN"), "Tennessee")
      .when((F.col("state__clean") == "TEXAS") | (F.col("state__clean") == "TX") | (F.col("statecode__clean") == "TX"), "Texas")
      .when((F.col("state__clean") == "UTAH") | (F.col("state__clean") == "UT") | (F.col("statecode__clean") == "UT"), "Utah")
      .when((F.col("state__clean") == "VERMONT") | (F.col("state__clean") == "VT") | (F.col("statecode__clean") == "VT"), "Vermont")
      .when((F.col("state__clean") == "VIRGINIA") | (F.col("state__clean") == "VA") | (F.col("statecode__clean") == "VA"), "Virginia")
      .when((F.col("state__clean") == "WASHINGTON") | (F.col("state__clean") == "WA") | (F.col("statecode__clean") == "WA"), "Washington")
      .when((F.col("state__clean") == "WEST VIRGINIA") | (F.col("state__clean") == "WV") | (F.col("statecode__clean") == "WV"), "West Virginia")
      .when((F.col("state__clean") == "WISCONSIN") | (F.col("state__clean") == "WI") | (F.col("statecode__clean") == "WI"), "Wisconsin")
      .when((F.col("state__clean") == "WYOMING") | (F.col("state__clean") == "WY") | (F.col("statecode__clean") == "WY"), "Wyoming")
      .when(F.col("state__clean").isNull() == True, None)
      .otherwise("OTHER")
  )
)

# COMMAND ----------

state_counts = patient_table.groupBy(F.upper(F.col("state__clean"))).count().sort("count", ascending=True)
display(state_counts)

# State name variable looks a lot better

# COMMAND ----------

# Using formatted state code to fill in missing state names

patient_table = (patient_table
  .withColumn(
    "statecode__clean",
    F.col("statecode__clean")
    )
  .withColumn(
    "statecode__clean",
    F.when((F.col("state__clean") == "Alabama") | (F.col("statecode__clean") == "AL"), "AL")
      .when((F.col("state__clean") == "Alaska") | (F.col("statecode__clean") == "AK"), "AK")
      .when((F.col("state__clean") == "Arizona") | (F.col("statecode__clean") == "AZ"), "AZ")
      .when((F.col("state__clean") == "Arkansas") | (F.col("statecode__clean") == "AR"), "AR")
      .when((F.col("state__clean") == "California") | (F.col("statecode__clean") == "CA"), "CA")
      .when((F.col("state__clean") == "Colorado") | (F.col("statecode__clean") == "CO"), "CO")
      .when((F.col("state__clean") == "Connecticut") | (F.col("statecode__clean") == "CT"), "CT")
      .when((F.col("state__clean") == "Delaware") | (F.col("statecode__clean") == "DE"), "DE")
      .when((F.col("state__clean") == "Florida") | (F.col("statecode__clean") == "FL"), "FL")
      .when((F.col("state__clean") == "Georgia") | (F.col("statecode__clean") == "GA"), "GA")
      .when((F.col("state__clean") == "Hawaii") | (F.col("statecode__clean") == "HI"), "HI")
      .when((F.col("state__clean") == "Idaho") | (F.col("statecode__clean") == "ID"), "ID")
      .when((F.col("state__clean") == "Illinois") | (F.col("statecode__clean") == "IL"), "IL")
      .when((F.col("state__clean") == "Indiana") | (F.col("statecode__clean") == "IN"), "IN")
      .when((F.col("state__clean") == "Iowa") | (F.col("statecode__clean") == "IA"), "IA")
      .when((F.col("state__clean") == "Kansas") | (F.col("statecode__clean") == "KS"), "KS")
      .when((F.col("state__clean") == "Kentucky") | (F.col("statecode__clean") == "KY"), "KY")
      .when((F.col("state__clean") == "Louisiana") | (F.col("statecode__clean") == "LA"), "LA")
      .when((F.col("state__clean") == "Maine") | (F.col("statecode__clean") == "ME"), "ME")
      .when((F.col("state__clean") == "Maryland") | (F.col("statecode__clean") == "MD"), "MD")
      .when((F.col("state__clean") == "Massachusetts") | (F.col("statecode__clean") == "MA"), "MA")
      .when((F.col("state__clean") == "Michigan") | (F.col("statecode__clean") == "MI"), "MI")
      .when((F.col("state__clean") == "Minnesota") | (F.col("statecode__clean") == "MN"), "MN")
      .when((F.col("state__clean") == "Mississippi") | (F.col("statecode__clean") == "MS"), "MS")
      .when((F.col("state__clean") == "Missouri") | (F.col("statecode__clean") == "MO"), "MO")
      .when((F.col("state__clean") == "Montana") | (F.col("statecode__clean") == "MT"), "MT")
      .when((F.col("state__clean") == "Nebraska") | (F.col("statecode__clean") == "NE"), "NE")
      .when((F.col("state__clean") == "Nevada") | (F.col("statecode__clean") == "NV"), "NV")
      .when((F.col("state__clean") == "New Hampshire") | (F.col("statecode__clean") == "NH"), "NH")
      .when((F.col("state__clean") == "New Jersey") | (F.col("statecode__clean") == "NJ"), "NJ")
      .when((F.col("state__clean") == "New Mexico") | (F.col("statecode__clean") == "NM"), "NM")
      .when((F.col("state__clean") == "New York") | (F.col("statecode__clean") == "NY"), "NY")
      .when((F.col("state__clean") == "North Carolina") | (F.col("statecode__clean") == "NC"), "NC")
      .when((F.col("state__clean") == "North Dakota") | (F.col("statecode__clean") == "ND"), "ND")
      .when((F.col("state__clean") == "Ohio") | (F.col("statecode__clean") == "OH"), "OH")
      .when((F.col("state__clean") == "Oklahoma") | (F.col("statecode__clean") == "OK"), "OK")
      .when((F.col("state__clean") == "Oregon") | (F.col("statecode__clean") == "OR"), "OR")
      .when((F.col("state__clean") == "Pennsylvania") | (F.col("statecode__clean") == "PA"), "PA")
      .when((F.col("state__clean") == "Rhode Island") | (F.col("statecode__clean") == "RI"), "RI")
      .when((F.col("state__clean") == "South Carolina") | (F.col("statecode__clean") == "SC"), "SC")
      .when((F.col("state__clean") == "South Dakota") | (F.col("statecode__clean") == "SD"), "SD")
      .when((F.col("state__clean") == "Tennessee") | (F.col("statecode__clean") == "TN"), "TN")
      .when((F.col("state__clean") == "Texas") | (F.col("statecode__clean") == "TX"), "TX")
      .when((F.col("state__clean") == "Utah") | (F.col("statecode__clean") == "UT"), "UT")
      .when((F.col("state__clean") == "Vermont") | (F.col("statecode__clean") == "VT"), "VT")
      .when((F.col("state__clean") == "Virginia") | (F.col("statecode__clean") == "VA"), "VA")
      .when((F.col("state__clean") == "Washington") | (F.col("statecode__clean") == "WA"), "WA")
      .when((F.col("state__clean") == "West Virginia") | (F.col("statecode__clean") == "WV"), "WV")
      .when((F.col("state__clean") == "Wisconsin") | (F.col("statecode__clean") == "WI"), "WI")
      .when((F.col("state__clean") == "Wyoming") | (F.col("statecode__clean") == "WY"), "WY")
      .when(F.col("statecode__clean").isNull() == True, None)
      .otherwise("OTHER")
  )
)

# COMMAND ----------

state_counts = patient_table.groupBy(F.upper(F.col("statecode__clean"))).count().sort("count", ascending=True)
display(state_counts)

# State code looks better as well

# COMMAND ----------

# MAGIC %md
# MAGIC # Country and Country code

# COMMAND ----------

patient_table

# COMMAND ----------

display(
    patient_table.groupBy('countrycode').agg(F.collect_set('country'))
)

# COMMAND ----------

display(patient_table.filter('LOWER(countrycode) == "clearwater"'))

# COMMAND ----------

display(patient_table.filter('LOWER(countrycode) == "mckenzie"'))

# COMMAND ----------

display(patient_table.filter('LOWER(countrycode) == "309221"'))

# COMMAND ----------

display(patient_table.filter('LOWER(countrycode) == "lewis and clark"'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Marital Status

# COMMAND ----------

display(
    patient_table.groupBy(F.lower(F.col('maritalstatustext'))).agg(F.collect_set('maritalstatustext'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Zipcode

# COMMAND ----------

display(
    patient_table
    .groupBy(F.length('zipcode'))
    .agg(
        F.collect_set('zipcode')
    )
)

# COMMAND ----------

from pyspark.sql.functions import substring

# Reformatting the zip code variable for the sake of consistancy

patient_table = (patient_table
  .withColumn( "zipcode__clean", substring("zipcode", 1, 5))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # City

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT LOWER(city) FROM cdh_abfm_phi.patient
# MAGIC
# MAGIC -- Even with alligned cases, the city variable has >1,000 distinct values; not feasible for manual cleaning

# COMMAND ----------

# Reformatting the city variable for consistency

patient_table = (patient_table
  .withColumn("city__clean",
    F.lower(F.col("city"))
))

# COMMAND ----------

# MAGIC %md
# MAGIC # Religious Affiliation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT LOWER(religiousaffiliationtext) FROM cdh_abfm_phi.patient
# MAGIC
# MAGIC -- Over 1,000 distinct values, also causes duplicates for patientuid; I think this variable is more trouble than it's worth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   cdh_abfm_phi.patient
# MAGIC WHERE
# MAGIC   LOWER(religiousaffiliationtext) = "grove oak"

# COMMAND ----------

# MAGIC %md
# MAGIC # Practiceid

# COMMAND ----------

# Practiceid is int with 0% missing values, cleaning not necessary

patient_table = (patient_table
  .withColumn("practiceid__clean",
    F.col("practiceid")
))

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaned Table

# COMMAND ----------

# Snapshop of old and new values
display(patient_table.limit(50))

# COMMAND ----------

# New dataset consisting of only cleaned variables
clean_patient_table = patient_table[["patientuid","age","gender__clean", "isDeceased__clean", "zipcode__clean", "city__clean", "countrycode__clean", "country__clean", "statecode__clean", "state__clean", "maritalstatustext__clean", "practiceid__clean"]]

# Religious affiliation dropped due to being mostly empty and irrelevant
# Serviceprovidernpi dropped for being over 99% empty

display(clean_patient_table.limit(50))

# COMMAND ----------

clean_patient_table = clean_patient_table.distinct()
clean_patient_table.count()

# Remember, number of obs removed up to this point is negligable
# 9,320,534 - 6,990,145 = 2,330,389 redundent ids removed
# 6,990,145 - 6,655,464 = 334,681 redundent values remain

# COMMAND ----------

# MAGIC %md
# MAGIC # In Conclusion
# MAGIC
# MAGIC - Data is largely redundant without cleaning, consistant fromatting lessens redundancy.
# MAGIC - I have some vendor questions regarding the marital status variable.
# MAGIC - There is some erroneous DoB data.
# MAGIC - Religious affiliation and serviceprovidernpi variables are mostly empty and lead to redundancy.
# MAGIC - It may be possible to remove more redundant IDs if more variables were excluded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cast(substring_index(dob, "-", 1) AS int) AS vYear,
# MAGIC   count(dob)
# MAGIC FROM 
# MAGIC   cdh_abfm_phi.patient
# MAGIC GROUP BY
# MAGIC   cast(substring_index(dob, "-", 1) AS int)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(dob) AS count
# MAGIC FROM 
# MAGIC   cdh_abfm_phi.patient
# MAGIC WHERE
# MAGIC   substring_index(dob, "-", 1) > 2022

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(dob) AS count
# MAGIC FROM 
# MAGIC   cdh_abfm_phi.patient
# MAGIC WHERE
# MAGIC   substring_index(dob, "-", 1) < 1922

# COMMAND ----------


