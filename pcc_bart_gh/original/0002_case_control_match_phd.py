# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## PCC Case-Control Matching (V1)
# MAGIC
# MAGIC A crude attempt at applying case-control matching algorithm, for the sake of testing.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Ka'imi Kahihikolo (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [rvn4@cdc.gov](rvn4@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC   
# MAGIC - Author: Adewole Oyalowo (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [sve5@cdc.gov](sve5@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W


# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES 

# COMMAND ----------

# Columns to match on
MATCHING_COLUMNS = ["age", "gender", "index_month", "io_flag"]

# Ration to match cases to controls
CASE_CONTROL_RATIO = 2

# COMMAND ----------

# Table created in `0001_create_hvid_cssr_tte_table_all_individuals` notebook
# Verified name change is correct (from wide_table_name to wide_output_name)
condition_claims_df = (
    spark.table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')
)
#RE_xwalk = spark.table(f"{DB_READ}.race_ethnicity_xwalk")

#test = (
#    spark.table(f'{DB_WRITE}.{WIDE_TABLE_NAME}')
#)

# COMMAND ----------

condition_claims_df = (
    condition_claims_df
        .filter(
            ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
            & F.col('age').between(0,64)
            & F.col('gender').isin('M','F')
            #& F.col('patient_state').isNotNull()
            & (F.col('days_enrolled') >= 1)
        )
)

# COMMAND ----------

# These counts are different than the old version
condition_claims_df.select("medrec_key").count()

# COMMAND ----------

# Old version save for check
#condition_claims_df.select('medrec_key').count()

# COMMAND ----------

# Filter for age < 65, 500 days of enrollment
# Convert patient_state to region
# Look up race/ethnicity information
condition_claims_df = (
    condition_claims_df
        .filter(
            ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
            & F.col('age').between(0,64)
            & F.col('gender').isin('M','F')
            #& F.col('patient_state').isNotNull()
            & (F.col('days_enrolled') >= 1)
        )
        .select(
            "medrec_key", "covid", "age", "gender", "prov_division", "RaceEth", "io_flag", F.date_trunc("MM", F.col("index_date")).alias("index_month"),
        )
)

# Define cases as covid == 1
cases = condition_claims_df.filter(F.col("covid") == 1).drop("covid")

# Define cases as covid == 0
controls = condition_claims_df.filter(F.col("covid") == 0).drop("covid")

# COMMAND ----------

condition_claims_df.select("medrec_key").count()

# COMMAND ----------

#condition_claims_df.select('medrec_key').count()

# COMMAND ----------

print(controls.select('medrec_key').count()) #41433682
print(cases.select('medrec_key').count()) #1835252

# COMMAND ----------

#controls.select('medrec_key').count() #41433682
# cases.select('medrec_key').count() #1835252
#675354

# COMMAND ----------

# Assigns a unique value of case_rn for every id in cases that has the same age and gender and region
case_type = (
    cases
        .select(
            F.col("medrec_key").alias("case_id"),
            *MATCHING_COLUMNS,
            F.row_number().over(
                W.Window.partitionBy(MATCHING_COLUMNS).orderBy("medrec_key")
            ).alias("case_rn")
        )
)

# Assigns a unique value to assigned_person_number for every id in controls that the same age and gender
control_person_number = (
    controls
        .select(
            F.col("medrec_key").alias("person_id"),
            *MATCHING_COLUMNS,
            F.row_number().over(
                W.Window.partitionBy(MATCHING_COLUMNS).orderBy(F.expr("uuid()"))
            ).alias("assigned_person_number")
        )
)

# COMMAND ----------

# display(case_type)

# COMMAND ----------

display(control_person_number)

# COMMAND ----------

# TODO: Convert `on` to accept MATCHING_COLUMNS list
# for each covariate (gender, index month, age), it assigns the first CASE_CONTROL_RATIO persons to the first case_rn
# the second CASE_CONTROL_RATIO to the second case_rn, etc.
# Since for each covariate each person gets a unique assigned_person_number, no person can end up in two groups.
matches = (
    case_type
        .join(
            control_person_number,
            on=(
                (control_person_number.gender == case_type.gender) &
                (control_person_number.index_month == case_type.index_month) &
                (control_person_number.age == case_type.age) &
                (control_person_number.io_flag == case_type.io_flag) &
#                 (control_person_number.prov_region == case_type.prov_region) &
#                 (control_person_number.RaceEth == case_type.RaceEth) &
                (
                    control_person_number.assigned_person_number
                    .between(CASE_CONTROL_RATIO*(F.col("case_rn") - 1)+1, CASE_CONTROL_RATIO*F.col("case_rn"))
                )
            ),
            how="inner"
        )
        .orderBy("case_id", "person_id")
        .select(
            case_type["case_id"].alias("case_medrec"),
            control_person_number["person_id"].alias("control_medrec"),
            case_type["age"],
            case_type["gender"],
            case_type["index_month"],
            case_type["io_flag"],
        )
)

# COMMAND ----------

display(matches)

# COMMAND ----------

(
    matches
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_WRITE}.{MATCHES_TABLE}')
)

# COMMAND ----------

matches = (
    spark
    .table(f"{DB_WRITE}.{MATCHES_TABLE}")
)

# COMMAND ----------

display(
    matches
    .groupBy("case_medrec")
    .agg(F.count("*").alias("n_matches"))
    .groupBy("n_matches")
    .count()
)

# COMMAND ----------

# Compute the number of individuals with full/complete matches
display(
    matches
    .groupBy("case_medrec")
    .agg(F.count("*").alias("n_matches"))
    .groupBy("n_matches")
    .count()
)

# COMMAND ----------

669579/675285

# COMMAND ----------

complete_matched = (
    matches
    .select(
        '*',
        rn := F.row_number().over(W.Window.partitionBy('case_medrec').orderBy(F.rand(seed=42))).alias('rn'),
        max_rn := F.max(rn).over(W.Window.partitionBy('case_medrec')).alias('max_rn')
    )
    .filter(
        f'max_rn = {CASE_CONTROL_RATIO}'
    )
)

matched_medrec = (
    
    complete_matched
    .select(F.col('case_medrec').alias('medrec_key'))
    .distinct()
    .union(
        complete_matched
        .select(F.col('control_medrec').alias('medrec_key'))
        .distinct()
    )

)

# COMMAND ----------

(
    matched_medrec
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_WRITE}.{MATCHES_UID_TABLE}')
)
