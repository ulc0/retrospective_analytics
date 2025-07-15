# Databricks notebook source
# MAGIC %md
# MAGIC # Time to Event Analysis (Time Varying)
# MAGIC
# MAGIC Date: 2022-11-08
# MAGIC
# MAGIC Description: TBD

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
# MAGIC - Author: Wilson Chow (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [ruz2@cdc.gov](ruz2@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC - Author: Matthew Heym (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [twz0@cdc.gov](twz0@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imports and Settings

# COMMAND ----------

# MAGIC %pip install lifelines

# COMMAND ----------

import pyspark.sql.functions as F
from lifelines import CoxPHFitter, CoxTimeVaryingFitter
import pandas as pd
# Disabling a particular pandas warning
pd.options.mode.chained_assignment = None
import numpy as np

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filters and Definitions

# COMMAND ----------

dbutils.widgets.dropdown("adjusted", "True", ["True", "False"])
dbutils.widgets.dropdown("outcome_date_column", "hospital_date_covid", ["hospital_date", "hospital_date_covid", "icu_imv_date", "icu_imv_date_covid"])

# COMMAND ----------

DB_WRITE = "cdh_hv_race_ethnicity_covid_exploratory"
ADJUSTED = True if dbutils.widgets.get("adjusted") == "True" else False
OUTCOME_DATE_COLUMN = dbutils.widgets.get("outcome_date_column")
OUTPUT_TABLE = f"{DB_WRITE}.ml_paxlovid_tte_{OUTCOME_DATE_COLUMN}_Adjusted_{ADJUSTED}"

print("Adjusted?", ADJUSTED)
print("Outcome Date Column?", OUTCOME_DATE_COLUMN)
print("Output Table Name:", OUTPUT_TABLE)

COVARIATES = {
#     "exposed_cohort": "exposed_cohort",
#     "exposed": "exposed",
    "paxlovid_flag": "paxlovid_flag",
    "age_group": "C(age_group)",
    "patient_gender": "patient_gender",
    "race": "C(race)",
    "ethnicity": "C(ethnicity)",
    "umc_count": "C(umc_count)",
    "prior_encounters": "C(prior_encounters)",
#     "reinfected": "reinfected",
#     "vaccine_strata": "C(vaccine_strata)"
    "vaccine_strata": "vaccine_strata"
}

cph_output_column_remap = {
    "covariate": "covariate",
    "coef": "coef",
    "exp(coef)": "exp_coef",
    "se(coef)": "se_coef",
    "coef lower 95%": "coef_lower_95",
    "coef upper 95%": "coef_upper_95",
    "exp(coef) lower 95%": "exp_coef_lower_95",
    "exp(coef) upper 95%": "exp_coef_upper_95",
    "cmp to": "cmp_to",
    "z": "z",
    "p": "p",
    "-log2(p)": "-log2_p"
}

stats = []

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect Data
# MAGIC *What data do I need? From where?*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data
# MAGIC
# MAGIC Convert the data to "long format" for those who have time varying exposure. Specifically, those in cohort 1 with different covid and paxlovid dates.

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table cdh_hv_race_ethnicity_covid_exploratory.qyc9_tte_cohort_final_20221130_zeroday

# COMMAND ----------

spark.sql(f"select * from {DB_WRITE}.qyc9_tte_cohort_final_20221130_zeroday limit 1").printSchema()

# COMMAND ----------

cohort_df = (
    spark.table(f"{DB_WRITE}.qyc9_tte_cohort_final_20221130_zeroday")
        .select(
            "*",
            F.col(OUTCOME_DATE_COLUMN).isNotNull().alias("event"),
            F.when(
#                 F.col("exposed_cohort").isin([1, 2]), #anypax index date is paxlovid
                F.col("exposed_cohort") == 2, # zeroday (main study)
                F.datediff(OUTCOME_DATE_COLUMN, "paxlovid_date")
            ).otherwise(
                F.datediff(OUTCOME_DATE_COLUMN, "covid_diag_date")
            ).alias("duration"),
        )
        .na.fill({
            "duration": 30
        })
)

# For anypax we treat Exposed1 the same as Exposed2
# Handle those who have different paxlovid and covid dates 

cohort_group_1 = (
    cohort_df
#         .filter(F.col("paxlovid_date") == F.col("covid_diag_date")) #anypax, treat exp1 like exp2, this reduces this amount of recoding
        .filter(F.col("paxlovid_date") != F.col("covid_diag_date")) #zeroday (main study)
        .select(
            "hvid",
            "exposed_cohort", 
            F.col(OUTCOME_DATE_COLUMN).alias("outcome_date"),
            F.datediff("paxlovid_date", "covid_diag_date").alias("delta"),
            F.create_map(F.lit("covid"), "covid_diag_date", F.lit("pax"), "paxlovid_date").alias("dates"),
        )
        .select(
            "hvid",
            "exposed_cohort",
            "outcome_date",
            "delta",
            F.explode("dates"),
        )
        .select(
            "hvid",
            # Define start date:
            # covid: 0 ; paxlovid: datediff(paxlovd, covid) 
            start := F.when(
                (F.col("exposed_cohort") == 1) & (F.col("key") == "pax"),
                F.col("delta")
            ).otherwise(0).alias("start"),
            # Define end date:
            # covid: datediff(paxlovid, covid) or 0; paxlovid: start + datediff(outcome, paxlovid)
            F.when(
                (F.col("exposed_cohort") == 1) & (F.col("key") == "covid"),
                F.greatest(F.lit(0), F.col("delta"))
            ).when(
                (F.col("exposed_cohort") == 1) & (F.col("key") == "pax") & (F.col("outcome_date").isNotNull()),
                start+F.datediff("outcome_date", "value")
            ).otherwise(30).alias("stop"),
            # Paxlovid flag. covid: 0; paxlovid: 1
            F.when(
                F.col("key") == "covid",
                0
            ).otherwise(1).alias("paxlovid_flag"),
            # Event flag. True for paxlovid and outcome.
            F.when(
                (F.col("exposed_cohort") == 1) & (F.col("key") == "pax") & (F.col("outcome_date").isNotNull()),
                True
            ).otherwise(False).alias("event"),
        )
)

# Stack time varying sub-cohort with the remaining patients
cohort_df = (
    cohort_df
        .join(
            cohort_group_1,
            "hvid",
            "left"
        )
        # Replace null starts with 0
        .na.fill({
            "start": 0
        })
        # Overwrite stop
        .withColumn(
            "stop",
            F.when(
                (F.col("start") == 0) & (F.col("stop") == 0),
                1
            ).otherwise(F.coalesce("stop", "duration"))
        )
        # Overwrite paxlovid flag
        .withColumn(
            "paxlovid_flag",
            F.when(
                F.col("paxlovid_flag").isNull() & (F.col("exposed_cohort") == 2),
                1
            ).when(
                F.col("paxlovid_flag").isNull() & (F.col("exposed_cohort") == 0),
                0
            ).when(
                F.col("paxlovid_flag").isNull() & (F.col("exposed_cohort") == 1),
                1
            )
            .otherwise(F.col("paxlovid_flag"))
        )
        .withColumn(
            "t_event",
            F.coalesce(cohort_group_1["event"], cohort_df["event"]).alias("event")
        )
        .drop("event")
        .withColumnRenamed("t_event", "event")
        .drop_duplicates()
)

# try:
#     # Grant ownership of output table to hv r/e group
#     spark.sql(f"ALTER TABLE {OUTPUT_TABLE} OWNER TO `gp-u-EDAV-CDH-HV-RACE-ETHNICITY-COVID-ANALYSTS-AAD`")
# except:
#     print("")
cohort_df.write.mode("overwrite").format("parquet").saveAsTable(OUTPUT_TABLE)
# try:
#     # Grant ownership of output table to hv r/e group
#     spark.sql(f"ALTER TABLE {OUTPUT_TABLE} OWNER TO `gp-u-EDAV-CDH-HV-RACE-ETHNICITY-COVID-ANALYSTS-AAD`")
# except:
#     print("")

cohort_df = spark.table(OUTPUT_TABLE)
cohort_df.cache()
cohort_df.display()

### Filters for exp1/exp2 v unexp breakdowns, original analysis combines exp1,2 v unexp
## Create unfiltered for first three HR models & make cohort_df the filter needed so the whole code doesn't need editing
# cohort_df_unfiltered = cohort_df
# cohort_df = cohort_df.filter(F.col("exposed_cohort").isin([0, 2]))

## filter for stratified days to paxlovid
# cohort_df_sameday = cohort_df.filter(((F.datediff('paxlovid_date', 'covid_diag_date') >= 6) & (F.col('exposed_cohort') == 1)) | (F.col('exposed_cohort') == 0))
 


# COMMAND ----------

# MAGIC %md
# MAGIC ### KaplanMeier Curve

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

exposed1_unexposed_df = (
    cohort_df
        .filter(F.col("exposed_cohort").isin([0, 1]))
        .withColumn('Cohort', F.lit('Exposed1_Unexposed'))
        .select(
            F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
            'duration',
            'Cohort'
        )
)

exposed2_unexposed_df = (
    cohort_df
        .filter(F.col("exposed_cohort").isin([0, 2]))
        .withColumn('Cohort', F.lit('Exposed2_Unexposed'))
        .select(
            F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
            'duration',
            'Cohort'
        )
)
exposed12_unexposed = (
    cohort_df
        .withColumn('Cohort', F.lit('Exposed12_Unexposed'))
        .select(
            F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
            'duration',
            'Cohort'
        )
)

total_pdf = (
    exposed1_unexposed_df.union(exposed2_unexposed_df).union(exposed12_unexposed).toPandas()
)


# configure the plot
plt.figure(figsize=(12,8))
ax = plt.subplot(111)
plt.title('By Cohort', fontsize='xx-large')

# configure the x-axis
plt.xlabel('Timeline (Days)', fontsize='x-large')
plt.xlim((0, 30))
plt.xticks(np.arange(0,30,5))
#plt.xticks(range(0,30,5))

# configure the y-axis
plt.ylabel('Survival Rate', fontsize='x-large')
plt.ylim((0.9970, 1.0))
plt.yticks(np.arange(0.9970,1.0,0.0005))

# graph each curve on the plot
for name, grouped_pd in total_pdf.groupby('Cohort'):
    kmf = KaplanMeierFitter(alpha=0.05)
    kmf.fit(
      grouped_pd['duration'], 
      grouped_pd['event'], 
      label='Cohort {0}'.format(name)
      )
    kmf.plot(ax=ax)


# COMMAND ----------

# from lifelines import (WeibullFitter, ExponentialFitter,
# LogNormalFitter, LogLogisticFitter, NelsonAalenFitter,
# PiecewiseExponentialFitter, GeneralizedGammaFitter, SplineFitter)

# import matplotlib.pyplot as plt

# exposed1_unexposed_df = (
#     cohort_df
#         .filter(F.col("exposed_cohort").isin([0, 1]))
#         .withColumn('Cohort', F.lit('Exposed1_Unexposed'))
#         .select(
#             F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
#             'duration',
#             'Cohort'
#         )
# )

# exposed2_unexposed_df = (
#     cohort_df
#         .filter(F.col("exposed_cohort").isin([0, 2]))
#         .withColumn('Cohort', F.lit('Exposed2_Unexposed'))
#         .select(
#             F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
#             'duration',
#             'Cohort'
#         )
# )
# exposed12_unexposed = (
#     cohort_df
#         .withColumn('Cohort', F.lit('Exposed12_Unexposed'))
#         .select(
#             F.when(F.col('event')=='false', 0).otherwise(1).alias('event'),
#             'duration',
#             'Cohort'
#         )
# )

# total_pdf = (
#     exposed1_unexposed_df.union(exposed2_unexposed_df).union(exposed12_unexposed).toPandas()
# )


# # configure the plot
# plt.figure(figsize=(12,8))
# plt.title('By Cohort', fontsize='xx-large')

# # configure the x-axis
# plt.xlabel('Timeline (Days)', fontsize='x-large')
# plt.xlim((0, 30))
# plt.xticks(np.arange(0,30,5))

# # configure the y-axis
# plt.ylabel('Cumulative Hazard', fontsize='x-large')
# plt.ylim((0, 0.003))
# plt.yticks(np.arange(0,0.003,0.0005))




# for name, grouped_pd in total_pdf.groupby('Cohort'):
#     naf = NelsonAalenFitter()
#     naf.fit(
#         total_pdf['duration'],
#         event_observed=total_pdf['event'],
#         label='Cohort {0}'.format(name)
#     )
#     naf.plot_cumulative_hazard()
#     wbf = WeibullFitter()
#     wbf.fit(
#         total_pdf['duration'],
#         event_observed=total_pdf['event'],
#         label='Cohort {0}'.format(name)
#     )
#     wbf.plot_cumulative_hazard()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Model
# MAGIC
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

def generate_event_pdf(input_df, filters=None):

    if filters is None:
        event_pdf = input_df
    else:
        event_pdf = (
            input_df
                .filter(filters)
        )
        
    event_pdf = (
        event_pdf
#             .withColumn(
#                 "exposed",
#                 F.when(F.col("exposed_cohort") == 0, 0).otherwise(1)
#             )
            .select(
                F.col("hvid").alias("id"),
                *["paxlovid_flag"] if not ADJUSTED else [],
#                 *["exposed"] if not ADJUSTED else [],
                *COVARIATES.keys() if ADJUSTED else [],
                "event",
                "start",
                "stop"
            )
            .toPandas()
        
    )
    event_pdf.index.duplicated() # drop duplicates
    
    return event_pdf
    
def generate_formula(covariates_all:dict=COVARIATES, strata_names:list=[]):
    cov_names= [i for i in covariates_all.keys() if i not in strata_names]
    formula = " + ".join([covariates_all[i] for i in cov_names])
    
    return formula

def fit_coxph(
    df, outcome="hospitalization",
    strata=None,
    covariates=ADJUSTED,
    formula=None,
    fit_options=None,
):
    cph = CoxTimeVaryingFitter()
    cph.fit(
        df,
        id_col="id",
        start_col="start",
        stop_col="stop",
        event_col="event",
        formula=formula if covariates else None,
        strata=strata,
        fit_options=fit_options
    )

    # Postprocessing summary outputs
    summary_stats_pdf = cph.summary
    summary_stats_pdf.reset_index(inplace=True)
    summary_stats_pdf.rename(cph_output_column_remap, axis="columns", inplace=True)
    summary_stats_pdf.replace([np.inf, -np.inf], np.nan, inplace=True)

    summary_stats_pdf['n_event'] = cph.weights[cph.event_observed > 0].sum()
    summary_stats_pdf['n_at_risk'] = cph.weights.sum()
    summary_stats_pdf['error_flag'] = False

    summary_stats_pdf["outcome"] = outcome
    
    return summary_stats_pdf

# NOTE: changed covariate from exposed to paxlovid_flag
def postprocess_summary(summary_pdf, covariate="paxlovid_flag", cat=None, subcat=None):
    temp = summary_pdf[summary_pdf["covariate"] == covariate]
    
    response = [
        cat,
        subcat,
        temp["exp_coef"].values[0],
        temp["exp_coef_lower_95"].values[0],
        temp["exp_coef_upper_95"].values[0],
        temp["p"].values[0]
    ]
    
    return response

def compute_hr(
    cohort_df,
    filters=None,
    strata=[],
    covariates=ADJUSTED,
    category_text="Overall",
    subcategory_text="Exposed1 vs. Unexposed",
    verbose=False
):

    event_pdf = generate_event_pdf(
        cohort_df,
        filters=filters
    )
    
    try:
        if covariates:
            formula = generate_formula(covariates_all=COVARIATES, strata_names=strata)
            print("Using formula:", formula)
            summary_pdf = fit_coxph(event_pdf, covariates=True, formula=formula)
        else:
            summary_pdf = fit_coxph(event_pdf, covariates=False, formula=None)
        
        if verbose:
            display(summary_pdf)
        
        payload = postprocess_summary(            
            summary_pdf=summary_pdf,
            cat=category_text,
            subcat=subcategory_text
        )
        
    except Exception as e:
        print(f"Failed: {category_text}{subcategory_text}", e)
        payload= [category_text, subcategory_text, None, None, None, None]
    
    finally:
        return payload

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Overall

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Exposed 1 vs Unexposed
# MAGIC
# MAGIC Index date: covid date

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("exposed_cohort").isin([0, 1])),
    category_text="Overall",
    subcategory_text="Exposed1 vs. Unexposed",
    verbose=True
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Sensitivity analysis for same day paxlovid and covid
# response = compute_hr(
#     cohort_df_sameday,
#     filters=(F.col("exposed_cohort").isin([0, 1])),
#     category_text="Overall",
#     subcategory_text="Exposed1 vs. Unexposed",
#     verbose=True
# )

# stats.append(response)

# COMMAND ----------

# DBTITLE 1,Sensitivity analysis for paxlovid date and covid date > 0
# response = compute_hr(
#     cohort_df_sameday,
#     filters=(F.col("exposed_cohort").isin([0, 1])),
#     category_text="Overall",
#     subcategory_text="Exposed1 vs. Unexposed",
#     verbose=True
# )

# stats.append(response)

# COMMAND ----------

# # TEST
# response = compute_hr(
#     cohort_df,
#     filters=(F.col("exposed_cohort").isin([0, 1])),
#     strata=['prior_encounters'],
#     category_text="Overall",
#     subcategory_text="Exposed1 vs. Unexposed",
#     verbose=True
# )

# print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Exposed 2 vs Unexposed
# MAGIC
# MAGIC Index date: paxlovid date

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("exposed_cohort").isin([0, 2])),
    category_text="Overall",
    subcategory_text="Exposed2 vs. Unexposed",
    verbose=True
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Exposed 1,2 vs Unexposed
# MAGIC
# MAGIC Index date: paxlovid date or covid_date

# COMMAND ----------

response = compute_hr(
    cohort_df,
    category_text="Overall",
    subcategory_text="Exposed1,2 vs. Unexposed",
    verbose=True
)

stats.append(response)

# COMMAND ----------

display(
    pd.DataFrame(
        stats,
        columns=["Category", "Subcategory", "HR", "LCL", "UCL", "p-value"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Age Stratification

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### <50 years

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="Age Stratification",
    subcategory_text="Age <50 years"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 50+ years

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("age_group") > 0),
    strata=["age_group"],
    category_text="Age Stratification",
    subcategory_text="Age 50+ years"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Vaccine Stratification
# MAGIC
# MAGIC `vaccine_strata` encoding:
# MAGIC - 3: 3+
# MAGIC - 2: Full
# MAGIC - 1: Any
# MAGIC - 0: None

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Unvaccinated

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("vaccine_strata") == 0),
    strata=["vaccine_strata"],
    category_text="Vaccine Stratification",
    subcategory_text="Unvaccinated"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Any Vaccination

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("vaccine_strata") >= 1),
    strata=["vaccine_strata"],
    category_text="Vaccine Stratification",
    subcategory_text="Any Vaccination"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Incomplete Vaccination

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("vaccine_strata") == 1),
    strata=["vaccine_strata"],
    category_text="Vaccine Stratification",
    subcategory_text="Incomplete"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Full Vaccine

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("vaccine_strata") == 2),
    strata=["vaccine_strata"],
    category_text="Vaccine Stratification",
    subcategory_text="Full Vaccine"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Vaccinated 3+ times

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=(F.col("vaccine_strata") == 3),
    strata=["vaccine_strata"],
    category_text="Vaccine Stratification",
    subcategory_text="Vaccinated 3+ times"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vaccine and Age Stratification

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unvaccinated Age < 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 0)  & (F.col("age_group") == 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Unvaccinated and age",
    subcategory_text="less50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unvaccinated Age > 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 0)  & (F.col("age_group") > 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Unvaccinated and age",
    subcategory_text="greater50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Any Vaccination Age < 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") >= 1)  & (F.col("age_group") == 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Any vaccination and age",
    subcategory_text="less50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Any Vaccination Age > 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") >= 1)  & (F.col("age_group") > 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Any vaccination and age",
    subcategory_text="greater50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incomplete Vaccination Age < 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 1)  & (F.col("age_group") == 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Incomplete Vaccination and age",
    subcategory_text="less50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Incomplete Vaccination Age > 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 1)  & (F.col("age_group") > 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Incomplete Vaccination and age",
    subcategory_text="greater50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Vaccine < 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 2)  & (F.col("age_group") == 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Full Vaccine and age",
    subcategory_text="less50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Vaccine > 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 2)  & (F.col("age_group") > 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Full Vaccine and age",
    subcategory_text="greater50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vaccinated 3+ times < 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 3)  & (F.col("age_group") == 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Vaccinated 3+ times and age",
    subcategory_text="less50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vaccinated 3+ times > 50

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("vaccine_strata") == 3)  & (F.col("age_group") > 0)),
    strata=["vaccine_strata", "age_group"],
    category_text="Vaccinated 3+ times and age",
    subcategory_text="greater50"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### UMC Stratification Greater than 50 
# MAGIC
# MAGIC Strata:
# MAGIC - Lung_Condition
# MAGIC - Obesity
# MAGIC - Heart
# MAGIC - Diabetes
# MAGIC - Disabilities
# MAGIC - Immune_Condition
# MAGIC - Smoking
# MAGIC - Mental_Health
# MAGIC - Physical_Inactivity
# MAGIC - Kidney
# MAGIC - Liver
# MAGIC - Cerebrovascular

# COMMAND ----------

# DBTITLE 1,Lung Conditions
response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Lung Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Lung Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Obesity
response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Obesity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Obesity - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Heart Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Heart - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Heart - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Diabetes
response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Diabetes - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Diabetes - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Disabilities
response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Disability - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Disability - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Immune Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Immune Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Immune Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Smoking
response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Smoking - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Smoking - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Mental Health
response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Mental Health - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Mental Health - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Physical Inactivity
response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Physical Inactivity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Physical Inactivity - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Kidney
response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Kidney - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Kidney - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Liver
response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Liver - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Liver - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Cerebrovascular
response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 0) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Cerebrovascular - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 1) & (F.col("age_group") > 0)),
    strata=["age_group"],
    category_text="UMC Stratification greater 50",
    subcategory_text="Cerebrovascular - yes"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UMC Stratification Less than 50

# COMMAND ----------

# DBTITLE 1,Lung Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Lung Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Lung Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Obesity
response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Obesity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Obesity - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Heart Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Heart - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Heart - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Diabetes
response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Diabetes - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Diabetes - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Disability
response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Disability - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Disability - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Immune Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Immune Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Immune Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Smoking
response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Smoking - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Smoking - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Mental Health
response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Mental Health - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Mental Health - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Physical Inactivity
response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Physical Inactivity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Physical Inactivity - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Kidney
response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Kidney - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Kidney - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Liver
response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Liver - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Liver - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Cerebrovascular
response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 0) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Cerebrovascular - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 1) & (F.col("age_group") == 0)),
    strata=["age_group"],
    category_text="UMC Stratification less 50",
    subcategory_text="Cerebrovascular - yes"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UMC Not Age Stratified

# COMMAND ----------

# DBTITLE 1,Lung Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Lung Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Lung_Condition") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Lung Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Obesity
response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Obesity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Obesity") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Obesity - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Heart Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Heart - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Heart") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Heart - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Diabetes
response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Diabetes - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Diabetes") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Diabetes - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Disability
response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Disability - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Disabilities") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Disability - yes"
)

stats.append(response)

# COMMAND ----------

# DBTITLE 1,Immune Condition
response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Immune Condition (combined) - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Immune_Condition") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Immune Condition (combined) - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Smoking - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Smoking") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Smoking - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Mental Health - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Mental_Health") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Mental Health - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Physical Inactivity - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Physical_Inactivity") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Physical Inactivity - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Kidney - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Kidney") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Kidney - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Liver - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Liver") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Liver - yes"
)

stats.append(response)

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 0)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Cerebrovascular - no"
)

stats.append(response)

response = compute_hr(
    cohort_df,
    filters=((F.col("Cerebrovascular") == 1)),
    strata=[],
    category_text="UMC Not Age Stratified",
    subcategory_text="Cerebrovascular - yes"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### UMC Count Stratification

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### All Ages

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### no UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 0)),
    strata=['umc_count'],
    category_text="UMC Stratification - overall",
    subcategory_text="no UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1 UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 1)),
    strata=['umc_count'],
    category_text="UMC Stratification - overall",
    subcategory_text="1 UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 2plus UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 2)),
    strata=['umc_count'],
    category_text="UMC Stratification - overall",
    subcategory_text="2+ UMC"
)
stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=((F.col("umc_count") == 2) | (F.col("umc_count") == 3)),
#     strata=['umc_count'],
#     category_text="UMC Stratification - overall",
#     subcategory_text="2+ UMC"
# )
# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 3+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=((F.col("umc_count") == 3)),
#     strata=['umc_count'],
#     category_text="UMC Stratification - overall",
#     subcategory_text="3+ UMC"
# )
# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### <50 years

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1 UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 1) & (F.col("age_group") == 0)),
    strata=["age_group",'umc_count'],
    category_text="UMC Stratification - <50 years",
    subcategory_text="1 UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 2plus UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 2) & (F.col("age_group") == 0)),
    strata=["age_group",'umc_count'],
    category_text="UMC Stratification - <50 years",
    subcategory_text="2+ UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=(((F.col("umc_count") == 2) | (F.col("umc_count") == 3)) & (F.col("age_group") == 0)),
#     strata=["age_group",'umc_count'],
#     category_text="UMC Stratification - <50 years",
#     subcategory_text="2+ UMC"
# )

# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 3+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=((F.col("umc_count") == 3) & (F.col("age_group") == 0)),
#     strata=["age_group",'umc_count'],
#     category_text="UMC Stratification - <50 years",
#     subcategory_text="3+ UMC"
# )
# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 50+ years

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### no UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 0) & (F.col("age_group") > 0)),
    strata=["age_group",'umc_count'],
    category_text="UMC Stratification - 50+ years",
    subcategory_text="0 UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1 UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 1) & (F.col("age_group") > 0)),
    strata=["age_group",'umc_count'],
    category_text="UMC Stratification - 50+ years",
    subcategory_text="1 UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2plus UMC

# COMMAND ----------

response = compute_hr(
    cohort_df,
    filters=((F.col("umc_count") == 2) & (F.col("age_group") > 0)),
    strata=["age_group",'umc_count'],
    category_text="UMC Stratification - 50+ years",
    subcategory_text="2+ UMC"
)

stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=(((F.col("umc_count") == 2) | (F.col("umc_count") == 3)) & (F.col("age_group") > 0)),
#     strata=["age_group",'umc_count'],
#     category_text="UMC Stratification - 50+ years",
#     subcategory_text="2+ UMC"
# )

# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 3+ UMC

# COMMAND ----------

# response = compute_hr(
#     cohort_df,
#     filters=((F.col("umc_count") == 3) & (F.col("age_group") > 0)),
#     strata=["age_group",'umc_count'],
#     category_text="UMC Stratification - 50+ years",
#     subcategory_text="3+ UMC"
# )
# stats.append(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Results

# COMMAND ----------

display(
    pd.DataFrame(
        stats,
        columns=["Category", "Subcategory", "HR", "LCL", "UCL", "p-value"]
    )
)
