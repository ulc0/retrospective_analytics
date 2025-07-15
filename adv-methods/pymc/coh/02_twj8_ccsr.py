# Databricks notebook source
# MAGIC %md
# MAGIC # Specific CCSR Datasets
# MAGIC
# MAGIC The function in this file will create the tte dataset for specific ccsr codes.
# MAGIC
# MAGIC Each ccsr code dataset will need to be run seperately
# MAGIC
# MAGIC CCSRS:
# MAGIC - END005
# MAGIC - CIR013
# MAGIC - RSP010
# MAGIC - NVS014
# MAGIC - NEO059
# MAGIC - NEO064
# MAGIC - CIR005
# MAGIC - NVS018
# MAGIC - CIR015
# MAGIC - RSP002
# MAGIC - INF002

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

# COMMAND ----------

coh_tte = spark.read.table("bart_pcc.05_ccsrs_tte")

# COMMAND ----------

demographics = spark.table("bart_pcc.02_demographics")

# COMMAND ----------

def get_ccsr_specific(coh_tte, ccsr_code):
    lambda_ccsr = lambda x: x == ccsr_code
    p1 = (
        coh_tte
        .filter(
            F.col("p1_ind") == 1
        )
        .withColumn(
            "p1_ccsr",
            F.max(
                F.when(
                    F.exists(F.col("ccsr_set"), lambda_ccsr)
                    ,1
                ).otherwise(0)
            ).over(W.Window.partitionBy(F.col("medrec_key")))
        )
        .filter(F.col("p1_ccsr") == 1)
        .select("medrec_key", "p1_ccsr")
        .distinct()
    )
    
    p2 = (
        coh_tte
        .filter(
            F.col("p2_ind") == 1
        )
        .withColumn(
            "p2_ccsr",
            F.max(
                F.when(
                    F.exists(F.col("ccsr_set"), lambda_ccsr)
                    ,1
                ).otherwise(0)
            ).over(W.Window.partitionBy(F.col("medrec_key")))
        )
        .filter(F.col("p2_ccsr") == 1)
        .select("medrec_key", "p2_ccsr")
        .distinct()
    )
    
    p3 = (
        coh_tte
        .filter(
            F.col("p3_ind") == 1
        )
        .withColumn(
            "p3_ccsr",
            F.when(
                F.exists(F.col("ccsr_set"), lambda_ccsr)
                ,1
            ).otherwise(0)
        )
        .filter(F.col("p3_ccsr") == 1)
        .withColumn(
            "p3_ccsr_first",
            F.when(
                F.col("tte_ccsr") ==
                F.min(F.col("tte_ccsr")).over(W.Window.partitionBy("medrec_key"))
                , 1
            ).otherwise(0)
        )
        .filter(F.col("p3_ccsr_first") == 1)
        .select("medrec_key", "tte_ccsr", "p3_ccsr_first")
        .distinct()
    )
    
    out = (
        coh_tte
        .select("medrec_key","prim_dfi", "p1_sum", "p2_sum", "p3_sum", "covid_patient")
        .distinct()
        .join(
            p1
            ,on="medrec_key"
            ,how="left"
        )
        .join(
            p2
            ,on="medrec_key"
            ,how="left"
        )
        .join(
            p3
            ,on="medrec_key"
            ,how="left"
        )
        .fillna(0, ["p1_ccsr", "p2_ccsr", "p3_ccsr_first"])
    )
    
    return out
    

    

# COMMAND ----------

coh_end005 = get_ccsr_specific(coh_tte, "END005")
# coh_end005.display()

# COMMAND ----------

coh_end005_dem = (
    coh_end005
    .join(
        demographics
        .select("medrec_key", "adm_mon", "age", "gender", "calc_los", "urban_rural", "prov_region", "prov_division", "prov_zip3", "std_payor_desc", "io_flag", "raceeth")
        ,on="medrec_key"
        ,how="left"
    )
)

# COMMAND ----------

coh_end005_dem.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_full")

# COMMAND ----------

coh_end005.count()

# COMMAND ----------

coh_end005.display()

# COMMAND ----------

coh_end005.filter(F.col("p3_ccsr_first")==1).count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC about 500,000 of 4 million (1/8) have the end005 ccsr in p3

# COMMAND ----------

s1 = coh_end005.sample(.001, 1)
s1.cache()
s1.count()

# COMMAND ----------

(
    s1
    .filter(
        (F.col("covid_patient") == 0)
        &
        (F.col("p3_ccsr_first") == 1)
    )
).count()

# COMMAND ----------

(
    s1
    .filter(
        (F.col("covid_patient") == 1)
        &
        (F.col("p3_ccsr_first") == 1)
    )
).count()

# COMMAND ----------

s1.display()

# COMMAND ----------

demographics.display()

# COMMAND ----------

s1_fin = (
    s1
    .join(
        demographics
        .select("medrec_key", "adm_mon", "age", "gender", "calc_los", "urban_rural", "prov_region", "prov_division", "prov_zip3", "std_payor_desc", "io_flag", "raceeth")
        ,on="medrec_key"
        ,how="left"
    )
)

s1_fin.cache()
s1_fin.count()
s1_fin.display()

# COMMAND ----------

s1_fin.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_ccsr_sample")

# COMMAND ----------

s2 = spark.table("bart_pcc.end005_ccsr_sample")

# COMMAND ----------

(
    s2.drop("medrec_key", "adm_mon", "prov_zip3")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC gen defs:
# MAGIC primary encounter - encounter defined as first covid visit or randomly drawn from the encounter window if non-covid patient
# MAGIC p1 - period -363 -> -8 days from primary encounter
# MAGIC p2 - period -7 -> 30 days from primary encounter
# MAGIC p3 - period 31 -> 180 days from primary encounter
# MAGIC
# MAGIC
# MAGIC vars:
# MAGIC prim_dfi - days from index of the primary encounter 
# MAGIC p1_sum - number of encounters in p1 range
# MAGIC p2_sum - number of encounters in p2 range
# MAGIC p3_sum - number of encounters in p3 range
# MAGIC covid_patient - indicator if patients primary encounter is covid (==1) else 0
# MAGIC p1_ccsr - indicator if ccsr is in the p1 range
# MAGIC p2_ccsr - indicator if ccsr is in p2 range
# MAGIC p3_ccsr_first - indicator if ccsr is in the p3 range, only takes the first instance of the ccsr in this range
# MAGIC tte_ccsr - time to event connected to the p3_ccsr_first indicator
# MAGIC
# MAGIC Other vars are demographics
# MAGIC age - age
# MAGIC urban_rural
# MAGIC prov_region
# MAGIC prov_division
# MAGIC std_payor_desc
# MAGIC raceeth - race ethnicity
# MAGIC io_flag - indicator for inpatient(1)/emergency(0)/outpatient(-1) for the primary encounter
# MAGIC
# MAGIC Other Notes:
# MAGIC For basic analyses only the tte_ccsr, p3_ccsr and covid_patient are necessary. 
# MAGIC The tte_ccsr may need to be expanded to a block grid for time frames to meet the BART input requirements

# COMMAND ----------

# MAGIC %md
# MAGIC # Get for all CCSR

# COMMAND ----------

ccsrs = ["END005",
        "CIR013",
        "RSP010",
        "NVS014",
        "NEO059",
        "NEO064",
        "CIR005",
        "NVS018",
        "CIR015",
        "RSP002",
        "INF002"]

# COMMAND ----------

t=[]
for i in ccsrs:
    t.append(get_ccsr_specific(coh_tte, "END005"))
    break

# COMMAND ----------

t = (get_ccsr_specific(coh_tte, "CIR013")
    .join(
        demographics
        .select("medrec_key", "adm_mon", "age", "gender", "calc_los", "urban_rural", "prov_region", "prov_division", "prov_zip3", "std_payor_desc", "io_flag", "raceeth")
        ,on="medrec_key"
        ,how="left"
    )
    .sample(.001, 1)
    )


# COMMAND ----------

(
    t
    .filter(
        (F.col("covid_patient") == 0)
        &
        (F.col("p3_ccsr_first") == 1)
    )
).count()

# COMMAND ----------

(
    t
    .filter(
        (F.col("covid_patient") == 1)
        &
        (F.col("p3_ccsr_first") == 1)
    )
).count()

# COMMAND ----------

t.count()
