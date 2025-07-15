# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.window as W

# COMMAND ----------

coh_tte = spark.read.table("bart_pcc.05_ccsrs_tte")
coh_tte.display()

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

ccsrs2 = ["END005",
        "RSP010",
        "NVS014",
        "CIR005",
        "CIR015",
        "RSP002"]

# COMMAND ----------

p3_ccsr = (coh_tte 
    .filter(F.col("p3_ind") == 1)
    # .groupBy("medrec_key")
    .withColumn("n", F.count(F.col("medrec_key")).over(W.Window.partitionBy(F.col("medrec_key"))))
)


# COMMAND ----------

p3_ccsr2 = (p3_ccsr
    .withColumn("set_intersect", F.array_intersect(F.col("ccsr_set"), F.lit(ccsrs2)))
    .withColumn("size", F.size(F.col("set_intersect")))
    .filter(F.col("size") == 3)
)

# COMMAND ----------

p3_ccsr2.display()

# COMMAND ----------

# p3_ccsr2.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.ccsr_subset_20230518")

# COMMAND ----------

p3 = spark.read.table("bart_pcc.ccsr_subset_20230518")
chgmstr=spark.read.table("cdh_premier.chgmstr")
patbill=spark.read.table("cdh_premier.patbill")

# COMMAND ----------

(p3
    .join(patbill, on="pat_key")
).display()

# COMMAND ----------

chgmstr

# COMMAND ----------

(patbill
    .filter(F.col("pat_key") == 1431331507)
    .join(chgmstr, on="std_chg_code")
).display()
