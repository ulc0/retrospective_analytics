# Databricks notebook source
# MAGIC %md
# MAGIC # Subsample
# MAGIC Subsample partially completed in "02_twj8_ccsr". This will provide more subsampling components

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

# COMMAND ----------

coh_tte = spark.read.table("bart_pcc.end005_full")

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic Stratified Sample
# MAGIC Stratified on covid

# COMMAND ----------

coh_tte.display()

# COMMAND ----------

coh_tte.count()

# COMMAND ----------

coh_tte2 = coh_tte.filter(
    (F.col("p1_ccsr")==0 )&
    (F.col("p2_ccsr")==0)
               )

# COMMAND ----------



# COMMAND ----------

coh_tte2.count()

# COMMAND ----------

coh_tte2.filter(F.col("p3_ccsr_first")==1).count()

# COMMAND ----------

coh_tte2.

# COMMAND ----------

# MAGIC %md
# MAGIC # Split into outcome and no outcome

# COMMAND ----------

ccsr0 = coh_tte2.filter(F.col("p3_ccsr_first") == 0)
ccsr1 = coh_tte2.filter(F.col("p3_ccsr_first") == 1)

# COMMAND ----------

# get a test sample
insmp, outsmp = ccsr0.randomSplit([0.01,0.99],seed=99)
print(insmp.count())
outsmp.count()

# COMMAND ----------

ccsr1_tst, ccsr1_trn = ccsr1.randomSplit([0.1,0.9], seed=99)
print(ccsr1_tst.count())
print(ccsr1_trn.count())

# COMMAND ----------

ccsr1.count()

# COMMAND ----------

# Get subsample 1
s1 = coh_tte2.sampleBy("p3_ccsr_first", fractions={0:0.05,1:0.05}, seed = 99)
s1.cache()
s1.count()

# COMMAND ----------

s1.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_ccsr_smp1_large")
# s2.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_ccsr_smp2")

# COMMAND ----------

s1.filter(F.col("p3_ccsr_first")==1).count()

# COMMAND ----------

# Remove subsample 1 from superset
coh_tte_s1m = coh_tte.subtract(s1)

# COMMAND ----------

# Get subsample 2
s2 = coh_tte_s1m.sampleBy("p3_ccsr_first", fractions={0:0.005,1:0.005}, seed = 99)

# COMMAND ----------

# MAGIC %md
# MAGIC # Covid Status and CCSR Outcome 

# COMMAND ----------

# S1 counts
s1_cx = s1.crosstab("covid_patient", "p3_ccsr_first").toPandas()
s1_cnt = s1.count()
s1_cx2 = s1_cx.copy()
s1_cx2["0"] = s1_cx2["0"]/s1_cnt
s1_cx2["1"] = s1_cx2["1"]/s1_cnt

# COMMAND ----------

print(s1_cx)
print(s1_cx2)

# COMMAND ----------

# S2 counts
s2_cx = s2.crosstab("covid_patient", "p3_ccsr_first").toPandas()
s2_cnt = s2.count()
s2_cx2 = s2_cx.copy()
s2_cx2["0"] = s2_cx2["0"]/s1_cnt
s2_cx2["1"] = s2_cx2["1"]/s1_cnt

# COMMAND ----------

print(s2_cx)
print(s2_cx2)

# COMMAND ----------

coh_cx = coh_tte.crosstab("covid_patient", "p3_ccsr_first").toPandas()
coh_cnt = coh_tte.count()
coh_cx2 = coh_cx.copy()
coh_cx2["0"] = coh_cx2["0"]/coh_cnt
coh_cx2["1"] = coh_cx2["1"]/coh_cnt

# COMMAND ----------

print(coh_cx)
print(coh_cx2)

# COMMAND ----------

# MAGIC %md
# MAGIC Crosstab between sampled and the original display relatively similar distributions on the outcome `p3_ccsr_first` and the treatment `p3_ccsr_first`

# COMMAND ----------

# MAGIC %md 
# MAGIC # TTE Comparison

# COMMAND ----------

# S1 tte distribution
(s1.select("tte_ccsr")
    # .na.fill(180)
    .groupBy("tte_ccsr")
    .agg(F.count("*"))
).display()

# COMMAND ----------

s1.select("tte_ccsr").describe().display()

# COMMAND ----------

# s2 tte distribution
(s2.select("tte_ccsr")
    # .na.fill(180)
    .groupBy("tte_ccsr")
    .agg(F.count("*"))
).display()

# COMMAND ----------

s2.select("tte_ccsr").describe().display()

# COMMAND ----------

# coh distribution
(coh_tte.select("tte_ccsr")
    # .na.fill(180)
    .groupBy("tte_ccsr")
    .agg(F.count("*"))
).display()

# COMMAND ----------

coh_tte.select("tte_ccsr").describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Similarily the tte measure in both the full and sample datasets have relative means, stdv.
# MAGIC
# MAGIC NOTE: removed the NULL measures that would default to 180 

# COMMAND ----------

s1.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_ccsr_smp1")
s2.write.mode("overwrite").format("delta").saveAsTable("bart_pcc.end005_ccsr_smp2")
