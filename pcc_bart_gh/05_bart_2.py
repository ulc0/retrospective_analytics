# Databricks notebook source
# MAGIC %md
# MAGIC # BART w/ Subset
# MAGIC # Goals
# MAGIC   - evaluate Train, Test with MLFLOW

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.functions as W
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Bucketizer
import pandas as pd

# COMMAND ----------

# MAGIC %r
# MAGIC library(BART)
# MAGIC library(SparkR)
# MAGIC library(tidyverse)

# COMMAND ----------

import mlflow
mlflow.autolog(disable=True)

# COMMAND ----------

# MAGIC %r
# MAGIC dat = SparkR::sql("SELECT * from bart_pcc.end005_ccsr_smp1")

# COMMAND ----------

# MAGIC %r
# MAGIC SparkR::createOrReplaceTempView(dat, "tmp_dat")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Python Prep

# COMMAND ----------

# read to python
dat_tmp = spark.table("tmp_dat")

# COMMAND ----------

dat_tmp.count()

# COMMAND ----------

# dat_tmp.display()
dat_tmp.select("p3_ccsr_first").groupBy("p3_ccsr_first").agg(F.count(F.col("p3_ccsr_first"))).display()

# COMMAND ----------

# Split
train, test = dat_tmp.randomSplit(weights = [0.8,0.2], seed=99)

# COMMAND ----------

train.select("p3_ccsr_first").groupBy("p3_ccsr_first").agg(F.count(F.col("p3_ccsr_first"))).display()
test.select("p3_ccsr_first").groupBy("p3_ccsr_first").agg(F.count(F.col("p3_ccsr_first"))).display()


# COMMAND ----------

print(1884/13730)
print(466/3545)

# COMMAND ----------

# counts for review
# dat_tmp.select("age").describe().display()
# dat_tmp.select("raceeth").groupBy("raceeth").count().display()

# COMMAND ----------

# Clean the dataset and adjust/coarsen time
def get_dtst_cln(dtst, xvar, coarse):
    out = (
        dtst
        .select("medrec_key", "tte_ccsr", "p3_ccsr_first", *xvar)
        .fillna(180, ["tte_ccsr"])
        .withColumn("tte_ccsr", F.col("tte_ccsr")-30)
        .withColumn("tte_ccsr", F.ceil(F.col("tte_ccsr")/coarse))
    )

    # age
    bucketizer = Bucketizer(splits=[ 17, 30, 40, 50, 66],inputCol="age", outputCol="age_bin")
    out = bucketizer.setHandleInvalid("keep").transform(out)
    age_dict = pd.DataFrame({0:"18-29", 1:"30-39", 2:"40-49", 3:"50-65"}, columns = ["id", "label"])
    out = (out
           .drop("age")
           .withColumn("age_bin", F.col("age_bin").cast("short"))
        )  
    
    # race
    model = StringIndexer(inputCol="raceeth", outputCol="race").fit(out)
    out = model.transform(out)
    out = out.withColumn("race", F.col("race").cast("short"))
    race_dict = out.select("raceeth","race").distinct().toPandas()
    out = out.drop("raceeth")

    # gender
    model = StringIndexer(inputCol="gender", outputCol="gendr").fit(out)
    out = model.transform(out)
    out = out.withColumn("gendr", F.col("gendr").cast("short"))
    gender_dict = out.select("gender","gendr").distinct().toPandas()
    out = out.drop("gender")

    return out, age_dict, race_dict, gender_dict

# COMMAND ----------

# set to a long format
def get_tte_long(dtst):
    out = (
            dtst
            .withColumn("tte_seq", F.sequence(F.lit(0), F.col("tte_ccsr")))
            # .withColumn("ev_arry", F.col("tte_seq"))
            .withColumn("ev_array", F.transform("tte_seq", lambda x: x*0))
            .withColumn("tte_val", 
                        F.concat(
                            F.slice(F.col("ev_array"), 1, (F.col("tte_ccsr"))),
                            F.transform(
                                F.slice(F.col("ev_array"), F.col("tte_ccsr"), 1),
                                lambda x: F.col("p3_ccsr_first")
                                )
                        )
            )
            .drop("ev_array")
            .withColumn("tmp", F.arrays_zip(F.col("tte_seq"), F.col("tte_val")))
            .withColumn("tmp", F.explode(F.col("tmp")))
            .drop("tte_seq", "tte_val")
            .select("*", 
                    F.col("tmp.tte_seq").cast("short").alias("tte_seq"), 
                    F.col("tmp.tte_val").cast("short").alias("tte_val"))
            .drop("tmp")
        )
    return out

# COMMAND ----------

xvars =  "covid_patient", "age", "gender", "io_flag", "raceeth"
coarsen = 30 #coarsen to months

# Apply cleaning Train
train2, age_dict, race_dict, gender_dict = get_dtst_cln(train, xvars, coarsen)

# Apply long format
train3 = (
    get_tte_long(train2)
    .withColumn("tte_ccsr", F.col("tte_ccsr").cast("short"))
    .withColumn("p3_ccsr_first", F.col("p3_ccsr_first").cast("short"))
    .withColumn("covid_patient", F.col("covid_patient").cast("short"))
    .withColumn("io_flag", F.col("io_flag").cast("short"))
)

###############################################################################
# Apply cleaning TEST
test2, age_dict2, race_dict2, gender_dict2 = get_dtst_cln(test, xvars, coarsen)

# Apply long format
test3 = (
    get_tte_long(test2)
    .withColumn("tte_ccsr", F.col("tte_ccsr").cast("short"))
    .withColumn("p3_ccsr_first", F.col("p3_ccsr_first").cast("short"))
    .withColumn("covid_patient", F.col("covid_patient").cast("short"))
    .withColumn("io_flag", F.col("io_flag").cast("short"))
)

# COMMAND ----------

# write temp datasets
train2.createOrReplaceTempView("train")
test2.createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %md
# MAGIC # R analyses

# COMMAND ----------

# MAGIC %r
# MAGIC # Read into R
# MAGIC dat = SparkR::sql("SELECT * from train")
# MAGIC df = SparkR::as.data.frame(dat)
# MAGIC print(names(df))
# MAGIC SparkR::show(df)

# COMMAND ----------

# MAGIC %r
# MAGIC df2 = bind_rows(df,df,df,df,df,df)
# MAGIC nrow(df2)

# COMMAND ----------

# MAGIC %r
# MAGIC # Set Vars
# MAGIC # NOTE: time is created as an independent var, but is also added to the first col of tte_seq
# MAGIC # time = df$tte_seq
# MAGIC time = df2$tte_ccsr
# MAGIC # delta = df$tte_val
# MAGIC delta = df2$p3_ccsr_first
# MAGIC # ytrain = df$tte_val
# MAGIC
# MAGIC # xtrain = df %>% 
# MAGIC #   select("tte_seq", "covid_patient","age_bin","gendr", "io_flag", "race") %>%
# MAGIC #   as.matrix()
# MAGIC xtrain = df2 %>% 
# MAGIC   select("covid_patient","age_bin","gendr", "io_flag", "race") %>%
# MAGIC   as.matrix()

# COMMAND ----------

# MAGIC %r
# MAGIC xtrain

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Check the py_surv_pre comparison to BART::surv_pre
# MAGIC
# MAGIC Probably want to wrap this all in a func

# COMMAND ----------

# MAGIC %r
# MAGIC # tst_df = df %>%
# MAGIC #   select("medrec_key", "tte_ccsr", "p3_ccsr_first", "covid_patient", "gender") %>%
# MAGIC #   distinct()
# MAGIC
# MAGIC # time2 = tst_df$tte_ccsr
# MAGIC # xtrain = as.matrix(cbind(tst_df$covid_patient, tst_df$gender))
# MAGIC # delta2 = tst_df$p3_ccsr_first

# COMMAND ----------

# MAGIC %r
# MAGIC # print(length(xtrain))
# MAGIC # print(length(delta2))

# COMMAND ----------

# MAGIC %r
# MAGIC # xtrain

# COMMAND ----------

# MAGIC %r
# MAGIC # time2

# COMMAND ----------

# MAGIC %r
# MAGIC # out = BART::surv.pre.bart(times=time2, delta = delta2, x.train = xtrain)
# MAGIC # str(out)

# COMMAND ----------

# MAGIC %r
# MAGIC # out$tx.train

# COMMAND ----------

# MAGIC %r
# MAGIC # print(delta2)
# MAGIC # print(time2)

# COMMAND ----------

# MAGIC %r
# MAGIC # print(out$y.train)
# MAGIC # print(out$tx.train)

# COMMAND ----------

# MAGIC %md
# MAGIC The comparison of times from the python_pre_bart and the BART::surv_pre_bart are comparable. The only major difference being the BART::surv_pre_bart does not automatically fill dates that aren't included in the original times col. Basically if there were no events at time 1, the suv.pre.bart does not include this in the time series. 
# MAGIC
# MAGIC In the python_pre_bart these timepoints are included and will be calculated at with a 100% survival. 
# MAGIC - It would make sense to remove these additional points as they will always be calculated at 100% surv and add unnecesary compute.

# COMMAND ----------

# MAGIC %md
# MAGIC # BART Run

# COMMAND ----------

# MAGIC %r
# MAGIC # check class
# MAGIC print(class(xtrain))
# MAGIC print(class(ytrain))

# COMMAND ----------

# MAGIC %r
# MAGIC # run bart
# MAGIC # out = BART::mc.surv.bart(x.train = xtrain, y.train=ytrain, x.test=xtrain, mc.cores=16, ndpost=500)
# MAGIC out = BART::mc.surv.bart(x.train = xtrain, delta=delta, times=time, x.test=xtrain, mc.cores=12, ndpost=200)
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC str(out)

# COMMAND ----------

# MAGIC %r
# MAGIC nr = length(out$surv.test.mean)
# MAGIC tn = max(out$times)
# MAGIC nn = nr/tn
# MAGIC
# MAGIC # mean
# MAGIC surv_mn = t(matrix(out$surv.test.mean, ncol=nn, nrow=tn))
# MAGIC prob_mn = t(matrix(out$prob.test.mean, ncol=nn, nrow=tn))
# MAGIC
# MAGIC #quantiles
# MAGIC surv_95 = apply(out$surv.test, 2, quantile, probs=0.95)
# MAGIC surv_95 = t(matrix(surv_95, ncol = nn, nrow = tn))
# MAGIC
# MAGIC surv_05 = apply(out$surv.test, 2, quantile, probs=0.05)
# MAGIC surv_05 = t(matrix(surv_05, ncol = nn, nrow = tn))

# COMMAND ----------

# MAGIC %r 
# MAGIC # get indexs pf covid/non-covid
# MAGIC cv_idx = xtrain[,"covid_patient"]==1
# MAGIC ncv_idx = xtrain[,"covid_patient"]==0

# COMMAND ----------

# MAGIC %r
# MAGIC # get plotable averages across all patients of each subgroup
# MAGIC svs = list(
# MAGIC   apply(surv_mn[ncv_idx,], 2, mean),
# MAGIC   apply(surv_mn[cv_idx,], 2, mean),
# MAGIC   apply(surv_95[ncv_idx,], 2, mean),
# MAGIC   apply(surv_95[cv_idx,], 2, mean),
# MAGIC   apply(surv_05[ncv_idx,], 2, mean),
# MAGIC   apply(surv_05[cv_idx,], 2, mean)
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC # R based checks

# COMMAND ----------

# MAGIC %r
# MAGIC # check hazard ratio (approximate not actual due to not doing complete test)
# MAGIC hz_ncv = apply(prob_mn[ncv_idx,],2,mean)
# MAGIC hz_cv = apply(prob_mn[cv_idx,], 2, mean)
# MAGIC
# MAGIC hz_cv/hz_ncv

# COMMAND ----------

# MAGIC %r
# MAGIC library(survival)
# MAGIC cph = coxph(Surv(time, delta) ~ xtrain[,"covid_patient"] + xtrain[,"age_bin"] + xtrain[,"gendr"] +xtrain[,"io_flag"] + xtrain[,"race"])
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC cph

# COMMAND ----------

# MAGIC %r
# MAGIC # check proportional hazard assumption
# MAGIC pp_check = cox.zph(cph)
# MAGIC print(pp_check)
# MAGIC plot(pp_check)

# COMMAND ----------

# MAGIC %r
# MAGIC # plot(svs[[1]])
# MAGIC tt = out$times
# MAGIC
# MAGIC plot(c(0,tt), c(1,svs[[1]]), ylim=c(0,1), type="s", col="darkblue", ylab="suvival", xlab="time(month)")
# MAGIC lines(c(0,tt), c(1,svs[[3]]), ylim=c(0,1), type="s", col="#99CCFF")
# MAGIC lines(c(0,tt), c(1,svs[[5]]), ylim=c(0,1), type="s", col="#99CCFF")
# MAGIC lines(c(0,tt), c(1,svs[[2]]), ylim=c(0,1), type="s", col="#FF0000")
# MAGIC lines(c(0,tt), c(1,svs[[4]]), ylim=c(0,1), type="s", col="#FFCCCC")
# MAGIC lines(c(0,tt), c(1,svs[[6]]), ylim=c(0,1), type="s", col="#FFCCCC")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Write out of R

# COMMAND ----------

# MAGIC %r
# MAGIC # save the mean survival
# MAGIC surv_sdf = SparkR::as.DataFrame(data.frame(surv_mn))
# MAGIC SparkR::createOrReplaceTempView(dat, "surv_mn")
# MAGIC
# MAGIC prob_sdf = SparkR::as.DataFrame(data.frame(prob_mn))
# MAGIC SparkR::createOrReplaceTempView(prob_sdf, "surv_mn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get subset of test
# MAGIC

# COMMAND ----------

test = spark.table("bart_pcc.end005_ccsr_smp2").sampleBy("p3_ccsr_first", fractions={0:0.5,1:0.5}, seed = 99)
test.count()

# COMMAND ----------

# set to test long format
def get_test_tte_long(dtst, K):
    out = (
            dtst
            .withColumn("tte_seq",
                        F.explode(
                        F.sequence(F.lit(0), F.lit(K))
                        )
                        )
            .withColumn("tte_seq", F.col("tte_seq").cast("short"))
        )
    return out

# COMMAND ----------

# get test subset
xvars =  "covid_patient", "age", "gender", "io_flag", "raceeth"
coarsen = 30 #coarsen to months
test_tmp2, t_age_dict, t_race_dict, t_gender_dict = get_dtst_cln(test, xvars, coarsen)

# Check the cleaned dtst
# test_tmp2.display()
# print(t_age_dict)
# print(t_race_dict)
# print(t_gender_dict)

# get_test_tte_long(test_tmp2, 6)

# Apply long format
test_tmp3 = (
    get_test_tte_long(test_tmp2, 6)
    .withColumn("tte_ccsr", F.col("tte_ccsr").cast("short"))
    .withColumn("p3_ccsr_first", F.col("p3_ccsr_first").cast("short"))
    .withColumn("covid_patient", F.col("covid_patient").cast("short"))
    .withColumn("io_flag", F.col("io_flag").cast("short"))
)

# Check clean dtst
test_tmp3.display()

# COMMAND ----------

test_tmp3.createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## BART TEST

# COMMAND ----------

# MAGIC %r
# MAGIC # Read into R
# MAGIC dat = SparkR::sql("SELECT * from test")
# MAGIC df = SparkR::as.data.frame(dat)
# MAGIC print(names(df))
# MAGIC SparkR::show(df)

# COMMAND ----------

# MAGIC %r
# MAGIC test_2 = df %>% 
# MAGIC     distinct(medrec_key, tte_ccsr, p3_ccsr_first) %>%
# MAGIC     select(tte_ccsr, p3_ccsr_first) %>%
# MAGIC     mutate(p3_ccsr_first = ifelse(p3_ccsr_first == 1, TRUE, FALSE))

# COMMAND ----------

# MAGIC %r
# MAGIC test_2

# COMMAND ----------

# MAGIC %r  
# MAGIC time = df$tte_seq
# MAGIC # delta = df$tte_val
# MAGIC # ytrain = df$tte_val
# MAGIC
# MAGIC xtest = df %>% 
# MAGIC   select("tte_seq", "covid_patient","age_bin","gendr", "io_flag", "race") %>%
# MAGIC   as.matrix()

# COMMAND ----------

# MAGIC %r  
# MAGIC pred = predict(out, xtest, mc.cores = 12)

# COMMAND ----------

# MAGIC %r 
# MAGIC str(pred)

# COMMAND ----------

# MAGIC %r  
# MAGIC # Quick check that the surv.test.mean is the mean val over the draws
# MAGIC mean_test = apply(pred$surv.test, 2, mean)
# MAGIC length(mean_test)
# MAGIC comp = tibble("orig" = pred$surv.test.mean, "calc" = mean_test)
# MAGIC
# MAGIC comp %>%
# MAGIC   filter(orig != calc)
# MAGIC
# MAGIC # because all orig == calc it is determined that the surv.test.mean can be used directly

# COMMAND ----------

# MAGIC %r  
# MAGIC pred_t = pred$surv.test.mean
# MAGIC dim(pred_t) = c(pred$K, length(pred$surv.test.mean)/pred$K)
# MAGIC pred_t = t(pred_t)
# MAGIC
# MAGIC # print(pred_t[1:10,])
# MAGIC # print(pred$surv.test.mean)

# COMMAND ----------

# MAGIC %r
# MAGIC # cbind(matrix(test_2), pred_t)
# MAGIC # test_m = as_tibble(cbind(as.matrix(test_2), pred_t))
# MAGIC # test_m
# MAGIC test_m = bind_cols(test_2, as_tibble(pred_t))
# MAGIC test_m

# COMMAND ----------

# MAGIC %r  
# MAGIC # join the surv probabilities with the test true
# MAGIC # test_results = tibble("")
# MAGIC # spdf = SparkR::createDataFrame(tibble("time"= time, "y_true" = ytrain, "surv_prob" = pred$surv.test.mean)) 
# MAGIC
# MAGIC spdf = SparkR::createDataFrame(test_m)
# MAGIC # SparkR::showDF(spdf)
# MAGIC SparkR::createOrReplaceTempView(spdf, "test_return")

# COMMAND ----------

test_r = spark.table("test_return")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnostics R

# COMMAND ----------

# MAGIC %r  
# MAGIC install.packages("pec")

# COMMAND ----------

# MAGIC %r
# MAGIC library(pec)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Diagnositcs
# MAGIC

# COMMAND ----------

# MAGIC %pip install scikit-survival

# COMMAND ----------

from sksurv.metrics import integrated_brier_score
import numpy as np

# COMMAND ----------

test_pd = test_r.toPandas()
# test_r.display()

# COMMAND ----------

test_pd

# COMMAND ----------

test_y = list((zip(test_pd.p3_ccsr_first, test_pd.tte_ccsr)))

test_pred = np.array(list(zip(test_pd.V1, test_pd.V2, test_pd.V3, test_pd.V4, test_pd.V5, test_pd.V6, test_pd.V7)))

# COMMAND ----------

dt = np.dtype([("cens","?"),("time", 'int')])
test_y = np.array(test_y, dtype=dt)
# np.array(test_y, dt

# COMMAND ----------

test_y

# COMMAND ----------

test_pred

# COMMAND ----------

times = np.array([4,5], dtype="int")
times

# COMMAND ----------

test_y

# COMMAND ----------

times2 = np.percentile(test_y["time"], [10,70])
# np.percentile(test_y["time"], 0)
# np.histogram(test_y["time"])
# np.min(test_y["time"])
times2 = np.arange(times2[0], times2[1]+1)
times2

# COMMAND ----------

print(test_y.shape)
print(test_pred.shape)
print(times.shape)
print(test_y)
print(times)

# COMMAND ----------

times = np.percentile(rsf_surv_funcs[0].x, np.linspace(10, 80, 2*len(rsf_surv_funcs[0].x)))

# COMMAND ----------

score = integrated_brier_score(test_y, test_y, test_pred, times)

# COMMAND ----------

for i in range(0,10790):
    test_pd[i]
