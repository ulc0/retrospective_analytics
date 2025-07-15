# Databricks notebook source
# MAGIC %md
# MAGIC ####NCCTG Lung Cancer Data
# MAGIC Survival in patients with advanced lung cancer from the North Central Cancer Treatment Group.
# MAGIC Performance scores rate how well the patient can perform usual aily activities  
# MAGIC
# MAGIC **Source:** Loprinzi CL. Laurie JA. Wieand HS. Krook JE. Novotny PJ. Kugler JW. Bartel J. Law M. Bateman
# MAGIC M. Klatt NE. et al. Prospective evaluation of prognostic variables from patient-completed questionnaires. North Central Cancer Treatment Group. Journal of Clinical Oncology. 12(3):601-7,
# MAGIC 1994

# COMMAND ----------

# MAGIC %md
# MAGIC **inst**: Institution code  
# MAGIC **time**: Survival time in days  
# MAGIC **status**: censoring status 1=censored, 2=dead  
# MAGIC **age**: Age in years  
# MAGIC **sex**: Male=1 Female=2  
# MAGIC **ph.ecog**: ECOG performance score (0=good 5=dead)  
# MAGIC **ph.karno**: Karnofsky performance score (bad=0-good=100) rated by physician  
# MAGIC **pat.karno**: Karnofsky performance score as rated by patient  
# MAGIC **meal.cal**: Calories consumed at meals  
# MAGIC **wt.loss**: Weight loss in last six months  

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC ## Grab data sets from BART
# MAGIC
# MAGIC library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
# MAGIC #sparkR.session()
# MAGIC sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "16g"))
# MAGIC if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
# MAGIC   Sys.setenv(SPARK_HOME = "/home/spark")
# MAGIC }
# MAGIC install.packages('BART')  
# MAGIC library(BART)  

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC times <- c(1.5, 2.5, 3.0)
# MAGIC delta <- c(1, 1, 0)
# MAGIC surv.pre.bart(times = times, delta = delta)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC ## load the advanced lung cancer example
# MAGIC data(lung)
# MAGIC
# MAGIC group <- which(is.na(lung[ , 7])) ## remove missing row for ph.karno
# MAGIC times <- lung[group, 2]   ##lung$time
# MAGIC delta <- lung[group, 3]-1 ##lung$status: 1=censored, 2=dead
# MAGIC                           ##delta: 0=censored, 1=dead
# MAGIC
# MAGIC summary(times)
# MAGIC table(delta)
# MAGIC print(sort(unique(lung$age)))
# MAGIC x.train <- as.matrix(lung[group, c(4, 5, 7)]) ## matrix of observed covariates
# MAGIC ## lung$age:        Age in years
# MAGIC ## lung$sex:        Male=1 Female=2
# MAGIC ## lung$ph.karno:   Karnofsky performance score (dead=0:normal=100:by=10)
# MAGIC ##                  rated by physician
# MAGIC
# MAGIC dimnames(x.train)[[2]] <- c('age(yr)', 'M(1):F(2)', 'ph.karno(0:100:10)')
# MAGIC
# MAGIC #summary(x.train[ , 1])
# MAGIC #table(x.train[ , 2])
# MAGIC #table(x.train[ , 3])
# MAGIC
# MAGIC x.test <- matrix(nrow=84, ncol=3) ## matrix of covariate scenarios
# MAGIC
# MAGIC dimnames(x.test)[[2]] <- dimnames(x.train)[[2]]
# MAGIC
# MAGIC i <- 1
# MAGIC #45 to 75 by 5 inclusive
# MAGIC for(age in 5*(9:15)) for(sex in 1:2) for(ph.karno in 10*(5:10)) {
# MAGIC     x.test[i, ] <- c(age, sex, ph.karno)
# MAGIC     i <- i+1
# MAGIC }
# MAGIC
# MAGIC pre <- surv.pre.bart(times=times, delta=delta, x.train=x.train, x.test=x.test)
# MAGIC str(pre)
# MAGIC print(summary(pre))

# COMMAND ----------

# MAGIC %r
# MAGIC ## load the advanced lung cancer example
# MAGIC data(lung)
# MAGIC
# MAGIC group <- -which(is.na(lung[ , 7])) ## remove missing row for ph.karno
# MAGIC times <- lung[group, 2]   ##lung$time
# MAGIC delta <- lung[group, 3]-1 ##lung$status: 1=censored, 2=dead
# MAGIC                           ##delta: 0=censored, 1=dead
# MAGIC
# MAGIC summary(times)
# MAGIC table(delta)
# MAGIC
# MAGIC x.train <- as.matrix(lung[group, c(4, 5, 7)]) ## matrix of observed covariates
# MAGIC ## lung$age:        Age in years
# MAGIC ## lung$sex:        Male=1 Female=2
# MAGIC ## lung$ph.karno:   Karnofsky performance score (dead=0:normal=100:by=10)
# MAGIC ##                  rated by physician
# MAGIC
# MAGIC dimnames(x.train)[[2]] <- c('age(yr)', 'M(1):F(2)', 'ph.karno(0:100:10)')
# MAGIC
# MAGIC #print(summary(x.train[ , 1]))
# MAGIC #print(table(x.train[ , 2]))
# MAGIC #print(table(x.train[ , 3]))
# MAGIC
# MAGIC x.test <- matrix(nrow=84, ncol=3) ## matrix of covariate scenarios
# MAGIC
# MAGIC dimnames(x.test)[[2]] <- dimnames(x.train)[[2]]
# MAGIC
# MAGIC i <- 1
# MAGIC
# MAGIC for(age in 5*(9:15)) for(sex in 1:2) for(ph.karno in 10*(5:10)) {
# MAGIC     x.test[i, ] <- c(age, sex, ph.karno)
# MAGIC     i <- i+1
# MAGIC }
# MAGIC #print(x.train)
# MAGIC #print(x.test)
# MAGIC pre <- surv.pre.bart(times=times, delta=delta, x.train=x.train, x.test=x.test)
# MAGIC #str(pre)
# MAGIC print(pre)

# COMMAND ----------

# MAGIC %r
# MAGIC data(lung)
# MAGIC
# MAGIC N <- length(lung$status)
# MAGIC
# MAGIC table(lung$ph.karno, lung$pat.karno)
# MAGIC
# MAGIC ## if physician's KPS unavailable, then use the patient's
# MAGIC h <- which(is.na(lung$ph.karno))
# MAGIC lung$ph.karno[h] <- lung$pat.karno[h]
# MAGIC
# MAGIC times <- lung$time
# MAGIC delta <- lung$status-1 ##lung$status: 1=censored, 2=dead
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC ## this study reports time in days rather than weeks or months
# MAGIC ## coarsening from days to weeks or months will reduce the computational burden
# MAGIC times <- ceiling(times/30)
# MAGIC ##times <- ceiling(times/7)  ## weeks
# MAGIC
# MAGIC print(table(times))
# MAGIC table(delta)
# MAGIC
# MAGIC ## matrix of observed covariates
# MAGIC x.train <- cbind(lung$sex, lung$age, lung$ph.karno)
# MAGIC
# MAGIC ## lung$sex:        Male=1 Female=2
# MAGIC ## lung$age:        Age in years
# MAGIC ## lung$ph.karno:   Karnofsky performance score (dead=0:normal=100:by=10)
# MAGIC ##                  rated by physician
# MAGIC
# MAGIC dimnames(x.train)[[2]] <- c('M(1):F(2)', 'age(39:82)', 'ph.karno(50:100:10)')
# MAGIC
# MAGIC #table(x.train[ , 1])
# MAGIC #summary(x.train[ , 2])
# MAGIC #table(x.train[ , 3])
# MAGIC #print(x.train)
# MAGIC
# MAGIC pre <- surv.pre.bart(times=times, delta=delta, x.train=x.train,
# MAGIC                      x.test=x.train)# , K=50)
# MAGIC
# MAGIC #summary(pre)
# MAGIC print(pre$K)
# MAGIC K <- pre$K
# MAGIC print(pre$tx.test)
# MAGIC
# MAGIC
# MAGIC pre$tx.test <- rbind(pre$tx.test, pre$tx.test)
# MAGIC pre$tx.test[ , 2] <- c(rep(1, N*K), rep(2, N*K))
# MAGIC ## sex pushed to col 2, since time is always in col 1
# MAGIC print(pre$tx.test)
# MAGIC pred <- predict(post, newdata=pre$tx.test, mc.cores=8)
# MAGIC
# MAGIC pd <- matrix(nrow=M, ncol=2*K)
# MAGIC
# MAGIC ## run one long MCMC chain in one process
# MAGIC ## set.seed(99)
# MAGIC ## post <- surv.bart(x.train=x.train, times=times, delta=delta, x.test=x.test)
# MAGIC
# MAGIC ## in the interest of time, consider speeding it up by parallel processing
# MAGIC ## run "mc.cores" number of shorter MCMC chains in parallel processes
# MAGIC post <- mc.surv.bart(x.train=x.train, times=times, delta=delta,
# MAGIC                      K=50, type='lbart', mc.cores=8, seed=99)
# MAGIC
# MAGIC
# MAGIC M <- nrow(post$yhat.train)
# MAGIC
# MAGIC for(j in 1:K) {
# MAGIC     h <- seq(j, N*K, by=K)
# MAGIC     pd[ , j] <- apply(pred$surv.test[ , h], 1, mean)
# MAGIC     pd[ , j+K] <- apply(pred$surv.test[ , h+N*K], 1, mean)
# MAGIC }
# MAGIC
# MAGIC pd.mu  <- apply(pd, 2, mean)
# MAGIC pd.025 <- apply(pd, 2, quantile, probs=0.025)
# MAGIC pd.975 <- apply(pd, 2, quantile, probs=0.975)
# MAGIC
# MAGIC males <- 1:K
# MAGIC females <- males+K
# MAGIC
# MAGIC par(mfrow=c(2, 1))
# MAGIC
# MAGIC plot(c(0, pre$times), c(1, pd.mu[males]), type='s', col='blue',
# MAGIC      ylim=0:1, ylab='S(t, x)', xlab='t (days)', ##xlab='t (weeks)',
# MAGIC      main=paste('Advanced Lung Cancer ex. (BART::lung)',
# MAGIC                 "Friedman's partial dependence function",
# MAGIC                 'Top: Logistic BART, Bottom: Probit BART',
# MAGIC                 sep='\n'))
# MAGIC lines(c(0, pre$times), c(1, pd.025[males]), col='blue', type='s', lty=2)
# MAGIC lines(c(0, pre$times), c(1, pd.975[males]), col='blue', type='s', lty=2)
# MAGIC lines(c(0, pre$times), c(1, pd.mu[females]), col='red', type='s')
# MAGIC lines(c(0, pre$times), c(1, pd.025[females]), col='red', type='s', lty=2)
# MAGIC lines(c(0, pre$times), c(1, pd.975[females]), col='red', type='s', lty=2)
# MAGIC
# MAGIC source(system.file('demo/lung.surv.bart.R', package='BART'))
# MAGIC
# MAGIC par(mfrow=c(1, 1))
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC library(BART)
# MAGIC demo("concord.surv.bart", package = "BART")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC library(BART)
# MAGIC data(lung,overwrite=T)
# MAGIC lungc <- copy_to(sc,lung, "lung",overwrite=T)
# MAGIC

# COMMAND ----------

lungc.show()

# COMMAND ----------

#import pandas as pd
import pyspark.pandas as ps
#import numpy as np
lungc_dtypes={'inst':str, 'time':int, 'status':int, 'age':int, 'sex':int, 'ph.ecog':float,
       'ph.karno':float, 'pat.karno':float, 'meal.cal':float, 'wt.loss':float}
lungc_rename={"inst": "person_id", "time": "days","ph.ecog":"ecog","ph.karno":"karno_physician","pat.karno":"karno_patient","meal.cal":"meal_calories","wt.loss":"weight_loss"}
plung=pd.read_csv("test-data/lung_original.csv",dtype=lung_dtypes).rename(columns=lungs_rename)
plung.drop('Unnamed: 0', axis=1, inplace=True)
###print(plung)
#boolean_cols=[]
#plung[column_names] = plung[column_names].astype(bool)

# COMMAND ----------


# configure analytic dataset

# adjust time to months, weeks
#plung["days"]=plung["time"]
#plung["months"] = (plung["days"]//30)+1
##plung["weeks"] = (plung["days"]//7)+1
#plung["years"] = (plung["days"]//365)+1
plung["is_female"] = (plung.sex-1).astype(bool) # Not Boolean
# delta ==> True=Death, False=Censored
plung["event"] = (plung.status - 1 ).astype(bool) # NOt Boolean
#plung["karno"] = plung["ph.karno"].fillna(plung["pat.karno"])
## use
## from pyspark.sql.functions import coalesce
##df.withColumn("karno",coalesce(df.B,df.A))
# print(plung.columns)
plung.drop(['status','sex'],axis=1,inplace=True)
###print(plung)
dcols=list(plung.columns)
print(dcols)
#get rid of dot


# COMMAND ----------

# MAGIC %md
# MAGIC dcols=[d.replace(".","_") for d in dcols]
# MAGIC #dcols=list(plung.columns)
# MAGIC print(dcols)
# MAGIC plung.columns=dcols
# MAGIC #get rid of dot

# COMMAND ----------

time="days"
event="event"
covcols=list(set(dcols)-set([time]+[event]))
print(covcols)
scols=list([event]+[time]+covcols)        
print(scols)
lungs=spark.createDataFrame(plung[scols])

# COMMAND ----------


db="cdh_reference_data"
tbl="ml_lung_cancer"
lungs.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{db}.{tbl}")
