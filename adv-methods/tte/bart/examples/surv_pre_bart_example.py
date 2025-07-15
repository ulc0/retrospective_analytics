# Databricks notebook source
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
# MAGIC summary(x.train[ , 1])
# MAGIC table(x.train[ , 2])
# MAGIC table(x.train[ , 3])
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
# MAGIC str(x.train)
# MAGIC str(x.test)
# MAGIC pre <- surv.pre.bart(times=times, delta=delta, x.train=x.train, x.test=x.test)
# MAGIC str(pre)
