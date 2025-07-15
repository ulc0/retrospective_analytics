# Databricks notebook source
library(BART)
## load the advanced lung cancer example
data(lung)
str(lung)

# COMMAND ----------



group <- -which(is.na(lung[ , 7])) ## remove missing row for ph.karno
times <- lung[group, 2]   ##lung$time
delta <- lung[group, 3]-1 ##lung$status: 1=censored, 2=dead
                          ##delta: 0=censored, 1=dead

table(times)
table(delta)
#table(lung[group])

# COMMAND ----------


x.train <- as.matrix(lung[group, c(4, 5, 7)]) ## matrix of observed covariates
## lung$age:        Age in years
## lung$sex:        Male=1 Female=2
## lung$ph.karno:   Karnofsky performance score (dead=0:normal=100:by=10)
##                  rated by physician

dimnames(x.train)[[2]] <- c('age(yr)', 'M(1):F(2)', 'ph.karno(0:100:10)')

summary(x.train[ , 1])
table(x.train[ , 2])
table(x.train[ , 3])

x.test <- matrix(nrow=84, ncol=3) ## matrix of covariate scenarios

dimnames(x.test)[[2]] <- dimnames(x.train)[[2]]

i <- 1

for(age in 5*(9:15)) for(sex in 1:2) for(ph.karno in 10*(5:10)) {
    x.test[i, ] <- c(age, sex, ph.karno)
    i <- i+1
}

pre <- surv.pre.bart(times=times, delta=delta, x.train=x.train, x.test=x.test)
str(pre)

# COMMAND ----------


par(mfrow=c(2, 1))

plot(c(0, pre$times), c(1, pd.mu[males]), type='s', col='blue',
     ylim=0:1, ylab='S(t, x)', xlab='t (days)', ##xlab='t (weeks)',
     main=paste('Advanced Lung Cancer ex. (BART::lung)',
                "Friedman's partial dependence function",
                'Top: Logistic BART, Bottom: Probit BART',
                sep='\n'))
lines(c(0, pre$times), c(1, pd.025[males]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[males]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.mu[females]), col='red', type='s')
lines(c(0, pre$times), c(1, pd.025[females]), col='red', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[females]), col='red', type='s', lty=2)

source(system.file('demo/lung.surv.bart.R', package='BART'))

par(mfrow=c(1, 1))
