# Databricks notebook source
#https://rdrr.io/cran/BART/man/surv.bart.html
library(BART)


# COMMAND ----------

tte <- c(5,	3,	8,	21,	13)
event <- c(0,	1,	0,	1,	0)
surv.pre.bart(times = tte, delta = event)

#pre <- surv.pre.bart(times=tte, delta=event, x.train=x.train,x.test=x.train, K=50)


# COMMAND ----------


## load survival package for the advanced lung cancer example
data(lung)

N <- length(lung$status)

table(lung$ph.karno, lung$pat.karno)

## if physician's KPS unavailable, then use the patient's
h <- which(is.na(lung$ph.karno))
lung$ph.karno[h] <- lung$pat.karno[h]

tte <- lung$time
event <- lung$status-1 ##lung$status: 1=censored, 2=dead
##event: 0=censored, 1=dead

## this study reports time in days rather than weeks or months
## coarsening from days to weeks or months will reduce the computational burden
##tte <- ceiling(tte/30)
##tte <- ceiling(tte/7)  ## weeks

##table(tte)
table(event)


# COMMAND ----------


## matrix of observed covariates

x.train <- cbind(lung$sex, lung$age, lung$ph.karno)

## lung$sex:        Male=1 Female=2
## lung$age:        Age in years
## lung$ph.karno:   Karnofsky performance score (dead=0:normal=100:by=10)
##                  rated by physician

dimnames(x.train)[[2]] <- c('M(1):F(2)', 'age(39:82)', 'ph.karno(50:100:10)')

table(x.train[ , 1])
summary(x.train[ , 2])
table(x.train[ , 3])


# COMMAND ----------

print(x.train)
print(tte)
print(event)

## run one long MCMC chain in one process
set.seed(99)
#post <- surv.bart(x.train=x.train, times=tte, delta=event, x.test=x.test)

## in the interest of time, consider speeding it up by parallel processing
## run "mc.cores" number of shorter MCMC chains in parallel processes
post <- mc.surv.bart(x.train=x.train, times=tte, delta=event,
                     K=50, type='lbart', mc.cores=8, seed=99)


# COMMAND ----------


## run one long MCMC chain in one process
set.seed(99)
#post <- surv.bart(x.train=x.train, times=tte, delta=event, x.test=x.test)

## in the interest of time, consider speeding it up by parallel processing
## run "mc.cores" number of shorter MCMC chains in parallel processes
post <- mc.surv.bart(x.train=x.train, times=tte, delta=event,
                     K=50, type='lbart', mc.cores=8, seed=99)


# COMMAND ----------


pre <- surv.pre.bart(times=tte, delta=event, x.train=x.train,x.test=x.train, K=50)


# COMMAND ----------


K <- pre$K
M <- nrow(post$yhat.train)

pre$tx.test <- rbind(pre$tx.test, pre$tx.test)
pre$tx.test[ , 2] <- c(rep(1, N*K), rep(2, N*K))
## sex pushed to col 2, since time is always in col 1


# COMMAND ----------


pred <- predict(post, newdata=pre$tx.test) ##, mc.cores=8)

pd <- matrix(nrow=M, ncol=2*K)

for(j in 1:K) {
  h <- seq(j, N*K, by=K)
  pd[ , j] <- apply(pred$surv.test[ , h], 1, mean)
  pd[ , j+K] <- apply(pred$surv.test[ , h+N*K], 1, mean)
}

pd.mu  <- apply(pd, 2, mean)
pd.025 <- apply(pd, 2, quantile, probs=0.025)
pd.975 <- apply(pd, 2, quantile, probs=0.975)

males <- 1:K
females <- males+K


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
