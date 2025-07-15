# Databricks notebook source
#install.packages("BART")
#install.packages("ggfortify")
# install.packages("tidyverse")
# install.packages("mlflow")

# COMMAND ----------

library(SparkR)
library(BART)
library(tidyverse)
# library(mlflow)
# library(ggfortify)

# COMMAND ----------

dat = SparkR::sql("SELECT * from bart_pcc.end005_ccsr_sample")

# COMMAND ----------

end005 = SparkR::as.data.frame(dat)

# COMMAND ----------

str(end005)

# COMMAND ----------

# print(table(end005$tte_ccsr))

# replace NA with 180
# recode gender 0,1
end005_cln = end005 %>%
  mutate(tte_ccsr = ifelse(is.na(tte_ccsr), 180, tte_ccsr)) %>%
  mutate(gender = ifelse(str_detect(gender, "M"), 0, 1)) 
  

# Coarsen time
# time = ceiling(end005_cln$tte_ccsr/30)
time = ceiling(end005_cln$tte_ccsr/7)

# select indicator
delta = end005_cln$p3_ccsr_first

# bind predictors and cohourse to matrix
x.train = cbind(end005_cln$gender, end005_cln$covid_patient, end005_cln$age)

# Name matrix
dimnames(x.train)[[2]] = c("gender", "covid", "age")

# COMMAND ----------

# check x.train
print(class(x.train))
print(x.train)


# COMMAND ----------

# set seed
set.seed(99)

# COMMAND ----------

# Train the model initially
post = mc.surv.bart(x.train=x.train, times=time, delta=delta, mc.cores=16)

# there are a number of parameters that could be tuned here
# but first step would be to split into a train and validation set and perform the validation
# Additionally think about using the multi-core features


# COMMAND ----------

parallel::detectCores()

# COMMAND ----------

str(post)

# COMMAND ----------

# try with 8 cores
post = mc.surv.bart(x.train=x.train, times=time, delta=delta, mc.cores=1)


# COMMAND ----------

#str(post)

# COMMAND ----------


# Plot relative variabe importance

varcount = post$varcount.mean
varcount_prop = varcount/sum(varcount)

tb = tibble(names = names(varcount_prop), prop = varcount_prop)
ggplot(tb, aes(x = names, y = prop)) +
  geom_point() +
  ylim(0,1)


# COMMAND ----------

# Now build out the test set for marginalized evaluation
# initially we only care about marginalizing on the covid status
pre = surv.pre.bart(x.train=x.train, times=time, delta=delta, x.test = x.train)

# COMMAND ----------

str(pre)

# COMMAND ----------

# get dimmensions
# K is the number of time points
K = pre$K
# M is the number of draws
M = post$ndpost
# N is the number of patients
N = length(end005$covid_patient)

# COMMAND ----------

# Create the marginal test dataset
tx.test = rbind(pre$tx.test, pre$tx.test)
# set the covid status
tx.test[,3] = c(rep(0, N*K), rep(1, N*K))

# COMMAND ----------

# create prediction
pred = predict(post, newdata=tx.test, mc.cores=8)

# COMMAND ----------

str(pred)

# COMMAND ----------

pred

# COMMAND ----------

# create summarized matrix of surv times

# create a empyt matrix with nrow=number of draws and ncols = number of timepoints * 2
pd = matrix(nrow = M, ncol= 2*K)

# Each time point gets the noncovid and covid set of patients
# in surv.test the first ~19000 cols are for noncovid and the ~19000-38000 are the covid in each row. Each patient has 5 time points in each set. 
# We want to select each time point for the noncovid (1-5) and the covid (6-10)
# So each iteration sets j as the base timepoint (1,2,3,4,5) and then h is a sequence of all of the j timepoints from each patients set. 
# They are collected seperately as set 1,2,3,4,5 noncovid and set 6,7,8,9,10 covid
# The result is a matrix 1-22 columns were each ij is the mean value for the set j columns for each sepearate draw (row)
for(j in 1:K){
  h = seq(j, N*K, by=K)
  pd[,j] = apply(pred$surv.test[,h], 1, mean)
  pd[,j+K] = apply(pred$surv.test[,h+N*K], 1, mean)
}

# COMMAND ----------

prob_pd = matrix(nrow=M, ncol=2*K)

for(j in 1:K){
  h = seq(j, N*K, by=K)
  prob_pd[,j] = apply(pred$prob.test[,h], 1, mean)
  prob_pd[,j+K] = apply(pred$prob.test[,h+N*K], 1, mean)
}

# COMMAND ----------

options(width=800)

# COMMAND ----------

prob_pd

# COMMAND ----------

# pred$surv.test[,h+N*K]

# COMMAND ----------

pd

# COMMAND ----------


# get the mean across draws and the credible intervals
pd.mu = apply(pd, 2, mean)
pd.025 = apply(pd, 2, quantile, probs=0.025)
pd.975 = apply(pd, 2, quantile, probs=0.975)

print(head(pd.mu))
print(head(pd.025))
print(head(pd.975))

# COMMAND ----------

prob_pd.mu = apply(prob_pd, 2, mean)
prob_pd.025 = apply(prob_pd, 2, quantile, probs=0.025)
prob_pd.975 = apply(prob_pd, 2, quantile, probs=0.975)

print(head(prob_pd.mu))
print(head(prob_pd.025))
print(head(prob_pd.975))

# COMMAND ----------

non_cov = prob_pd[,1:22]
cov = prob_pd[,23:44]

cov

# COMMAND ----------

prob_pd

# COMMAND ----------

# get the subsets noncovid and covid
noncov = 1:K
cov = noncov+K

# COMMAND ----------

# probability


# COMMAND ----------

plot(c(0, pre$times), 
  c(1, pd.mu[noncov]), 
  type='s', 
  col='blue',
  ylim=0:1, 
  ylab='S(t, x)', 
  xlab='t (weeks)')

     ## main=paste('Advanced Lung Cancer ex. (BART::lung)',
     ##            "Friedman's partial dependence function",
      ##            'Male (blue) vs. Female (red)', sep='\n'))
lines(c(0, pre$times), c(1, pd.025[noncov]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[noncov]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.mu[cov]), col='red', type='s')
lines(c(0, pre$times), c(1, pd.025[cov]), col='red', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[cov]), col="red", type="s", lty=2)


# COMMAND ----------

K

# COMMAND ----------

ncol(pd)

# COMMAND ----------

# get the difference between each time point

noncovid_pd = pd[,noncov]
covid_pd = pd[,cov]

# ncol(noncovid_pd)


# COMMAND ----------

# dim(pred$surv.test)/22
# 87384

ddd = pred$surv.test.mean
# length(ddd)
# print(ddd)

means_pat = matrix(nrow = 1, ncol= (length(ddd)/2))

# ddd[1] - ddd[1 + length(ddd)/2]

row = 1

for(i in 1:length(ddd)/2){
  diff = ddd[i + length(ddd)/2] - ddd[i]
  means_pat[i] = diff
}


# dim(ddd) = c(7944, 22)
# dim(ddd)
# ddd[1,]

# COMMAND ----------

# dim(means_pat)/22
# 3972
# dim(means_pat) = c(3972, 22)
means_tb = tibble(diff = means_pat[1,], t = rep(1:22, 3972))
means_tb

# COMMAND ----------

means_tb2 = means_tb %>%
  group_by(t) %>%
  mutate(mean_diff = mean(diff),
        ci_025 = quantile(diff, probs=0.025),
        ci_975 = quantile(diff, probs=0.975)
  ) %>%
  select(-diff) %>%
  distinct()

means_tb2

# COMMAND ----------

means_tb2 %>%
  ggplot(aes(x=t, ymin = ci_025,  lower = mean_diff, middle = mean_diff,  upper = mean_diff, ymax = ci_975, group=t)) +
  geom_boxplot(stat = "identity")


# COMMAND ----------

dim(means_pat)
round(means_pat[1,],3)

# COMMAND ----------

# surv1 = survfit(Surv(end005_cln$tte_ccsr, end005_cln$p3_ccsr_first)
sv = survfit(Surv(ceiling(tte_ccsr/7), p3_ccsr_first) ~ covid_patient, data=end005_cln)
sv1 =summary(sv)
sv1

# COMMAND ----------

plot(c(0, sv1$time[1:22]), 
  c(1, sv1$surv[1:22]), 
  type='s', 
  col='blue',
  ylim=0:1, 
  ylab='S(t, x)', 
  xlab='t (weeks)')

     ## main=paste('Advanced Lung Cancer ex. (BART::lung)',
     ##            "Friedman's partial dependence function",
      ##            'Male (blue) vs. Female (red)', sep='\n'))
lines(c(0, sv1$time[1:22]), c(1, sv1$lower[1:22]), col='blue', type='s', lty=2)
lines(c(0, sv1$time[1:22]), c(1, sv1$upper[1:22]), col='blue', type='s', lty=2)
lines(c(0, sv1$time[23:41]), c(1, sv1$surv[23:41]), col='red', type='s')
lines(c(0, sv1$time[23:41]), c(1, sv1$lower[23:41]), col='red', type='s', lty=2)
lines(c(0, sv1$time[23:41]), c(1, sv1$upper[23:41]), col="red", type="s", lty=2)

# COMMAND ----------

# print(pre$y.train)
print(pre$tx.train)
