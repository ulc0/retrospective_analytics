# Databricks notebook source
# install.packages("BART")

# COMMAND ----------



# COMMAND ----------

library(BART)
library(tidyverse)

# COMMAND ----------

# demo("lung.surv.bart", package="BART")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run BART Lung

# COMMAND ----------

# get data
data(lung)

# N is length of dataframe
N = length(lung$status)
print("N patients:")
print(N)

# Use Patient Karno where physician karno is NA
h = which(is.na(lung$ph.karno))
lung$ph.karno[h] = lung$pat.karno[h]

# Set time as time to event
time = lung$time
# Set time to monthly intervals (30 day approx months)
times = ceiling(time/30)

# Set event indicator as delta (modified to 0,1 rather than 1,2)
delta = lung$status -1 

# Create the training dataset and set names
x.train = cbind(lung$sex, lung$age, lung$ph.karno)
dimnames(x.train)[[2]] = c("M(1):F(2)", "age(39:82)", "ph.karno(50:100:10)")

print("M:F")
print(table(x.train[,1]))
print("AGE")
print(summary(x.train[,2]))
print("Karno Value")
print(table(x.train[,3]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train BART

# COMMAND ----------

# BART TRAIN with multi core
post = mc.surv.bart(x.train=x.train, times=times, delta=delta, mc.cores=8, seed=99)

# COMMAND ----------

# evaluate model posterior
str(post)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the predictive dataset
# MAGIC
# MAGIC Key features:
# MAGIC - Expandaded time to event (discreate)
# MAGIC - Duplicated and Male vs Female set as "factual and counterfactuals"
# MAGIC
# MAGIC NOTE:
# MAGIC - When a certain time doesn't have any events occur for all patients it will carry the same probability as the prior time that has events.
# MAGIC   - In this example there is no events occur at day 17 across all patients. The time vector therefore won't have a time 17. In a plot the time 17 will have the same probability as day 16.

# COMMAND ----------

set.seed(99)
# Create long format 
pre = surv.pre.bart(x.train=x.train, times=times, delta=delta, x.test=x.train)

# Set K as number of Date times
K = pre$K
# Set M as the number of draws
M = post$ndpost

# Duplicate the tx.test output (length-wise)
# Set the M:F column to 1 for N*K rows and 2 for the second N*K rows
pre$tx.test = rbind(pre$tx.test, pre$tx.test)
pre$tx.test[,2] = c(rep(1, N*K), rep(2, N*K))

print("N")
print(N)
print("M")
print(M)
print("K")
print(K)
print("Length tx.test")
print(dim(pre$tx.test))
print(pre$tx.test)

# COMMAND ----------

# Get the prediction dataset
pred = predict(post, newdata=pre$tx.test, mc.cores=8)

# COMMAND ----------

# pred$---.test format example:
#       |patient 1              | patient 2           | ....
#       |t1      |t2    |t3     |t1     |t2     |t3   | ....
#       |----------cond_a-----------------------------||---------------cond b-------------------------|
#       pt1a_t1 pt1a_t2 pt1a_t3 pt2a_t1 pt2a_t2 pt2a_t3 pt1b_t1 pt1b_t2 pt1b_t3 pt2b_t1 pt2b_t2 pt2b_t3
# draw1 .       .       .       .       .       .       .       .       .       .       .       .
# draw2 .       .       .       .       .       .       .       .       .       .       .       .   
# draw3 .       .       .       .       .       .       .       .       .       .       .       .

# Posterior Predictive derivations are based on generating the summary statistic for each draw. 
# Then using the draws to derive the posterior predictive intervals
# Additionally factual-counterfactuals can be matched at their respective index

str(pred)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reshape the output matrix
# MAGIC 1. arrange_post
# MAGIC   - Converts post matrix into a M:K matrix (M is draws and K is times).
# MAGIC   - Each time is the average of all patient/time for each draw
# MAGIC 2. rearrange_out
# MAGIC   - Converts post matrix into a tibble M*N:K\*2 
# MAGIC   - Does not do any averaging
# MAGIC   - May be easier to work out different posterior analyses

# COMMAND ----------

# Arrange posterior
# - For each time point (K_i), get mean of all K_i across patients for each draw.
arrange_post = function(metric, M, K, N){
  pd = matrix(nrow=M, ncol=2*K)

  for(j in 1:K) {
      h <- seq(j, N*K, by=K)
      # print(h)
      pd[ , j] <- apply(metric[ , h], 1, mean)
      pd[ , j+K] <- apply(metric[ , h+(N*K)], 1, mean)
  }

  return(pd)
}

# COMMAND ----------

surv_p = arrange_post(pred$surv.test, M, K, N)
prob_p = arrange_post(pred$prob.test, M, K, N)

# COMMAND ----------

print(dim(surv_p))
print(dim(prob_p))

# COMMAND ----------

# Rearrange_out

rearrange_out = function(pred, M, N, K, times){
  # empty table
  ll = tibble()

  # for each draw
  # - get the set of obs/times on treatment @ value 1
  # - get the set of obs/times on condition @ value 2
  # - rearange each as matrix with single patient per row, col is times
  # - concat matrix on rows 
  # - set matrix as tibble
  # - add sample identifier and draw identifier
  # - bind rows to ll
  for(i in 1:M){
    mm1 = matrix(pred[i,1:(N*K)], nrow=N, byrow=TRUE)
    mm2 = matrix(pred[i, (N*K+1):(N*K*2)], nrow=N, byrow=TRUE)
    
    tf = as_tibble(cbind(mm1, mm2))
    tf$sample = seq(1:N)
    tf$draw = i
    
    ll = bind_rows(ll, tf)
    # 
  }

  # rename ll
  names(ll) = c(paste0("t1_", times), paste0("t2_", times), "sample", "draw")
  return(ll)
}



# COMMAND ----------

# 
surv = rearrange_out(pred$surv.test, M, N, K, pred$times)
prob = rearange_out(pred$prob.test, M, N, K, pred$times)

# COMMAND ----------

print(surv %>% glimpse)
print(prob %>% glimpse)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hazard (Prob) and Cumulative Hazard (Cumulative Prob)

# COMMAND ----------

# plot just the probabilities
# groupby draw and average
prob1 = prob %>% 
  group_by(draw) %>%
  summarise_all(mean) %>% 
  ungroup() %>%
  select(-sample, -draw) %>%
  pivot_longer(cols = contains("t"), names_to = "t", values_to="prob") %>%
  separate(t, into=c("group", "t"), "_") %>%
  mutate(t = as.numeric(t)) %>%
  group_by(group, t) %>%
  summarise(mean = mean(prob),
            lower = quantile(prob, 0.025),
            upper = quantile(prob, 0.975))#,
            # cumsum = cumsum(prob))
  # mutate(sum_prob = cumsum(prob))

prob1 

# COMMAND ----------

prob1 %>%
  # group_by(group) %>%
  ggplot() +
  # geom_line(aes(x = log(t), y = -log(sum_prob), group=group, color= group))
  geom_line(aes(x = t, y = mean, group=group, color=group)) +
  geom_ribbon(aes(x = t, ymin = lower, ymax= upper, group = group, fill = group), alpha=0.05)# +
  # geom_line(aes(x = t, y = lower, group = group, color = group))
  # geom_line(aes(x = t, y = sum_prob, group=group, color= group))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Risk Ratio

# COMMAND ----------

# group_by(draw)
# mean
# rr
# mean
rr1 = prob %>%
  group_by(draw) %>%
  summarise_all(mean) %>% 
  select(-sample) %>%
  pivot_longer(cols = contains("t"), names_to = "t", values_to="surv") %>%
  separate(t, into = c("g", "t"), "_") %>%
  mutate(t = as.integer(t)) %>%
  pivot_wider(names_from = g, values_from=surv) %>%
  mutate(ratio = t2/t1) %>% 
  # group_by(t) %>%
  # select(t, ratio) %>%
  # summarise(mean = mean(ratio),
  #           lower = quantile(ratio, 0.025),
  #           upper = quantile(ratio, 0.975)) %>%
  print(n=100)


# COMMAND ----------

rr1 %>%
  ggplot() +
  geom_line(aes(x = t, y=mean), color="cornflowerblue")+
  geom_ribbon(aes(x=t, ymin = lower, ymax=upper), fill="cornflowerblue", alpha=0.05)
  # geom_line(aes(x = t, y=lower), color="yellow") +
  # geom_line(aes(x=t, y=upper), color = "yellow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Survival Plot

# COMMAND ----------

# surv1 = surv %>% 
#   group_by(draw) %>%
#   summarise_all(mean) %>% 
#   ungroup() %>%
#   summarise_all(mean ~ mean(.x)) %>%
#   select(-sample, -draw) %>%
#   pivot_longer(cols = everything(), names_to = "t", values_to="surv") %>%
#   separate(t, into=c("group", "t"), "_") %>%
#   mutate(t = as.numeric(t))

# surv1
surv1 = surv %>% 
  group_by(draw) %>%
  summarise_all(mean) %>% 
  select(-sample) %>%
  pivot_longer(cols = contains("t"), names_to = "t", values_to="surv") %>%
  separate(t, into = c("g", "t"), "_") %>%
  mutate(t = as.integer(t)) %>%
  pivot_wider(names_from = g, values_from=surv) %>%
  mutate(diff = t2 - t1) %>%
  # mutate(ratio = t2/t1) %>% 
  group_by(t) %>%
  # select(t, ratio) %>%
  summarise(mean_t1 = mean(t1),
            lower_t1 = quantile(t1, 0.025),
            upper_t1 = quantile(t1, 0.975),
            mean_t2 = mean(t2),
            lower_t2 = quantile(t2, 0.025),
            upper_t2 = quantile(t2, 0.975),
            mean_diff = mean(diff),
            lower_diff = quantile(diff, 0.025),
            upper_diff = quantile(diff, 0.975),
            ) 
          
surv1

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the marginal survival functions for M(blue) and F(yellow). Difference between the curves F-M is in purple.

# COMMAND ----------

surv1 %>%
  ggplot() +
  geom_step(aes(x=t, y=mean_t1), color="cornflowerblue")+
  geom_ribbon(aes(x = t, y = mean_t1, ymin = lower_t1, ymax=upper_t1), fill= "cornflowerblue", alpha=0.1) +
  geom_step(aes(x=t, y=mean_t2), color="gold") +
  geom_ribbon(aes(x=t, ymin=lower_t2, ymax=upper_t2), fill="gold", alpha=0.1) +
  geom_line(aes(x=t, y=mean_diff), color="slateblue")+
  geom_ribbon(aes(x=t, ymin=lower_diff, ymax=upper_diff), fill="slateblue", alpha=0.2)

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the evaluation of the difference in Survival rates at each time point. 

# COMMAND ----------

surv1 %>%
  ggplot(aes(x = t, y = mean_diff, ymin = lower_diff, ymax=upper_diff)) +
  geom_pointrange() +
  coord_flip()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Original rearrange and plotting method
# MAGIC
# MAGIC This is the marginal survival plots using the package example.
# MAGIC It is identical to the new rearrange method.

# COMMAND ----------

pd.mu = apply(surv_p, 2, mean)
pd.025 = apply(surv_p, 2, quantile, probs=0.025)
pd.975 = apply(surv_p, 2, quantile, probs=0.975)

males = 1:K
females = males + K

# COMMAND ----------

plot(c(0, pre$times), 
  c(1, pd.mu[males]), 
  type='s', 
  col='blue',
  ylim=0:1, 
  ylab='S(t, x)', 
  xlab='t (weeks)')

     ## main=paste('Advanced Lung Cancer ex. (BART::lung)',
     ##            "Friedman's partial dependence function",
     ##            'Male (blue) vs. Female (red)', sep='\n'))
lines(c(0, pre$times), c(1, pd.025[males]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[males]), col='blue', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.mu[females]), col='red', type='s')
lines(c(0, pre$times), c(1, pd.025[females]), col='red', type='s', lty=2)
lines(c(0, pre$times), c(1, pd.975[females]), col='red', type='s', lty=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classic Survival Methods

# COMMAND ----------

# demo("concord.surv.bart", package="BART")

# COMMAND ----------

lung2 = lung %>%
  mutate(time = ceiling(time/30))

lung2

# COMMAND ----------

res.cox <- coxph(Surv(time, status) ~ age + sex + ph.karno, data =  lung2)
summary(res.cox)

# COMMAND ----------

# install.packages("survminer")

# COMMAND ----------

library(survminer)

# COMMAND ----------

sex_df <- with(lung,
               data.frame(sex = c(1, 2), 
                          age = rep(mean(age, na.rm = TRUE), 2),
                          ph.karno = rep(mean(ph.karno), 2)
                          )
               )
sex_df

# COMMAND ----------

fit <- survfit(res.cox, newdata = sex_df)

ggsurvplot(fit, data=lung, conf.int = TRUE, legend.labs=c("Sex=1", "Sex=2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison of average hazard ratios from BART and hazard ratios

# COMMAND ----------

x = rr1 %>% summarize(mean = mean(ratio),
                  lower = quantile(ratio, 0.025),
                  upper = quantile(ratio, 0.975))
print(x)

print(summary(res.cox))

# COMMAND ----------

# MAGIC %md
# MAGIC Proportional Hazard Ratio for sex is 0.59, CI 0.43-0.83.
# MAGIC Averaging the discrete Risk Ratio 0.67 with an interval range 0.361 - 1.34
# MAGIC
# MAGIC While there is no expectation that these two values would agree, given that the conditions are mostly proportional and the fact that Risk Ratio approximates HR with rare events, these two values should be relatively approximate. 
# MAGIC
# MAGIC I would not recommend using the average RR in other analyses, since if the marginalized survival curves are not proprotional then average RR provides little information.
