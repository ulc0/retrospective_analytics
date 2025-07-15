# Databricks notebook source
# MAGIC %r 
# MAGIC library(BART)
# MAGIC
# MAGIC ## estimate concordance probability: P(t1<t2)
# MAGIC N <- 2000
# MAGIC r1 <- 0.1
# MAGIC r2 <- 0.3
# MAGIC ## true concordance
# MAGIC true.p <- r1/(r1+r2)
# MAGIC
# MAGIC set.seed(12)
# MAGIC x <- rbinom(N, 1, 0.5)
# MAGIC t <- ceiling(rexp(N, r1*x+r2*(1-x)))
# MAGIC c <- ceiling(rexp(N, 0.035))
# MAGIC delta <- (t<c)
# MAGIC table(delta)/N
# MAGIC t <- delta*t+(1-delta)*c
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC true.p

# COMMAND ----------

# MAGIC %r 
# MAGIC print(x)
# MAGIC print(delta)
# MAGIC print(t)

# COMMAND ----------

# MAGIC %r
# MAGIC rbind(0,1)

# COMMAND ----------

# MAGIC %r  
# MAGIC post <- mc.surv.bart(x.train=cbind(x), x.test=rbind(0, 1),
# MAGIC                      times=t, delta=delta, mc.cores=8, seed=99) 
# MAGIC
# MAGIC str(post)

# COMMAND ----------

# MAGIC %r  
# MAGIC K <- post$K
# MAGIC q <- 1-pnorm(post$yhat.test)
# MAGIC print(q)
# MAGIC

# COMMAND ----------

# MAGIC %r  
# MAGIC post$yhat.test

# COMMAND ----------

# MAGIC %r  
# MAGIC # itterate over times
# MAGIC for(j in 1:K) {
# MAGIC     if(j==1) P <- q[ , K+1]-q[ , 1]
# MAGIC     else P <- P+(q[ , K+j]-q[ , j])*post$surv.test[ , K+j-1]*post$surv.test[ , j-1]
# MAGIC }
# MAGIC C <- 0.5*(1-P)
# MAGIC ## estimate concordance
# MAGIC summary(C)

# COMMAND ----------

# MAGIC %md
# MAGIC # Concordance 2

# COMMAND ----------

# MAGIC %r  
# MAGIC library(BART)
# MAGIC
# MAGIC N <- 1000
# MAGIC K <- 25
# MAGIC set.seed(12)
# MAGIC x <- rbinom(N, 1, 0.5)
# MAGIC l1 <- 0.5
# MAGIC l2 <- 2
# MAGIC T <- rexp(N, l1*(1-x)+x*l2)
# MAGIC C <- rexp(N, 0.25)
# MAGIC delta <- (T < C)
# MAGIC times <- delta*T+(1-delta)*C
# MAGIC table(delta)/N
# MAGIC
# MAGIC post <- mc.surv.bart(x.train=x, times=times, 
# MAGIC                      delta=delta, 
# MAGIC                      x.test=matrix(0:1, nrow=2, ncol=1), 
# MAGIC                      K=25, mc.cores=8, seed=99)
# MAGIC
# MAGIC c.true <- l1/(l1+l2)
# MAGIC c.true
# MAGIC
# MAGIC print(str(post))
# MAGIC print(c.true)
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC c.est <- (1-post$prob.test[ , 1])-(1-post$prob.test[ , 26])
# MAGIC for(j in 2:K)
# MAGIC     c.est <- c.est+((1-post$prob.test[ , j])-(1-post$prob.test[ , j+K]))*
# MAGIC         post$surv.test[ , j-1]*post$surv.test[ , j-1+K]
# MAGIC c.est <- 0.5*(1-c.est)
# MAGIC print(mean(c.est))
# MAGIC print(quantile(c.est, probs=c(0.025, 0.975)))
# MAGIC print(mean(c.est-c.true))
# MAGIC print(quantile(c.est-c.true, probs=c(0.025, 0.975)))
