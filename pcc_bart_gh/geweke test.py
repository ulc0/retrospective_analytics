# Databricks notebook source
# MAGIC %r  
# MAGIC library(BART)
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %r
# MAGIC f <- function(x) ## adpated Friedman's five dimensional test function
# MAGIC     3+sin(pi*x[ , 1]*x[ , 2])-2*(x[ , 3]-0.5)^2+x[ , 4]-0.5*x[ , 5]
# MAGIC
# MAGIC N <- 100
# MAGIC A <- 1/155
# MAGIC P <- 20 #number of covariates
# MAGIC
# MAGIC set.seed(12)
# MAGIC
# MAGIC x.train <- matrix(runif(N*P), nrow=N, ncol=P)
# MAGIC x.test <- matrix(runif(N*P), nrow=N, ncol=P)
# MAGIC
# MAGIC # get the distribution based on time
# MAGIC T <- ceiling(rweibull(N, shape=2, scale=exp(f(x.train))))
# MAGIC
# MAGIC
# MAGIC C <- ceiling(rexp(N, A))
# MAGIC delta <- 1*(T<C)
# MAGIC # if delta = 1 then use T otherwise if delta = 0 then use C
# MAGIC times <- delta*T+(1-delta)*C
# MAGIC table(delta)/N
# MAGIC
# MAGIC C = 8

# COMMAND ----------

# MAGIC %r
# MAGIC # print(head(x.train))
# MAGIC # x.test
# MAGIC # f(x.train)
# MAGIC # C
# MAGIC # print(times)
# MAGIC # table(delta)/N
# MAGIC # ceiling(rexp(N,A))
# MAGIC # T

# COMMAND ----------

# MAGIC %r  
# MAGIC ##run BART with C cores in parallel
# MAGIC post = mc.surv.bart(x.train, times=times, delta=delta, mc.cores=C, seed=99,
# MAGIC                     keepevery=50)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %r  
# MAGIC # check post
# MAGIC str(post)

# COMMAND ----------

# MAGIC %r  
# MAGIC # copy to x
# MAGIC x <- x.train
# MAGIC
# MAGIC # create a x4 column of 0-1 by 0.1
# MAGIC x4 <- seq(0, 1, length.out=10)
# MAGIC print(x4)
# MAGIC
# MAGIC # expand x.test to include a x4(1-10) for each row of x
# MAGIC for(i in 1:10) {
# MAGIC     # x[4] set to x4[i] for all rows 
# MAGIC     x[ , 4] <- x4[i]
# MAGIC
# MAGIC     # initialize x test
# MAGIC     if(i==1) x.test <- x
# MAGIC     # rbind x.test to new x
# MAGIC     else x.test <- rbind(x.test, x)
# MAGIC }
# MAGIC
# MAGIC
# MAGIC # x.test copied 10 times, each set has x[4] set to val 0-1 by 0.1
# MAGIC print(x.test)

# COMMAND ----------

# MAGIC %r  
# MAGIC pre = surv.pre.bart(times=times, delta=delta,
# MAGIC                     x.train=x.train, x.test=x.test)
# MAGIC
# MAGIC str(pre)
# MAGIC

# COMMAND ----------

# MAGIC %r  
# MAGIC ##run predict with C cores in parallel
# MAGIC pred <- predict(post, newdata=pre$tx.test, mc.cores=C)
# MAGIC
# MAGIC K <- pred$K

# COMMAND ----------

# MAGIC %r  
# MAGIC str(pred)

# COMMAND ----------

# MAGIC %r  
# MAGIC # K is the number of distinct timepoints
# MAGIC K

# COMMAND ----------

# MAGIC %r  
# MAGIC # list(1:10)
# MAGIC hh = (1-1)*N*K+seq(1, N*K, by=K)
# MAGIC apply(pred$surv.test[,hh], 1, mean)

# COMMAND ----------

# MAGIC %r  
# MAGIC ##create Friedman's partial dependence function for x4
# MAGIC surv <- list(1:10)
# MAGIC
# MAGIC for(i in 1:10) {
# MAGIC     # create empty matrix of 1000 rows and K (timepoints) cols
# MAGIC     surv[[i]] <- matrix(nrow=1000, ncol=K)
# MAGIC
# MAGIC     # create sets of indexes, each set being the patient/time point. 
# MAGIC     # So if 5 patients and 10 timepoints, we would have 50 total timepoints in pre$surv.test
# MAGIC     # and we want the mean across draws for each patient/time
# MAGIC     # so we want the mean draws in calculated from columns 1,11,21,31,41 
# MAGIC     # which corresponds to the first timepoint for patients 1,2,3,4,5
# MAGIC     # The i loop before this iterates over the 10 sets of x4 vals created 
# MAGIC     # this provides us with 10 surv matrices 
# MAGIC     # In the end we have 10 surv matrices
# MAGIC     # each has K cols representing each timepoint and 1000 rows representing the average over all patient/timepoints for a draw
# MAGIC     for(j in 1:K) {
# MAGIC         h <- (i-1)*N*K+seq(j, N*K, by=K)
# MAGIC         surv[[i]][ , j] <- apply(pred$surv.test[ , h], 1, mean)
# MAGIC     }
# MAGIC
# MAGIC     # get the mean over all draws
# MAGIC     surv[[i]] <- apply(surv[[i]], 2, mean)
# MAGIC }
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %r  
# MAGIC # plot the FPD plots for x4
# MAGIC for(i in 1:10) {
# MAGIC     if(i==1) plot(c(0, pre$times), c(1, surv[[i]]), type='s',
# MAGIC                   xlim=c(0, 50), ylim=0:1, xlab='t', ylab='S(t, x)')
# MAGIC     else lines(c(0, pre$times), c(1, surv[[i]]), type='s', col=i)
# MAGIC     j <- min(which(surv[[i]]<0.5))
# MAGIC     text(pre$times[j], 0.5, paste(round(x4[i], digits=1)), col=i, pos=2)
# MAGIC }

# COMMAND ----------

# MAGIC %r 
# MAGIC
# MAGIC autocorr_plot = function(yhat){
# MAGIC     ## acf plots for 10 subjects
# MAGIC     k <- floor(seq(1, N, length.out=10))
# MAGIC     j. <- seq(-0.5, 0.4, length.out=10)
# MAGIC
# MAGIC     for(j in 1:K) {
# MAGIC         for(i in 1:10) {
# MAGIC             h <- (k[i]-1)*K+j
# MAGIC
# MAGIC             auto.corr <- acf(yhat[ , h], plot=FALSE)
# MAGIC             max.lag <- max(auto.corr$lag[ , 1, 1])
# MAGIC
# MAGIC                 if(i==1)
# MAGIC                     plot(1:max.lag+j.[i], auto.corr$acf[1+(1:max.lag), 1, 1],
# MAGIC                         type='h', xlim=c(0, max.lag+1), ylim=c(-1, 1),
# MAGIC                         sub=paste0('t=', pre$times[j]), ylab='acf', xlab='lag')
# MAGIC                 else
# MAGIC                     lines(1:max.lag+j.[i], auto.corr$acf[1+(1:max.lag), 1, 1],
# MAGIC                         type='h', col=i)
# MAGIC             }
# MAGIC
# MAGIC         Sys.sleep(1)
# MAGIC     }
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC autocorr_plot(pred$yhat.test)

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC ## trace plots for 10 subjects
# MAGIC trace_plot = function(yhat){
# MAGIC     k <- floor(seq(1, N, length.out=10))
# MAGIC
# MAGIC
# MAGIC     for(j in 1:K) {
# MAGIC         for(i in 1:10) {        
# MAGIC             h <- (k[i]-1)*K+j
# MAGIC             if(i==1) {
# MAGIC                 plot(yhat[ , h], type='l',
# MAGIC                     ylim=c(-4, 4), sub=paste0('t=', pre$times[j]),
# MAGIC                     ylab=expression(Phi(f(x))), xlab='m')
# MAGIC             } else {
# MAGIC                 lines(pred$yhat.test[ , h], type='l', col=i)
# MAGIC             }
# MAGIC         }
# MAGIC         Sys.sleep(1)
# MAGIC     }
# MAGIC }
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC trace_plot(pred$yhat.test)

# COMMAND ----------

# MAGIC %r
# MAGIC ## Geweke plot for 10 subjects
# MAGIC gew = function(yhat) {
# MAGIC     k <- floor(seq(1, N, length.out=10))
# MAGIC
# MAGIC     geweke <- list(1:10)
# MAGIC
# MAGIC     # sample number of patients
# MAGIC     # compute gewekediag
# MAGIC     for(i in 1:10) {
# MAGIC         h <- (k[i]-1)*K+1:K
# MAGIC         geweke[[i]] <- gewekediag(yhat[ , h])
# MAGIC     }
# MAGIC
# MAGIC     max.t <- max(pre$times)
# MAGIC     min.t <- -max.t/10
# MAGIC
# MAGIC     for(i in 1:10) {
# MAGIC         if(i==1) {
# MAGIC             plot(pre$times, geweke[[i]]$z, type='l',
# MAGIC                 ylab='z', xlab='t', ylim=c(-5, 5), xlim=c(min.t, max.t))
# MAGIC             lines(pre$times, rep(-1.96, K), type='l', col=6)
# MAGIC             lines(pre$times, rep(+1.96, K), type='l', col=6)
# MAGIC             lines(pre$times, rep(-2.576, K), type='l', col=5)
# MAGIC             lines(pre$times, rep(+2.576, K), type='l', col=5)
# MAGIC             lines(pre$times, rep(-3.291, K), type='l', col=4)
# MAGIC             lines(pre$times, rep(+3.291, K), type='l', col=4)
# MAGIC             lines(pre$times, rep(-3.891, K), type='l', col=3)
# MAGIC             lines(pre$times, rep(+3.891, K), type='l', col=3)
# MAGIC             lines(pre$times, rep(-4.417, K), type='l', col=2)
# MAGIC             lines(pre$times, rep(+4.417, K), type='l', col=2)
# MAGIC             text(c(0, 0), c(-1.96, 1.96), pos=2, cex=0.6, labels='0.95')
# MAGIC             text(c(0, 0), c(-2.576, 2.576), pos=2, cex=0.6, labels='0.99')
# MAGIC             text(c(0, 0), c(-3.291, 3.291), pos=2, cex=0.6, labels='0.999')
# MAGIC             text(c(0, 0), c(-3.891, 3.891), pos=2, cex=0.6, labels='0.9999')
# MAGIC             text(c(0, 0), c(-4.417, 4.417), pos=2, cex=0.6, labels='0.99999')
# MAGIC         }
# MAGIC         else lines(pre$times, geweke[[i]]$z, type='l')
# MAGIC     }
# MAGIC }
# MAGIC

# COMMAND ----------

# MAGIC %r  
# MAGIC gew(pred$yhat.test)
