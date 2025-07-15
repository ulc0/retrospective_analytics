setwd("pymc/cdh_bart_leuk")

library(BART)
# AML
data("cancer", package="survival")
aml

# get the pre train dataset
# note that the binary variable 

# recode maintain
aml["m_1"] = as.vector(ifelse(aml["x"] == "Maintained", 1,0))

# get surv.pre
aml_pre = surv.pre.bart(times=aml$time, delta=aml$status, x.train=aml["m_1"])
aml_pre

# create counterfactual
L = nrow(aml)
test = rbind(aml, aml)
# set values
test[1:L,"m_1"] = 0
test[(L+1):(L+L), "m_1"] = 1

# get longform pre
test_pre = surv.pre.bart(times = aml$time, delta=aml$status, x.train=aml["m_1"], x.test = test["m_1"])
test_pre

# train model
post = mc.surv.bart(x.train=aml_pre$tx.train, y.train=aml_pre$y.train, mc.cores=6)

str(post)

# test model
pred = predict(post, test_pre$tx.test, mc.cores=8)
str(pred)

# get vars for rearrange
K = pred$K
l1 = length(pred$surv.test.mean)
r1 = l1/K

# reshape matrix of output
surv = matrix(pred$surv.test.mean) 
dim(surv) = c(K,r1)
surv = t(surv)

# separate on the conditions
# note since there is only one variable we could
# have tested on a 1 and 0 dataset instead of the full 
# counterfactual
m_0 = apply(surv[1:L,], 2, mean)
m_1 = apply(surv[(1+L):(L+L),], 2, mean)

# get x axis for plots
pred_times = pred$times

# plot 
plot(c(0,pred_times), c(1,m_0), type="s", col=1)
lines(c(0,pred_times), c(1,m_1), type = "s", col=2)

# write outputs
write.csv(aml, "aml_simple.csv")
write.csv(test, "aml_test.csv")
write.csv(test_pre,"aml_test_pre.csv")
write.csv(post,"aml_post.csv")
write.csv(cbind(aml_pre$y.train, aml_pre$tx.train), "aml_simple_train_long.csv")
write.csv(test_pre$tx.test, "aml_simple_test_long.csv")
write.csv(cbind(pred_times, m_0, m_1), "aml_simple_surv_mean.csv")

