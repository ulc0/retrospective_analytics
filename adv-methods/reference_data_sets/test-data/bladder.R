library(BART)
data("cancer", package="survival")

# Bladder dataset is established for multiple recurrence
# We can use as searching for the first recurrence
blad = bladder1
blad = blad[blad["enum"]==1, ]
# drop patients whose stop time = start time (no relevant information)
blad = blad[blad["stop"]!=0, ]
# recode recur as 1 and all other censoring as 0
blad["status"] = as.vector(ifelse(blad["status"] == 1, 1, 0))
# time/7
blad["stop"] = ceiling(blad["stop"]/7)

# leave treatment as a categorical
cols = c("treatment", "number", "size", "status", "stop")
blad = blad[cols]
write.csv(blad,"blad_core.csv")


# get train long
train_cols = c("treatment", "number", "size")
train_blad = surv.pre.bart(times=blad$stop, delta=blad$status, x.train = blad[train_cols])
str(train_blad)
write.csv(train_blad,"blad_train.csv")

# get test data on the treatment variable
counter_blad = rbind(blad, blad, blad)
counter_blad
L = nrow(blad)

cond = unique(blad$treatment)


for(i in 1:length(cond)){
    str = (i-1) * L + 1
    end = i*L 
    counter_blad[str:end,]["treatment"]=cond[i]
}
write.csv(counter_blad,"blad_counter.csv")
# get test long
test_blad = surv.pre.bart(time=blad$stop, delta=blad$status, x.train = blad[train_cols], x.test=counter_blad[train_cols])
str(test_blad)
write.csv(test_blad,"test_blad_core.csv")

# train model
post = mc.surv.bart(x.train=train_blad$tx.train, y.train=train_blad$y.train, mc.cores=8)
str(post)
write.csv(post,"post_blad_post.csv")

# test model
pred = predict(post, test_blad$tx.test, mc.cores=8)
write.csv(pred,"blad_pred.csv")
# get vars
K = pred$K
l1 = length(pred$surv.test.mean)
r1 = l1/K

surv = matrix(pred$surv.test.mean)
dim(surv) = c(K, r1)
surv = t(surv)
surv

times = pred$times


mean_surv = matrix(nrow=length(cond), ncol=length(times))
for(i in 1:length(cond)){
        str = (i-1) * L + 1
        end = i*L 
        m = apply(surv[str:end, ], 2, mean)
        mean_surv[i,] = m
}

plot(c(0,times), c(1,mean_surv[1,]), type="s", col=1)
lines(c(0,times), c(1,mean_surv[2,]), type="s", col=2)
lines(c(0,times), c(1,mean_surv[3,]), type="s", col=3)
# str(pred$tx.test$treatment)
# pred$tx.test[3000,]


write.csv(blad, "blad.csv")

write.csv(cbind(train_blad$y.train, train_blad$tx.train), "blad_train_long.csv")
write.csv(test_blad$tx.test, "blad_test_long.csv")
write.csv(cbind(times,t(mean_surv)), "blad_surv_mean.csv")

# colon
head(colon, n=100)
is.na(colon$time)
options(width=400)
