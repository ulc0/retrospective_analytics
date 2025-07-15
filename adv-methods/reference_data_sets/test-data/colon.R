library(BART)
data("cancer", package="survival")
#setwd("pymc/cdh_bart_leuk")

# colon has recurrence and death data
# we will use the death data
# censoring variable indicates 1=event 0=censored
options(width=800)
colond = colon
head(colond)
colond = colond[colond["etype"] == 2,] # select death

# adjust time
colond["time"] = ceiling(colond["time"]/90)
head(colond)

cols = c("rx", "sex", "obstruct", "perfor", "adhere", "nodes", "differ", "extent", "node4", "surg", "status", "time")
colond = colond[cols]
head(colond)

# get train long
train_cols = c("rx", "sex", "obstruct", "perfor", "adhere", "nodes", "differ", "extent", "node4", "surg")

train_colon = surv.pre.bart(times=colond$time, delta=colond$status, x.train=colond[train_cols])
str(train_colon)


# get test data
# stratify on rx and sex
# count_colon = 
head(colond)
rx = unique(colond$rx)
sex = unique(colond$sex)

comb = as.matrix(expand.grid(rx, sex))
L = nrow(colond)
N = ncol(colond[train_cols]) 
test = as.data.frame(matrix(nrow = L*nrow(comb), ncol = N))
test_select_cols = c("obstruct", "perfor", "adhere", "nodes", "differ", "extent", "node4", "surg")
test_select_names = c("rx", "sex")

for(i in 1:nrow(comb)) {
    temp = rep(comb[i,],L) 
    dim(temp) = c(2,L)
    temp = t(temp)
    colnames(temp) = test_select_names 
    r1 = ((i-1) * L) + 1
    r2 = r1 + (L-1)
    temp = cbind(temp, colond[test_select_cols])
    test[r1:r2, ] = temp
}
colnames(test) = c(test_select_names, test_select_cols)
head(test)
test["sex"] = as.numeric(test$sex)
test["rx"] = as.factor(test$rx)
head(test["sex"])
str(test)

# try with just the treatment
head(colond)
counter_colon = rbind(colond, colond, colond)
L = nrow(colond)
cond = levels(colond$rx)

for(i in 1:length(cond)){
    str = (i-1) * L + 1
    end = i*L
    counter_colon[str:end, ]["rx"]=cond[i]
}

str(counter_colon)
head(counter_colon)

# get test long
# test_colon = surv.pre.bart(time=colond$time, delta=colond$status, x.train=colond[train_cols], x.test=test)
test_colon = surv.pre.bart(time=colond$time, delta=colond$status, x.train=colond[train_cols], x.test=counter_colon[train_cols])
str(test_colon)



# train model
post = mc.surv.bart(x.train=train_colon$tx.train, y.train=train_colon$y.train, mc.cores=8)
str(post)

# test model
pred = predict(post, test_colon$tx.test, mc.cores=8)
str(pred)


# get vars
K = pred$K
l1 = length(pred$surv.test.mean)
r1 = l1/K

surv = matrix(pred$surv.test.mean)
dim(surv) = c(K, r1)
surv= t(surv)

times = pred$times

mean_surv = matrix(nrow=length(cond), ncol=length(times))
for(i in 1:length(cond)){
        str = (i-1) * L + 1
        end = i*L 
        str
        end
        m = apply(surv[str:end, ], 2, mean)
        mean_surv[i,] = m
}

mean_surv
# get labels
# NN = nrow(colond)
# labels = matrix(nrow=6, ncol=2)
# for(i in 1:nrow(comb)){
#     t = ((i-1) * NN) + 1
#     labels[i,] = as.matrix(test[t,c("rx", "sex")])
# }
# labels
# labels2 = paste0(labels[,1], "_", labels[,2])
# labels2

plot(c(0,times), c(1,mean_surv[1,]), type="s", col=1)
lines(c(0,times), c(1, mean_surv[2,]), type="s", col=2)
lines(c(0,times), c(1, mean_surv[3,]), type="s", col=3)
# lines(c(0,times), c(1, mean_surv[4,]), type="s", col=4)
# lines(c(0,times), c(1, mean_surv[5,]), type="s", col=5)
# lines(c(0,times), c(1, mean_surv[6,]), type="s", col=6)
legend("topright", col=c(1,2,3,4,5,6), lty=1, lwd=2, legend = cond )





write.csv(colond, "colon.csv")

write.csv(cbind(train_colon$y.train, train_colon$tx.train), "colon_train_long.csv")
write.csv(test_colon$tx.test, "colon_test_long.csv")

cb = cbind(times,t(mean_surv))
colnames(cb) = c("t", cond)
write.csv(cb, "colon_surv_mean.csv")

