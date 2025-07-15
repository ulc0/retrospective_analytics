library(lung)
library(BART)

leuk = data(leukemia)

leukemia
write.csv(leukemia, "leukemia.csv")



# TD - time to death or on study time
leukemia$TD=ceiling(leukemia$TD/30)
# TB - Disease free SV time (time to relapse, Death or EOS)
leukemia$TB=ceiling(leukemia$TB/30)
# TA - time to acute GVHD
leukemia$TA=ceiling(leukemia$TA/30)
# TC - time to chronic GVHD
leukemia$TC=ceiling(leukemia$TC/30)
# TP - time of platelets returning to normal levels
leukemia$TP=ceiling(leukemia$TP/30)
# X7 - waiting time to transplant in days
leukemia$X7=ceiling(leukemia$X7/30)



# number of patients
N=137

# Events = 1-88 sparse 
# between td and tb
events=unique(sort(c(leukemia$TD, leukemia$TB)))
events

# K = 64 distinct events
K=length(events)

# T = 88
T=events[K]



    ## the following covariates appear to be important
    ## G, TB, R, TA, A, TC,  C, TP,  P, X2, X8
pick=c(1,  3, 5,  7, 8,  9, 10, 11, 12, 14, 20) # not actually used
x.train3=as.matrix(leukemia[ , -c(2, 4, 6)])
# Cols in x.train3
# "G"   "TB"  "R"   "TA"  "A"   "TC"  "C"   "TP"  "P"   "X1"  "X2"  "X3" "X4"  "X5"  "X6"  "X7"  "X8"  "X9"  "X10"


# Create a counter factual/test
P=ncol(x.train3)
# P = 19 number of columns

# L is the number of combinations created
# there are 5 binary conditions so the counterfactual required is 2^5 = 32
L=32

# empty matrix of l*N rows with P columns
x.test3=matrix(nrow=L*N, ncol=P)
dimnames(x.test3)=dimnames(x.train3) # just pasing names of columns

# h is a rowset of patients(137 rows) so rows 1-137 in iter 1 then rows 138-274 in iter2

k=1
# replased 0 = no, 1 = true
for(R in 0:1)
    # acute gvhd 0=no 1=yes
    for(A in 0:1)
        # chronic gvhd 0=no 1 = yes
        for(C in 0:1)
            # platelet recovery 0=no, 1 = yes
            for(P in 0:1)
                # age 20 or 40
                for(X2 in c(20, 40)) {
                    h=(k-1)*N+1:N
                    print(h)
                    break
                    x.test3[h, ]=x.train3
                    x.test3[h, 'TB']=R*8+(1-R)*T # switch between 8 and 88. 88 is the longest event free survival time, IDK why it is 8 not 1 for the min
                    x.test3[h, 'R']=R # R = 0 & TB=8, R = 1 TB=88
                    x.test3[h, 'TA']=A*1+(1-A)*T # switch between 88 and 1
                    x.test3[h, 'A']=A # A=0 & TA = 88, A=1 & TA=1
                    x.test3[h, 'TC']=C*5+(1-C)*T # switch between 5 and 88
                    x.test3[h, 'C']=C # C=0 & TC = 88, C=1 & TC=5
                    x.test3[h, 'TP']=P*1+(1-P)*T # swtich between 1 and 88
                    x.test3[h, 'P']=P # P=0 & TP = 88, P=1 & TP = 1
                    x.test3[h, 'X2']=X2 # switch between 20 and 40
                    k=k+1
                }

str(x.test3)
# X1,3,4,5,6,7,8,9,10 are the actual patient vars and Disease GROUP (G)
# We are evaluating over Doner Age (X2), Platelet recover (P, TP), Chronic GVHD (C, TC), Acute CVHD (A, TA), Relapse (R, TB)   


# Train wit time as Time to Death, delata as Death
post3=mc.surv.bart(x.train=x.train3, times=leukemia$TD, delta=leukemia$D, 
                   events=events, ztimes=c(2, 4, 6, 8), zdelta=c(3, 5, 7, 9),
                   sparse=TRUE, mc.cores=8, seed=99)

state3=surv.pre.bart(leukemia$TD, leukemia$D, x.train3, x.test3,
                     events=events,
                     ztimes=c(2, 4, 6, 8), zdelta=c(3, 5, 7, 9))

# test data is of length 64 distinct times * 32 combo * 137 pat 


# state3$tx.test[4284:4500,]
# str(x.test3)
# str(state3$tx.test)
# 4384/137


# evaluate the first patient
zzz = 137*(1:31)+1
zzz
x.test3[zzz,]
state3$tx.train[1:64,]
state3$tx.test[1:64,]
# check the combination of testX
nnn = 6
xxx = (64*137*nnn + 1):(64*137*nnn + 64) 
state3$tx.test[xxx,]


# time to event variables
# has paired time and var ind
# expand patient on time to main event and expand var time with same index
# for each row-time 
    # if var switches from previous, reset vartime count at 0
# example
#   G  TB   R  TA   A  TC   C  TP   P  X1  X2  X3  X4  X5  X6  X7  X8  X9 X10 
#  1  70   0   3   1   5   1   1   1  26  33   1   0   1   1   4   0
# TD = 70a
# this patient has a TD (main event time = 70)
# they also have a TB = 70 (relapse time) and relapse ind = 0
# also a TA = 3 (acute GVHD) with A (acute GVHD ind = 1)
# also a TC = 5 (chronic GVHD) with C (chronic GVHD ind = 1)
# also a TP = 1 (time platelet recover) and P (platlet ind = 1)
# so we want to expand the TA, TC, and TP to create set of days the patient was in 
# a certain state (as given by the inds A,C,P) and index the sets relative to the main time
#       t G TB R TA A TC C TP P X1 X2 X3 X4 X5 X6 X7 X8 X9 X10
# [1,]  1 1  1 0  1 0  1 0  0 1 26 33  1  0  1  1  4  0  1   0
# [2,]  2 1  2 0  2 0  2 0  1 1 26 33  1  0  1  1  4  0  1   0
# [3,]  3 1  3 0  0 1  3 0  2 1 26 33  1  0  1  1  4  0  1   0
# [4,]  4 1  4 0  1 1  4 0  3 1 26 33  1  0  1  1  4  0  1   0
# [5,]  5 1  5 0  2 1  0 1  4 1 26 33  1  0  1  1  4  0  1   0
# [6,]  6 1  6 0  3 1  1 1  5 1 26 33  1  0  1  1  4  0  1   0
# [7,]  7 1  7 0  4 1  2 1  6 1 26 33  1  0  1  1  4  0  1   0
# [8,]  8 1  8 0  5 1  3 1  7 1 26 33  1  0  1  1  4  0  1   0
#           *1   *2   *3    *4
# *1 shows that R=0 so TB is the same as T
# *2 shows the for time 1-2, A is 0, then at t=3 A = 1, so TA resets to 0 
    # and extends through the reset of the time points 
# *3 shows the C = 0 for t 1-4, and at t=5 then C=1 so TC resets to 0 
    # and extends through the rest of teh time points
# *4 show P=1 at t=1, so TP resets to 0 and extends through the rest of the timepoints
# Final note is that when there is a skip in the main times that skip will be shown in the var times
# we will need a python implementation of this.

    # this patient has a TB at 0
## post3=mc.surv.bart(state3$tx.train, state3$y.train,
##                    x.test=state3$tx.train,
##                    sparse=TRUE,
##                    mc.cores=8, seed=99)


#  x.train2 x.test2 are without recurrence
x.train2=x.train3[ , -(2:3)]
x.test2=x.test3[ , -(2:3)]
post2=mc.surv.bart(x.train=x.train2, times=leukemia$TB, delta=leukemia$R, 
                   events=events, ztimes=c(2, 4, 6), zdelta=c(3, 5, 7),
                   sparse=TRUE, mc.cores=8, seed=99)

state2=surv.pre.bart(leukemia$TB, leukemia$R, x.train2, x.test2,
                     events=events, ztimes=c(2, 4, 6), zdelta=c(3, 5, 7))

## post2=mc.surv.bart(state2$tx.train, state2$y.train,
##                    x.test=state2$tx.train,
##                    sparse=TRUE,
##                    mc.cores=8, seed=99)

##pdf(file='leuk.pdf')

# This is the visualization
# L = 32 
# N = 137
# K = 64
# m is just a tracker


par(mfrow=c(2, 2))
m=0
# for each combo L
for(l in 1:L) {
    # l = 4
    h=(l-1)*N*K+1:(N*K)
    # print(h[1:10])
    break
    # Additional combinations on G and X8
    # for each G-X8 combination,
    # 
    for(G in 1:5) {
        if(G==1) {
            state3$tx.test[h, 'G']=1
            state3$tx.test[h, 'X8']=0
        } else if(G %in% 2:3) {
            state3$tx.test[h, 'G']=2
            state3$tx.test[h, 'X8']=G-2
        } else if(G %in% 4:5) {
            state3$tx.test[h, 'G']=3
            state3$tx.test[h, 'X8']=G-4
        } 
        # copy from state3 to state2
        state2$tx.test[h, 'G']=state3$tx.test[h, 'G']
        state2$tx.test[h, 'X8']=state3$tx.test[h, 'X8']
        # predict with replase
        pred3=predict(post3, state3$tx.test[h, ], mc.cores=8)
        # predict without relapse
        pred2=predict(post2, state2$tx.test[h, ], mc.cores=8)

        # getting the titles for grpahs
        i=(l-1)*N+1
        R=x.test3[i, 'R']
        string=paste0(' R=', R, ' A=', x.test3[i, 'A'], 
                      ' C=', x.test3[i, 'C'], ' P=', x.test3[i, 'P'], 
                      ' X2=', x.test3[i, 'X2'])
        
        state0.mean=double(K)
        state1.mean=double(K)
        for(j in 1:K) {
            k=seq(j, N*K, by=K) 
            state0.mean[j]=mean(apply(pred3$surv.test[ , k], 1, mean))
            state1.mean[j]=mean(apply(pred2$surv.test[ , k]*
                                      pred3$surv.test[ , k], 1, mean))
            ## state2.mean[j]=mean(apply((1-pred2$surv.test[ , k])*
            ##                           pred3$surv.test[ , k], 1, mean))
        }

        if(R==1) state1.mean[8:K]=0
        
        if(G==1) 
        plot(c(0, pred3$times), c(1, state0.mean), type='s', lwd=2, lty=G,
             ylim=0:1, xlab='t (months)', ylab='P(t, state)', main=string)
        else
        lines(c(0, pred3$times), c(1, state0.mean), type='s', lwd=2, lty=G)
        lines(c(0, pred3$times), c(1, state1.mean), lty=G,
              type='s', lwd=2, col=2)
        lines(c(0, pred3$times), c(0, state0.mean-state1.mean), lty=G,
              type='s', lwd=2, col=4)
        if((l%%4)==0) {
            legend('topright', col=c(1, 2, 4), lty=1, lwd=2,
                   legend=c('alive', 'remission', 'relapsed'))
        }
        else if((l%%2)==0) {
            legend('topright', lty=1:5, lwd=2,
                   legend=c('G=1 X8=0', 'G=2 X8=0', 'G=2 X8=1',
                            'G=3 X8=0', 'G=3 X8=1')) 
        }
        m=m+1
        if((m%%20)==0) dev.copy2pdf(file=paste0('leuk', m/20, '.pdf'))
    }
}
##dev.off()
par(mfrow=c(1, 1))