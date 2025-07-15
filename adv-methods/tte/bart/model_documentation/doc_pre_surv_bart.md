R: Data construction for survival analysis with BART    

[surv.pre.bart {BART}](https://search.r-project.org/CRAN/refmans/BART/html/surv.pre.bart.html)

R Documentation

Data construction for survival analysis with BART
-------------------------------------------------

### Description

Survival data contained in `(t,\delta, x)` must be translated to data suitable for the BART survival analysis model; see `surv.bart` for more details.

### Usage

    surv.pre.bart( times, delta, x.train=NULL, x.test=NULL,
                   K=NULL, events=NULL, ztimes=NULL, zdelta=NULL )
    

### Arguments

`times`

The time of event or right-censoring.  

`delta`

The event indicator: 1 is an event while 0 is censored.  

`x.train`

Explanatory variables for training (in sample) data.  
If provided, must be a matrix with (as usual) rows corresponding to observations and columns to variables.  

`x.test`

Explanatory variables for test (out of sample) data.  
If provided, must be a matrix and have the same structure as x.train.  

`K`

If provided, then coarsen `times` per the quantiles `1/K, 2/K, ..., K/K`.

`events`

If provided, then use for the grid of time points.

`ztimes`

If provided, then these columns of `x.train` (and `x.test` if any) are the times for time-dependent covariates. They will be transformed into time-dependent covariate sojourn times.

`zdelta`

If provided, then these columns of `x.train` (and `x.test` if any) are the delta for time-dependent covariates. They will be transformed into time-dependent covariate binary events.

### Value

`surv.pre.bart` returns a list. Besides the items listed below, the list has a `times` component giving the unique times and `K` which is the number of unique times.

`y.train`

A vector of binary responses.

`tx.train`

A matrix with rows consisting of time and the covariates of the training data.

`tx.test`

A matrix with rows consisting of time and the covariates of the test data, if any.

### See Also

`[surv.bart](../../BART/help/surv.bart.html)`

### Examples

    
    ## load the advanced lung cancer example
    data(lung)
    
    group <- -which(is.na(lung[ , 7])) ## remove missing row for ph.karno
    times <- lung[group, 2]   ##lung$time
    delta <- lung[group, 3]-1 ##lung$status: 1=censored, 2=dead
                              ##delta: 0=censored, 1=dead
    
    summary(times)
    table(delta)
    
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
        
    

* * *

\[Package _BART_ version 2.9.4 [Index](00Index.html)\]