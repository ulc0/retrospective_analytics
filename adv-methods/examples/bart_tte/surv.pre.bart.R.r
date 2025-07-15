# Databricks notebook source
## BART: Bayesian Additive Regression Trees
## Copyright (C) 2017-2022 Robert McCulloch and Rodney Sparapani

## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.

## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

## You should have received a copy of the GNU General Public License
## along with this program; if not, a copy is available at
## https://www.R-project.org/Licenses/GPL-2


## you call this function before surv.bart()
## this function takes traditional time/event
## survival variables and regressors (if any)
## and it constructs the corresponding
## tx.train, y.train and tx.test appropriate
## for use with pbart()

# COMMAND ----------



surv.pre.bart <- function(
                      tte,
                      ## vector of survival tte

                      event,
                      ## vector of event indicators: 1 event, 0 censoring

                      x.train=NULL,
                      ## matrix of covariate regressors
                      ## can be NULL, i.e. KM analog

                      x.test=NULL,
                      ## matrix of covariate regressors at tx.test settings

                      K=NULL,
                      ## if specified, then use K quantiles for time grid

                      events=NULL,
                      ## if specified, then use events for time grid

                      ztte=NULL,
                      zevent=NULL
                      ## column numbers of (ztte, zevent) time-dependent pairs

                      ##u.train=NULL
                      ## shared cluster identifiers
                      ) {
    ## currently does not handle time dependent Xs
    ## can be extended later
    ## most likely via the alternative counting process notation

    ##binaryOffset <- qnorm(1-exp(-sum(event)/sum(tte)))

    N <- length(tte)

    if(N!=length(event))
        stop('The length of tte and event must be identical')

    if(length(x.train)>0 && N!=nrow(x.train))
        stop('The length of tte and the number of rows in x.train, if any, must be identical')

    L <- length(ztte)

    if(L!=length(zevent))
        stop('The length of ztte and zevent, if any, must be identical')

    if(length(K)>0 || length(events)>0) {
        if(length(events)==0)
            events <- unique(quantile(tte, probs=(1:K)/K))
        else if(!all(events==unique(events)))
            stop(paste0('events must be unique: ', events))
        attr(events, 'names') <- NULL

        events=sort(events)
        K <- length(events)

        for(i in 1:N) {
            if(tte[i]>events[K]) {
                event[i]=0
                tte[i]=events[K]
            } else {
                k <- min(which(tte[i]<=events))
                tte[i] <- events[k]
            }
        }
    }
    else {
        events <- unique(sort(tte))
        ## time grid of events including censoring tte
        K <- length(events)
    }

    ##K <- length(events)

    if(events[1]<=0)
        stop('Time points exist less than or equal to time zero.')

    ## if(events[1]<0)
    ##     stop('Time points exist less than time zero.')
    ## else if(events[1]==0) {
    ##     warning('Time points exist equal to time zero.')
    ##     events=events[-1]
    ##     K=K-1
    ## }

    y.train <- integer(N) ## y.train is at least N long

    k <- 1

    for(i in 1:N) for(j in 1:K) if(events[j] <= tte[i]) {
        y.train[k] <- event[i]*(tte[i] == events[j])

        k <- k+1
    }

    m <- length(y.train)

    ##binaryOffset <- qnorm(mean(y.train))

    ## if(length(u.train)>0) {
    ##     makeU = TRUE
    ##     U.train <- integer(m)
    ## }
    ## else {
    ##     makeU = FALSE
    ##     U.train = NULL
    ## }

    if(length(x.train)==0) {
        p <- 0
        n <- 1

        X.train <- matrix(nrow=m, ncol=1, dimnames=list(NULL, 't'))
    } else {
        if(class(x.train)[1]=='data.frame') x.train=bartModelMatrix(x.train)

        p <- ncol(x.train)

        if(length(x.test)>0) {
            if(class(x.test)[1]=='data.frame') x.test=bartModelMatrix(x.test)
            n <- nrow(x.test)
        }

        X.train <- matrix(nrow=m, ncol=p+1)

        if(length(dimnames(x.train)[[2]])>0)
            dimnames(X.train)[[2]] <- c('t', dimnames(x.train)[[2]])
        else dimnames(X.train)[[2]] <- c('t', paste0('x', 1:p))
    }

    k <- 1
    
    for(i in 1:N) for(j in 1:K) if(events[j] <= tte[i]) {
        ##if(makeU) U.train[k] <- u.train[i]
        if(p==0) X.train[k, ] <- c(events[j])
        else X.train[k, ] <- c(events[j], x.train[i, ])

        k <- k+1
    }

    if(p==0 | length(x.test)>0) {
        X.test <- matrix(nrow=K*n, ncol=p+1, dimnames=dimnames(X.train))

        for(i in 1:n) for(j in 1:K) {
            if(p==0) X.test[j, ] <- c(events[j])
            else X.test[(i-1)*K+j, ] <- c(events[j], x.test[i, ])
        }
    }
    else X.test <- matrix(nrow=0, ncol=0)*0

    if(L>0) {
        ztte=ztte+1
        zevent=zevent+1

        for(l in 1:L) {
            i=ztte[l]
            j=zevent[l]
            X.train[ , j]=X.train[ , j]*(X.train[ , 1]>=X.train[ , i])
            X.train[ , i]=X.train[ , 1]-X.train[ , j]*X.train[ , i]
            if(length(x.test)>0) {
                X.test[ , j]=X.test[ , j]*(X.test[ , 1]>=X.test[ , i])
                X.test[ , i]=X.test[ , 1]-X.test[ , j]*X.test[ , i]
            }
        }
    }

    return(list(y.train=y.train, tx.train=X.train, tx.test=X.test,
                tte=events, K=K))
    ##, u.train=U.train ##binaryOffset=binaryOffset
