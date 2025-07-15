autocorr_plot = function(yhat){
    ## acf plots for 10 subjects
    k <- floor(seq(1, N, length.out=10))
    j. <- seq(-0.5, 0.4, length.out=10)

    for(j in 1:K) {
        for(i in 1:10) {
            h <- (k[i]-1)*K+j

            auto.corr <- acf(yhat[ , h], plot=FALSE)
            max.lag <- max(auto.corr$lag[ , 1, 1])

                if(i==1)
                    plot(1:max.lag+j.[i], auto.corr$acf[1+(1:max.lag), 1, 1],
                        type='h', xlim=c(0, max.lag+1), ylim=c(-1, 1),
                        sub=paste0('t=', pre$times[j]), ylab='acf', xlab='lag')
                else
                    lines(1:max.lag+j.[i], auto.corr$acf[1+(1:max.lag), 1, 1],
                        type='h', col=i)
            }

        Sys.sleep(1)
    }
}



## trace plots for 10 subjects
trace_plot = function(yhat){
    k <- floor(seq(1, N, length.out=10))


    for(j in 1:K) {
        for(i in 1:10) {        
            h <- (k[i]-1)*K+j
            if(i==1) {
                plot(yhat[ , h], type='l',
                    ylim=c(-4, 4), sub=paste0('t=', pre$times[j]),
                    ylab=expression(Phi(f(x))), xlab='m')
            } else {
                lines(pred$yhat.test[ , h], type='l', col=i)
            }
        }
        Sys.sleep(1)
    }
}


gew = function(yhat) {
    k <- floor(seq(1, N, length.out=10))

    geweke <- list(1:10)

    # sample number of patients
    # compute gewekediag
    for(i in 1:10) {
        h <- (k[i]-1)*K+1:K
        geweke[[i]] <- gewekediag(yhat[ , h])
    }

    max.t <- max(pre$times)
    min.t <- -max.t/10

    for(i in 1:10) {
        if(i==1) {
            plot(pre$times, geweke[[i]]$z, type='l',
                ylab='z', xlab='t', ylim=c(-5, 5), xlim=c(min.t, max.t))
            lines(pre$times, rep(-1.96, K), type='l', col=6)
            lines(pre$times, rep(+1.96, K), type='l', col=6)
            lines(pre$times, rep(-2.576, K), type='l', col=5)
            lines(pre$times, rep(+2.576, K), type='l', col=5)
            lines(pre$times, rep(-3.291, K), type='l', col=4)
            lines(pre$times, rep(+3.291, K), type='l', col=4)
            lines(pre$times, rep(-3.891, K), type='l', col=3)
            lines(pre$times, rep(+3.891, K), type='l', col=3)
            lines(pre$times, rep(-4.417, K), type='l', col=2)
            lines(pre$times, rep(+4.417, K), type='l', col=2)
            text(c(0, 0), c(-1.96, 1.96), pos=2, cex=0.6, labels='0.95')
            text(c(0, 0), c(-2.576, 2.576), pos=2, cex=0.6, labels='0.99')
            text(c(0, 0), c(-3.291, 3.291), pos=2, cex=0.6, labels='0.999')
            text(c(0, 0), c(-3.891, 3.891), pos=2, cex=0.6, labels='0.9999')
            text(c(0, 0), c(-4.417, 4.417), pos=2, cex=0.6, labels='0.99999')
        }
        else lines(pre$times, geweke[[i]]$z, type='l')
    }
}

