testsamplecontamination <- function(baf, abgeno, maf, subset=NULL, ...) {
	stopifnot(all(length(baf)==length(maf), length(baf)==length(abgeno)))
	maf[maf>.5] <- 1-maf[maf>.5]
	amaf <- ifelse(abgeno==2, -maf, maf)
	callrate <- 1-mean(abgeno==3)
	genocat <- factor(abgeno, levels=c(0,2))
	subs <- abgeno==2 | abgeno==0
	if (!is.null(subset)) subs<-subs & subset
	if (all(table(genocat[subs])>0)) {
		fit <- lm(baf~amaf+genocat, subset=subs, ...)
	} else {
		fit <- lm(baf~amaf, subset=subs, ...)
	}
	a <- c(coefficients(summary(fit))[2,], callrate, nrow(fit$model))
	names(a) <- c("estimate","stderr","tval","pval","callrate", "Nhom")
	return(a)
}

testsamples <- function(samples, maf, getgtc) {
    for (i in seq_along(samples$sample)) {    
        #dd <- do.call(getrawdata, c(list(file, sample=samples$sample[i]), options))
        dd <- getgtc(samples$sample[i])
        reg <- testsamplecontamination(dd$BAF, dd$ABGENO, maf)
        if (i==1) {
            allcoef <- matrix(0, ncol=length(reg), nrow=length(samples$sample))
            colnames(allcoef)<-names(reg)
        }
        allcoef[i,] <- reg
    }
    dd <- data.frame(sample=samples$sample, allcoef)
    return(dd)
}