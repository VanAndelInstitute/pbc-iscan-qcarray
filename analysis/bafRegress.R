testsamplecontamination <- function(baf, abgeno, maf, subset=NULL, ...) {
    stopifnot(all(length(baf)==length(maf), length(baf)==length(abgeno)))
    maf[maf>.5] <- 1-maf[maf>.5]
    amaf <- ifelse(abgeno=="BB", -maf, maf)
    callrate <- 1-mean(abgeno=="NC")
    genocat <- factor(abgeno, levels=c(0,2))
    subs <- abgeno=="BB" | abgeno=="AA"
    if (all(table(genocat[subs])>0)) {
        fit <- lm(baf~amaf+genocat, subset=subs, ...)
    } else {
        fit <- lm(baf~amaf, subset=subs, ...)
    }
    a <- c(coefficients(summary(fit))[2,], callrate, nrow(fit$model))
    names(a) <- c("estimate","stderr","tval","pval","callrate", "Nhom")
    return(a)
}
