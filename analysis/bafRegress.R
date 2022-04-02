library(reticulate)
library(glue)

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

bead_array_files <- import('IlluminaBeadArrayFiles')
bucket <- 'pbc-iscan-qcarrays'
s3 <- paws::s3()

testsamples <- function(samples, maf) {
    for (i in seq_along(samples$Sample_ID)) {
        filename <- glue('{samples$Barcode[i]}_{samples$Position[i]}.gtc')
        s3$download_file(bucket, filename, filename)

        gtc <- bead_array_files$GenotypeCalls(filename)
        baf <- gtc$get_ballele_freqs()
        abgeno <- unlist(sapply(gtc$get_genotypes(), function(x) bead_array_files$code2genotype[x+1]))
        reg <- testsamplecontamination(baf, abgeno, maf)
        if (i==1) {
            allcoef <- matrix(0, ncol=length(reg), nrow=length(samples$Sample_ID))
            colnames(allcoef)<-names(reg)
        }
        allcoef[i,] <- reg
    }
    dd <- data.frame(Sample_ID=samples$Sample_ID, allcoef)
    return(dd)
}