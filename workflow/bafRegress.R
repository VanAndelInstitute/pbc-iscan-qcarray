library(dplyr, warn.conflicts = FALSE)
library(readr)
library(glue)

bucket <- snakemake@config[['gtc_bucket']]
batch <- snakemake@config[['JIRA']]
output <- snakemake@output[[1]]

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
    tibble(
        names = c("estimate","stderr","tval","pval","callrate", "Nhom"),
        fit = c(coefficients(summary(fit))[2,], callrate, nrow(fit$model)))
}

# Test each sample for contamination
gtc_data <- arrow::open_dataset(glue("s3://{bucket}/parquet/{batch}/"))
popmaf <- read_tsv("popmaf.txt")
baf_results <- gtc_data %>%
    select(Sample_ID, baf, abgeno) %>%
    group_by(Sample_ID) %>%
    collect() %>%
    group_modify(~ testsamplecontamination(.$baf, .$abgeno, popmaf$maf)) %>%
    tidyr::spread(names, fit)
write_tsv(baf_results, output)