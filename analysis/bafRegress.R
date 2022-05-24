library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(glue)

gtc_bucket <- snakemake@config[["gtc_bucket"]]
batch_name <- snakemake@config[["JIRA"]]

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

# Calculate the population MAFs by averaging across all batches
batch_mafs <- open_dataset(glue("s3://{gtc_bucket}/maf/"))
popmaf <- batch_mafs %>%
    group_by(marker) %>%
    collect() %>%
    summarize(maf = sum(maf) / n())

# Test each sample for contamination
gtc_data <- open_dataset(glue("s3://{gtc_bucket}/parquet/{batch_name}/"))
baf_results <- gtc_data %>%
    select(Sample_ID, baf, abgeno) %>%
    group_by(Sample_ID) %>%
    collect() %>%
    group_modify(~ testsamplecontamination(.$baf, .$abgeno, popmaf$maf)) %>%
    tidyr::spread(names, fit)
readr::write_csv(baf_results, file = "bafRegress.txt")