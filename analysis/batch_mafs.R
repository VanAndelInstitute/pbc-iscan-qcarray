library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(glue)

gtc_bucket <- snakemake@config[["gtc_bucket"]]
batch_name <- snakemake@config[["JIRA"]]

# Get the batch frequencies for a given project/batch
gtc_data <- open_dataset(glue("s3://{gtc_bucket}/parquet/{batch_name}/"))
# See if the MAFs for this project/batch are in the cache
tryCatch({
    open_dataset(glue("s3://{gtc_bucket}/maf/{batch_name}/"))
}, error = function(e) {
    if (!startsWith(e$message,'IOError: Path does not exist')) {
        stop(e$message)
    }
    batch_maf <- gtc_data %>%
        group_by(marker, abgeno) %>%
        filter(abgeno != "NC") %>%
        collect() %>%
        summarize(n = n(), .groups = "drop_last") %>%
        tidyr::spread(abgeno, n) %>%
        replace(is.na(.), 0) %>%
        summarize(maf = (AB+2*BB)/(2*(AA+AB+BB)))

    # Cache the batch frequencies for this project/batch
    write_dataset(batch_maf, glue("s3://{gtc_bucket}/maf/{batch_name}/"))
})