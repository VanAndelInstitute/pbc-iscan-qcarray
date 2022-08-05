library(dplyr, warn.conflicts = FALSE)
library(glue)

bucket <- snakemake@config[['gtc_bucket']]
batch <- snakemake@config[['JIRA']]

# Get the batch frequencies for a given project/batch
gtc_data <- arrow::open_dataset(glue("s3://{bucket}/parquet/{batch}/"))
batch_maf <- gtc_data %>%
    group_by(marker, abgeno) %>%
    filter(abgeno != "NC") %>%
    collect() %>%
    summarize(n = n(), .groups = "drop_last") %>%
    tidyr::spread(abgeno, n) %>%
    replace(is.na(.), 0) %>%
    summarize(maf = (AB+2*BB)/(2*(AA+AB+BB)))

# Cache the batch frequencies for this project/batch
arrow::write_parquet(batch_maf, snakemake@output[[1]])