library(dplyr, warn.conflicts = FALSE)
library(readr)
library(glue)

bucket <- snakemake@config[['gtc_bucket']]
output <- snakemake@output[[1]]

# Calculate the population MAFs by averaging across all batches
batch_mafs <- arrow::open_dataset(glue("{bucket}/maf/"))
popmaf <- batch_mafs %>%
    group_by(marker) %>%
    collect() %>%
    summarize(maf = sum(maf) / n())
write_tsv(popmaf, output)