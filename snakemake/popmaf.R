library(dplyr, warn.conflicts = FALSE)
library(readr)

# Calculate the population MAFs by averaging across all batches
batch_mafs <- arrow::open_dataset(snakemake@params[[1]])
popmaf <- batch_mafs %>%
    group_by(marker) %>%
    collect() %>%
    summarize(maf = sum(maf) / n())
write_tsv(popmaf, "popmaf.txt")