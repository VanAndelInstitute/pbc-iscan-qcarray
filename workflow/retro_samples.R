library(dplyr, warn.conflicts = FALSE)
library(purrr)

sample_info <- arrow::open_dataset("s3://pbc-qcarray-sample-info/")
retro_ids <- sample_info %>%
    select(BSI_ID_Previous) %>% filter(!is.na(BSI_ID_Previous)) %>%
    collect %>%
    flatten %>% flatten_chr
retro_samples <- sample_info %>%
    filter(Sample_ID %in% retro_ids) %>% collect