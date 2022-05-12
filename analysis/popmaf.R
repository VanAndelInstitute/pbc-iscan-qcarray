library(data.table, warn.conflicts = FALSE)

# Fastest way to get sample counts
num_rows <- function(JIRA) {
    ParquetFileReader$create(glue('s3://pbc-qcarray-sample-info/{JIRA}/sample_info.parquet'))$num_rows
}

# Calculate the population frequencies by averaging across
# all projects/batches in the study/cohort
calc_popmaf <- function(batches, bucket) {
    npop <- batches %>% summarize(npop = sum(nsamples)) %>% unlist
    popmaf <- batches %>%
        group_by(JIRA) %>%
        summarize(get_batch_freq(JIRA,bucket), .groups="keep") %>%
        group_by(snp) %>%
        summarize(maf=sum(maf) / npop)
    return(popmaf)
}

# Get the batch frequencies for a given project/batch
get_batch_freq <- function(jira, bucket) {
    # See if the MAFs for this project/batch are in the cache
    batch_freq <- tryCatch({
        open_dataset(glue("s3://{bucket}/maf/{jira}/")) %>%
            collect() %>%
            mutate(snp=manifest$names)
    }, error = function(e) {
        if (startsWith(e$message,'IOError: Path does not exist')) {
            return(tibble())
        }
        stop(e$message)
    })
    if (nrow(batch_freq) == 0) {
        # else calculate them
        batch_freq <- sample.info %>% 
            filter(JIRA==jira) %>%
            group_by(Sample_ID) %>%
            collect() %>%
            summarize(get_genotypes(Barcode,Position,bucket), .groups="drop") %>%
            group_by(snp, abgeno) %>%
            summarize(n=n(), .groups="drop_last") %>%
            spread(abgeno, n) %>%
            replace(is.na(.), 0) %>%
            summarize(maf=(AB+2*BB)/(2*(AA+AB+BB)))

        # Cache the batch frequencies for this project/batch
        batch_freq %>% select(maf) %>%
            write_dataset(glue("s3://{bucket}/maf/{jira}/"))
    }
    return(batch_freq)
}

# Get genotype data for each sample in the sample list
get_genotypes <- function(Barcode, Position, bucket) {
    gtc <- bead_array_files$GenotypeCalls(glue("s3://{bucket}/{Barcode}/{Position}.gtc"))
    abgeno <- gtc$get_genotypes()
    tibble(snp=manifest$names,abgeno=abgeno) %>%
        mutate(abgeno=bead_array_files$code2genotype[abgeno+1])
}
