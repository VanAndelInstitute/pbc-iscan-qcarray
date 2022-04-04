library(data.table, warn.conflicts = FALSE)

# Calculate the population frequencies by averaging across
# all projects/batches in the study/cohort
generate.popmaf <- function(batches, manifest) {
    batches %>% map(~ .$nsamples) %>% unlist %>% as.integer %>% sum -> npop
    batches %>% map(~ get.batch.freq(.,manifest)) %>%
        bind_rows(.id="JIRA") %>%
        group_by(snp) %>%
        summarise(maf=sum(maf) / npop) -> popmaf
    return(popmaf)
}

# Get the batch frequencies for a given project/batch
get.batch.freq <- function(batch, manifest) {
    # See if the MAFs for this project/batch are in the cache
    s3 <- paws::s3()
    key <- paste0(batch$JIRA,".freq.gz")
    filename <- paste0("/tmp/",key)
    if (file.exists(filename)) {
        print(glue::glue("File {filename} exists!"))
        fread(filename) %>% 
            as_tibble() %>% 
            mutate(snp=manifest$names) %>%
            relocate(snp) -> batch.freq
        return(batch.freq)
    }
    result <- try(s3$download_file(bucket, key, filename), silent=TRUE)
    if (!inherits(result, "try-error")) {
        print(glue::glue("Key {key} exists!"))
        fread(filename) %>% 
            as_tibble() %>% 
            mutate(snp=manifest$names) %>%
            relocate(snp) -> batch.freq
        return(batch.freq)
    }
    if(!grepl("NoSuchKey", result)) {
        stop(result)
    }
    
    # else calculate them
    #json <- httr::content(httr::GET("http://{endpoint}/sampleinfo?batchid={batch$JIRA}"))
    json <-
    '[
        {"Sample_ID" : "CPT0nnnnn_0001", "Barcode" : "203323200003", "Position" : "R11C02"},
        {"Sample_ID" : "CPT0nnnnn_0002", "Barcode" : "203323200003", "Position" : "R11C02"}
    ]'
    sample.info <- jsonlite::fromJSON(json, simplifyDataFrame=FALSE)
    names(sample.info) <- map(sample.info, ~ .$Sample_ID)
    marker.freq <- function(AA, AB, BB) (AB+2*BB)/(2*(AA+AB+BB))
    sample.info %>% 
        map(~ data.frame(snp=manifest$names,abgeno=get.allele.data(.)) %>%
        mutate(snp=manifest$names) %>%
        relocate(snp) %>%
        bind_rows(.id="Sample_ID") %>%
        group_by(snp, abgeno) %>%
        summarise(n=n(), .groups="drop_last") %>%
        spread(abgeno, n) %>%
        replace(is.na(.), 0) %>%
        summarise(maf=marker.freq(AA,AB,BB)) -> batch.freq

    # Cache the batch frequencies for this project/batch
    batch.freq %>% select(maf) %>% as.data.table() %>% fwrite(filename)
    s3$put_object(Body=filename,
                  Bucket=bucket,
                  Key=key)
    
    return(batch.freq)
}

get.allele.data <- function(sample, manifest) {
    key <- glue::glue('{sample$Barcode}_{sample$Position}.gtc')
    filename <- paste0("/tmp/",key)
    if (!file.exists(filename)) {
        s3$download_file(bucket, key, filename)
    }
    gtc <- bead.array.files$GenotypeCalls(filename)
    return(unlist(map(gtc$get_genotypes(),
            ~ bead.array.files$code2genotype[.+1])))
}
