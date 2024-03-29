library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(glue)

gtc_bucket <- snakemake@config[["gtc_bucket"]]
batch_name <- snakemake@config[["JIRA"]]
output <- snakemake@output[[1]]
coordinates_filename <- "InfiniumQCArray-24v1-0_A3_Physical-and-Genetic-Coordinates.txt"
strand_report_filename <- "InfiniumQCArray-24v1-0_A3_StrandReport_FDT.txt"

gtc_data <- open_dataset(glue("s3://{gtc_bucket}/parquet/{batch_name}/"))
coordinates_file_path <- glue("s3://{gtc_bucket}/{coordinates_filename}")
strand_report_file_path <- glue("s3://{gtc_bucket}/{strand_report_filename}")

# Construct marker map
phys_loc = read_tsv_arrow(coordinates_file_path) %>%
    dplyr::select(marker=Name,chr=Chr,pos=MapInfo,cM="deCODE(cM)")
allele_map = read_tsv_arrow(strand_report_file_path, skip=5) %>%
    dplyr::select(marker=SNP_Name, A1=Forward_Allele1, A2=Forward_Allele2)

# Merge them by marker (SNP_Name)
marker_map <- merge(phys_loc, allele_map, by = "marker") %>%
    dplyr::select(chr,marker,cM,pos,A1,A2) %>%
    mutate_all(as.character)
row.names(marker_map) <- marker_map$marker


source("argyle-io.R")

# Downloads and import .gtc data
raw_data <- read.beadarrayfiles(gtc_data, marker_map)
sink(output)
summary(raw_data)
head(raw_data)
sink()