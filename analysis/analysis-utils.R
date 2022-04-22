library(plyr)
library(dplyr, warn.conflicts = FALSE)


merge.sheets <- function(manifest, sample_sheets){
  library(dplyr)
  # Re-generate appropriate sample sheets for Genome Studio
  # execute for different pairings. (e.g.: Tumor/Normal, Tumor/NAT)
  
  # import the sample info for this pairing (e.g. Tumor/Normal) 
  samp_info = dplyr::select(manifest, c("BSI_ID","Subject ID", "Anatomic Site", "Sample Type", "Within batch pairing"))
  # reformat column names
  colnames(samp_info) = c("Sample_ID", "Sample_Name", "Anatomic_Site", "Sample_Type","Pairing")
  
  # define header from sample_sheet #1 and add column for Sample_Name  
  header = sample_sheets[[1]][1:which(sample_sheets[[1]][1] == "Sample_ID"),]
  header$`X.7` = ""
  header$X.7[nrow(header)] = "Sample_Name"
  
  # iterate over each sample sheet.
  for (i in 1:length(sample_sheets)){
    
    # import sample sheet, replace the sample_sheet list's filename with with the imported dataframe. add column for Sample_Name
    sample_sheets[[i]]$`X.7` = ""
    sample_sheets[[i]]$X.7[which(sample_sheets[[i]][1] == "Sample_ID")] = "Sample_Name"
    
    # define header
    header = sample_sheets[[i]][1:which(sample_sheets[[i]][1] == "Sample_ID"),]
    
    # Separate actual dataframe from the header
    sample_sheets[[i]] = sample_sheets[[i]][(which(sample_sheets[[i]][1] == "Sample_ID")+1):nrow(sample_sheets[[i]]), ]
    colnames(sample_sheets[[i]]) = header[nrow(header),]
    # remove rows without CPT identifiers
    sample_sheets[[i]] = sample_sheets[[i]] %>% dplyr::filter(grepl("CPT",sample_sheets[[i]]$Sample_ID))
  }
  
  # concatenate sample_sheets
  merged = dplyr::bind_rows(sample_sheets[1:length(sample_sheets)])
  
  # match to Sample_Name (C3N/C3L) from the manifest.
  merged = join(dplyr::select(merged,c("Sample_ID","SentrixBarcode_A","SentrixPosition_A","Sample_Plate","Sample_Well","PI")),
                dplyr::select(samp_info, c("Sample_ID","Sample_Type","Anatomic_Site","Sample_Name")),
                by="Sample_ID") %>%
                  # reorder rows
                  dplyr::select("Sample_ID","SentrixBarcode_A","SentrixPosition_A","Sample_Plate",
                                "Sample_Well","Sample_Type","Anatomic_Site","PI","Sample_Name")
            
  # bind the header back onto the merged SampleSheet
    # first make the colnames match
    colnames(header) = colnames(merged)
    merged = rbind(header,merged)
    # then change back colnames of sheet with header
    colnames(merged) = c("[Header]", "", "", "", "", "", "", "", "")
  return(merged)
}

s3 <- paws::s3()
bucket <- "pbc-iscan-qcarrays"
filename <- "InfiniumQCArray-24v1-0_A3_Physical-and-Genetic-Coordinates.txt"
s3$download_file(bucket, filename, paste0("/tmp/", filename))
filename <- "InfiniumQCArray-24v1-0_A3_StrandReport_FDT.txt"
s3$download_file(bucket, filename, paste0("/tmp/", filename))

source("argyle-io.R")

import.gencalls <- function(sample.info, manifest) {

  # Construct marker map
  phys_loc = read.delim("/tmp/InfiniumQCArray-24v1-0_A3_Physical-and-Genetic-Coordinates.txt", header = TRUE, sep= '\t')
  allele_map = as.data.frame(read.delim("/tmp/InfiniumQCArray-24v1-0_A3_StrandReport_FDT.txt", header = FALSE,sep="\t", stringsAsFactors = F))

    #remove header
    allele_map = allele_map[-c(1:5), ]
    colnames(allele_map) = allele_map[1,]
    allele_map = allele_map[-c(1), ] %>% dplyr::select(SNP_Name, Forward_Allele1, Forward_Allele2)
  
  #Re-name cols for argyle input
  colnames(phys_loc) <- c("marker", "chr", "pos", "cM")
  colnames(allele_map) <- c("marker", "A1", "A2")
  
  #Merge them by marker (SNP_Name)
  marker_map <- merge(phys_loc, allele_map, by = "marker") %>% dplyr::select(chr,marker,cM,pos,A1,A2) %>% mutate_all(as.character)
  row.names(marker_map) <- marker_map$marker
  # Downloads and import .gtc data
  return(read.beadarrayfiles(sample.info, snps = marker_map, manifest = manifest))
}


myplot.QC.result <- function (qc, show = c("point", "label"), theme.fn = ggplot2::theme_bw, ...){
  calls <- qc$calls
  intens <- qc$intensity
  if (is.null(calls$filter)) 
    calls$filter <- FALSE
  show <- match.arg(show)
  p1 <- ggplot2::ggplot(calls, ggplot2::aes(x = N, y = H, label = iid, 
                                            colour = filter))
  if (show == "point") 
    p1 <- p1 + ggplot2::geom_point()
  else if (show == "label") 
    p1 <- p1 + ggplot2::geom_text()
  p1 <- p1 + ggplot2::scale_colour_manual(values = c("black", 
                                                     scales::muted("red")), na.value = "grey") + ggplot2::scale_x_continuous(label = function(x) sprintf("%.1f", 
                                                                                                                                                         x/1000)) + ggplot2::scale_y_continuous(label = function(x) sprintf("%.1f", 
                                                                                                                                                                                                                            x/1000)) + ggplot2::guides(colour = FALSE) + ggplot2::xlab("\ncount of N calls (x1000)") + 
    ggplot2::ylab("count of H calls (x1000)\n") + theme.fn()
  if (is.null(intens)) 
    return(p1)
  p2 <- ggplot2::ggplot(intens) + ggplot2::geom_line(ggplot2::aes(x = iid, 
                                                                  y = value, group = q, colour = q)) + ggplot2::scale_colour_distiller("quantile", 
                                                                                                                                       palette = "Spectral", label = scales::percent, breaks = seq(0, 
                                                                                                                                                                                                   1, 0.2)) + ggplot2::xlab("\nsamples (sorted by median intensity)") + 
    ggplot2::ylab("\nintensity quantiles\n") + theme.fn() + 
    ggplot2::theme(axis.text.x = ggplot2::element_blank(), 
                   panel.grid = ggplot2::element_blank())
  calls.m <- reshape2::melt(calls, id.vars = c("iid", "filter"))
  colnames(calls.m) <- c("iid", "filter", "call", "value")
  calls.m$iid <- factor(calls.m$iid, levels = levels(intens$iid))
  calls.m$call <- factor(calls.m$call, levels = c("A", "B", 
                                                  "H", "N"))
  calls.m$filter.y <- 0
  call.cols <- RColorBrewer::brewer.pal(4, "Spectral")
  p3 <- ggplot2::ggplot(calls.m) + ggplot2::geom_bar(ggplot2::aes(x = iid, 
                                                                  y = value, fill = call), position = "stack", stat = "identity") + 
    ggplot2::geom_point(data = subset(calls.m, filter), ggplot2::aes(x = iid, 
                                                                     y = filter.y), size = 3, shape = 21, fill = "white", 
                        colour = "black") + ggplot2::scale_fill_manual("genotype\ncall", 
                                                                       values = rev(call.cols)) + ggplot2::scale_y_continuous(label = function(x) sprintf("%.1f", 
                                                                                                                                                          x/1000)) + ggplot2::guides(colour = FALSE) + ggplot2::ylab("# calls (x1000)\n") + 
    theme.fn() + ggplot2::theme(axis.text.x = ggplot2::element_blank(), 
                                axis.title.x = ggplot2::element_blank(), panel.grid = ggplot2::element_blank())
  rez <- gtable:::rbind_gtable(ggplot2::ggplotGrob(p3), ggplot2::ggplotGrob(p2), 
                               size = "first")
  panels <- rez$layout$t[grep("panel", rez$layout$name)]
  #rez$heights[panels] <- lapply(c(1,2), grid::unit, "null")
  return(rez)
}

myqcplot <- function (gty, draw = TRUE, ...){
  if (!inherits(gty, "genotypes")) 
    stop("Please supply an object of class 'genotypes'.")
  if (is.null(attr(gty, "qc"))) 
    gty <- run.sample.qc(gty, ...)
  p <- myplot.QC.result(gty$qc, ...)
  if (draw) {
    if (inherits(p, "ggplot")) 
      plot(p)
    else if (inherits(p, "gtable")) 
      gtable:::plot.gtable(p)
  }
  invisible(p)
}


