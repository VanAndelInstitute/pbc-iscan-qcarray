## io.R
## functions for import and export of data from 'genotypes' objects
library(argyle, warn.conflicts = FALSE)
library(reticulate)

#' Read genotype calls and hybridization from Illumina BeadStudio output.
#'
#' @param sample.info dataframe containing sample metadata for the project
#' @param snps dataframe containing marker map for this array, in PLINK's \code{*.bim} format
#'  (chromosome, marker name, cM position, bp position); rownames should be set to marker names,
#'  and those names should match those in the BeadStudio output.
#' @param ... ignored
#'
#' @return A \code{genotypes} object with genotype calls, marker map, sample metadata and (as requested)
#'  intensity data.
#'
#' @details This function initializes a \code{genotypes} object from Illumina BeadStudio output. (For an
#'  example of the format, see the files in this package's \code{data/} directory.)  The two relevant
#'  files are \code{Sample_Map.zip} and \code{*FinalReport.zip}, which contain the sample manifest
#'  and genotype/intensity data, respectively.  On platforms with \code{unzip} available on the
#'  command line, files will be unzipped on the fly.  Otherwise \code{FinalReport.zip} (but not
#'  \code{Sample_Map.zip}) must be unzipped first.  This is due to the use of \code{data.table} to
#'  handle the usually very large genotypes file.
#'  
#'  The behavior of this function with respect to missing data in the genotypes versus the contents
#'  of \code{snps} is asymmetric.  Markers in \code{snps} which are absent in the input files will
#'  be present in the output, but with missing calls and intensities.  Markers in the input files
#'  which are missing from \code{snps} will simply be dropped.  If that occurs, check that the marker
#'  names in \code{snps} match exactly those in the input file.
#'  
#'  For provenance of the resulting object, a timestamp and checksum are provided in 
#'  \code{attr(,"timestamp")} and \code{attr(,"md5")}.
#'
#' @references
#'  Inspiration from Dan Gatti's DOQTL package: <https://github.com/dmgatti/DOQTL/blob/master/R/extract.raw.data.R>
#'
#' @export
read.beadarrayfiles <- function(sample.info, snps, bpm, ...) {

    ## stop here if marker map is not well-formed
    if (!.is.valid.map(snps)) {
        if (!all(rownames(snps) == snps$marker))
            stop(paste("Marker manifest is not well-formed.  It should follow the format of a PLINK",
                       "*.bim file: a dataframe with columns <chr,marker,cM,pos> with rownames",
                       "same as 'marker' column.  If genetic positions are unknown, set them to zero."))
    }
    
    ## read files from Illumina .gtc BeadArray files
    data <- .read.illumina.raw(sample.info, snps = length(snps), bpm = bpm)
    rownames(data$samples) <- gsub(" ","", rownames(data$samples))
    
    ## convert to matrices using data.table's optimized code
    calls <- .raw.to.matrix(data$intens, snps, keep.map = TRUE, value.col = "call")
    
    x <- .raw.to.matrix(data$intens, snps, value.col = "x", keep.map = FALSE)
    y <- .raw.to.matrix(data$intens, snps, value.col = "y", keep.map = FALSE)
    ## verify that shapes match
    all(dim(calls) == dim(x), dim(calls) == dim(y))
    ## verify that sample names are in sync
    all(colnames(calls) == colnames(x), colnames(calls) == colnames(y))
    ## verify that marker names are in sync
    all(rownames(calls) == rownames(x), rownames(calls) == rownames(y))
    
    samples.kept <- colnames(calls)
    #make.fam
    sex <- data$samples[ samples.kept,"Gender" ]
    fam <- data.frame(fid = samples.kept, iid = samples.kept,
                      mom = 0, dad = 0, sex = sex, pheno = 0,
                      stringsAsFactors = FALSE)
    rownames(fam) <- as.character(fam$iid)
    
    ## construct the return 'genotypes' object
    calls <- genotypes(.copy.matrix.noattr(calls),
                        map = attr(calls, "map"), ped = fam,
                        alleles = "native",
                        intensity = list(x = x, y = y), normalized = FALSE,
                        check = TRUE)
    
    ## make a checksum, then record file source and timestamp (which would mess up checksum comparisons)
    attr(calls, "md5") <- digest::digest(calls, algo = "md5")
    attr(calls, "timestamp") <- Sys.time()
    
    return(calls)
    
}

s3 <- paws::s3()
## process .gtc files into a dataframe (of samples) and data.table (of calls/intensities)
.read.illumina.raw <- function(sample.info, nsnps, bpm, ...) {
    
    #data <- data.table::fread(piper, skip = 9, showProgress = interactive(), stringsAsFactors = FALSE, sep = "\t")
    bead.array.files <- import('IlluminaBeadArrayFiles')
    manifest <- bead.array.files$BeadPoolManifest(bpm)
    data <- data.table::data.table(marker = character(), iid = character(), x = numeric(), y = numeric(),
                                   call1 = character(), call2 = character())
    gender <- array(numeric(), c(nrow(sample.info)))
    U <- 0
    M <- 1
    F <- 2
    
    for (i in seq_along(sample.info$Sample_ID)) {
        # Download the genotype data
        key <- glue::glue('{sample.info$Barcode[i]}_{sample.info$Position[i]}.gtc')
        filename <- paste0("/tmp/", key)
        s3$download_file(bucket, key, filename)
        
        # Read in the genotype data from binary file
        gtc <- bead.array.files$GenotypeCalls(filename)
        gender[i] <- get(as.character(gtc$get_gender()))
        calls <- gtc$get_base_calls_forward_strand(manifest$snps, manifest$source_strands)
        intensities <- unlist(gtc$get_normalized_intensities(manifest$normalization_lookups))
        sample.data <- data.table::data.table(
            marker = manifest$names, iid = sample.info$Sample_ID[i],
            x = intensities[1], y = intensities[2], 
            call1 = calls[1], call2 = calls[2])
        data <- rbind(data, sample.data)
    }
    gender[gender==3] <- 0
    samples.df <- data.frame(Name = sample.info$Sample_ID, Gender = gender)

    ## rename samples by index
	renamer <- make.unique(as.character(samples.df$Name))
    rownames(samples.df) <- renamer
    newids <- rep(renamer, 1, each = nsnps)
    #print(tail(cbind(data, newid = newids)))
    data.table::set(data, i = NULL, "iid", newids)
    ## convert 2-column allele calls to single column; mark hets, missing, etc.
    data.table::set(data, i = NULL, "call", paste0(data$call1, data$call2))
    data.table::set(data, i = NULL, "is.het", (data$call1 != data$call2))
    data.table::set(data, i = NULL, "is.na", (data$call1 == "-" | data$call2 == "-"))
    data.table::set(data, i = which(data$is.het), "call", "H")
    data.table::set(data, i = which(data$is.na), "call", "N")
    data.table::set(data, i = NULL, "call", substr(data$call, 1, 1))
    
    ## pre-key by marker (SNP name) for next step
    data.table::setkey(data, marker)
    
    return( list(samples = samples.df, intens = data) )
    
}

## convert data.table of calls/intensities to a (sites x samples) matrix
.raw.to.matrix <- function(data, snps, keep.map = FALSE,
                           sample.id.col = "iid", value.col = "call", ...) {
    
    if (!inherits(data, "data.table"))
        stop("Input should be an object of class 'data.table'.")
    
    ## strip column names which might conflict between input and marker map
    if ("cM" %in% colnames(data))
        data.table::set(data, i = NULL, "cM", NULL)
    if ("chr" %in% colnames(data))
        data.table::set(data, i = NULL, "chr", NULL)
    if ("pos" %in% colnames(data))
        data.table::set(data, i = NULL, "pos", NULL)
    
    ## reshape to big matrix
    fm <- paste("marker ~", sample.id.col)
    gty.mat <- data.table::dcast.data.table(data, as.formula(fm), value.var = value.col)
    data.table::setkey(gty.mat, marker)
    
    .map <- data.table::data.table(snps[ ,c("chr","marker","cM","pos") ])
    data.table::setkey(.map, marker)

    gty.mat <- data.table:::merge.data.table(gty.mat, .map)
    
    ## sort by position
    data.table::setorder(gty.mat, chr, pos, cM, marker)
    cn <- names(gty.mat)
    cols <- c("chr","marker","cM","pos")
    oth <- setdiff(cn, cols)
    data.table::setcolorder(gty.mat, c(cols, oth))
    
    ## demote back to dataframe
    gty.mat <- as.data.frame(gty.mat)
    newmap <- gty.mat[ ,1:4, drop = FALSE ]
    rownames(newmap) <- as.character(newmap$marker)
    newmap <- data.frame(newmap, snps[ rownames(newmap),!(colnames(snps) %in% c("chr","marker","cM","pos")) ])
    gty.mat <- as.matrix(gty.mat[ ,-(1:4), drop = FALSE ])
    
    rownames(gty.mat) <- as.character(newmap$marker)
    colnames(gty.mat) <- gsub(" ","", colnames(gty.mat))
    
    if (keep.map)
        attr(gty.mat, "map") <- newmap
    
    return(gty.mat)
    
}

## copy matrix and keep row/column names, but drop other attributes (including class!)
.copy.matrix.noattr <- function(x, ...) {
    
    if (!is.matrix(x))
        stop("Input not a matrix.")
    
    rez <- matrix(as.vector(x), ncol = ncol(x), nrow = nrow(x))
    colnames(rez) <- colnames(x)
    rownames(rez) <- rownames(x)
    return(rez)
    
}

## internal helpers for validating the 'genotypes' data structure and its parts

.is.valid.map <- function(mm, ...) {
    
    pass <- is.data.frame(mm)
    pass <- pass && all(colnames(mm)[1:4] == c("chr","marker","cM","pos"))
    if ("marker" %in% colnames(mm))
        pass <- all(rownames(mm) == as.character(mm$marker))
    else
        pass <- FALSE
    
    return(pass)
    
}