# pbc-iscan-qcarray
The Pathology and Biorepository Core at the Van Andel Research Institute uses the Illumina Infinium QC Array BeadChip on the Illumina iScan to check biospecimens for contamination and mislabling. 

This workflow is implemented in two parts, (1) automated, event-driven data wrangling, and (2) manually run QC analysis.

## Event-driven pre-processing
This first part of the workflow handles the input data wrangling. It's implemented as an AWS Serverless, event-driven processes. The general steps are:
- gencall - Generate Genotype Call files (.gtc) from BeadArray Intensity Data files (.idat).
- normalization - Calculate normalized intensities, since `gencall` doesn't do this automatically, and store the results in Apache Parquet column-oriented data files.
- metadata - Merge run/batch metadata from CSV/Excel into Apache Parquet column-oriented data files partioned by run/batch.
- popmaf - Calculate population frequencies for the current run/batch, for later aggregation over the entire study/cohort.

### Prerequisites
The infrastructure code is currently a [SAM app](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html). You'll need to install the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) if you want to make changes and/or deploy it.

## Manual QC analysis
The second part of the workflow implements the actual QC/analysis that attempts to determine whether sample contamination or mislabling has occurred. It's implemented as a Snakemake workflow.

### bafRegress — Contamination testing
The [bafRegress](https://genome.sph.umich.edu/wiki/BAFRegress) code has been pared down to only the part that does the linear regression. The parts for Python/R marshalling are no longer needed.

Also, the population minor allele frequencies (popmaf) are calculated from the genotype call data directly, rather than generating .ped files and using plink.

### argyle
Code was added to initialize the `genotypes` obect from the genotype call data directly instead of generating and using a GenomeStudio final report text file.

## Workflow overview
 ![](workflow.png)
