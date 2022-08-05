# pbc-iscan-qcarray
The Pathology and Biorepository Core at the Van Andel Research Institute uses the Illumina Infinium QC Array BeadChip on the Illumina iScan to check biospecimens for contamination and mislabling. 

This workflow is implemented in two parts, (1) automated, event-driven data wrangling, and (2) manually run QC analysis.

## Event-driven pre-processing
This first part of the workflow handles the input data wrangling for the individual sample files. It's implemented as an AWS Serverless, event-driven processes. The general steps are:
- gencall - Generate Genotype Call files (.gtc) from BeadArray Intensity Data files (.idat);
- normalization - Calculate normalized intensities, since `gencall` doesn't do this automatically, and store the results in Apache Parquet column-oriented data files.

### Prerequisites
The pre-processing infrastructure is implemented in code as a [SAM app](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html). You'll need to install the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) if you want to make changes and/or deploy it.

## Manual QC analysis
The second part of the workflow is implemented using [Snakemake](https://snakemake.github.io/). The general steps are:
- metadata - Merge run/batch metadata from CSV/Excel into Apache Parquet column-oriented data files partioned by run/batch;
- popmaf - Calculate minor allele frequencies for eash run/batch, then aggregate over the entire study/cohort (i.e., the population);
- bafRegress - Fit a linear regression model between the B allele frequencies and the population minor allele frequencies for each sample;
- QC/analysis - determine whether sample contamination or mislabling has occurred.

## Workflow overview
 ![](workflow.png)
