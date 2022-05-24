# pbc-iscan-qcarray - Analysis
This part of the workflow (QC) is implemented as a Snakemake workflow.

## Setup
For a new Cloud 9 environment, run install_tools.sh first to configure it.

## bafRegress
The bafRegress code has been pared down to only the part that does the linear regression. The parts for Python/R marshalling are no longer needed.

Also, the population minor allele frequencies (popmaf) are calculated from the genotype call (.gtc) files directly, rather than generating .ped files and using plink.

## argyle
Code was added to initialize the `genotypes` object from the genotype call (.gtc) files directly instead of generating and using a GenomeStudio final report text file.
