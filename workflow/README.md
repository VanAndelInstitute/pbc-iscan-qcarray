# pbc-iscan-qcarray > Snakemake
This part of the project is implemented as a Snakemake workflow.

## Setup
For a new Cloud 9 environment, run setup.sh first to increase the EBS storage volume and update to AWS CLI v2. This script also has example commands to build,push,run a Docker image for use with AWS Batch that includes R, Mamba, [Snakemake](https://snakemake.readthedocs.io/), [argyle](https://rdrr.io/github/andrewparkermorgan/argyle/), [Apache Arrow](https://arrow.apache.org/docs/r/index.html), [AWS Data Wrangler](https://aws-data-wrangler.readthedocs.io/) and other dependencies.

## bafRegress
The [bafRegress](https://genome.sph.umich.edu/wiki/BAFRegress) code has been pared down to only the part that does the linear regression. The parts for Python/R marshalling are no longer needed.

Also, the population minor allele frequencies (popmaf) are calculated from the genotype call (.gtc) files directly, rather than generating .ped files and using plink.

## argyle
Code was added to initialize the `genotypes` object from the genotype call (.gtc) files directly instead of generating and using a GenomeStudio final report text file.
