#! /bin/bash

# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-linux-ami-basics.html#extras-library
sudo amazon-linux-extras install R4
# https://github.com/conda-forge/miniforge#unix-like-platforms
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh"
bash Mambaforge-$(uname)-$(uname -m).sh
# https://snakemake.readthedocs.io/en/stable/getting_started/installation.html
mamba create -c conda-forge -c bioconda -n snakemake snakemake
mamba activate snakemake
# https://arrow.apache.org/docs/r/articles/install.html#method-1a---binary-r-package-containing-libarrow-binary-via-rspmconda
mamba install -y -c conda-forge --strict-channel-priority awswrangler r-arrow r-dplyr r-tidyr r-readr r-glue liblapack libblas r-biocmanager r-r.utils r-devtools
Rscript -e 'BiocManager::install("preprocessCore")'
Rscript -e 'devtools::install_github("andrewparkermorgan/argyle")'
