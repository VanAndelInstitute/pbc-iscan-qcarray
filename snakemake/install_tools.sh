#! /bin/bash
./resize.sh 20

# Uninstall AWS CLI version 1
sudo pip uninstall -y awscli
# Install AWS CLI version 2
wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
unzip awscli-exe-linux-x86_64.zip
sudo ./aws/install
rm -rf aws awscli-exe-linux-x86_64.zip

# Install R v4
sudo amazon-linux-extras install R4

# Use Mamba to install Snakemake, AWS Wrangler, and R deps via conda-forge
# https://github.com/conda-forge/miniforge#unix-like-platforms
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh"
bash Mambaforge-$(uname)-$(uname -m).sh
rm Mambaforge-$(uname)-$(uname -m).sh

### restart shell ###

# https://snakemake.readthedocs.io/en/stable/getting_started/installation.html
mamba create -c conda-forge -c bioconda -n snakemake snakemake
mamba activate snakemake
# https://aws-data-wrangler.readthedocs.io/en/stable/install.html#conda
# https://arrow.apache.org/docs/r/articles/install.html#method-1a---binary-r-package-containing-libarrow-binary-via-rspmconda
mamba install -y -c conda-forge -c bioconda --strict-channel-priority awswrangler r-arrow r-dplyr r-tidyr r-readr r-glue r-data.table r-remotes bioconductor-preprocesscore

# Install argyle
# https://rdrr.io/github/andrewparkermorgan/argyle/
Rscript -e 'remotes::install_github("andrewparkermorgan/argyle")'
