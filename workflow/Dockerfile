FROM snakemake/snakemake:v7.9.0

# https://aws-data-wrangler.readthedocs.io/en/stable/install.html#conda
# https://arrow.apache.org/docs/r/articles/install.html#method-1a---binary-r-package-containing-libarrow-binary-via-rspmconda
RUN mamba install -y -c conda-forge -c bioconda --strict-channel-priority awswrangler r-arrow r-dplyr r-tidyr r-readr r-glue r-data.table r-remotes bioconductor-preprocesscore
# Install argyle
# https://rdrr.io/github/andrewparkermorgan/argyle/
RUN Rscript -e 'remotes::install_github("andrewparkermorgan/argyle")'

WORKDIR /root

COPY *.R *.py config.yaml Snakefile ./
COPY awsbatch-profile/ .config/snakemake/awsbatch-profile/