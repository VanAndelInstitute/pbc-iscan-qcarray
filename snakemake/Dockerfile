FROM rocker/r-ver

RUN /rocker_scripts/install_tidyverse.sh
RUN /rocker_scripts/install_python.sh
RUN R --quiet -e 'BiocManager::install("preprocessCore")' \
    && R --quiet -e 'devtools::install_github("andrewparkermorgan/argyle")'
RUN pip3 install awswrangler