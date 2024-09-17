FROM ubuntu:24.04

LABEL maintainer="Wesley Goi <wesley_goi@nea.gov.sg>"

# Set environment variables for non-interactive installs
ENV DEBIAN_FRONTEND=noninteractive

# Update and install necessary system packages
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    software-properties-common \
    dirmngr \
    g++ \
    git \
    wget \
    libudunits2-dev \
    bind9 \
    libcurl4-openssl-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    lib32z1 \
    libgsl-dev \
    && apt-get update \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc \
    && add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" \
    && apt-get install -y --no-install-recommends \
    r-base \
    r-base-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages(c(\
    'sf', 'tidyverse', 'dplyr', 'tidyr', 'data.table', 'janitor', 'lubridate', \
    'here', 'openxlsx', 'ggplot2', 'ggpubr', 'corrplot', 'cowplot', 'RColorBrewer', \
    'MetBrewer', 'scales', 'INLA', 'nnet', 'splines', 'tibble', 'stringr', 'showtext', \
    'sysfonts', 'purrr', 'scoringutils', 'pROC', 'zoo', 'tidyquant', 'hydroGOF', 'futile.logger'\
    ))"

# Clean up
RUN apt-get clean && \
    rm -rf /tmp/downloaded_packages/ /var/tmp/*

# Copy function
COPY predict.R /workspace/predict.R

# Clone model
RUN git clone https://github.com/EmilieFinch/dengue-singapore /workspace/dengue-singapore
RUN mkdir -p /workspace/output

# Set a working directory
WORKDIR /workspace

CMD ["Rscript", "predict.R"]
