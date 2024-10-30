FROM r-base:4.4.1
LABEL maintainer="Wesley Goi <wesley_goi@nea.gov.sg>"

RUN	apt-get update -qq && apt-get install -y --no-install-recommends \
    software-properties-common \
    dirmngr \
    g++ \
    git \
    cmake \
    wget \
    libudunits2-dev \
    bind9 \
    libssl-dev \
    libfontconfig1-dev \
    libcurl4-openssl-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libz1 \
    libgsl-dev \
    gdal-bin \
    proj-bin \
    libgdal-dev \
    libproj-dev \
    && apt-get update \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN R -e "install.packages(c('lubridate', 'purrr', 'glue', 'argparse', 'data.table', 'sf'))"
RUN R -e "install.packages('INLA',repos=c(getOption('repos'),INLA='https://inla.r-inla-download.org/R/stable'), dep=TRUE)"

# Copy function
COPY predict.R /workspace/predict.R

# Clone model
# RUN git clone https://github.com/EmilieFinch/dengue-singapore /workspace/dengue-singapore
# RUN mkdir -p /workspace/output

# Set a working directory
WORKDIR /workspace

# CMD ["Rscript", "predict.R"]
