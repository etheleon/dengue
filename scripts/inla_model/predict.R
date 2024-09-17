#!/usr/bin/env Rscript

# Geospatial and spatial analysis
library(sf)

# Data manipulation and wrangling
library(tidyverse)  # Includes dplyr, tidyr, ggplot2, and more
library(dplyr)      # Already part of tidyverse but listed for clarity
library(tidyr)      # Already part of tidyverse but listed for clarity
library(data.table)
library(janitor)

# Date and time manipulation
library(lubridate)  # Loaded once, removing duplicate

# File path and Excel handling
library(here)
library(openxlsx)

# Visualization
library(ggplot2)    # Already part of tidyverse but listed for clarity
library(ggpubr)
library(corrplot)
library(cowplot)
library(RColorBrewer)
library(MetBrewer)
library(scales)

# Spatial modeling and Bayesian inference
library(INLA)

# Machine learning and statistical modeling
library(nnet)
library(splines)

# Utilities and fonts
library(tibble)
library(stringr)
library(showtext)
library(sysfonts)
library(purrr)
library(scoringutils)
library(pROC)

# Time series and data transformation
library(zoo)
library(tidyquant)

# Hydrological goodness of fit
library(hydroGOF)


