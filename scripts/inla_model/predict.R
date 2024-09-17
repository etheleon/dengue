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

library(futile.logger)

# Config
root_dir = "/home/wesley/github/etheleon/national_analysis"
model_root_dir = "../inla_model"


source(file.path(model_root_dir, "R/create-lagged-data_fn.R")
source(file.path(model_root_dir, "R/fit-inla_fn.R")
source(file.path(model_root_dir, "R/tscv-prediction_fn.R")
source(file.path(model_root_dir, "R/utils_fn.R")


dengue_singapore = read_csv(file.path(model_root_dir, "data", "dengue-cases-climate.csv")) %>%
  mutate(date = as.Date(date, format = "%d/%m/%Y"))

df_model = lag_data(dengue_singapore)
flog.info("lag data created")

sero_climate = "+f(inla.group(time_since_switch, n = 18), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(max_t_scale_12_wk_avg_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(nino34_12_wk_avg_4, n = 12), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(days_no_rain_12_wk_total_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)"

sero_only = "+f(inla.group(time_since_switch, n = 18), model = 'rw2', scale.model = TRUE, hyper = precision_prior)"

climate_only = "+f(inla.group(max_t_scale_12_wk_avg_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(nino34_12_wk_avg_4, n = 12), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(days_no_rain_12_wk_total_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)"

forms = c(sero_climate, climate_only, sero_only)

c_args <- list(0, "all", "estimated") # To run all models for forecast horizon of 0 weeks
df_eval <- df_model  |>
  group_by(year, month) |>
  mutate(month_index = cur_group_id()) |>
  ungroup() |>
  mutate(date_index = row_number()) |>
  dplyr::select(
    date, date_index, year, year_index, month, month_index,
    eweek, cases, pop,
    time_since_switch,
    time_since_switch_2,
    time_since_switch_4,
    time_since_switch_6,
    time_since_switch_8,
    max_t_scale_12_wk_avg_0,
    max_t_scale_10_wk_avg_2,
    max_t_scale_8_wk_avg_4,
    max_t_scale_6_wk_avg_6,
    max_t_scale_4_wk_avg_8,
    nino34_12_wk_avg_4,
    nino34_10_wk_avg_6,
    nino34_8_wk_avg_8,
    days_no_rain_12_wk_total_0,
    days_no_rain_10_wk_total_2,
    days_no_rain_8_wk_total_4,
    days_no_rain_6_wk_total_6,
    days_no_rain_4_wk_total_8
  ) |>
  # As days without rain is a cumulative variable, scale up the lagged versions to what would be expected over a 12 week period
  mutate(
    days_no_rain_10_wk_total_2 = days_no_rain_10_wk_total_2 * 12 / 10,
    days_no_rain_8_wk_total_4 = days_no_rain_8_wk_total_4 * 12 / 8,
    days_no_rain_6_wk_total_6 = days_no_rain_6_wk_total_6 * 12 / 6,
    days_no_rain_4_wk_total_8 = days_no_rain_4_wk_total_8 * 12 / 4
  ) |>
  mutate(
    time_since_switch_2 = time_since_switch_2 + 2,
    time_since_switch_4 = time_since_switch_4 + 4,
    time_since_switch_6 = time_since_switch_6 + 6,
    time_since_switch_8 = time_since_switch_8 + 8
  )

year_month <- df_eval |>
  group_by(year_index, month) |>
  filter(year_index >= 10) |>
  summarise(.groups = "keep")
thresholds <- purrr::map2_df(year_month$month, year_month$year_index, possibly(calculate_thresholds), data_input = df_eval, quantile = 0.75, .progress = TRUE)

df_eval <- df_eval |> left_join(thresholds, by = c("year_index", "month"))

# Here we define an outbreak week where the number of cases is > seasonal moving 75th percentile
# threshold, and an outbreak year as having more than 8 outbreak weeks
df_eval <- df_eval |>
  mutate(outbreak_week = case_when(cases > threshold ~ 1, TRUE ~ 0)) |>
  group_by(year) |>
  mutate(outbreak_year = case_when(sum(outbreak_week) > 12 ~ 1, TRUE ~ 0)) |>
  mutate(outbreak_year = case_when(year == 2004 | year == 2005 | year == 2007 ~ 1, TRUE ~ outbreak_year)) |> # Add outbreak years pre 2010
  ungroup()

flog.info("Eval Dataset Generated")

horizon <- c_args[[1]]
form_input <- c_args[[2]]
yearly_re <- c_args[[3]]

flog.info("Generating time series cross-validated predictions.")

# Run TSCV predictions ----------------------------------------------------------------------------------------------------------------------------

tscv_predictions_weekly(
  data_input = df_eval,
  horizon = as.numeric(horizon), # One of: 0, 2, 4, 6, 8
  form_input = as.character(form_input), # One of: "all", "covariate-only", "sero_clim_form", "clim_form", "sero_form", "baseline", "baseline-year"
  yearly_re = as.character(yearly_re), # Either: "estimated", "na"
  filename = "tscv-preds-weekly"
)

flog.info(paste0("TSCV predictions finished for horizon ", horizon, "."))
