#!/usr/bin/env Rscript

# Accept positional and kw args
suppressPackageStartupMessages(library(argparse))

# Geospatial and spatial analysis
suppressPackageStartupMessages(library(sf))

# Data manipulation and wrangling
suppressPackageStartupMessages(library(tidyverse)) # Includes dplyr, tidyr, ggplot2, and more
suppressPackageStartupMessages(library(dplyr)) # Already part of tidyverse but listed for clarity
suppressPackageStartupMessages(library(tidyr)) # Already part of tidyverse but listed for clarity
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(janitor))

# Date and time manipulation
suppressPackageStartupMessages(library(lubridate)) # Loaded once, removing duplicate

# File path and Excel handling
suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(openxlsx))

# Visualization
suppressPackageStartupMessages(library(ggplot2)) # Already part of tidyverse but listed for clarity
suppressPackageStartupMessages(library(ggpubr))
suppressPackageStartupMessages(library(corrplot))
suppressPackageStartupMessages(library(cowplot))
suppressPackageStartupMessages(library(RColorBrewer))
suppressPackageStartupMessages(library(MetBrewer))
suppressPackageStartupMessages(library(scales))

# Spatial modeling and Bayesian inference
suppressPackageStartupMessages(library(INLA))

# Machine learning and statistical modeling
suppressPackageStartupMessages(library(nnet))
suppressPackageStartupMessages(library(splines))

# Utilities and fonts
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(showtext))
suppressPackageStartupMessages(library(sysfonts))
suppressPackageStartupMessages(library(purrr))
suppressPackageStartupMessages(library(scoringutils))
suppressPackageStartupMessages(library(pROC))

# Time series and data transformation
suppressPackageStartupMessages(library(zoo))
suppressPackageStartupMessages(library(tidyquant))

# Hydrological goodness of fit
suppressPackageStartupMessages(library(hydroGOF))

suppressPackageStartupMessages(library(futile.logger))

# Create a parser object
parser <- ArgumentParser()

# Add arguments
parser$add_argument(
  "--model_type",
  type = "character",
  default = "sero_climate",
  help = "Specify the type of the model as a string (e.g., 'sero_climate', 'climate_only', 'sero_only')"
)

parser$add_argument(
  "--horizon",
  type = "integer",
  default = 0,
  help = "specify the time horizon"
)

parser$add_argument(
  "--csv_file_path",
  type = "character",
  default = "/workspace/dengue-singapore/data/dengue-cases-climate.csv",
  help = "Specify the path to the CSV file"
)

# Parse the arguments
args <- parser$parse_args()


# Config
model_root_dir <- "/workspace/dengue-singapore"

lag_data <- function(data) {

  df_model <- data |>
    rename(max_t = maximum_temperature,
           cases = dengue_cases
           pop = total_population,
	) |>
    mutate(
      max_t_scale = max_t - mean(max_t, na.rm = TRUE),
      nino34 = nino34,
      days_no_rain = days_no_rain,
      time_since_switch = time_since_switch
    )

  # Generate running averages for max_t_scale, nino34, and totals for days_no_rain over 12 weeks
  df_model <- df_model |>
    bind_cols(setNames(lapply(df_model  |> dplyr::select(max_t_scale), rollmean, k = 12, fill = NA, align = "right"),
                       c("max_t_scale_12_wk_avg_0"))) |>
    bind_cols(setNames(lapply(df_model  |> dplyr::select(nino34), rollmean, k = 12, fill = NA, align = "right"),
                       c("nino34_12_wk_avg_4"))) |>
    bind_cols(setNames(lapply(df_model  |> dplyr::select(days_no_rain), rollsum, k = 12, fill = NA, align = "right"),
                       c("days_no_rain_12_wk_total_0"))))

  # Add lagged versions for max_t_scale, nino34, and days_no_rain
  df_model <- df_model |>
    bind_cols(setNames(shift(df_model$max_t_scale_12_wk_avg_0, seq(2, 8, by = 2)),
                       c(paste0("max_t_scale_12_wk_avg_", seq(2, 8, by = 2))))) |>
    bind_cols(setNames(shift(df_model$nino34_12_wk_avg_4, seq(2, 8, by = 2)),
                       c(paste0("nino34_12_wk_avg_", seq(2, 8, by = 2))))) |>
    bind_cols(setNames(shift(df_model$days_no_rain_12_wk_total_0, seq(2, 8, by = 2)),
                       c(paste0("days_no_rain_12_wk_total_", seq(2, 8, by = 2)))))

  # Add lagged version of time_since_switch
  df_model <- df_model |>
    bind_cols(setNames(shift(df_model$time_since_switch, seq(2, 8, by = 2)),
                       c(paste0("time_since_switch_", seq(2, 8, by = 2)))))

  # Keep the target variable (cases)
  df_model <- df_model |> dplyr::select(c("date", "cases", "max_t_scale_12_wk_avg_0", starts_with("max_t_scale_12_wk_avg_"),
                                          "nino34_12_wk_avg_4", starts_with("nino34_12_wk_avg_"),
                                          "days_no_rain_12_wk_total_0", starts_with("days_no_rain_12_wk_total_"),
                                          "time_since_switch", starts_with("time_since_switch_")))

  return(df_model)
}



# source(file.path(model_root_dir, "R/create-lagged-data_fn.R"))
source(file.path(model_root_dir, "R/fit-inla_fn.R"))
source(file.path(model_root_dir, "R/tscv-prediction_fn.R"))
source(file.path(model_root_dir, "R/utils_fn.R"))

dengue_singapore <- read_csv(args$csv_file_path) %>%
  mutate(date = as.Date(date, format = "%d/%m/%Y"))

df_model <- lag_data(dengue_singapore)
flog.info("lag data created")

# nolint start
sero_climate <- "+f(inla.group(time_since_switch, n = 18), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(max_t_scale_12_wk_avg_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(nino34_12_wk_avg_4, n = 12), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(days_no_rain_12_wk_total_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)"
sero_only <- "+f(inla.group(time_since_switch, n = 18), model = 'rw2', scale.model = TRUE, hyper = precision_prior)"
climate_only <- "+f(inla.group(max_t_scale_12_wk_avg_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(nino34_12_wk_avg_4, n = 12), model = 'rw2', scale.model = TRUE, hyper = precision_prior)+f(inla.group(days_no_rain_12_wk_total_0),model = 'rw2', scale.model = TRUE, hyper = precision_prior)"
# nolint end

forms <- c()
if (args$model_type == 'sero_climate') {
  forms <- c(forms, sero_climate)
} else if (args$model_type == 'climate_only'){
  forms <- c(forms, climate_only)
} else if (args$model_type == 'sero_only'){
  forms <- c(forms, sero_only)
} else {
  q(status=0)
}
flog.info(sprintf("Running predict using %s model.", args$model_type))

c_args <- list(0, "sero_climate", "estimated") # To run all models for forecast horizon of 0 weeks
df_eval <- df_model |>
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
  # As days without rain is a cumulative variable,
  # scale up the lagged versions to what would be expected over a 12 week period
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
thresholds <- purrr::map2_df(year_month$month, year_month$year_index,
  possibly(calculate_thresholds),
  data_input = df_eval,
  quantile = 0.75,
  # TODO(Wesley): Consult CC for this
  .progress = TRUE
)

df_eval <- df_eval |> left_join(thresholds, by = c("year_index", "month"))

# Here we define an outbreak week where the number of cases is > seasonal moving 75th percentile
# threshold, and an outbreak year as having more than 8 outbreak weeks
df_eval <- df_eval |>
  mutate(outbreak_week = case_when(cases > threshold ~ 1, TRUE ~ 0)) |>
  group_by(year) |>
  mutate(outbreak_year = case_when(sum(outbreak_week) > 12 ~ 1, TRUE ~ 0)) |>
  # Add outbreak years pre 2010
  mutate(outbreak_year = case_when(year == 2004 | year == 2005 | year == 2007 ~ 1, TRUE ~ outbreak_year)) |>
  ungroup()

flog.info("Eval Dataset Generated")

horizon <- args$horizon
form_input <- args$model_type
yearly_re <- c_args[[3]]

flog.info("Generating time series cross-validated predictions.")

# Run TSCV predictions ---------------------------------------------------------------------

tscv_predictions_weekly(
  data_input = df_eval,
  horizon = as.numeric(horizon), # One of: 0, 2, 4, 6, 8
  form_input = as.character(form_input), # One of: "all", "covariate-only", "sero_clim_form",
  # "clim_form", "sero_form", "baseline", "baseline-year"
  yearly_re = as.character(yearly_re), # Either: "estimated", "na"
  filename = "tscv-preds-weekly"
)

flog.info(paste0("TSCV predictions finished for horizon ", horizon, "."))
