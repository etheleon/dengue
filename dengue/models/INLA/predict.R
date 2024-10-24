#!/usr/bin/env Rscript

# Accept positional and kw args
suppressPackageStartupMessages(library(argparse))
suppressPackageStartupMessages(library(yaml))

# Data manipulation and wrangling
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(janitor))

# Date and time manipulation
suppressPackageStartupMessages(library(lubridate))

# Spatial modeling and Bayesian inference
suppressPackageStartupMessages(library(INLA))

# Machine learning and statistical modeling
suppressPackageStartupMessages(library(nnet))
suppressPackageStartupMessages(library(splines))

# Utilities and fonts
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


#' Parse model parameters from YAML Configuration
#'
#' This function reads a YAML configuration file containing features and random effects
#' of an INLA formula and constructs the corresponding formula object dynamically.
#'
#' @param yaml_file A string specifying the path to the YAML configuration file.
#' @param response A string specifying the response variable (e.g., 'cases').
#'
#' @return A formula object representing the complete dynamically constructed INLA formula.
#' 
#' @examples
#' \dontrun{
#' # Example of YAML Structure
#' model:
#'  horizon: 0
#'  hyperparameters: 
#'    - prec:
#'        prior: 'pc.prec'
#'        param: [0.5, 0.01]
#' features:
#'   - name: time_since_switch
#'     variable_type: group
#'     bins: 18
#'     model: 'rw2'
#'     scale_model: true
#'   - name: max_t_scale_12_wk_avg_0
#'     variable_type: group
#'     model: 'rw2'
#'     scale_model: true
#'   - name: nino34_12_wk_avg_4
#'     variable_type: group
#'     bins: 12
#'     model: 'rw2'
#'     scale_model: true
#'   - name: days_no_rain_12_wk_total_0
#'     variable_type: group
#'     model: 'rw2'
#'     scale_model: true
#' random_effects:
#'   - name: year_index
#'     model: 'iid'
#'   - name: eweek
#'     model: 'rw2'
#'     cyclic: true
#' }
#' @import yaml
#' @export
parse_config <- function(yaml_file, response = "cases") {
  yaml_data <- yaml.load_file(yaml_file)
  
  features <- yaml_data$features
  random_effects <- yaml_data$random_effects
  hyperparams <- yaml_data$model$hyperparameters
  
  formula_str <- ""
  
  # Loop through each feature to build the formula
  for (feature in features) {
    if (feature$variable_type == "group") {
      f_component <- paste0("f(inla.group(", feature$name)
      
      # Add the number of bins if present
      if (!is.null(feature$bins)) {
        f_component <- paste0(f_component, ", n = ", feature$bins)
      }
      
      f_component <- paste0(f_component, "), model = '", feature$model, 
                            "', scale.model = ", tolower(as.character(feature$scale_model)), 
                            ", hyper = hyperparameters)")
    } else {
      stop("Unsupported variable_type: ", feature$variable_type)
    }
    
    formula_str <- paste0(formula_str, "+", f_component)
  }
  
  # Remove the initial "+" from formula string
  formula_str <- substring(formula_str, 2)
  
  # Create hyperparameter list
  hyperparameters <- list(prec = list(prior = hyperparams[[1]]$prec$prior, param = unlist(hyperparams[[1]]$prec$param)))
  
  # Initialize random effects string
  random_effects_str <- ""
  
  # Loop through each random effect
  for (random_effect in random_effects) {
    if (random_effect$model == "rw2") {
      if (random_effect$cyclic) {
        random_effects_str <- paste0(random_effects_str, " + f(", random_effect$name, ", model = 'rw2', cyclic = TRUE, hyper = hyperparameters)")
      } else {
        random_effects_str <- paste0(random_effects_str, " + f(", random_effect$name, ", model = 'rw2', hyper = hyperparameters)")
      }
    } else if (random_effect$model == "iid") {
      random_effects_str <- paste0(random_effects_str, " + f(", random_effect$name, ", model = 'iid', hyper = hyperparameters)")
    }
  }
  
  # Remove initial "+" from random effects string
  random_effects_str <- substring(random_effects_str, 2)
  
  # Construct the full formula, starting with the response variable
  full_formula_str <- paste0(response, "~1 + ", formula_str, " + ", random_effects_str)
  print(full_formula_str)
  full_formula <- as.formula(full_formula_str)
  
  return(list(
    formula = full_formula,
    hyperparameters = hyperparameters
  ))
}


read_data <- function(csv_file_path) {

}

parser <- ArgumentParser()

parser$add_argument(
  "--config", 
  type = "character",
  default = "workspace/dengue-singapore/model_config.yaml",
  help = "Specify the path to the model config file"
)

args <- parser$parse_args()

# Config
model_root_dir <- "/workspace/dengue-singapore"
config <- parse_config(args$config)

datasets = train_pred_split(
  args$csv_file_path, 
  train_start = config$train_start_time, 
  train_end = config$train_end_time,
  pred_start = config$pred_start_time,
  pred_end = config$pred_end_time
)
datasets$train_df

datasets$pred_df

# dengue_singapore <- read_csv() %>%
#   mutate(date = as.Date(date, format = "%d/%m/%Y"))
# 
# df_eval <- df_model |>
#   group_by(year, month) |>
#   mutate(month_index = cur_group_id()) |>
#   ungroup() |>
#   mutate(date_index = row_number()) |>
#   dplyr::select(
#     date, date_index, year, year_index, month, month_index,
#     eweek, cases, pop,
#     time_since_switch,
#     max_t_scale_12_wk_avg_0,
#     nino34_12_wk_avg_4,
#     days_no_rain_12_wk_total_0,
#   ) |>
#   # As days without rain is a cumulative variable,
#   # scale up the lagged versions to what would be expected over a 12 week period
#   mutate(
#     days_no_rain_10_wk_total_2 = days_no_rain_10_wk_total_2 * 12 / 10,
#     days_no_rain_8_wk_total_4 = days_no_rain_8_wk_total_4 * 12 / 8,
#     days_no_rain_6_wk_total_6 = days_no_rain_6_wk_total_6 * 12 / 6,
#     days_no_rain_4_wk_total_8 = days_no_rain_4_wk_total_8 * 12 / 4
#   ) |>
#   mutate(
#     time_since_switch_2 = time_since_switch_2 + 2,
#     time_since_switch_4 = time_since_switch_4 + 4,
#     time_since_switch_6 = time_since_switch_6 + 6,
#     time_since_switch_8 = time_since_switch_8 + 8
#   )
# 
# year_month <- df_eval |>
#   group_by(year_index, month) |>
#   filter(year_index >= 10) |>
#   summarise(.groups = "keep")
# thresholds <- purrr::map2_df(year_month$month, year_month$year_index,
#   possibly(calculate_thresholds),
#   data_input = df_eval,
#   quantile = 0.75,
#   # TODO(Wesley): Consult CC for this
#   .progress = TRUE
# )
# 
# df_eval <- df_eval |> left_join(thresholds, by = c("year_index", "month"))
# 
# # Here we define an outbreak week where the number of cases is > seasonal moving 75th percentile
# # threshold, and an outbreak year as having more than 8 outbreak weeks
# df_eval <- df_eval |>
#   mutate(outbreak_week = case_when(cases > threshold ~ 1, TRUE ~ 0)) |>
#   group_by(year) |>
#   mutate(outbreak_year = case_when(sum(outbreak_week) > 12 ~ 1, TRUE ~ 0)) |>
#   # Add outbreak years pre 2010
#   mutate(outbreak_year = case_when(year == 2004 | year == 2005 | year == 2007 ~ 1, TRUE ~ outbreak_year)) |>
#   ungroup()
# 
# flog.info("Eval Dataset Generated")
# 
# form_input <- args$model_type
# yearly_re <- c_args[[3]]
# 
# flog.info("Generating time series cross-validated predictions.")
# 
# # Run TSCV predictions ---------------------------------------------------------------------
# 
# tscv_predictions_weekly(
#   data_input = df_eval,
#   horizon = horizon,
#   form_input = args$model_type,
#   yearly_re = args$year_as_re, # Either: "estimated", "na"
#   filename = "tscv-preds-weekly"
# )
# 
# flog.info(paste0("TSCV predictions finished for horizon ", horizon, "."))
