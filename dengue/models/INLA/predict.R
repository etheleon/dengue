#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(argparse))
suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(INLA))

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
#' @import yaml glue stringr
#' @export
parse_config <- function(yaml_file, response = "cases") {
  yaml_data <- yaml.load_file(yaml_file)
  hyperparams <- yaml_data$model$hyperparameters
  build_feature_str <- function(f) glue::glue("f(inla.group({f$name}, n = {f$bins}), model = '{f$model}', scale.model = {as.character(f$scale_model)}, hyper = hyperparameters)")
  build_random_effect_str <- function(re) glue::glue("f({re$name}, model = '{re$model}', cyclic = {re$cyclic}, hyper = hyperparameters)")
  formula_str <- str_c(lapply(yaml_data$features, build_feature_str), collapse = " + ")
  random_effects_str <- str_c(lapply(yaml_data$random_effects, build_random_effect_str), collapse = " + ")
  hyperparameters <- list(prec = list(prior = hyperparams[[1]]$prec$prior, param = unlist(hyperparams[[1]]$prec$param)))
  offset <- glue::glue("offset({yaml_data$model$inla$offset})")
  full_formula <- as.formula(glue::glue("{response} ~ 1 + {formula_str} + {random_effects_str} + {offset}"))

  list(
    formula = full_formula,
    inla_model = yaml_data$model$inla,
    hyperparameters = hyperparameters,
    input_s = yaml_data$input_file,
    train_start_time = yaml_data$train$start_time,
    train_end_time = yaml_data$train$end_time,
    test_start_time = yaml_data$test$start_time,
    test_end_time = yaml_data$test$end_time
  )
}


#' Get Dataset
#'
#' This function reads dengue forecasting dataset from specified CSV file path and returns a data frame.
#'
#' @param csv_file_path A string representing the path to the CSV file.
#'                      Default is "/workplace/data.csv".
#'
#' @return A data frame containing the dataset read from the CSV file.
#'
#' @examples
#' data <- get_dataset("/path/to/your/data.csv")
#' head(data)
#' @title Get Dataset
#' @description Given the train and test periods censor the cases column and keep original column as cases_actual
#' The function also includes a transformation step where it alters the 'cases' column.
#'
#' @param csv_file_path A string representing the file path to the CSV file. Default is "/workplace/data.csv".
#'
#' @return A dataframe containing the dataset with the transformed 'date' and 'cases' columns.
#'
#' @examples
#' \dontrun{
#' dataset <- get_dataset("/path/to/your/data.csv")
#' }
get_dataset <- function(csv_file_path = "/workplace/data.csv",
                        train_start,
                        train_end,
                        pred_start,
                        pred_end,
                        horizon = 0) {

  create_newdate <- function(date_s) {  # nolint
    as.integer(paste0(lubridate::isoyear(date_s), str_pad(lubridate::isoweek(date_s), width = 2, pad = "0")))
  }
  readr::read_csv(csv_file_path, show_col_types = FALSE) |>
    tidyr::mutate(yearmonth = as.integer(paste0(year, str_pad(eweek, width = 2, pad = "0")))) |>  # nolint
    tidyr::mutate(cases_actual = cases) |>  # nolint
    tidyr::mutate(cases = dplyr::case_when(  # nolint
      yearmonth >= create_newdate(train_start) & yearmonth < create_newdate(train_end) ~ cases,
      yearmonth >= create_newdate(pred_start) & yearmonth < create_newdate(pred_end) ~ NA,
      TRUE ~ NA
    ))
}


predict <- function(df, formula, inla_m) {
  model <- inla::inla(formula,
    family = inla_m$family,
    control.inla = inla_m$control$inla,
    control.predictor = inla_m$control$predictor,
    control.compute = inla_m$control$compute,
    control.fixed = inla_m$control$fixed,
    num.threads = inla_m$num.threads,
    verbose = inla_m$verbose,
    data = df
  )
  model
}


parser <- ArgumentParser()
parser$add_argument(
  "--config",
  type = "character",
  default = "/workspace/config.yaml",
  help = "Specify path to config.yaml"
)
args <- parser$parse_args()

config <- parse_config(args$config)

df <- get_dataset(
  csv_file_path = config$input_s,
  train_start = config$train_start_time,
  train_end = config$train_end_time,
  pred_start = config$test_start_time,
  pred_end = config$test_end_time
)
model <- predict(df, config$formula, config$inla_m)
# model$summary.fixed
