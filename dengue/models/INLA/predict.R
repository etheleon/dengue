#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(argparse))
suppressPackageStartupMessages(library(yaml))

suppressPackageStartupMessages(library(tidyverse))
# suppressPackageStartupMessages(library(janitor))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(glue))


# Spatial modeling and Bayesian inference
suppressPackageStartupMessages(library(INLA))

# Machine learning and statistical modeling
suppressPackageStartupMessages(library(nnet))
suppressPackageStartupMessages(library(splines))

# Utilities and fonts
# suppressPackageStartupMessages(library(stringr))
# suppressPackageStartupMessages(library(showtext))
# suppressPackageStartupMessages(library(sysfonts))
# suppressPackageStartupMessages(library(scoringutils))
# suppressPackageStartupMessages(library(pROC))

# Time series and data transformation
# suppressPackageStartupMessages(library(zoo))
# suppressPackageStartupMessages(library(tidyquant))

# Hydrological goodness of fit
# suppressPackageStartupMessages(library(hydroGOF))

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
#' @import yaml tidyverse
#' @export
parse_config <- function(yaml_file, response = "cases") {
  yaml_data <- yaml.load_file(yaml_file)
  input_s <- yaml_data$input_file
  features <- yaml_data$features
  random_effects <- yaml_data$random_effects
  hyperparams <- yaml_data$model$hyperparameters
  train_start_time <- yaml_data$train$start_time
  train_end_time <- yaml_data$train$end_time
  test_start_time <- yaml_data$test$start_time
  test_end_time <- yaml_data$test$end_time
  formula_str <- ""

  # Loop through each feature to build the formula
  for (feature in features) {
    if (feature$variable_type == "group") {
      # Construct the feature component using glue for better readability
      f_component <- glue("f(inla.group({feature$name}, n = {feature$bins}), model = '{feature$model}', scale.model = {tolower(as.character(feature$scale_model))}, hyper = hyperparameters)")

      # Append the feature component to formula_str
      formula_str <- glue("{formula_str} + {f_component}")
    } else {
      stop("Unsupported variable_type: ", feature$variable_type)
    }
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
        random_effects_str <- glue("{random_effects_str} + f({random_effect$name}, model = 'rw2', cyclic = TRUE, hyper = hyperparameters)")
      } else {
        random_effects_str <- glue("{random_effects_str} + f({random_effect$name}, model = 'rw2', hyper = hyperparameters)")
      }
    } else if (random_effect$model == "iid") {
      random_effects_str <- glue("{random_effects_str} + f({random_effect$name}, model = 'iid', hyper = hyperparameters)")
    }
  }

  # Remove the initial "+" from random effects string
  random_effects_str <- substring(random_effects_str, 2)

  # Construct the full formula, starting with the response variable
  full_formula_str <- glue("{response} ~ 1 + {formula_str} + {random_effects_str}")
  print(full_formula_str)
  full_formula <- as.formula(full_formula_str)

  list(
    formula = full_formula,
    hyperparameters = hyperparameters,
    input_s = input_s,
    train_start_time = train_start_time,
    train_end_time = train_end_time,
    test_start_time = test_start_time,
    test_end_time = test_end_time
  )
}

train_pred_split <- function(csv_file_path = "/workplace/data.csv",
                             train_start,
                             train_end,
                             pred_start,
                             pred_end,
                             horizon = 0) {
  df <- tidyverse::read_csv(csv_file_path)
  train_df <- df |> filter(date >= train_start, date < train_end)
  pred_df <- df |> filter(date >= pred_start, date < pred_end)
  list(train_df = train_df, pred_df = pred_df)
}

# make this an object
train <- function(df, formula) {
  model <- inla(formula,
    family = "nbinomial",
    offset = log(pop / 100000),
    control.inla = list(strategy = "adaptive"),
    control.predictor = list(link = 1, compute = TRUE),
    control.compute = list(return.marginals.predictor = TRUE, dic = TRUE, waic = TRUE, cpo = TRUE, config = TRUE), # which model assessment criteria to include
    control.fixed = list(correlation.matrix = TRUE, prec.intercept = 1, prec = 1),
    num.threads = 4,
    verbose = FALSE,
    data = df
  )
  model
}

#' @param nsamples N(samples) to extract from model posterior
predict <- function(model, nsamples) {
  xx <- inla.posterior.sample(nsamples, mod)
  xx_s <- inla.posterior.sample.eval(
    function(...) {
      c( # nolint
        theta[1], # This is the size parameter of the negative binomial distribution (overdispersion)
        Predictor
      )
    },
    xx
  )
  xx_s <- xx_s[c(1, n + 1), ]
  y_pred <- matrix(NA, 1, nsamples)
  for (i in 1:nsamples) {
    xx_sample <- xx_s[, i]
    y_pred[, i] <- rnbinom(1, mu = exp(xx_sample[-1]), size = xx_sample[1])
    if (is.na(y_pred[, i])) {
      print(paste0("NA prediction generated with mu = ", exp(xx_sample[-1]), " and size = ", xx_sample[1]))
    }
  }
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

datasets <- train_pred_split(
  csv_file_path = config$input_file,
  train_start = config$train_start_time,
  train_end = config$train_end_time,
  pred_start = config$pred_start_time,
  pred_end = config$pred_end_time
)

model <- INLA_model(config)
model.train(datasets$train_df)
model.predict(datasets$pred_df)

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
#
# df_eval <- df_eval |> left_join(thresholds, by = c("year_index", "month"))
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
