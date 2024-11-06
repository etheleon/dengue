suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(INLA))

inla_forecast_model <- function(config_file) {
  self <- list(
    formula = NULL,
    inla_model = NULL,
    hyperparameters = NULL,
    train_start_time = NULL,
    train_end_time = NULL,
    test_start_time = NULL,
    test_end_time = NULL,
    data = NULL,
    model = NULL, # This will hold the INLA model after fitting
    fitted_values = NULL
  )
  # Set the class attribute to define it as an S3 object
  class(self) <- "inla_forecast_model"
  # Call the parse_config method to initialize the object with config data
  self <- inla_forecast_model.parse_config(self, config_file)
  # Return the constructed object
  return(self)
}

inla_forecast_model.parse_config <- function(self, config_file) {
  yaml_data <- yaml.load_file(config_file)
  self$inla_model <- yaml_data$model$inla
  self$train_start_time <- yaml_data$train$start_time
  self$train_end_time <- yaml_data$train$end_time
  self$test_start_time <- yaml_data$test$start_time
  self$test_end_time <- yaml_data$test$end_time

  build_feature_str <- function(f) glue::glue("f(inla.group({f$name}, n = {f$bins}), model = '{f$model}', scale.model = {as.character(f$scale_model)}, hyper = hyperparameters)")
  build_random_effect_str <- function(re) glue::glue("f({re$name}, model = '{re$model}', cyclic = {re$cyclic}, hyper = hyperparameters)")
  formula_str <- str_c(lapply(yaml_data$features, build_feature_str), collapse = " + ")
  random_effects_str <- str_c(lapply(yaml_data$random_effects, build_random_effect_str), collapse = " + ")
  offset <- glue::glue("offset({yaml_data$model$inla$offset})")
  self$formula <- as.formula(glue::glue("{yaml_data$target} ~ 1 + {formula_str} + {random_effects_str} + {offset}"))

  # Build hyperparameters
  self$hyperparameters <- list(
    prec = list(
      prior = yaml_data$model$hyperparameters[[1]]$prec$prior,
      param = unlist(yaml_data$model$hyperparameters[[1]]$prec$param)
    )
  )
  return(self)
}

inla_forecast_model.generate_dataset <- function(self, input_s) {
  create_newdate <- function(date_s) {
    as.integer(paste0(lubridate::isoyear(date_s), str_pad(lubridate::isoweek(date_s), width = 2, pad = "0")))
  }

  self$data <- readr::read_csv(input_s, show_col_types = FALSE) |>
    dplyr::mutate(yearmonth = as.integer(paste0(year, str_pad(eweek, width = 2, pad = "0")))) |>
    dplyr::mutate(cases_actual = cases) |>
    dplyr::mutate(date = as.character(date)) |> # issue with tibble 2 py https://github.com/rpy2/rpy2/issues/758
    dplyr::mutate(cases = dplyr::case_when(
      yearmonth >= create_newdate(self$train_start_time) & yearmonth < create_newdate(self$train_end_time) ~ cases,
      yearmonth >= create_newdate(self$test_start_time) & yearmonth < create_newdate(self$test_end_time) ~ NA,
      TRUE ~ cases
    ))
  return(self)
}

inla_forecast_model.fit <- function(self) {
  hyperparameters <- self$hyperparameters
  environment(self$formula) <- environment()
  model <- INLA::inla(
    self$formula,
    family = self$inla_model$family,
    control.inla = self$inla_model$control$inla,
    control.predictor = self$inla_model$control$predictor,
    control.compute = self$inla_model$control$compute,
    control.fixed = self$inla_model$control$fixed,
    num.threads = self$inla_model$num.threads,
    verbose = self$inla_model$verbose,
    data = self$data
  )
  self$model <- model

  self$fitted_values <- data.frame(
    date = as.character(self$data$date),
    mean = self$model$summary.fitted.values$mean,
    lower = self$model$summary.fitted.values$`0.025quant`,
    upper = self$model$summary.fitted.values$`0.975quant`
  )
  return(self)
}
