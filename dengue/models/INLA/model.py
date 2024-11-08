"""INLA forecast models."""

import importlib.resources
from dataclasses import asdict, dataclass, field
from typing import List, Optional

import pandas as pd
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from yaml import safe_load

from dengue.metrics.ml import crps
from dengue.utils import download_dataframe_from_db

pandas2ri.activate()

with importlib.resources.path("dengue.models.INLA", "model.R") as r_script_path:
    robjects.r(f"""source('{r_script_path}')""")

inla_forecast_model_constructor = robjects.r["inla_forecast_model"]

parse_config_method = robjects.r["inla_forecast_model.parse_config"]
transform_method = robjects.r["inla_forecast_model.transform"]
fit_method = robjects.r["inla_forecast_model.fit"]


@dataclass
class ModelMetrics:
    """ModelMetrics is a class that holds various metrics used to evaluate the performance of a model.

    Attributes:
        waic (Optional[float]): Watanabe-Akaike Information Criterion, a measure of model fit.
        dic (Optional[float]): Deviance Information Criterion, another measure of model fit.
        cpo (Optional[List[float]]): Conditional Predictive Ordinate, used for model comparison.
        crps (Optional[List[float]]): Continuous Ranked Probability Score, used to assess the accuracy of probabilistic predictions.
    """

    waic: Optional[float] = field(default=None, metadata={"description": "Watanabe-Akaike Information Criterion"})
    dic: Optional[float] = field(default=None, metadata={"description": "Deviance Information Criterion"})
    cpo: Optional[List[float]] = field(default=None, metadata={"description": "Conditional Predictive Ordinate"})
    crps: Optional[List[float]] = field(default=None, metadata={"description": "Continuous Ranked Probability Score"})


@dataclass
class INLAForecastModel_V1:
    """INLAForecastModel_V1 is a class that interfaces with an R-based INLA forecast model.

    Attributes:
        r_instance: An instance of the R_INLAForecastModel class initialized with the provided configuration file.

    """

    config_file: str
    config: dict = field(init=False)
    tablename: str = field(init=False)
    train_test: pd.DataFrame = field(init=False)
    preds: pd.DataFrame = field(init=False)
    metrics: ModelMetrics = field(init=False)

    def __post_init__(self):
        """Initialize the INLAForecastModel with a configuration file."""
        with open(self.config_file, "r", encoding="UTF8") as file:
            self.config = safe_load(file)
        self.table = self.config["dataset"]
        self.r_instance = inla_forecast_model_constructor(self.config_file)

    def transform(self):
        """Generate the dataset based on the configuration."""
        self.train_test = download_dataframe_from_db(f"SELECT * FROM {self.tablename}")
        self.train_test.to_csv("/tmp/data.csv", index=False)
        self.r_instance = transform_method(self.r_instance, "/tmp/data.csv")

    def fit(self):
        """Fit the model with transformed dataset."""
        self.r_instance = fit_method(self.r_instance)

    def get_fitted_values(self) -> Optional[pd.DataFrame]:
        """Retrieve model fitted values as a pandas DataFrame.

        The DataFrame contains the following columns:
            - date: The date corresponding to the fitted value.
            - mean: The mean of the fitted values.
            - lower: The lower bound of the fitted values.
            - upper: The upper bound of the fitted values.

        Example DataFrame:
                date        mean        lower       upper
            1   2009-12-28  219.733483  41.688335   690.991823
            2   2010-01-04  76.789649   64.387377   90.970357
            3   2010-01-11  76.878453   64.608937   90.886052
            4   2010-01-18  70.263280   58.834690   83.233067
            5   2010-01-25  69.366993   58.206487   82.009114
            ... ...         ...         ...         ...
            730 2023-11-27  206.222467  106.178183  363.391169
            731 2023-12-04  263.090369  137.043212  459.715267
            732 2023-12-11  260.032489  135.635338  453.847960
            733 2023-12-18  269.187181  141.337764  467.770858
            734 2023-12-25  288.948302  152.469822  500.270516

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the fitted values if the model has been fitted,
                                    otherwise None.
        """
        if "fitted_values" in self.r_instance.names:
            self.preds = pandas2ri.rpy2py(self.r_instance.rx2("fitted_values"))
            # self.preds.rename(columns={"mean": "cases_predicted"}, inplace=True)
            self.train_test.assign(
                cases_predicted=self.preds["mean"],
                cases_predicted_low=self.preds["lower"],
                cases_predicted_high=self.preds["upper"],
                inplace=True,
            )
            self.train_test["date"] = pd.to_datetime(self.train_test["date"], format="%Y-%m-%d")
        else:
            print("Model has not been fitted.")
            return None
        return self.preds

    def get_model_metrics(self) -> Optional[pd.DataFrame]:
        """Retrieve model metrics."""
        if "model_statistics" in self.r_instance.names:
            model_statistics = pandas2ri.rpy2py(self.r_instance.rx2("metrics"))
            self.metrics = ModelMetrics(
                waic=model_statistics["waic"],
                dic=model_statistics["dic"],
                # cpo=model_statistics["cpo"][0],
                crps=crps(self.train_test["cases_actual"].values, self.train_test["cases_predicted"].values),
            )
            metrics_df = pd.DataFrame.from_dict(asdict(self.metrics), orient="index", columns=["value"])
            return metrics_df
        else:
            print("Model has not been fitted.")
            return None
