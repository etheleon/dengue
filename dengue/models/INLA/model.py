"""INLA forecast models."""

import importlib.resources

import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from yaml import safe_load

from dengue.utils import download_dataframe_from_db

pandas2ri.activate()

with importlib.resources.path("dengue.models.INLA", "model.R") as r_script_path:
    robjects.r(f"""source('{r_script_path}')""")

inla_forecast_model_constructor = robjects.r["inla_forecast_model"]

parse_config_method = robjects.r["inla_forecast_model.parse_config"]
generate_dataset_method = robjects.r["inla_forecast_model.generate_dataset"]
fit_method = robjects.r["inla_forecast_model.fit"]


class INLAForecastModel_V1:
    """INLAForecastModel_V1 is a class that interfaces with an R-based INLA forecast model.

    Attributes:
        r_instance: An instance of the R_INLAForecastModel class initialized with the provided configuration file.

    Methods:
        __init__(self, config_file):
            Initializes the INLAForecastModel_V1 with a configuration file.

        generateDataset(self):
            Generates the dataset based on the configuration.

        fit(self):
            Fits the model with the generated dataset.

        get_data(self):
            Retrieves the dataset as a pandas DataFrame.

        get_fitted_values(self):
            Retrieves fitted values as a pandas DataFrame.
    """

    def __init__(self, config_file):
        """Initialize the INLAForecastModel with a configuration file."""
        with open(config_file, "r") as file:
            config = safe_load(file)
        tablename = config["dataset"]
        download_dataframe_from_db(f"SELECT * FROM {tablename}").to_csv("/tmp/data.csv", index=False)
        self.r_instance = inla_forecast_model_constructor(config_file)

    def generateDataset(self):
        """Generate the dataset based on the configuration."""
        self.r_instance = generate_dataset_method(self.r_instance, "/tmp/data.csv")

    def fit(self):
        """Fit the model with the generated dataset."""
        self.r_instance = fit_method(self.r_instance)

    def get_data(self):
        """Retrieve the dataset as a pandas DataFrame."""
        if "data" in self.r_instance.names:
            return pandas2ri.rpy2py(self.r_instance.rx2("data"))
        else:
            print("Data has not been generated.")
            return None

    def get_fitted_values(self):
        """Retrieve fitted values as a pandas DataFrame."""
        if "fitted_values" in self.r_instance.names:
            return pandas2ri.rpy2py(self.r_instance.rx2("fitted_values"))
        else:
            print("Model has not been fitted.")
            return None
