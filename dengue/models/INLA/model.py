"""INLA forecast models."""

import importlib.resources

import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri, r

# Activate pandas conversion
pandas2ri.activate()

# Load the R script containing the INLAForecastModel class
with importlib.resources.path("dengue.models.INLA", "model.R") as r_script_path:
    robjects.r["source"](str(r_script_path))

# Reference to the R class INLAForecastModel
R_INLAForecastModel = r["INLAForecastModel"]


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
        self.r_instance = R_INLAForecastModel.new(config_file=config_file)

    def generateDataset(self):
        """Generate the dataset based on the configuration."""
        self.r_instance.generateDataset()

    def fit(self):
        """Fit the model with the generated dataset."""
        self.r_instance.fit()

    def get_data(self):
        """Retrieve the dataset as a pandas DataFrame."""
        return pandas2ri.rpy2py(self.r_instance.data)

    def get_fitted_values(self):
        """Retrieve fitted values as a pandas DataFrame."""
        return pandas2ri.rpy2py(self.r_instance.fitted_values)
