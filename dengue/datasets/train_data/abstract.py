"""Abstract TrainData class for handling training data configuration and operations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd
from yaml import safe_load

from dengue.utils import upsert_dataframe_to_db


@dataclass
class TrainData(ABC):
    """TrainData class for handling training data configuration and operations.

        start_time (datetime): Training start time from the configuration.
        end_time (datetime): Testing end time from the configuration.
        tablename (str): Name of the table in the database.
        data (Optional[pd.DataFrame]): Data to be uploaded to the data warehouse.

    Methods:
        __post_init__(): Post-initialization method to load configuration from a file.
        get() -> pd.DataFrame: Retrieve the training data as a pandas DataFrame.
        upload_to_dwh() -> None: Uploads the data to the data warehouse.
    """

    config_file: str
    config: dict = field(init=False)
    start_time: datetime = field(init=False)
    tablename: str = field(init=False)
    end_time: datetime = field(init=False)
    data: pd.DataFrame = field(init=False)

    def __post_init__(self):
        """Post-initialization method to load configuration from a file.

        This method is automatically called after the object's initialization.
        It opens the configuration file specified by `self.config_file`, reads its
        contents, and loads it into `self.config` using `safe_load`. It also sets
        the `start_time` and `end_time` attributes based on the configuration.

        Attributes:
            config_file (str): Path to the configuration file.
            config (dict): Loaded configuration data.
            start_time (str): Training start time from the configuration.
            end_time (str): Testing end time from the configuration.
        """
        with open(self.config_file, "r", encoding="UTF8") as file:
            self.config = safe_load(file)
        self.start_time = self.config["train"]["start_time"]
        self.end_time = self.config["test"]["end_time"]
        self.tablename = self.config["dataset"]

    @abstractmethod
    def get(self) -> None:
        """Retrieve the training data as a pandas DataFrame.

        Returns:
            pd.DataFrame: An empty pandas DataFrame.
        """
        return None

    def upload_to_dwh(self) -> None:
        """Uploads the data to the data warehouse.

        This method uses the `upsert_dataframe_to_db` function to upload the data
        stored in the `self.data` attribute to the database. The table name is
        specified by the `self.tablename` attribute. The `ddl_file` parameter is
        set to None by default.

        Returns:
            None
        """
        upsert_dataframe_to_db(self.data, ddl_file=None, table_name=self.tablename)
