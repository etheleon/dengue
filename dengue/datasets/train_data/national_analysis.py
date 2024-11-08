"""TrainData class for national analysis data retrieval."""

import pandas as pd

from dengue.datasets.train_data.abstract import TrainData
from dengue.datasets.utils import get_population, get_target
from dengue.features.climate import get_days_no_rain, get_elnino34_ssta_weekly, get_temp_weekly
from dengue.features.serology import get_time_since_switch


class NationalAnalysisTrainData(TrainData):
    """TrainData class for retrieving, combining feature datasets with target dataset for National Analysis.

    This class is designed to create a training and testing DataFrame for dengue case prediction.
    """

    def get(self) -> None:
        """Retrieves and combines various feature datasets with target dataset.

        Function creates to create a training and testing DataFrame for dengue case prediction.

        Args:
            start_time (str): The start date for the data retrieval in 'YYYY-MM-DD' format.
            end_time (str): The end date for the data retrieval in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: A DataFrame containing combined data from multiple sources, including temperature,
                        time since serotype switch, El Niño 3.4 SSTA, days without rain, and dengue cases.
                        The DataFrame is indexed by 'year' and 'eweek' (epidemiological week).
        """
        temp_df = get_temp_weekly(self.start_time, self.end_time)
        sero_df = get_time_since_switch(self.start_time, self.end_time)
        elnino_df = get_elnino34_ssta_weekly(self.start_time, self.end_time)
        days_no_rain_df = get_days_no_rain(self.start_time, self.end_time)
        target_df = get_target(self.start_time, self.end_time)
        population_df = get_population()

        # TODO(WESLEY): Investigate why there's duplicates
        df = (
            temp_df.join(sero_df, on=["year", "eweek"])
            .join(elnino_df, on=["year", "eweek"])
            .join(days_no_rain_df, on=["year", "eweek"])
            .join(target_df, on=["year", "eweek"])
            .join(population_df, on=["year"])
            .reset_index()
        )
        df["date"] = pd.to_datetime(df["year"].astype(str) + df["eweek"].astype(str) + "1", format="%G%V%u")
        df.days_since_switch = df.days_since_switch.fillna(0)
        df.days_since_switch = df.days_since_switch.astype(int)
        self.data = df
        return None
