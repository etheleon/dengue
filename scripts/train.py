#!/usr/bin/env python
"""Script to prepare training data.

>>> print(df)
      year  eweek  max_t_scale_12_wk_avg_0  days_since_switch  nino34_12_wk_avg_4  days_no_rain_12_wk_total_0
0     1981     53                -4.109591                  0           -0.416667                           0
1     1982      1                -4.059591                  0           -0.433333                           1
2     1982      2                -3.300067                  0           -0.466667                           4
3     1982      3                -2.623876                  0           -0.458333                           9
4     1982      4                -2.183876                  0           -0.441667                          13
...    ...    ...                      ...                ...                 ...                         ...
2225  2024     31                 0.580932                 49            0.325000                           8
2226  2024     32                 0.512382                 56            0.291667                           8
2227  2024     33                 0.500020                 63            0.241667                           8
2228  2024     34                 0.447261                 70            0.208333                           8
2229  2024     35                 0.417329                 77            0.166667                          12
"""


from datetime import datetime

import pandas as pd

from dengue.datasets.utils import get_target
from dengue.features.climate import get_days_no_rain, get_elnino34_ssta_weekly, get_temp_weekly
from dengue.features.serology import get_time_since_switch
from dengue.utils import upsert_dataframe_to_db


def get_train_test_data(start_time, end_time) -> pd.DataFrame:
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
    temp_df = get_temp_weekly(start_time, end_time)
    sero_df = get_time_since_switch(start_time, end_time)
    elnino_df = get_elnino34_ssta_weekly(start_time, end_time)
    days_no_rain_df = get_days_no_rain(start_time, end_time)
    target_df = get_target(start_time, end_time)

    df = (
        temp_df.join(sero_df, on=["year", "eweek"])
        .join(elnino_df, on=["year", "eweek"])
        .join(days_no_rain_df, on=["year", "eweek"])
        .join(target_df, on=["year", "eweek"])
        .reset_index()
    )
    df.days_since_switch = df.days_since_switch.fillna(0)
    df.days_since_switch = df.days_since_switch.astype(int)
    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prepare training data for dengue case prediction.")
    parser.add_argument("--start_time", type=str, default="2020-01-01", help="Start date for data retrieval.")
    parser.add_argument("--end_time", type=str, default="2023-12-31", help="End date for data retrieval.")
    args = parser.parse_args()

    start_time = datetime.strptime(args.start_time, "%Y-%m-%d")
    end_time = datetime.strptime(args.end_time, "%Y-%m-%d")

    df = get_train_test_data(start_time, end_time)
    df.drop_duplicates(inplace=True)
    print(df)
    upsert_dataframe_to_db(
        df=df, ddl_file=None, schema="national_analysis", table_name="inla_model_ds", connection_params_path=None
    )
