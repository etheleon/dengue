#!/usr/bin/env python
"""Module to calculate time since switch event feature for INLA model."""

import pandas as pd
from utils import download_dataframe_from_db


def get_mean_centered_avg_daily_temp(schema="national_analysis", table="temperature", window=12) -> pd.DataFrame:
    """Calculate rolling mean of mean-centered max daily temperature over specified window.

    This function queries a database to retrieve temperature data, calculates the mean-centered maximum temperature,
    and then computes the rolling mean of this value over a specified window of days.

    Args:
        schema (str): The database schema to query. Defaults to "national_analysis".
        table (str): The table within the schema to query. Defaults to "temperature".
        window (int): The number of days over which to calculate the rolling mean. Defaults to 12.

    Returns:
        pd.DataFrame: A DataFrame containing the date and the rolling mean of the mean-centered maximum temperature.
    """
    if window < 1:
        raise ValueError("Window must be a positive integer")

    query = f"""--sql
      WITH natl_avg AS (
      SELECT
        date,
        AVG(dbt_max) AS temperature_max
      FROM (
        SELECT 
            station_id,
            date,
            dbt_max
        FROM {schema}.{table}
      )
      GROUP BY date
      ), scaled AS (
      SELECT
        date,
        temperature_max - AVG(temperature_max) OVER () AS temperature_max_mean_centered
      FROM natl_avg
      )
      SELECT
        date,
        AVG(temperature_max_mean_centered) OVER (ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS max_t_scale_{window}_day_avg
      FROM scaled
    """

    df = download_dataframe_from_db(query)
    return df
