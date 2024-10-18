"""Module to calculate climate related ML Features."""

import pandas as pd

from dengue.utils import download_dataframe_from_db


def get_temp_weekly(schema="national_analysis", table="temperature", window=12, lag=0) -> pd.DataFrame:
    """Calculate rolling mean of mean-centered max weekly temperature over specified window.

    This function queries a database to retrieve daily temperature data, calculates the
    mean-centered maximum temperature at the weekly level,
    and then computes the rolling mean of this value over a specified window of weeks.

    Args:
        schema (str): The database schema to query. Defaults to "national_analysis".
        table (str): The table within the schema to query. Defaults to "temperature".
        window (int): The number of over which to calculate the rolling mean. Defaults to week.
                lag (int): The number of

    Returns:
        pd.DataFrame: A DataFrame containing the date and the rolling mean of the mean-centered maximum temperature.
    """
    if window < 1:
        raise ValueError("Window must be a positive integer")

    query = f"""--sql
      WITH natl_avg AS (
        SELECT
          year,
          eweek,
          AVG(dbt_max) AS temperature_max
        FROM (
          SELECT
              station_id,
              to_char(date, 'IYYY') AS year,
              to_char(date, 'IW') AS eweek,
              dbt_max
          FROM {schema}.{table}
        )
        GROUP BY 1, 2
      ),
      scaled AS (
        SELECT
          year,
          eweek,
          temperature_max - AVG(temperature_max) OVER () AS temperature_max_mean_centered
        FROM natl_avg
      ),
      averaged AS (
        SELECT
          year,
          eweek,
          AVG(temperature_max_mean_centered) OVER (
            ORDER BY year, eweek 
            ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
          ) AS max_t_scale_{window}_wk_avg
        FROM scaled
      )
      SELECT
        year,
        eweek,
        LAG(max_t_scale_{window}_wk_avg, {lag}, 0) OVER (ORDER BY year, eweek) AS max_t_scale_{window}_wk_avg_{lag}
      FROM averaged
    """

    df = download_dataframe_from_db(query)
    df.year = df.year.astype(int)
    df.eweek = df.eweek.astype(int)
    df = df.set_index(["year", "eweek"])
    return df


def get_elnino34_ssta_weekly(schema="national_analysis", table="elnino34", window=12, lag=4) -> pd.DataFrame:
    """Retrieves the weekly averaged Sea Surface Temperature Anomalies (SSTA) for the El Niño 3.4 region with lag.

    Retrieves raw data from specified database table, applying a moving average window and a lag.

    Args:
        schema (str): The database schema where the table is located. Default is "national_analysis".
        table (str): The name of the table containing the SSTA data. Default is "elnino34".
        window (int): The size of the moving average window in weeks. Must be a positive integer. Default is 12.
        lag (int): The number of weeks to lag the averaged SSTA values. Default is 4.

    Returns:
        pd.DataFrame: A DataFrame containing the year, week, and the lagged moving average SSTA values.

    Raises:
        ValueError: If the window is less than 1.
    """
    if window < 1:
        raise ValueError("Window must be a positive integer")

    query = f"""--sql
      WITH averaged AS (
        SELECT
          to_char(date, 'IYYY') AS year,
          to_char(date, 'IW') AS eweek,
          AVG(ssta) OVER (ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS nino34_{window}_wk_avg
        FROM {schema}.{table}
      )
      SELECT
        year,
        eweek,
        LAG(nino34_{window}_wk_avg, {lag}, 0) OVER (ORDER BY year, eweek) AS nino34_{window}_wk_avg_{lag}
      FROM averaged
    """

    df = download_dataframe_from_db(query)
    df.year = df.year.astype(int)
    df.eweek = df.eweek.astype(int)
    df = df.set_index(["year", "eweek"])
    return df
