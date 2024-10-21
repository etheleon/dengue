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


def get_days_no_rain(schema="national_analysis", table="rainfall", window=12, lag=0) -> pd.DataFrame:
    """Calculate the number of days with no rain over a specified rolling window and lag.

    This function generates a SQL query to calculate the number of days with no rain
    over a specified rolling window and lag, executes the query, and returns the result
    as a pandas DataFrame.

    Args:
        schema (str): The database schema where the table is located. Default is "national_analysis".
        table (str): The name of the table rainfall data. Default is "rainfall".
        window (int): The size of the moving average window in weeks. Must be a positive integer. Default is 12.
        lag (int): The number of weeks to lag the rolling sum of no rain days. Default is 0.

    Returns:
        pd.DataFrame: A DataFrame indexed by year and epidemiological week (eweek),
        containing the total number of days with no rain over the specified rolling window and lag.

    Raises:
        ValueError: If the window parameter is less than 1.
    """
    if window < 1:
        raise ValueError("Window must be a positive integer")

    query = f"""--sql
      WITH natl_rainfall AS (
        SELECT
          date,
          to_char(date, 'IW')::int AS eweek,
          to_char(date, 'IYYY')::int AS year,
          SUM(rainfall_amt_total) as rainfall_tot
        FROM {schema}.{table}
        GROUP BY 1
      ),
      num_days_no_rain_weekly AS (
        SELECT
          year,
          eweek,
          SUM(CASE
          WHEN rainfall_tot = 0 OR rainfall_tot IS NULL THEN 1
          ELSE 0
          END) days_no_rain
        FROM natl_rainfall
        GROUP BY 1, 2
      ),
      rolling_sum AS (
        SELECT
          year,
          eweek,
          days_no_rain,
          SUM(days_no_rain) OVER (
            PARTITION BY year ORDER BY eweek ROWS {window - 1} PRECEDING
          ) AS days_no_rain_12_wk_total
        FROM num_days_no_rain_weekly
      )
      SELECT
        year,
        eweek,
        -- days_no_rain_12_wk_total,
        LAG(days_no_rain_{window}_wk_total, {lag}, 0) OVER (
            ORDER BY year, eweek
        ) AS days_no_rain_{window}_wk_total_{lag}
      FROM rolling_sum
    """
    df = download_dataframe_from_db(query)
    df.year = df.year.astype(int)
    df.eweek = df.eweek.astype(int)
    df[f"days_no_rain_{window}_wk_total_{lag}"] = df[f"days_no_rain_{window}_wk_total_{lag}"].astype(int)
    df = df.set_index(["year", "eweek"])
    return df
