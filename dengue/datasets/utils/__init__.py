#!/usr/env/bin python
"""Module to get get_dengue_cases."""


from datetime import datetime

import numpy as np
import pandas as pd

from dengue.utils import download_dataframe_from_db


def get_target(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Retrieves dengue case data aggregated by epidemiological week within the specified date range.

    Args:
      start_date (datetime): The start date for the query.
      end_date (datetime): The end date for the query.

    Returns:
      pd.DataFrame: A DataFrame containing the year, epidemiological week, and total cases of dengue.
    """
    start_time_s = start_date.strftime("%Y-%m-%d")
    end_time_s = end_date.strftime("%Y-%m-%d")

    query = f"""--sql
      SELECT 
        to_char(date, 'IYYY') AS year,
        to_Char(date, 'IW') AS eweek,
        cases_total AS cases
      FROM national_analysis.dengue_agg
      WHERE date >= timestamp '{start_time_s}' AND date < timestamp '{end_time_s}'
    """
    df = download_dataframe_from_db(query)
    df.year = df.year.astype(int)
    df.eweek = df.eweek.astype(int)
    df = df.set_index(["year", "eweek"])
    return df


def get_population() -> pd.DataFrame:
    """Retrieves population data.

    Returns:
      pd.DataFrame: A DataFrame containing the year, epidemiological week, and total cases of dengue.
    """
    query = """--sql
      SELECT 
        year,
        population
      FROM national_analysis.population
    """
    df = download_dataframe_from_db(query)
    df.year = df.year.astype(int)
    df = df.set_index(["year"])
    # df = df.assign(population=lambda x: np.log(x.population/ 100000))
    return df
