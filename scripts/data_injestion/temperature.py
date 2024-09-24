#!/usr/bin/env python

"""Script to read weather data and populate temperature table.
"""
import argparse
import os
from glob import glob
from typing import List

import numpy as np
import pandas as pd

from .utils import parse_date_with_filename


def standardize(columns: List[str]) -> List[str]:
    """Removes space and lowers."""
    newcols = []
    for col in columns:
        newcols.append(col.strip().lower().replace(" ", "").replace('"', ""))
    return newcols


def read_dbt_rh_2009_2021(data_root: str) -> pd.DataFrame:
    """This function reads temperature data for 2009 to 2021."""
    df = pd.read_csv(os.path.join(data_root, "weather_1982_2021", "Daily DBT 2009-2021.csv"))

    df.columns = standardize(df.columns.tolist())  # type: ignore
    column_mappings = {
        "id_station": "station_id",
        "dateasia/singapore(+0800)": "date",
        "maxdbt": "dbt_max",
        "mindbt": "dbt_min",
        "meandbt": "dbt_mean",
        "meanrh": "rh_mean",
    }
    df.rename(columns=column_mappings, inplace=True)

    expected_columns = ["rh_max", "rh_min", "rh_mean"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    return df


def read_dbt_rh_2022(data_root: str) -> pd.DataFrame:
    """This function reads temperature data for 2022."""
    snake_case_names = [
        "station_id",
        "year",
        "month",
        "day",
        "dbt_mean",
        "rh_mean",
        "dbt_max",
        "dbt_min",
        "wind_direction_scalar_mean",
        "wind_speed_scalar_kts_mean",
        "wind_direction_max",
        "wind_speed_kts_max",
    ]

    df = pd.read_csv(
        os.path.join(data_root, "weather_2022", "Temp 2022.csv"),
        skiprows=1,
        names=snake_case_names,
        encoding="ISO-8859-1",
    ).loc[
        :, :"dbt_min"  # type: ignore[misc]
    ]

    expected_columns = ["rh_max", "rh_min", "rh_mean"]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df[["year", "month", "day"]])
    df.drop(columns=["year", "month", "day"], inplace=True)
    return df


def read_dbt_rh_2023_2024(data_dir: str, year: str) -> pd.DataFrame:
    """This function reads temperature data for 2023."""
    column_mapping = {
        "stationcode": "station_id",
        "id_station": "station_id",
        "date": "date",
        "dateasia/singapore(+0800)": "date",
        "maxdbt": "dbt_max",
        "mindbt": "dbt_min",
        "meandbt": "dbt_mean",
        "maxrh": "rh_max",
        "minrh": "rh_min",
        "meanrh": "rh_mean",
    }

    standard_columns = [
        "date",
        "station_id",
        "dbt_max",
        "dbt_min",
        "dbt_mean",
        "rh_max",
        "rh_min",
        "rh_mean",
    ]

    csv_files = glob(os.path.join(data_dir, year, "Weekly DBT_RH*.csv"))
    df_list = []

    for file in csv_files:
        df = pd.read_csv(file)
        df.columns = standardize(df.columns.tolist())  # type: ignore
        df.rename(columns=column_mapping, inplace=True)
        df_filtered = df[[col for col in standard_columns if col in df.columns]]
        if "date" in df_filtered.columns:
            df_filtered["date"] = df_filtered["date"].apply(
                lambda x: parse_date_with_filename(x, file)  # pylint: disable=W0640
            )
        df_list.append(df_filtered)

    df_final = pd.concat(df_list, ignore_index=True)
    df_final["date"] = pd.to_datetime(df_final["date"], errors="coerce")
    return df_final


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process a CSV file and write it to PostgreSQL.")
    parser.add_argument(
        "data_dir",
        type=str,
        help="Directory containing the data CSV file",
        default="/home/wesley/github/etheleon/national_analysis/data/climate/",
    )
    args = parser.parse_args()

    pd.concat(
        [
            read_dbt_rh_2009_2021(args.data_dir),
            read_dbt_rh_2022(args.data_dir),
            read_dbt_rh_2023_2024(args.data_dir, "weather_2023"),
            read_dbt_rh_2023_2024(args.data_dir, "weather_2024"),
        ]
    )
