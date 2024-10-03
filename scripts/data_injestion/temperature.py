#!/usr/bin/env python

"""Script to read weather data and populate temperature table."""
import argparse
import logging
import os
from glob import glob
from os.path import join

import numpy as np
import pandas as pd
from utils import parse_date_with_filename, spawn_logger, standardize, upsert_dataframe_to_db

logger = spawn_logger("temperature")
logger.setLevel(logging.INFO)


def read_temperature_v1(data_root: str) -> pd.DataFrame:
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


def read_temperature_v2(data_root: str) -> pd.DataFrame:
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
    file_paths = glob(os.path.join(data_root, "weather_2022", "Temp*.csv"))
    df = pd.concat(
        [
            pd.read_csv(
                fp,
                skiprows=1,
                names=snake_case_names,
                encoding="ISO-8859-1",
                usecols=snake_case_names[:8],
            )
            for fp in file_paths
        ],
        ignore_index=True,
    )
    df.columns = standardize(df.columns.tolist())
    expected_columns = ["rh_max", "rh_min", "rh_mean"]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df[["year", "month", "day"]])
    df.drop(columns=["year", "month", "day"], inplace=True)
    df = pd.concat(
        [df, read_temperature_v3(join(data_root, "weather_2022"), "From 27Jun2022")],
        ignore_index=True,
    )

    return df


def read_temperature_v3(data_dir: str, year: str) -> pd.DataFrame:
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

    def process_file(file):
        df = pd.read_csv(file)
        df.columns = standardize(df.columns.tolist())  # type: ignore
        df.rename(columns=column_mapping, inplace=True)

        df_filtered = df[[col for col in standard_columns if col in df.columns]]

        if "date" in df_filtered.columns:
            df_filtered.loc[:, "date"] = df_filtered["date"].apply(
                lambda x: parse_date_with_filename(x, file)  # pylint: disable=W0640
            )

        return df_filtered

    df = pd.concat(
        [process_file(file) for file in glob(os.path.join(data_dir, year, "Weekly DBT_RH*.csv"))],
        ignore_index=True,
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process a CSV file and write it to PostgreSQL.")
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Directory containing the data CSV file",
        default="/home/wesley/github/etheleon/national_analysis/data/climate/",
    )
    parser.add_argument(
        "--secret",
        type=str,
        default=".secrets.toml",
        help="secrets file",
    )
    parser.add_argument(
        "--ddl_root",
        type=str,
        default="./tables/create_temperature_table.sql",
        help="dir for keeping DDL",
    )

    args = parser.parse_args()

    combined_df = pd.concat(
        [
            read_temperature_v1(args.data_dir),
            read_temperature_v2(args.data_dir),
            read_temperature_v3(args.data_dir, "weather_2023"),
            read_temperature_v3(args.data_dir, "weather_2024"),
        ],
        ignore_index=True,
    ).loc[:, ["date", "station_id", "dbt_max", "dbt_min", "dbt_mean"]]
    logger.info(
        "Removing null date entries",
        extra={"nrows": len(combined_df.query("date != date"))},
    )
    combined_df = combined_df.query("date == date")
    combined_df = combined_df.sort_values(by="date")
    combined_df = combined_df.drop_duplicates(subset=["date", "station_id"])
    upsert_dataframe_to_db(
        combined_df,
        ddl_file=args.ddl_root,
        table_name="temperature",
        connection_params_path=args.secret,
    )
