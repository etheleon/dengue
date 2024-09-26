#!/usr/bin/env python

"""Script to read weather data and populate temperature table.
"""
import argparse
import logging
import os
from glob import glob
from typing import List

import numpy as np
import pandas as pd
import utils
from psycopg2.errors import UniqueViolation
from pythonjsonlogger import jsonlogger

# Set up the logger
logger = logging.getLogger("db_logger")
logger.setLevel(logging.INFO)

# Create a stream handler that outputs logs to the console in JSON format
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


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
    params = {
        "skiprows": 1,
        "names": snake_case_names,
        "encoding": "ISO-8859-1",
        "usecols": snake_case_names[:8],
    }
    df = pd.concat(
        [
            pd.read_csv(os.path.join(data_root, "weather_2022", "Temp 2022.csv"), **params),
            pd.read_csv(os.path.join(data_root, "weather_2022", "Temp 202206.csv"), **params),
        ],
        ignore_index=True,
    )

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

    def process_file(file):
        df = pd.read_csv(file)
        df.columns = standardize(df.columns.tolist())  # type: ignore
        df.rename(columns=column_mapping, inplace=True)

        # Filter columns
        df_filtered = df[[col for col in standard_columns if col in df.columns]]

        # Parse date if "date" column exists
        if "date" in df_filtered.columns:
            df_filtered.loc[:, "date"] = df_filtered["date"].apply(
                lambda x: utils.parse_date_with_filename(x, file)  # pylint: disable=W0640
            )

        return df_filtered

    # Process all files and concatenate the results into a single DataFrame
    df = pd.concat(
        [process_file(file) for file in glob(os.path.join(data_dir, year, "Weekly DBT_RH*.csv"))],
        ignore_index=True,
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def insert_data(df, table_name):
    try:
        with utils.postgres_connection() as engine:
            try:
                df.to_sql(
                    table_name,
                    engine,
                    schema="national_analysis",
                    if_exists="append",
                    index=False,
                )
                logger.info(
                    "Data inserted successfully",
                    extra={
                        "table_name": table_name,
                        "row_count": len(df),
                    },
                )
            except UniqueViolation as e:
                logger.error(
                    "Duplicate entry found while inserting into %s",
                    extra={
                        "table_name": table_name,
                        "error": str(e),
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to insert data",
                    extra={
                        "error": str(e),
                        "table_name": table_name,
                    },
                )
    except Exception as e:
        logger.error(
            "Failed to insert data",
            extra={
                "error": str(e),
                "table_name": table_name,
            },
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process a CSV file and write it to PostgreSQL.")
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Directory containing the data CSV file",
        default="/home/wesley/github/etheleon/national_analysis/data/climate/",
    )
    args = parser.parse_args()

    df = pd.concat(
        [
            read_dbt_rh_2009_2021(args.data_dir),
            read_dbt_rh_2022(args.data_dir),
            read_dbt_rh_2023_2024(args.data_dir, "weather_2023"),
            read_dbt_rh_2023_2024(args.data_dir, "weather_2024"),
        ],
        ignore_index=True,
    ).loc[:, ["date", "station_id", "dbt_max", "dbt_min", "dbt_mean"]]
    logger.info(
        "Removing null date entries",
        extra={"nrows": len(df.query("date != date"))},
    )
    df = df.query("date == date")
    insert_data(df, "temperature")
