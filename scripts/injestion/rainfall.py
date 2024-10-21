#!/usr/bin/env python
"""For injesting rain data."""

import argparse
import logging
from glob import glob
from os.path import join

import numpy as np
import pandas as pd
from utils import parse_date_with_filename, standardize, upsert_dataframe_to_db

logger = logging.getLogger("rainfall_processor")
logger.setLevel(logging.INFO)


def read_rainfall_v1(data_root: str) -> pd.DataFrame:
    """Reads rainfall data for 1982 to 2021."""
    file_path = join(data_root, "weather_1982_2021", "Daily Rain 1982-2021.csv")
    df = pd.read_csv(file_path)

    df.columns = standardize(df.columns.tolist())

    column_mappings = {
        "id_station": "station_id",
        "dateasia/singapore(+0730)": "date",
        "totalrain": "rainfall_amt_total",
    }
    df.rename(columns=column_mappings, inplace=True)

    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

    if "rainfall_duration_min" not in df.columns:
        df["rainfall_duration_min"] = np.nan
    df = df.drop_duplicates()
    return df


def read_rainfall_v2(data_root: str) -> pd.DataFrame:
    """Reads rainfall data for 2022.

    From Jan to July, the files follow this format 'Rain 202207.csv'
    After july the data is in another file
    .
    ├── From 27Jun2022
    ├── Rain 202206.csv
    ├── Rain 202207.csv
    ├── Rain 2022.csv
    ├── Rain_2022.csv.bak
    ├── Temp 202206.csv
    ├── Temp 2022.csv
    └── Weather 202207.xlsx
    """
    file_paths = glob(join(data_root, "weather_2022", "Rain*.csv"))
    df = pd.concat([pd.read_csv(fp) for fp in file_paths], ignore_index=True)

    df.columns = standardize(df.columns.tolist())
    column_mappings = {
        "station": "station_id",
        "totalrainfall(mm)": "rainfall_amt_total",
        "totalduration": "rainfall_duration_min",
    }
    df.rename(columns=column_mappings, inplace=True)
    df["date"] = pd.to_datetime(df[["year", "month", "day"]])
    df.drop(columns=["year", "month", "day"], inplace=True)
    df = df.loc[:, ["station_id", "date", "rainfall_amt_total", "rainfall_duration_min"]]
    df = pd.concat(
        [df, read_rainfall_v3(join(data_root, "weather_2022", "From 27Jun2022"))],
        ignore_index=True,
    )
    df = df.drop_duplicates(subset=["date", "station_id"])
    return df


def read_rainfall_v3(data_root: str) -> pd.DataFrame:
    """Reads rainfall data for newer csv."""
    column_mapping = {
        "id_station": "station_id",
        "stationcode": "station_id",
        "dailyrainamount(mm)": "rainfall_amt_total",
        "rainamount(mm)": "rainfall_amt_total",
        "dailyduration(minutes)": "rainfall_duration_min",
        "dailyduration(minute)": "rainfall_duration_min",
        "totalrain": "rainfall_amt_total",
        "date": "date",
        "date.singapore...0800.": "date",
        "datesingapore(+0800)": "date",
        "dateasia/singapore(+0800)": "date",
        "sttaioncode": "station_id",
    }
    standard_columns = [
        "date",
        "station_id",
        "rainfall_amt_total",
        "rainfall_duration_min",
    ]

    def process_file(file):
        df = pd.read_csv(file)
        df.columns = standardize(df.columns.tolist())  # type: ignore
        df.rename(columns=column_mapping, inplace=True)
        expected_columns = ["rainfall_amt_total", "rainfall_duration_min"]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = np.nan
        df = df.loc[:, standard_columns]
        if "date" in df.columns:
            df.loc[:, "date"] = df["date"].apply(lambda x: parse_date_with_filename(x, file))  # pylint: disable=W0640
        return df

    df = pd.concat(
        [process_file(file) for file in glob(join(data_root, "Weekly Rain*.csv"))],
        ignore_index=True,
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.drop_duplicates(subset=["date", "station_id"])
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a rainfall data and write it to PostgreSQL.")
    parser.add_argument(
        "--data_dir",
        type=str,
        default="./data/climate/",
        help="Directory containing the climate data CSV file",
    )
    parser.add_argument(
        "--secret",
        type=str,
        default=".secrets.toml",
        help="secrets file",
    )
    parser.add_argument(
        "--table-ddl",
        type=str,
        default="./tables/create_rainfall_table.sql",
        help="dir for keeping DDL",
    )
    args = parser.parse_args()

    combined_df = pd.concat(
        [
            read_rainfall_v1(args.data_dir),
            read_rainfall_v2(args.data_dir),
            read_rainfall_v3(join(args.data_dir, "weather_2023")),
            read_rainfall_v3(join(args.data_dir, "weather_2024")),
        ],
        ignore_index=True,
    )
    logger.info(
        "Removing null date entries",
        extra={"nrows": len(combined_df.query("date != date"))},
    )
    combined_df = combined_df.query("date == date")
    combined_df = combined_df.sort_values(by="date")
    combined_df = combined_df.drop_duplicates(subset=["date", "station_id"])
    # TODO(Wesley): this needs to be checked
    duplicates = combined_df[combined_df.duplicated(subset=["station_id", "date"], keep=False)]
    upsert_dataframe_to_db(
        combined_df,
        ddl_file=args.ddl_root,
        table_name="rainfall",
        connection_params_path=args.secret,
    )
