#!/usr/bin/env python
"""Script to update the Nino 3.4 index data in the database."""

import pandas as pd

from dengue.utils import upsert_dataframe_to_db

URL = "https://www.cpc.ncep.noaa.gov/data/indices/wksst9120.for"


def retrieve_nino34_data(url: str) -> pd.DataFrame:
    """Retrieves Nino 3.4 index data from a specified URL and returns it as a pandas DataFrame.

    Weekly SST data starts week centered on 2Sept1981

                    Nino1+2      Nino3        Nino34        Nino4
    Week          SST SSTA     SST SSTA     SST SSTA     SST SSTA
    02SEP1981     20.6-0.1     24.8-0.1     26.5-0.2     28.3-0.3
    09SEP1981     20.1-0.6     24.7-0.2     26.5-0.2     28.4-0.2
    16SEP1981     19.7-0.9     24.6-0.3     26.5-0.2     28.4-0.3

    * SST (Sea Surface Temperature)
    * SSTA (Sea Surface Temperature Anomaly)

    For example for Nino34 on 1981-09-02, the SST value is 26.5 and the SSTA is -0.2
    Args:
        url (str): The URL to the CSV file containing the Nino 3.4 index data.

    Returns:
        pd.DataFrame: A DataFrame containing the Nino 3.4 index data with columns
                      ["date", "nino12", "nino3", "nino34", "nino4"].
                      The "date" column is formatted as "YYYY-MM-DD".
    """
    columns = ["date", "nino12", "nino3", "nino34", "nino4"]
    df = pd.read_csv(url, skiprows=4, names=columns, sep=r"\s{5}", engine="python")
    df["date"] = pd.to_datetime(df["date"], format="%d%b%Y")
    df[["sst", "ssta"]] = df["nino34"].str.extract(r"([0-9.]+)\s*([+-]?[0-9.]+)")
    df["sst"] = df["sst"].astype(float)
    df["ssta"] = df["ssta"].astype(float)
    return df.loc[:, ["date", "sst", "ssta"]]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download refresh elnino34 sst and ssta data")
    parser.add_argument(
        "--secret",
        type=str,
        default=".secrets.toml",
        help="secrets file",
    )
    parser.add_argument(
        "--ddl_root",
        type=str,
        default="./tables/create_elnino34_table.sql",
        help="dir for keeping DDL",
    )
    args = parser.parse_args()

    df = retrieve_nino34_data(URL)
    upsert_dataframe_to_db(
        df,
        ddl_file=args.ddl_root,
        table_name="elnino34",
        connection_params_path=args.secret,
    )
