#!/usr/bin/env python
"""Script for injesting release data."""

from os.path import join

import numpy as np
import pandas as pd

from dengue.utils import read_excel_file, upsert_dataframe_to_db


def parse_release_site_data(data_root: str, file="hdb.xlsx") -> pd.DataFrame:
    """Reads release site data."""
    df = read_excel_file(join(data_root, file))
    df = df[["Postal", "PremiseType", "Sector_ID", "FirstSustainedReleaseDate_Postal", "TotalDwelling"]].rename(
        columns={
            "Postal": "postal",
            "PremiseType": "premise_type",
            "Sector_ID": "sector_id",
            "FirstSustainedReleaseDate_Postal": "release_date",
            "TotalDwelling": "total_dwelling",
        }
    )
    df = df.assign(release_date=lambda x: np.where(pd.notnull(x.release_date), x.release_date, None))
    df = df.assign(release_date=lambda x: pd.to_datetime(x.release_date, errors="coerce", unit="ns"))
    df = df.assign(total_dwelling=lambda x: np.where(pd.notnull(x.total_dwelling), x.total_dwelling, None))
    df = df.groupby(["postal", "premise_type", "sector_id", "release_date"])["total_dwelling"].sum().reset_index()
    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Release site data")
    parser.add_argument(
        "--data_dir",
        type=str,
        default="./data/release_site",
        help="Directory containing the release_site CSV file",
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
        default="./tables/create_release_site_table.sql",
        help="dir for keeping DDL",
    )
    args = parser.parse_args()

    df = pd.concat(
        [
            parse_release_site_data(args.data_dir, "hdb.xlsx"),
            parse_release_site_data(args.data_dir, "landed.xlsx"),
            parse_release_site_data(args.data_dir, "rct.xlsx"),
        ]
    )
    print(df)
    upsert_dataframe_to_db(df=df, ddl_file=args.ddl_root, table_name="site_release")
