import re
from contextlib import contextmanager

import pandas as pd
import psycopg2
import toml


def read_excel_file(file_path, sheet_name="Sheet1"):
    # Read the Excel file using pandas
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df


def load_db_config(config_file="secrets.toml"):
    config = toml.load(config_file)
    return config["database"]


@contextmanager
def postgres_connection(config_file="secrets.toml"):
    conn = None
    try:
        # Load the database configuration
        config = load_db_config(config_file)

        # Establish the PostgreSQL connection using the config
        conn = psycopg2.connect(
            host=config["host"],
            database=config["dbname"],
            user=config["user"],
            password=config["password"],
            port=config.get("port", 5432),  # Use port 5432 if not specified
        )
        # Provide the connection to the block within 'with'
        yield conn
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        # Close the connection
        if conn is not None:
            conn.close()


def parse_date_with_filename(date_str, filename):
    """
    Try to parse a date string in either day/month/year or month/day/year format based on the file name's date range.
    Return the parsed date if it falls within the file's date range; otherwise, return pd.NaT.
    """
    # Extract the date range from the file name (e.g., '20230109-20230115')
    match = re.search(r"(\d{8})-(\d{8})", filename)

    if match:
        # Extract start and end dates from the file name
        start_date = pd.to_datetime(match.group(1), format="%Y%m%d")
        end_date = pd.to_datetime(match.group(2), format="%Y%m%d")

        # Try parsing the date string with both formats
        try:
            # Try day/month/year format
            date_dmy = pd.to_datetime(date_str, format="%d/%m/%Y")
        except ValueError:
            date_dmy = pd.NaT

        try:
            # Try month/day/year format
            date_mdy = pd.to_datetime(date_str, format="%m/%d/%Y")
        except ValueError:
            date_mdy = pd.NaT

        # Check if either parsed date falls within the start and end dates
        if pd.notna(date_dmy) and start_date <= date_dmy <= end_date:
            return date_dmy.strftime("%Y-%m-%d")
        elif pd.notna(date_mdy) and start_date <= date_mdy <= end_date:
            return date_mdy.strftime("%Y-%m-%d")
        else:
            return pd.NaT  # Return NaT if neither date is valid or in range
    else:
        return pd.NaT  # Return NaT if no date range is found in the filename
