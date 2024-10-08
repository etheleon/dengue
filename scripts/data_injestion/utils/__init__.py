"""Utils for running ingestion."""

import logging
import re
from contextlib import contextmanager
from logging import Logger
from typing import List

import pandas as pd
import psycopg2
import toml
from pythonjsonlogger import jsonlogger
from sqlalchemy import create_engine, text

logger = logging.getLogger("db_logger")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)

logger.addHandler(handler)


def read_excel_file(file_path, sheet_name="Sheet1"):
    """Reads an Excel file and returns its contents as a DataFrame.

    Args:
        file_path (str): The path to the Excel file.
        sheet_name (str, optional): The name of the sheet to read. Defaults to "Sheet1".

    Returns:
        pd.DataFrame: The contents of the specified sheet in the Excel file.
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df


def load_db_config(config_file="secrets.toml"):
    """Load the database configuration from a TOML file.

    Args:
        config_file (str): The path to the TOML configuration file. Defaults to "secrets.toml".

    Returns:
        dict: A dictionary containing the database configuration.
    """
    config = toml.load(config_file)
    return config["database"]


@contextmanager
def postgres_connection(config_file=".secrets.toml"):
    """Establish a connection to a PostgreSQL database using the provided configuration file.

    Args:
        config_file (str): Path to the configuration file containing database credentials. Defaults to ".secrets.toml".

    Yields:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine object for the PostgreSQL connection.

    Raises:
        Exception: If an error occurs during the connection process, it prints the error message.

    Note:
        The function ensures that the connection is properly closed in the event of an error or after the operation is complete.
    """
    conn = None
    try:
        cfg = load_db_config(config_file)
        connection_string = "postgresql+psycopg2://{}:{}@{}:5432/{}".format(
            cfg["user"],
            cfg["passwd"],
            cfg["host"],
            cfg["dbname"],
        )
        engine = create_engine(connection_string)
        yield engine
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def parse_date_with_filename(date_str, filename):
    """Parses a date string and checks if it falls within the date range specified in the filename.

    The filename is expected to contain two dates in the format YYYYMMDD-YYYYMMDD. The function
    will attempt to parse the date_str in both DD/MM/YYYY and MM/DD/YYYY formats. If the parsed
    date falls within the range specified in the filename, it returns the date in YYYY-MM-DD format.
    Otherwise, it returns pandas.NaT.

    Args:
        date_str (str): The date string to be parsed.
        filename (str): The filename containing the date range in the format YYYYMMDD-YYYYMMDD.

    Returns:
        str or pandas.NaT: The parsed date in YYYY-MM-DD format if it falls within the range,
                           otherwise pandas.NaT.
    """
    match = re.search(r"(\d{8})-(\d{8})", filename)

    if match:
        start_date = pd.to_datetime(match.group(1), format="%Y%m%d")
        end_date = pd.to_datetime(match.group(2), format="%Y%m%d")

        try:
            date_dmy = pd.to_datetime(date_str, format="%d/%m/%Y")
        except ValueError:
            date_dmy = pd.NaT

        try:
            date_mdy = pd.to_datetime(date_str, format="%m/%d/%Y")
        except ValueError:
            date_mdy = pd.NaT

        if pd.notna(date_dmy) and start_date <= date_dmy <= end_date:
            return date_dmy.strftime("%Y-%m-%d")
        elif pd.notna(date_mdy) and start_date <= date_mdy <= end_date:
            return date_mdy.strftime("%Y-%m-%d")
        else:
            return pd.NaT
    else:
        return pd.NaT


def standardize(columns: List[str]) -> List[str]:
    """Standardizes a list of column names.

    By stripping whitespace, converting to lowercase.
    and removing spaces and double quotes.

    Args:
        columns (List[str]): A list of column names to be standardized.

    Returns:
        List[str]: A list of standardized column names.
    """
    newcols = []
    for col in columns:
        newcols.append(col.strip().lower().replace(" ", "").replace('"', ""))
    return newcols


def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Removes rows with repeated headers.

    This function checks if the first row of the DataFrame contains the word "date"
    (case insensitive). If it does, it removes the first row and resets the index.

    Args::
        df (pd.DataFrame): The input DataFrame to be cleaned.

    Returns:
        pd.DataFrame: The cleaned DataFrame with the first row removed if it contained headers.
    """
    first_row = df.iloc[0].str.contains("date", case=False).any()
    if first_row:
        df = df[1:]

    return df.reset_index(drop=True)


def insert_data(df, table_name, logger: Logger):
    """Helper function to add table."""
    try:
        with postgres_connection() as engine:
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
            except psycopg2.Error as e:
                logger.error(
                    "Duplicate entry found while inserting into %s",
                    extra={
                        "table_name": table_name,
                        "error": str(e.pgcode),
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


def upsert_dataframe_to_db(df: pd.DataFrame, ddl_file: str, table_name: str, connection_params_path: str):
    """Upserts a DataFrame into a database table.

    This function reads a DDL file to create or update a database table, and then
    inserts the data from the given DataFrame into the specified table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be upserted.
        ddl_file (str): The path to the DDL file containing the SQL statements for creating/updating the table.
        table_name (str): The name of the table where the data will be upserted.
        connection_params_path (str): The path to the file containing the database connection parameters.

    Returns:
        None

    Raises:
    Exception: If an error occurs while executing the SQL statements.
    """
    with open(ddl_file, "r", encoding="UTF-8") as ddl:
        create_table_query = ddl.read()

    sql_statements = create_table_query.split(";")

    with postgres_connection(connection_params_path) as engine:
        with engine.begin() as connection:
            try:
                # Execute each SQL statement individually
                for statement in sql_statements:
                    statement = statement.strip()  # Remove leading/trailing whitespace
                    if statement:  # Skip empty statements
                        connection.execute(text(statement))
            except Exception as e:
                print(f"An error occurred while executing SQL: {e}")

    # # Insert data into the table
    insert_data(df, table_name, logger)


def spawn_logger(name: str, level=logging.INFO):
    """Create and configure a logger with the specified name and log level.

    Args:
        name (str): The name of the logger.
        level (int): The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Configured logger object.
    """
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a stream handler that outputs logs to the console in JSON format
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    handler.setFormatter(formatter)

    # Add the handler to the logger (if not already added)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger
