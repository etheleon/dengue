"""Utils."""

import logging
import os
import re
from contextlib import contextmanager
from logging import Logger
from typing import List, Optional

import pandas as pd
import psycopg2
import toml
from pythonjsonlogger import jsonlogger
from sqlalchemy import create_engine, text
from dynaconf import settings

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


@contextmanager
def postgres_connection():
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
        connection_string = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            settings.DB_USERNAME,
            settings.DB_PASSWORD,
            settings.DB_HOST,
            settings.DB_PORT,
            settings.DB_NAME,
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


def insert_data(df: pd.DataFrame, table_name: str, logger: Logger, schema: str = "national_analysis"):
    """Helper function to add table."""
    try:
        with postgres_connection() as engine:
            try:
                df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
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


def upsert_dataframe_to_db(
    df: pd.DataFrame,
    ddl_file: Optional[str],
    table_name: str,
    connection_params_path: Optional[str],
    schema="national_analysis",
):
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
    if ddl_file is None:
        logger.info("Inferring schema from DataFrame")
    else:
        with open(ddl_file, "r", encoding="UTF-8") as ddl:
            create_table_query = ddl.read()

        sql_statements = create_table_query.split(";")
        with postgres_connection() as engine:
            with engine.begin() as connection:
                try:
                    # Execute each SQL statement individually
                    for statement in sql_statements:
                        statement = statement.strip()  # Remove leading/trailing whitespace
                        if statement:  # Skip empty statements
                            connection.execute(text(statement))
                except Exception as e:
                    print(f"An error occurred while executing SQL: {e}")

    # Insert data into the table
    insert_data(df, table_name, logger, schema=schema)


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


def download_dataframe_from_db(query: str) -> pd.DataFrame:
    """Downloads data from a database based on the provided SQL query and returns it as a pandas DataFrame.

    Args:
        query (str): The SQL query to execute for retrieving the data.
        connection_params_path (str): The path to the file containing the database connection parameters.

    Returns:
        pd.DataFrame: The data retrieved from the database as a pandas DataFrame.

    Raises:
        Exception: If an error occurs during the data retrieval process.
    """
    try:
        with postgres_connection() as engine:
            try:
                df = pd.read_sql(query, engine)
                logger.info(
                    "Data retrieved successfully",
                    extra={
                        "query": query,
                        "row_count": len(df),
                    },
                )
                return df
            except Exception as e:
                logger.error(
                    "Failed to retrieve data",
                    extra={
                        "query": query,
                        "error": str(e),
                    },
                )
                raise e
    except Exception as e:
        logger.error(
            "Failed to establish database connection",
            extra={"error": str(e)},
        )
        raise e