from sqlalchemy import create_engine

import os

import pandas as pd
import psycopg2
from psycopg2 import sql

# Function to read the Excel file
def read_excel_file(file_path):
    # Read the Excel file using pandas
    df = pd.read_excel(file_path, sheet_name='Sheet1')
    return df

import numpy as np

import re

from glob import glob