#!/usr/bin/env python
"""Module to calculate time since switch event feature for INLA model."""

from datetime import datetime

import pandas as pd

from dengue.utils import download_dataframe_from_db


def get_time_since_switch(
    start_date: datetime, end_date: datetime, schema="national_analysis", table="dengue_agg"
) -> pd.DataFrame:
    """Computes the number of days since the last switch in the dominant dengue strain.

    This function queries a database to retrieve dengue serotype data, processes it to determine
    the dominant strain for each week, and calculates the number of days since the last switch
    in the dominant strain.

    Returns:
        pd.DataFrame: A DataFrame with two columns:
            - 'date': The date of the observation.
            - 'dominant_strain_n_days': The number of days since the last switch in the dominant strain.
    """
    start_time_s = start_date.strftime("%Y-%m-%d")
    end_time_s = end_date.strftime("%Y-%m-%d")

    query = f"""--sql
    WITH ranked_serotype AS (
      SELECT date,
        CAST(REPLACE(serotype_strain, 'D', '') AS INT) AS serotype_strain_int,
        RANK() OVER (partition by date order by serotype_count DESC) as rn
      FROM {schema}.{table}
      WHERE date >= timestamp '{start_time_s}' AND date < timestamp '{end_time_s}'
    ),
    dom_strain_unaltered AS (
        SELECT
          date,
          dominant_strain_curr_week,
          CARDINALITY(dominant_strain_curr_week) AS curr_len,
          LAG(dominant_strain_curr_week) OVER (ORDER BY date) AS dominant_strain_prev_week,
          CARDINALITY(LAG(dominant_strain_curr_week) OVER (ORDER BY date)) AS prev_len
        FROM (
          SELECT
            date,
            ARRAY_AGG(serotype_strain_int ORDER BY serotype_strain_int) AS dominant_strain_curr_week
          FROM ranked_serotype
          WHERE rn = 1
          GROUP BY date
        )
    )
    SELECT
        -- date,
		EXTRACT(YEAR from date) AS year,
		EXTRACT(WEEK from date) AS week,
        -- dominant_strain_curr_week,
        -- dominant_strain_prev_week,
        CASE
          -- Case 1: Current week strain has clear dominant strain
          WHEN curr_len = 1 THEN dominant_strain_curr_week
          
          -- Case 2: Current week has > 1 strains with max proportion
          WHEN curr_len > 1 AND prev_len = 1 THEN
            CASE
              -- IF previous week's dominant strain is present, then use that
              WHEN dominant_strain_prev_week[1] = ANY(dominant_strain_curr_week) THEN dominant_strain_prev_week
              -- Otherwise keep curr week 
              ELSE dominant_strain_curr_week
            END

          -- Case 3: Current week has >1 dominant strain and previous week also >1 dominant strain
          WHEN curr_len > 1 AND prev_len > 1 THEN dominant_strain_curr_week
          ELSE dominant_strain_curr_week -- Default case, just return current week's strain
        END AS dominant_strain_edited
    FROM dom_strain_unaltered
    """
    df = download_dataframe_from_db(query)

    event = []
    dom = []
    for i, row in df.iterrows():
        if set(dom) != set(row["dominant_strain_edited"]):
            event.append(True)
        else:
            event.append(False)
        dom = row["dominant_strain_edited"]
    df["switched"] = pd.Series(event)
    counter = 0
    time_since_switch = []
    for i, row in df.iterrows():
        if row["switched"]:
            counter = 0
        time_since_switch.append(counter)
        counter += 7  # days
    df["days_since_switch"] = pd.Series(time_since_switch)
    df.year = df.year.astype(int)
    df.week = df.week.astype(int)
    df.days_since_switch = df.days_since_switch.astype(int)
    df = df.set_index(["year", "week"])
    return df[["days_since_switch"]]
