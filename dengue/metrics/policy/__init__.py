"""Public Health and Policy related metrics."""

import pandas as pd

from dengue.utils import download_dataframe_from_db


def coverage(self) -> pd.DataFrame:
    """Compute the impact Wolbachia release coverage has on cases averted."""
    query = """--sql
        SELECT *
        FROM national_analysis.site_release
    """
    df = download_dataframe_from_db(query)
    return df
