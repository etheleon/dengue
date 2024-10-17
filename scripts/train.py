#!/usr/bin/env python
"""Script to prepare training data."""


from dengue.features.climate import get_elnino34_ssta_weekly, get_temp_weekly
from dengue.features.serology import get_time_since_switch

temp_df = get_temp_weekly().set_index(["year", "week"])
# print(temp_df.head())

sero_df = get_time_since_switch().set_index(["year", "week"])
# print(sero_df.head())

elnino_df = get_elnino34_ssta_weekly().set_index(["year", "week"])
# print(elnino_df.head())

df = temp_df.join(sero_df, on=["year", "week"]).join(elnino_df, on=["year", "week"]).reset_index()
# print(df)
