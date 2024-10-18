#!/usr/bin/env python
"""Script to prepare training data."""


from dengue.features.climate import get_elnino34_ssta_weekly, get_temp_weekly
from dengue.features.serology import get_time_since_switch

temp_df = get_temp_weekly()
print(temp_df.head())

sero_df = get_time_since_switch()
print(sero_df.head())

elnino_df = get_elnino34_ssta_weekly()
print(elnino_df.head())

df = temp_df.join(sero_df, on=["year", "eweek"]).join(elnino_df, on=["year", "eweek"]).reset_index()
df.days_since_switch = df.days_since_switch.fillna(0)
df.days_since_switch = df.days_since_switch.astype(int)
print(df)
