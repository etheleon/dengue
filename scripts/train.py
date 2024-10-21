#!/usr/bin/env python
"""Script to prepare training data."""


from dengue.features.climate import get_days_no_rain, get_elnino34_ssta_weekly, get_temp_weekly
from dengue.features.serology import get_time_since_switch

temp_df = get_temp_weekly()
sero_df = get_time_since_switch()
elnino_df = get_elnino34_ssta_weekly()
days_no_rain_df = get_days_no_rain()

df = (
    temp_df.join(sero_df, on=["year", "eweek"])
    .join(elnino_df, on=["year", "eweek"])
    .join(days_no_rain_df, on=["year", "eweek"])
    .reset_index()
)
df.days_since_switch = df.days_since_switch.fillna(0)
df.days_since_switch = df.days_since_switch.astype(int)

"""
>>> print(df)
      year  eweek  max_t_scale_12_wk_avg_0  days_since_switch  nino34_12_wk_avg_4  days_no_rain_12_wk_total_0
0     1981     53                -4.109591                  0           -0.416667                           0
1     1982      1                -4.059591                  0           -0.433333                           1
2     1982      2                -3.300067                  0           -0.466667                           4
3     1982      3                -2.623876                  0           -0.458333                           9
4     1982      4                -2.183876                  0           -0.441667                          13
...    ...    ...                      ...                ...                 ...                         ...
2225  2024     31                 0.580932                 49            0.325000                           8
2226  2024     32                 0.512382                 56            0.291667                           8
2227  2024     33                 0.500020                 63            0.241667                           8
2228  2024     34                 0.447261                 70            0.208333                           8
2229  2024     35                 0.417329                 77            0.166667                          12
"""
