import numpy as np
import pandas as pd


def apply_function(series, function_to_apply):
    if series.shape[0] > 0:
        return series.apply(function_to_apply, axis=1)
    return pd.Series()


def positive_part(series):
    series[series < 0] = 0
    return series


def floor_series(series):
    return np.floor(series)


def ceil_series(series):
    return np.ceil(series)


def filter_series_on_value(series, value):
    return series[series == value]


def get_first_value_from_series(series):
    if series.shape[0] == 0:
        return None
    return series.iloc[0]


def series_swap_index_values(series):
    return pd.Series(series.index.values, index=series)


def is_negative(series):
    return series < 0
