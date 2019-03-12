import numpy as np


def apply_function(series, function_to_apply):
    return series.apply(function_to_apply, axis=1)


def positive_part(series):
    series[series < 0] = 0
    return series


def floor_series(series):
    return np.floor(series)


def ceil_series(series):
    return np.ceil(series)
