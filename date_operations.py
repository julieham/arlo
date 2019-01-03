import pandas as pd

def to_datetime(date):
    return pd.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')


def time_since(date):
    return pd.datetime.now() - date