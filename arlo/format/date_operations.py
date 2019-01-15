import pandas as pd

def string_to_datetime(date):
    return pd.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')


def time_since(date):
    return pd.datetime.now() - date


def get_timestamp_now():
    return pd.datetime.timestamp(pd.datetime.now())


def timestamp_to_datetime(timestamp):
    return pd.datetime.fromtimestamp(int(timestamp)/1000)


def date_to_cycle(date):
    month = str(date.month_name())
    year = str(date.year)
    return month[:3]+year[-2:]
