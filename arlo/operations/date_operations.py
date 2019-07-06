import datetime
import time

import pandas as pd


def now():
    return pd.datetime.now()


def date_parser_for_reading(date):
    return pd.to_datetime(date, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')


def string_to_datetime(date):
    date = date.replace(date[10], ' ')
    return pd.datetime.strptime(date[:19], '%Y-%m-%d %H:%M:%S')


def short_string_to_datetime(date):
    return pd.datetime.strptime(date, '%Y-%m-%d')


def angular_string_to_timestamp(date):
    if pd.isnull(date):
        return get_timestamp_now()
    timestamp = pd.to_datetime(date).timestamp()
    time_now = now()
    hour_offset = 3600 * time_now.hour + 60 * time_now.minute + time_now.second
    return 1000 * (timestamp + hour_offset)


def datetime_to_timestamp(date):
    return date.timestamp()


def minutes_since(date):
    return (now() - date).total_seconds() // 60


def get_timestamp_now():
    return 1000 * pd.datetime.timestamp(now())


def timestamp_to_datetime(timestamp):
    return pd.datetime.fromtimestamp(int(timestamp)/1000)


def date_today():
    return pd.to_datetime('today')


def two_next_cycles():
    # TODO not dummy
    return ['DK19', 'Cali19']


def string_date_now():
    return date_today().strftime('%Y-%m-%d')


def now_for_lunchr():
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    now = datetime.datetime.now() + datetime.timedelta(days=1)
    return now.replace(microsecond=0, tzinfo=datetime.timezone(offset=utc_offset)).isoformat()
