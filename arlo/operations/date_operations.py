import datetime
import time

import pandas as pd

from arlo.operations.types_operations import sorted_set
from arlo.parameters.param import view_months_before, view_months_after


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


def date_to_string_short(date):
    return date.strftime('%Y-%m-%d')


def string_date_now():
    return date_to_string_short(date_today())


def now_for_lunchr():
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    date_now = datetime.datetime.now() + datetime.timedelta(days=1)
    return date_now.replace(microsecond=0, tzinfo=datetime.timezone(offset=utc_offset)).isoformat()


def first_day_of_the_display(date, month_before):
    year, month = date.year, date.month - month_before
    if month < 1:
        year += (month - 1) // 12
        month = month % 12
    month += 12 * (month == 0)
    return date_to_string_short(datetime.datetime(year=year, month=month, day=1))


def last_day_of_the_display(date, month_after):
    year, month = date.year, date.month + 1 + month_after
    if month > 12:
        year += (month // 12)
        month -= 12 * (month // 12)
    return date_to_string_short(datetime.datetime(year=year, month=month, day=1) - datetime.timedelta(days=1))


def get_calendar_around(calendar, date_now):
    calendar = calendar[first_day_of_the_display(date_now, view_months_before) <= calendar.index]
    calendar = calendar[calendar.index <= last_day_of_the_display(date_now, view_months_after)]

    cycles = []
    cycle_names = sorted_set(list(calendar))
    colors_correspondence = dict({cycle_name: cycle_names.index(cycle_name) for cycle_name in cycle_names})
    for month_code in sorted(set(calendar.index.str[:7])):
        dates = calendar[calendar.index.str[:7] == month_code]
        colors = [colors_correspondence[cycle] for cycle in list(dates)]
        cycles.append(dict({'dates': list(dates.index), 'cycles': list(dates), 'colors': colors}))

    return cycles
