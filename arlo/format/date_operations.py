import pandas as pd


def now():
    return pd.datetime.now()


def string_to_datetime(date):
    date = date.replace(date[10], ' ')
    return pd.datetime.strptime(date[:19], '%Y-%m-%d %H:%M:%S')


def angular_string_to_timestamp(date):
    if pd.isnull(date):
        return get_timestamp_now()
    timestamp = pd.to_datetime(date).timestamp()
    time_now = now()
    hour_offset = 3600 * time_now.hour + 60 * time_now.minute + time_now.second
    return 1000 * (timestamp + hour_offset)


def datetime_to_timestamp(date):
    return date.timestamp()


def time_since(date):
    return now() - date


def get_timestamp_now():
    return 1000 * pd.datetime.timestamp(now())


def timestamp_to_datetime(timestamp):
    return pd.datetime.fromtimestamp(int(timestamp)/1000)


def date_to_cycle(date):
    month = str(date.month_name())
    year = str(date.year)

    # TODO Overrule default dates to cycles

    return month[:3]+year[-2:]


def date_today():
    return pd.to_datetime('today')


def decode_cycle(cycle):
    if cycle == 'now':
        return date_to_cycle(date_today())
    return cycle
