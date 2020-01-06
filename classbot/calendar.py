from datetime import timedelta

from dateutil.utils import today


def format_date_for_classpass(date):
    return date.strftime(format='%Y-%m-%d')


def get_dates(start_date, long=False):
    nb_days = 14 + 14 * long
    date_now = start_date.date()
    last_monday = date_now - timedelta(days=date_now.weekday())
    range_length = nb_days + 7
    dates = [last_monday + timedelta(days=i) for i in range(range_length)]
    return [format_date_for_classpass(date) for date in dates]


def put_classes_in_calendar(classes, dates):
    class_calendar = dict({date: [] for date in dates})
    for classe in classes:
        class_calendar[format_date_for_classpass(classe['datetime'])].append(classe)
    class_calendar_array = [{'date': date, 'classes': class_calendar[date]} for date in dates]
    return class_calendar_array[:len(dates) - 7]


def now():
    return today()


def remove_empty_dates(calendar):
    return [day for day in calendar if len(day['classes']) > 0]
