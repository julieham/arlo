import json
from datetime import timedelta

import pandas as pd
import requests

from classbot.book import get_scheduled_classes
from classbot.users import get_token
from parameters.param import classpass_url, classpass_delta_seconds


class Classe(dict):

    def __init__(self, classe, scheduled_classes):
        super().__init__()
        self['id'] = classe['id']
        if 'teacher_name' in classe.keys():
            self['teacher'] = classe['teacher_name']
        else:
            self['teacher'] = ''
        class_starttime = classe['starttime']
        self['datetime'] = timestamp_to_datetime(class_starttime - classpass_delta_seconds)
        self['minutes'] = int((classe['endtime'] - class_starttime) // 60)
        self['venue'] = classe['venue']['name']
        self['name'] = classe['class']['name']
        self['cp_status'] = 'available' if classe['availability']['status'] == "available" else classe['availability'][
            'reason']
        if self['id'] in scheduled_classes:
            self['my_status'] = 'scheduled'
        elif self['cp_status'] == 'available':
            self['my_status'] = 'available_now'
        elif self['cp_status'] == 'reserved':
            self['my_status'] = 'booked'
        elif classe['availability']['reason'] == "before_opening_window":
            self['my_status'] = 'available_later'
        elif self['cp_status'] == 'top_up':
            self['my_status'] = "top_up"
        elif self['cp_status'] == 'overlaps':
            self['my_status'] = "overlaps"
        else:
            self['my_status'] = 'full'
        if self['my_status'] in ['available_now', 'top_up']:
            self['credits'] = str(classe['availability']['credits'])
        else:
            self['credits'] = -1
        self['bookable'] = self['my_status'] in ['available_now', 'available_later']


def format_date_for_classpass(date):
    return date.strftime(format='%Y-%m-%d')


def timestamp_to_datetime(timestamp):
    return pd.datetime.fromtimestamp(int(timestamp))


def now():
    return pd.datetime.now()


def get_classes(name, venue_id, start_date):
    request_url = classpass_url + '/v1/venues/' + str(venue_id) + '/schedules?date=' + format_date_for_classpass(
        start_date) + '&upcoming=true'
    header_token = {'CP-Authorization': "Token " + get_token(name)}
    classes = requests.get(request_url, headers=header_token)
    if classes.status_code == 504:
        return []
    scheduled_classes = get_scheduled_classes(name)
    return [Classe(c, scheduled_classes) for c in json.loads(classes.content)['schedules']]


def get_dates(start_date, long=False):
    nb_days = 14 + 14 * long
    date_now = start_date.date()
    last_monday = date_now - timedelta(days=date_now.weekday())
    range_length = nb_days + 7 * (last_monday != date_now)
    dates = [last_monday + timedelta(days=i) for i in range(range_length)]
    return [format_date_for_classpass(date) for date in dates]


def get_calendar_classes(name, venue_id, long=False):
    start_date = now()
    dates = get_dates(start_date, long=long)
    classes = get_classes(name, venue_id, start_date)
    if long:
        classes += get_classes(name, venue_id, start_date + timedelta(days=14))
    class_calendar = dict({date: [] for date in dates})
    for classe in classes:
        class_calendar[format_date_for_classpass(classe['datetime'])].append(classe)
    class_calendar_array = [{'date': date, 'classes': class_calendar[date]} for date in dates]
    return class_calendar_array[:(14 + 14 * long)]
