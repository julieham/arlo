import json

import pandas as pd
import requests

from classbot.users import get_token
from parameters.param import classpass_url, classpass_delta_seconds


class Classe(dict):

    def __init__(self, classe):
        super().__init__()
        self['id'] = classe['id']
        self['teacher'] = classe['teacher_name']
        class_starttime = classe['starttime']
        self['datetime'] = timestamp_to_datetime(class_starttime - classpass_delta_seconds)
        self['minutes'] = int((classe['endtime'] - class_starttime) // 60)
        self['available'] = classe['availability']['status'] == "available"
        self['venue'] = classe['venue']['name']
        self['name'] = classe['class']['name']
        self['bookLater'] = False
        self['status'] = 'available' if self['available'] else classe['availability']['reason']
        if not self['available']:
            self['bookLater'] = classe['availability']['reason'] == "before_opening_window"
            if self['bookLater']:
                self['credits'] = '?'
            elif self['status'] == 'reserved':
                self['credits'] = 'booked'
            else:
                self['credits'] = 'full'
        if self['available']:
            self['credits'] = str(classe['availability']['credits'])
        self['bookable'] = self['available'] | self['bookLater']


def format_date_for_classpass(date):
    return date.strftime(format='%Y-%m-%d')


def timestamp_to_datetime(timestamp):
    return pd.datetime.fromtimestamp(int(timestamp))


def now():
    return pd.datetime.now()


def get_classes(name, venue_id, start_date=now()):
    request_url = classpass_url + '/v1/venues/' + str(venue_id) + '/schedules?date=' + format_date_for_classpass(
        start_date) + '&upcoming=true'
    header_token = {'CP-Authorization': "Token " + get_token(name)}
    classes = requests.get(request_url, headers=header_token).content
    return [Classe(c) for c in json.loads(classes)['schedules']]
