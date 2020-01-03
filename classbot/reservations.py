import json

import requests

from classbot.book import get_scheduled_classes_ids
from classbot.calendar import get_dates, put_classes_in_calendar, remove_empty_dates
from classbot.schedule import Classe, now
from classbot.users import get_user_id, get_token
from parameters.param import classpass_url


def get_reservations(name):
    request_url = classpass_url + '/v2/users/' + get_user_id(name) + '/reservations'
    header_token = {'CP-Authorization': "Token " + get_token(name)}
    classes = requests.get(request_url, headers=header_token)
    if classes.status_code == 504:
        return []
    return [Classe(c, []) for c in json.loads(classes.content)['reservations']]


def get_classe_from_id(name, classe_id):
    request_url = classpass_url + '/v1/schedules/' + str(classe_id)
    header_token = {'CP-Authorization': "Token " + get_token(name)}
    classe = requests.get(request_url, headers=header_token)
    if classe.status_code == 504:
        return []
    return Classe(json.loads(classe.content), [classe_id])


def get_scheduled_classes(name):
    scheduled_not_booked = []
    for classe_id in get_scheduled_classes_ids(name):
        classe = get_classe_from_id(name, classe_id)
        if classe['my_status'] == 'scheduled':
            scheduled_not_booked.append(classe)
        else:
            print('already booked')
    return scheduled_not_booked


def get_calendar_upcoming(name, mobile=False):
    start_date = now()
    classes = get_reservations(name) + get_scheduled_classes(name)
    dates = get_dates(start_date, True)
    calendar = put_classes_in_calendar(classes, dates)
    if mobile == 'true':
        calendar = remove_empty_dates(calendar)
    return calendar
