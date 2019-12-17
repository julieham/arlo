import json
from datetime import datetime, timedelta

import requests
from crontab import CronTab

from classbot.users import get_token, get_user_id, make_header_token
from parameters.param import classpass_url


def book_class_with_info(user, class_id, class_credits):
    header_token = make_header_token(get_token(user))
    user_id = get_user_id(user)
    booking_response = requests.post(classpass_url + '/v1/users/' + user_id + '/reservations',
                                     data=json.dumps({'schedule': int(class_id),
                                                      'credits': int(class_credits)}),
                                     headers=header_token)
    return booking_response.status_code == 201


def first_booking(class_datetime):
    return datetime(year=class_datetime.year, month=class_datetime.month, day=class_datetime.day) + timedelta(
        hours=12) - timedelta(days=7)


def plan_booking(class_id, name, class_date):
    class_date = datetime.strptime(class_date, '%Y-%m-%d')
    booking_date = first_booking(class_date)
    job_name = '/home/arlo/classbot/book.sh ' + name + ' ' + str(class_id)
    create_cronjob(booking_date, job_name)


def create_cronjob(booking_date, job_name):
    user_cron = CronTab(user=True)
    job = user_cron.new(command=job_name)
    job.minute.on(booking_date.minute)
    job.hour.on(booking_date.hour)
    job.day.on(booking_date.day)
    job.month.on(booking_date.month)
    user_cron.write()
