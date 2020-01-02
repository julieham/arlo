import json
from datetime import datetime, timedelta

import requests
from crontab import CronTab

from classbot.users import get_token, get_user_id, make_header_token
from parameters.param import classpass_url
from tools.logging import info


def now():
    return datetime.now()


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


def get_scheduled_classes(user):
    user_cron = CronTab(user=True)
    scheduled_classes = [str(job).split()[6:8] for job in user_cron]
    return [int(classe) for username, classe in scheduled_classes if username == user]


def clean_cron():
    user_cron = CronTab(user=True)
    write = False
    for job in user_cron:
        today = now()
        values = str(job).split()
        month, day = int(values[2]), int(values[3])
        passed = (today.month - 6 < month < today.month) | ((day < today.day) & (month == today.month)) | (
                    today.month < 7 & month > 7)
        if passed:
            info('removing cron job : ' + str(job))
            user_cron.remove(job)
            write = True
    if write:
        user_cron.write()
