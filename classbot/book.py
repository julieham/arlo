import json
from datetime import datetime, timedelta

import requests
from crontab import CronTab

from classbot.users import get_token, get_user_id, make_header_token
from parameters.param import classpass_url

book_later_job_name = '/home/arlo/classbot/book.sh'

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
    job_name = book_later_job_name + ' ' + name + ' ' + str(class_id)
    create_cronjob(booking_date, job_name)


def create_cronjob(booking_date, job_name):
    user_cron = CronTab(user=True)
    job = user_cron.new(command=job_name)
    job.minute.on(booking_date.minute)
    job.hour.on(booking_date.hour)
    job.day.on(booking_date.day)
    job.month.on(booking_date.month)
    user_cron.write()


def cancel_scheduled_class(name, classe_id):
    user_cron = CronTab(user=True)
    write = 0
    for job in user_cron:
        job_fields = str(job).split()
        job_username, job_classe_id = job_fields[-2:]
        if (job_username == name) & (job_classe_id == classe_id):
            user_cron.remove(job)
            write += 1
    if write > 0:
        user_cron.write()
    return write > 0


def get_scheduled_classes_ids(user):
    user_cron = CronTab(user=True)
    jobs = [str(job).split()[5:] for job in user_cron]
    scheduled_classes = [job[1:3] for job in jobs if job[0] == book_later_job_name]
    return [int(classe) for username, classe in scheduled_classes if username == user]


def cancel_booked_class(name, classe_id):
    url = classpass_url + '/v1/users/' + get_user_id(name) + '/reservations/' + str(classe_id)
    header = make_header_token(get_token(name))
    header['Content-Type'] = 'application/json'
    cancel_dict = {"state": "cancel"}
    return requests.patch(url, headers=header, json=cancel_dict)
