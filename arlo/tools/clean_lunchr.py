import json
import requests

from arlo.format.date_operations import lunchr_date_to_datetime
from arlo.format.df_operations import make_a_df_from_dict
from arlo.format.formatting import dataframe_formatter
from arlo.parameters.credentials import login_lunchr
from arlo.parameters.param import lunchr_url


def get_token_info(login):
    values_token = {'grant_type': 'password', 'username': login.username, 'password': login.password,
                    "client_id": login.client_id}
    response_token = requests.post(lunchr_url + '/oauth/token', data=values_token)
    token_info = response_token.json()
    access_token = token_info['access_token']

    return access_token


def get_content_page_number(access_token, num_page):
    headers = {'authorization': 'Bearer ' + str(access_token), 'x-api-key': '9f93b63a760f915878da4f97d6a4c4eef16c6eb3'}
    response = requests.get(lunchr_url + '/api/v0/payments_history?page=' + str(num_page) + '&per=30', headers=headers)
    return json.loads(response.content.decode('utf8'))


def content_to_total_count(content):
    return int(content["pagination"]['total_count'])


def content_to_df_fields(content, dictionary, last_id):
    a = set()
    for t in content["payments_history"]:
        if t['type'] != 'PAYMENT_ATTEMPT':
            a.add(t['type'])
            t['transaction_number'] = 'lunchr-T-' + t['transaction_number']
            if t['transaction_number'] == last_id:
                print(a)
                return False
            dictionary['amount'].append(round(float(t['amount']['value']), 2))
            dictionary['bank_name'].append(t['name'])
            dictionary['date'].append(lunchr_date_to_datetime(t['executed_at']))
            dictionary['id'].append(t['transaction_number'])
    print(a)
    return True


def get_data_lunchr_since_last_id(last_id=None):
    num_page = -1
    total_count = 1
    keep_going = True

    access_token = get_token_info(login_lunchr)

    dico = dict({'bank_name': [], 'amount': [], 'date': [], 'id': []})

    while (num_page + 1) * 30 < total_count and keep_going:
        num_page += 1
        content = get_content_page_number(access_token, num_page)

        keep_going = content_to_df_fields(content, dico, last_id)

        if (num_page == 0 and last_id is None) or (keep_going and total_count == 1):
            total_count = content_to_total_count(content)

    lunchr_data = make_a_df_from_dict(dico)
    lunchr_data['type'] = 'LRP'

    return dataframe_formatter(lunchr_data, 'lunchr')
