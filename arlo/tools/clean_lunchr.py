import json

import requests

from arlo.operations.data_operations import remove_already_present_id
from arlo.operations.df_operations import drop_other_columns
from arlo.operations.types_operations import layered_dict_to_df
from arlo.parameters.credentials import login_lunchr
from arlo.parameters.param import lunchr_url, lunchr_dictionary
from arlo.tools.uniform_data_maker import format_lunchr_df
from operations.date_operations import now_for_lunchr
from read_write.reader import empty_data_dataframe
from tools.uniform_data_maker import process_lunchr_cb_transaction
from web.status import success_response, failure_response, is_successful


def get_token(login):
    values_token = {'grant_type': 'password', 'username': login.username, 'password': login.password,
                    "client_id": login.client_id}
    response_token = requests.post(lunchr_url + '/oauth/token', data=values_token)
    token_info = response_token.json()
    access_token = token_info['access_token']

    return access_token


def get_content_page_number(access_token):
    headers = {'authorization': 'Bearer ' + str(access_token), 'x-api-key': '9f93b63a760f915878da4f97d6a4c4eef16c6eb3'}
    response = requests.get(lunchr_url + '/api/v0/payments_history?before=' + now_for_lunchr() + '&per=30',
                            headers=headers)
    try:
        return success_response(), json.loads(response.content.decode('utf8'))
    except:
        return failure_response('Lunchr refresh failed'), ''


def is_processed(transaction):
    try:
        return transaction['declined_at'] is None and transaction['refunded_at'] is None
    except KeyError:
        return True


def remove_unprocessed_payments(transactions_list):
    return [transaction for transaction in transactions_list if is_processed(transaction)]


def lunchr_df_first_page(access_token):
    response, content = get_content_page_number(access_token)
    if not is_successful(response):
        return response, empty_data_dataframe()

    payments = content['payments_history']
    payments = remove_unprocessed_payments(payments)

    return response, layered_dict_to_df(payments)


def get_latest_lunchr():
    access_token = get_token(login_lunchr)
    response, lunchr_df = lunchr_df_first_page(access_token)
    if not is_successful(response):
        return empty_data_dataframe()
    drop_other_columns(lunchr_df, lunchr_dictionary.keys())
    format_lunchr_df(lunchr_df)
    account = list(lunchr_df['account'])[0]

    lunchr_df = process_lunchr_cb_transaction(lunchr_df)
    real_lunchr_df = remove_already_present_id(lunchr_df, account, limit=90)

    return real_lunchr_df
