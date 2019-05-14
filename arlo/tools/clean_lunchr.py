import json

import requests

from arlo.operations.data_operations import remove_already_present_id
from arlo.operations.df_operations import concat_lines, drop_other_columns
from arlo.operations.types_operations import layered_dict_to_df
from arlo.parameters.credentials import login_lunchr
from arlo.parameters.param import lunchr_url, lunchr_dictionary
from arlo.tools.uniform_data_maker import format_lunchr_df
from read_write.reader import empty_data_dataframe
from web.status import success_response, failure_response, is_successful


def get_token(login):
    values_token = {'grant_type': 'password', 'username': login.username, 'password': login.password,
                    "client_id": login.client_id}
    response_token = requests.post(lunchr_url + '/oauth/token', data=values_token)
    token_info = response_token.json()
    access_token = token_info['access_token']

    return access_token


def get_content_page_number(access_token, num_page):
    headers = {'authorization': 'Bearer ' + str(access_token), 'x-api-key': '9f93b63a760f915878da4f97d6a4c4eef16c6eb3'}
    response = requests.get(lunchr_url + '/api/v0/payments_history?page=' + str(num_page) + '&per=30', headers=headers)
    try:
        return success_response(), json.loads(response.content.decode('utf8'))
    except:
        return failure_response('Lunchr refresh failed'), ''




def how_many_pages(access_token):
    response, content = get_content_page_number(access_token, 100000)
    return response, (content['pagination']['pages_count'] if is_successful(response) else 0)



def extract_lunchr_refunds(transactions_list):
    refunds_transactions = []
    for transaction in transactions_list:
        if 'refunding_transaction' in transaction:
            refunds_transactions.append(transaction['refunding_transaction'])
            del transaction['refunding_transaction']

    return refunds_transactions + transactions_list


def lunchr_df_page_num(access_token, num_page):
    response, content = get_content_page_number(access_token, num_page)
    if not is_successful(response):
        return response, empty_data_dataframe()

    payments = content['payments_history']
    payments = extract_lunchr_refunds(payments)

    return response, layered_dict_to_df(payments)


def get_latest_lunchr():
    access_token = get_token(login_lunchr)
    response, lunchr_df = lunchr_df_page_num(access_token, 0)
    if not is_successful(response):
        return empty_data_dataframe()
    drop_other_columns(lunchr_df, lunchr_dictionary.keys())
    format_lunchr_df(lunchr_df)
    account = list(lunchr_df['account'])[0]
    return remove_already_present_id(lunchr_df, account, limit=30)


def get_all_lunchr_data():
    access_token = get_token(login_lunchr)
    num_pages = how_many_pages(access_token)

    all_content = [lunchr_df_page_num(access_token, page_num)[1] for page_num in range(num_pages)]
    all_data = concat_lines(all_content)
    format_lunchr_df(all_data)

    return all_data.drop_duplicates()
