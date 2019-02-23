import json
import requests

from arlo.format.data_operations import remove_already_present_id
from arlo.format.df_operations import vertical_concat
from arlo.format.types_operations import layered_dict_to_df
from arlo.parameters.credentials import login_lunchr
from arlo.parameters.param import lunchr_url
from arlo.tools.uniform_data_maker import format_lunchr_df


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


def how_many_pages(access_token):
    content = get_content_page_number(access_token, 100000)
    return content['pagination']['pages_count']


def extract_lunchr_refunds(transactions_list):
    refunds_transactions = []
    for transaction in transactions_list:
        if 'refunding_transaction' in transaction:
            refunds_transactions.append(transaction['refunding_transaction'])
            del transaction['refunding_transaction']

    return refunds_transactions + transactions_list


def lunchr_df_page_num(access_token, num_page):
    content = get_content_page_number(access_token, num_page)
    payments = content['payments_history']
    payments = extract_lunchr_refunds(payments)

    return layered_dict_to_df(payments)


def get_latest_lunchr():
    access_token = get_token_info(login_lunchr)
    lunchr_df = lunchr_df_page_num(access_token, 0)
    format_lunchr_df(lunchr_df)
    account = list(lunchr_df['account'])[0]
    return remove_already_present_id(lunchr_df, account, limit=30)


def get_all_lunchr_data():
    access_token = get_token_info(login_lunchr)
    num_pages = how_many_pages(access_token)
    all_content = [format_lunchr_df(lunchr_df_page_num(access_token, page_num)) for page_num in range(num_pages)]
    all_data = vertical_concat(all_content)
    return all_data.drop_duplicates()
