import requests

from arlo.operations.types_operations import list_of_dict_to_df
from arlo.parameters.param import n26_url
from parameters.credentials import login_N26
from parameters.param import n26_max_transactions_per_user
from read_write.reader import empty_data_dataframe
from tools.uniform_data_maker import format_n26_df
from web.status import failure_response, success_response


def get_token(name):
    values_token = {'grant_type': 'password', 'username': login_N26[name].username, 'password': login_N26[name].password}
    headers_token = {'Authorization': 'Basic YW5kcm9pZDpzZWNyZXQ='}
    response_token = requests.post(n26_url + '/oauth/token', data=values_token, headers=headers_token)
    token_info = response_token.json()
    if 'access_token' not in token_info:
        return False, ''
    return True, token_info['access_token']


def get_balance(name):
    if name not in login_N26:
        return False
    valid_token, access_token = get_token(name)
    headers = {'Authorization': 'bearer' + str(access_token)}
    req_balance = requests.get(n26_url + '/api/accounts', headers=headers)
    return req_balance.json()


def get_last_transactions_as_df(name, limit=n26_max_transactions_per_user):
    if name not in login_N26:
        return failure_response('Invalid name to log in to N26'), empty_data_dataframe()
    valid_token, access_token = get_token(name)
    if not valid_token:
        return failure_response('Invalid N26 token'), empty_data_dataframe()

    headers = {'Authorization': 'bearer' + str(access_token)}
    req_transactions = requests.get(n26_url + '/api/smrt/transactions?limit=' + str(limit), headers=headers)
    data = req_transactions.json()
    return success_response(), format_n26_df(list_of_dict_to_df(data), name)
