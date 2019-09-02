import json

import requests

from operations.data_operations import remove_already_present_id
from operations.types_operations import layered_dict_to_df
from parameters.credentials import login_bankin
from parameters.param import hello_acc_id, hello_fetched_transactions
from tools.logging import info
from tools.uniform_data_maker import format_bankin_df

bankin_url = 'https://sync.bankin.com'

bankin_headers = {'bankin-version': login_bankin.version}
bankin_params = {'client_id': login_bankin.id, 'client_secret': login_bankin.secret}
bankin_params_login = dict(bankin_params, **{'email': login_bankin.email, 'password': login_bankin.password})


def get_bankin_headers_with_token():
    device_url = bankin_url + "/v2/devices"
    device_params = dict(os='web', version='1.0.0', width=1920, height=1080, model='web', has_fingerprint=False)
    device_response = requests.post(device_url, headers=bankin_headers, json=device_params, params=bankin_params)
    bankin_device = json.loads(device_response.content)['udid']

    auth_url = bankin_url + "/v2/authenticate"
    auth_headers = dict(bankin_headers, **{"Bankin-Device": bankin_device})
    auth_response = requests.post(auth_url, headers=auth_headers, params=bankin_params_login)
    token_info = json.loads(auth_response.content)['access_token']

    return dict(bankin_headers, **{"Authorization": "Bearer " + token_info})


def get_accounts_ids(headers_with_token):
    accounts_url = bankin_url + "/v2/accounts"
    payload_accounts = dict(bankin_params, **{'limit': 20})

    response_accounts = requests.get(accounts_url, headers=headers_with_token, params=payload_accounts)
    accounts = json.loads(response_accounts.content)['resources']
    for account in accounts:
        print(account['last_refresh'][:10], account['name'].ljust(55), str(account['balance']).ljust(15), account['id'])


def get_transactions_from_account(account_id, headers_token, limit=hello_fetched_transactions):
    transactions_url = bankin_url + "/v2/accounts/" + account_id + "/transactions"
    payload_transactions = dict(bankin_params, **{'limit': limit})

    response_trans = requests.get(transactions_url, headers=headers_token, params=payload_transactions)

    return layered_dict_to_df(json.loads(response_trans.content)['resources'])


def get_latest_bankin():
    headers_token = get_bankin_headers_with_token()
    transactions = get_transactions_from_account(hello_acc_id, headers_token, hello_fetched_transactions)
    format_bankin_df(transactions)
    account = list(transactions['account'])[0]
    new_transactions = remove_already_present_id(transactions, account)

    return new_transactions


def force_refresh_bankin():
    headers = get_bankin_headers_with_token()
    force_refresh_url = bankin_url + '/v2/items/refresh'
    response = requests.post(force_refresh_url, headers=headers, params=bankin_params)
    info('Force refresh bankin : code ' + str(response.status_code))
