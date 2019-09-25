import json
import urllib
from time import sleep

import requests

from arlo.operations.types_operations import list_of_dict_to_df
from arlo.parameters.param import n26_url, directory_tokens
from parameters.credentials import login_N26
from parameters.param import n26_fetched_transactions
from read_write.reader import empty_data_dataframe
from tools.errors import TwoFactorsAuthError
from tools.logging import error, info
from tools.scheduler import resume_scheduler, pause_scheduler
from tools.uniform_data_maker import format_n26_df
from web.status import failure_response, success_response


def read_refresh_token(name):
    try:
        with open(directory_tokens + name + '.txt', mode='r') as file:
            return file.read()
    except FileNotFoundError:
        error('Token not found for ' + name)
        return ""


def save_refresh_token(name, token):
    with open(directory_tokens + name + '.txt', mode='w') as file:
        file.write(token)

"""def get_token(name):
    values_token = {'grant_type': 'password', 'username': login_N26[name].username, 'password': login_N26[name].password}
    headers_token = {'Authorization': 'Basic YW5kcm9pZDpzZWNyZXQ='}
    response_token = requests.post(n26_url + '/oauth/token', data=values_token, headers=headers_token)
    token_info = response_token.json()
    # if 'access_token' not in token_info:
    #     return False, ''
    # return True, token_info['access_token']
"""

USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/59.0.3071.86 Safari/537.36")


def authorize_mfa(mfa_token):
    req = urllib.request.Request("https://api.tech26.de/api/mfa/challenge",
                                 data=json.dumps({"challengeType": "oob", "mfaToken": mfa_token}).encode("utf-8"))
    req.add_header("Authorization",
                   "Basic bXktdHJ1c3RlZC13ZHBDbGllbnQ6c2VjcmV0")
    req.add_header("User-Agent", USER_AGENT)
    req.add_header('Content-Type', 'application/json')

    try:
        url = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        body = json.load(e)
        error('Error in askking mfa challenge')
        info(body)


def get_initial_2fa_token(name):
    data = [("username", login_N26[name].username), ("password", login_N26[name].password),
            ("grant_type", "password")]
    "Return the bearer."
    encoded_data = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request("https://api.tech26.de/oauth/token",
                                 data=encoded_data)
    req.add_header("Authorization",
                   "Basic bXktdHJ1c3RlZC13ZHBDbGllbnQ6c2VjcmV0")
    req.add_header("User-Agent", USER_AGENT)

    try:
        url = urllib.request.urlopen(req)
        return False, ''
    except urllib.error.HTTPError as e:
        body = json.load(e)
        if body.get("error", "") == "mfa_required":
            mfa_token = body["mfaToken"]
        else:
            return False, ''

    authorize_mfa(mfa_token)
    sleep(30)
    encoded_data = urllib.parse.urlencode([("grant_type", "mfa_oob"), ("mfaToken", mfa_token)]).encode("utf-8")
    req = urllib.request.Request("https://api.tech26.de/oauth/token",
                                 data=encoded_data)
    req.add_header("Authorization",
                   "Basic bXktdHJ1c3RlZC13ZHBDbGllbnQ6c2VjcmV0")
    req.add_header("User-Agent", USER_AGENT)
    try:
        url = urllib.request.urlopen(req)
        body = json.loads(url.read().decode("utf-8"))
        url.close()
        access_token = body["access_token"]
        save_refresh_token(name, body["refresh_token"])
        return True, access_token
    except urllib.error.HTTPError:
        return False, ''


def setup_2fa_for_all_accounts():
    for name in login_N26:
        valid_token, _ = get_initial_2fa_token(name)
        if not valid_token:
            raise TwoFactorsAuthError('TwoFactorsAuthError for ' + name)
    resume_scheduler()


def get_balance(name):
    if name not in login_N26:
        return False
    valid_token, access_token = get_initial_2fa_token(name)
    headers = {'Authorization': 'bearer' + str(access_token)}
    req_balance = requests.get(n26_url + '/api/accounts', headers=headers)
    return req_balance.json()


def get_access_token_from_refresh_token(name):
    refresh_token = read_refresh_token(name)
    refresh_url = 'https://api.tech26.de/oauth/token'

    values_token = {"grant_type": "refresh_token", 'refresh_token': refresh_token
        , 'username': login_N26[name].username, 'password': login_N26[name].password}
    headers_token = {'Authorization': 'Basic bXktdHJ1c3RlZC13ZHBDbGllbnQ6c2VjcmV0'}
    response = json.loads(requests.post(refresh_url, data=values_token, headers=headers_token).content)
    try:
        refresh_token = response['refresh_token']
        access_token = response['access_token']
        save_refresh_token(name, refresh_token)
        return True, access_token
    except KeyError:
        error('Refresh token failed for ' + name)
        return False, ''


def refresh_all_tokens():
    for name in login_N26:
        is_refreshed, _ = get_access_token_from_refresh_token(name)
        if not is_refreshed:
            error('Refresh failed for ' + name)
            pause_scheduler()
            return


def get_latest_n26(name, limit=n26_fetched_transactions):
    if name not in login_N26:
        return failure_response('Invalid name to log in to N26'), empty_data_dataframe()
    valid_token, access_token = get_access_token_from_refresh_token(name)
    if not valid_token:
        return failure_response('Invalid N26 token'), empty_data_dataframe()

    headers = {'Authorization': 'bearer' + str(access_token)}
    req_transactions = requests.get(n26_url + '/api/smrt/transactions?limit=' + str(limit), headers=headers)
    data = req_transactions.json()
    return success_response(), format_n26_df(list_of_dict_to_df(data), name)
