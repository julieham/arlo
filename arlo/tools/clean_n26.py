import json

import requests
from tenacity import sleep
from arlo.operations.types_operations import list_of_dict_to_df
from arlo.parameters.param import n26_url, directory_tokens, n26_auth_url, n26_auth_header
from arlo.objects.token import Token, InvalidToken, N26_Token, EmptyN26Token, get_token
from arlo.parameters.credentials import login_N26
from arlo.parameters.param import n26_fetched_transactions
from arlo.tools.errors import TwoFactorsAuthError
from arlo.tools.logging import error, info
from arlo.tools.scheduler import resume_scheduler
from arlo.tools.uniform_data_maker import format_n26_df
from read_write.reader import empty_data_dataframe
from web.status import failure_response, success_response


def read_refresh_token(name):
    try:
        with open(directory_tokens + name + '.txt', mode='r') as file:
            return Token(file.read())
    except FileNotFoundError:
        error('Token not found for ' + name)
        return InvalidToken()


def save_refresh_token(name, token):
    with open(directory_tokens + name + '.txt', mode='w') as file:
        file.write(token)


USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/59.0.3071.86 Safari/537.36")


def authorize_mfa(mfa_token):
    response = requests.post(
        n26_url + "/api/mfa/challenge",
        json={"challengeType": "oob", "mfaToken": mfa_token},
        headers={**n26_auth_header, "User-Agent": USER_AGENT, "Content-Type": "application/json"})
    response.raise_for_status()


def get_mfa_token(name):
    values_token = {"grant_type": 'password', "username": login_N26[name].username,
                    "password": login_N26[name].password}
    response = requests.post(n26_auth_url, data=values_token, headers=n26_auth_header)
    if response.status_code == 429:
        error('Too many login attempts')
        raise ValueError("impossible to get MFA token")
    if response.status_code != 403:
        error('unexpected response ' + str(response.status_code))
        raise ValueError("get_initial_2fa_token no mfa token - " + name)
    response_data = response.json()
    if response_data.get("error", "") == "mfa_required":
        return response_data["mfaToken"]
    else:
        raise ValueError("get_initial_2fa_token no mfa token - " + name)


def complete_2FA_authentication(name, mfa_token):
    mfa_response_data = {"grant_type": "mfa_oob", "mfaToken": mfa_token}
    response = requests.post(n26_auth_url, data=mfa_response_data, headers=n26_auth_header)
    print('#complete_2FA_authentication')
    print(response)
    print(response.content)
    response.raise_for_status()
    return N26_Token(response.json(), name)


def get_2fa_token(name):
    try:
        mfa_token = get_mfa_token(name)
        authorize_mfa(mfa_token)
        sleep(30)
        token = complete_2FA_authentication(name, mfa_token)
        token.save()
        return token
    except ValueError as e:
        error(e)
        return EmptyN26Token()
    except requests.HTTPError:
        return EmptyN26Token()


def setup_2fa_for_all_accounts():
    for name in login_N26:
        access_token = get_2fa_token(name)
        if access_token.is_invalid():
            raise TwoFactorsAuthError('TwoFactorsAuthError for ' + name)
    resume_scheduler()


def refresh_all_tokens():
    for name in login_N26:
        get_token(name)


def get_latest_n26(name, limit=n26_fetched_transactions):
    token = get_token(name)
    if token.is_invalid:
        error('#get_latest_n26 Invalid N26 token : ' + name)
        raise ValueError('Invalid N26 token')

    headers = {'Authorization': 'bearer' + str(token.access_token)}
    response = requests.get(n26_url + '/api/smrt/transactions?limit=' + str(limit), headers=headers)
    print(response.content)
    if response.status_code == 401:
        raise ValueError('Invalid token to get n26 data')
    if response.status_code == 429:
        raise ValueError('Too many log-in attempts')
    return format_n26_df(list_of_dict_to_df(response.json()), name)


def get_access_token_from_refresh_token(name):
    refresh_token = 'd49e5647-2dc1-406a-ba16-d464aefe6028'
    refresh_url = n26_url + '/oauth/token'

    values_token = {"grant_type": "refresh_token", 'refresh_token': refresh_token
        , 'username': login_N26[name].username, 'password': login_N26[name].password}
    headers_token = {'Authorization': 'Basic bXktdHJ1c3RlZC13ZHBDbGllbnQ6c2VjcmV0'}
    response = json.loads(requests.post(refresh_url, data=values_token, headers=headers_token).content)
    try:
        refresh_token = response['refresh_token']
        access_token = response['access_token']
        save_refresh_token(name, refresh_token)
        return access_token
    except KeyError:
        error('Refresh token failed for ' + name)
        info(response)
        return ''


def old_get_latest_n26(name, limit=n26_fetched_transactions):
    access_token = get_access_token_from_refresh_token(name)
    if access_token == '':
        error('#get_latest_n26 Invalid N26 token : ' + name)
        return failure_response('Invalid N26 token'), empty_data_dataframe()

    headers = {'Authorization': 'bearer' + str(access_token)}
    req_transactions = requests.get(n26_url + '/api/smrt/transactions?limit=' + str(limit), headers=headers)

    data = req_transactions.json()
    print(data)
    return success_response(), format_n26_df(list_of_dict_to_df(data), name)