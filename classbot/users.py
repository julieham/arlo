import json

import pandas as pd
import requests

from parameters.credentials import classpass_login
from parameters.param import classpass_url, classbot_directory


def get_users():
    return list(classpass_login.keys())


def get_json_login_data(name):
    try:
        data = classpass_login[name]['login']
    except KeyError:
        raise ValueError('name not found')
    data['client_id'] = "82ec597a-ad9d-11e4-bf55-22000b44012e"
    data = json.dumps(data)
    return data


def get_user_id(name):
    try:
        return classpass_login[name]['user_id']
    except KeyError:
        raise ValueError('name not found')


def _request_token(name):
    return json.loads(requests.post(classpass_url + '/v2/auth/login', data=get_json_login_data(name)).content)['token']


def make_header_token(token):
    return {'CP-Authorization': "Token " + token}


token_file = classbot_directory + 'tokens.csv'


def get_tokens():
    return pd.read_csv(token_file, parse_dates=['issue_date'])


def get_new_token(tokens, name):
    token = _request_token(name)
    time_now = pd.datetime.now()
    tokens = tokens.append({'name': name, 'token': token, 'issue_date': time_now}, ignore_index=True)
    tokens.to_csv(token_file, index=False)
    return token


def get_token(name):
    tokens = get_tokens()
    tokens = tokens[tokens['issue_date'] > pd.datetime.now() - pd.Timedelta('55 min')]
    valid_tokens = tokens[tokens['name'] == name]
    if valid_tokens.shape[0] > 0:
        return valid_tokens['token'].iloc[0]
    return get_new_token(tokens, name)
