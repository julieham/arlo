import secrets

import pandas as pd
from bcrypt import checkpw
from flask import make_response, jsonify
from flask_httpauth import HTTPTokenAuth
from flask_restful import Resource

from arlo.operations.date_operations import minutes_since, now
from arlo.operations.df_operations import apply_function_to_field_overrule, filter_df_one_value, get_one_field, \
    df_is_empty
from arlo.parameters.column_names import token_col, token_issue_date_col, token_user_col
from arlo.parameters.credentials import arlo_logins, arlo_admins
from arlo.parameters.param import minutes_valid_token
from arlo.read_write.file_manager import tokens_file
from arlo.read_write.reader import read_df_file
from arlo.read_write.writer import write_df_to_csv

auth = HTTPTokenAuth(scheme='Token')


@auth.verify_token
def verify_token(token):
    valid_tokens = get_valid_token()
    return token in valid_tokens


@auth.error_handler
def unauthorized():
    return make_response(jsonify({'message': 'Unauthorized access'}))


def token_is_still_valid(date):
    return minutes_since(date) < minutes_valid_token


def _get_all_valid_tokens():
    token_data = read_df_file(tokens_file, parse_dates=[token_issue_date_col])
    if df_is_empty(token_data):
        return set()
    apply_function_to_field_overrule(token_data, 'issue_date', token_is_still_valid, destination='is_valid')
    return filter_df_one_value(token_data, 'is_valid', True)


def get_valid_token():
    return set(get_one_field(_get_all_valid_tokens(), token_col))


def generate_new_token(user):
    token_data = read_df_file(tokens_file, parse_dates=[token_issue_date_col])
    new_token = secrets.token_hex(20)
    write_df_to_csv(token_data.append(
        pd.DataFrame([[new_token, now(), user]], columns=[token_col, token_issue_date_col, token_user_col])),
                    tokens_file, index=False)
    return new_token


def login_is_valid(user, password):
    if user not in arlo_logins.keys():
        print('user not found in user_list')
        return False
    print('checking password')
    return checkpw(password.encode(), arlo_logins[user].encode())


def token_is_admin(token):
    valid_tokens = _get_all_valid_tokens()
    return valid_tokens[(valid_tokens['token'] == token) & (valid_tokens['user'].isin(arlo_admins))].shape[0] > 0


class ResourceWithAuth(Resource):
    decorators = [auth.login_required]


def admin_required(f):
    def decorated(*args, **kwargs):
        if token_is_admin(auth.get_auth()['token']):
            return f(*args, **kwargs)
        else:
            return auth.auth_error_callback()

    return decorated


class ResourceWithAuthAdmin(Resource):
    decorators = [auth.login_required, admin_required]
