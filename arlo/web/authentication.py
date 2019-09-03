import secrets

import pandas as pd
from bcrypt import checkpw
from flask import make_response, jsonify
from flask_httpauth import HTTPTokenAuth
from flask_restful import Resource

from arlo.operations.date_operations import minutes_since, now
from arlo.operations.df_operations import apply_function_to_field_overrule, filter_df_one_value, get_one_field, \
    df_is_empty
from arlo.parameters.column_names import token_col, token_issue_date_col
from arlo.parameters.credentials import arlo_user, arlo_password
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


def get_valid_token():
    token_data = read_df_file(tokens_file, parse_dates=[token_issue_date_col])
    if df_is_empty(token_data):
        return set()
    apply_function_to_field_overrule(token_data, 'issue_date', token_is_still_valid, destination='is_valid')
    return set(get_one_field(filter_df_one_value(token_data, 'is_valid', True), token_col))


def generate_new_token():
    token_data = read_df_file(tokens_file, parse_dates=[token_issue_date_col])
    new_token = secrets.token_hex(20)
    write_df_to_csv(token_data.append(pd.DataFrame([[new_token, now()]], columns=[token_col, token_issue_date_col])), tokens_file, index=False)
    return new_token


def login_is_valid(user, password):
    return user == arlo_user and checkpw(password.encode(),arlo_password.encode())


class ResourceWithAuth(Resource):
    decorators = [auth.login_required]
