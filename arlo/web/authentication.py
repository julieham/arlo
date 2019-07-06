from flask import make_response, jsonify
from flask_httpauth import HTTPTokenAuth
from flask_restful import Resource

from operations.date_operations import minutes_since
from operations.df_operations import apply_function_to_field_overrule, filter_df_one_value, get_one_field
from parameters.param import data_directory, minutes_valid_token
from read_write.reader import read_df_file

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
    token_data = read_df_file(data_directory + 'tokens.csv', parse_dates=['issue_date'])
    apply_function_to_field_overrule(token_data, 'issue_date', token_is_still_valid, destination='is_valid')
    return set(get_one_field(filter_df_one_value(token_data, 'is_valid', True), 'token'))


class ResourceWithAuth(Resource):
    decorators = [auth.login_required]
