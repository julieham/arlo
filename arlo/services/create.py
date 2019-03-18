from operations.data_operations import missing_valid_amount, missing_mandatory_field
from operations.types_operations import dict_to_df_with_all_columns
from read_write.file_manager import add_new_data
from tools.autofill_manager import _add_name_references
from tools.recurring_manager import fill_missing_with_default_values
from tools.uniform_data_maker import format_manual_transaction, format_recurring_transaction
from web.status import my_response, success_response, is_successful


def create_manual_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)

    valid_response = is_valid_transaction_df(df)
    if is_successful(valid_response):
        format_manual_transaction(df)
        add_new_data(df)
    return valid_response


def create_recurring_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)
    fill_missing_with_default_values(df)

    valid_response = is_valid_transaction_df(df)
    if is_successful(valid_response):
        format_recurring_transaction(df)
        add_new_data(df)
    return valid_response


def is_valid_transaction_df(df):
    if missing_valid_amount(df):
        return my_response(False, 'no valid amount')

    if missing_mandatory_field(df):
        return my_response(False, 'missing mandatory field')

    return success_response()


def create_name_references_if_possible(bank_name, name, category):
    if _not_possible_to_add_name_references(bank_name, name, category):
        return my_response(False, 'nothing to add')
    return _add_name_references(bank_name, name, category)


def _not_possible_to_add_name_references(bank_name, name, category):
    return name is None or (bank_name, category) is (None, None)
