from arlo.operations.df_operations import filter_df_on_id, filter_df_one_value, new_dataframe, not_series, total_amount, \
    apply_function_to_field_overrule, \
    add_column_with_value
from arlo.operations.series_operations import get_first_value_from_series
from arlo.operations.types_operations import df_field_to_numeric_with_sign, value_is_nan
from arlo.parameters.column_names import *
from arlo.parameters.param import data_columns_mandatory_fields
from arlo.read_write.file_manager import read_data, read_deposit_input
from operations.df_operations import assign_new_column, get_one_field, series_is_null, assign_value_to_bool_rows, \
    drop_columns, rename_columns
from parameters.column_names import amount_orig_col, currency_orig_col, amount_euro_col, currency_col
from parameters.param import default_currency
from tools.clean_currency_converter import latest_rate_from_euro
from tools.logging import info


def set_amounts_to_numeric(df, is_positive=True):
    fields = [amount_euro_col, amount_orig_col]

    for field in fields:
        df_field_to_numeric_with_sign(df, field, is_positive)


def remove_already_present_id(df, account, limit=None):
    data = read_data()
    data_account = filter_df_one_value(data, account_col, account)
    if limit:
        data_account = data_account.head(limit)
    present_ids = data_account[id_col]
    return df[not_series(df[id_col].isin(present_ids))]


def missing_valid_amount(df):
    try:
        valid_amount = not_series(series_is_null(df[amount_euro_col]))
        # valid_original = not_series(series_is_null(get_one_field(df, amount_orig_col))) & not_series(series_is_null(get_one_field(df, currency_orig_col)))
        # amounts = new_dataframe({'valid_amount': valid_amount, 'valid_original': valid_original})
        return not valid_amount.any(axis=None)  # amounts.any(axis=None)
    except KeyError:
        return True


def missing_mandatory_field(df):
    try:
        missing_fields = new_dataframe({field: series_is_null(df[field]) for field in data_columns_mandatory_fields})
        return missing_fields.any(axis=None)
    except KeyError:
        return True


def get_bank_name_from_id(id):
    transaction = filter_df_on_id(read_data(), id)
    return get_first_value_from_series(get_one_field(transaction, bank_name_col))


def get_deposit_name_from_provision_id(id):
    transaction = filter_df_on_id(read_deposit_input(), id)
    return get_first_value_from_series(get_one_field(transaction, deposit_name_col))


def convert_non_euro_amounts(data):
    if value_is_nan(total_amount(data)):
        info('#Recap converting currencies with current rate')
        exchange_rate_col = 'exchange_rate'
        process_currency_data(data)
        apply_function_to_field_overrule(data, currency_col, latest_rate_from_euro, destination=exchange_rate_col)
        assign_new_column(data, 'amount', get_one_field(data, amount_euro_col) / get_one_field(data, exchange_rate_col))
        drop_columns(data, [exchange_rate_col])
    add_column_with_value(data, currency_col, default_currency)


def process_currency_data(data):
    temp_amount_col, temp_currency_col = 'my_amount', 'my_currency'
    assign_new_column(data, temp_amount_col, get_one_field(data, amount_orig_col))
    assign_new_column(data, temp_currency_col, get_one_field(data, currency_orig_col))

    has_euro_amount = series_is_null(get_one_field(data, amount_euro_col)) == False

    assign_value_to_bool_rows(data, has_euro_amount, temp_amount_col, get_one_field(data, amount_euro_col))
    assign_value_to_bool_rows(data, has_euro_amount, temp_currency_col, default_currency)
    drop_columns(data, [amount_euro_col, amount_orig_col, currency_orig_col])
    rename_columns(data, {temp_amount_col: amount_euro_col, temp_currency_col: currency_col})
