import pandas as pd

from arlo.operations.types_operations import df_field_to_numeric_with_sign
from arlo.parameters.param import data_columns_mandatory_fields
from arlo.read_write.file_manager import read_data, read_deposit_input
from operations.df_operations import filter_df_on_id, get_one_field, filter_df_one_value
from operations.series_operations import get_first_value_from_series
from parameters.column_names import *


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
    return df[df[id_col].isin(present_ids) == False]


def missing_valid_amount(df):
    try:
        valid_amount = pd.isnull(df[amount_euro_col]) == False
        # valid_original = (pd.isnull(df['originalAmount']) == False) & (pd.isnull(df['originalCurrency']) == False)
        # amounts = pd.DataFrame({'valid_amount': valid_amount, 'valid_original': valid_original})
        return not valid_amount.any(axis=None)  # amounts.any(axis=None)
    except KeyError:
        return True


def missing_mandatory_field(df):
    try:
        missing_fields = pd.DataFrame({field: pd.isnull(df[field]) for field in data_columns_mandatory_fields})
        return missing_fields.any(axis=None)
    except KeyError:
        return True


def get_bank_name_from_id(id):
    transaction = filter_df_on_id(read_data(), id)
    return get_first_value_from_series(get_one_field(transaction, bank_name_col))


def get_deposit_name_from_provision_id(id):
    transaction = filter_df_on_id(read_deposit_input(), id)
    return get_first_value_from_series(get_one_field(transaction, deposit_name_col))
