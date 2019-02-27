from numpy import NaN

from arlo.format.data_operations import set_amounts_to_numeric
from arlo.tools.autofillManager import add_new_column_autofilled
from arlo.format.date_operations import date_to_cycle, get_timestamp_now, timestamp_to_datetime, \
    string_to_datetime, angular_string_to_timestamp
from arlo.format.df_operations import drop_other_columns, add_prefix_to_column, remove_invalid_ids, \
    apply_function_to_field_overrule, add_field_with_default_value, sort_df_by_descending_date, \
    apply_function_to_field_no_overrule
from arlo.format.formatting import make_bank_name, remove_original_amount_when_euro, remove_original_currency_when_euro
from arlo.format.types_operations import string_is_AA, encode_id
from arlo.parameters.param import lunchr_dictionary, lunchr_fields, column_names, lunchr_id_prefix, lunchr_account_name, \
    default_values


def create_id(df):
    df['id'] = df['name']
    df['id'] += '*' + (1000000*df['timestamp']).astype(int).astype(str)
    df['id'] += '*' + (100*df['amount'].fillna('0').astype(float)).astype(int).astype(str)
    df['id'] += '*' + df['account']
    apply_function_to_field_overrule(df, 'id', encode_id)


def fill_with_default_values(df):
    for field_name in default_values:
        df[field_name].fillna(default_values[field_name], inplace=True)


def format_lunchr_df(lunchr_df):
    lunchr_df.rename(columns=lunchr_dictionary, inplace=True)
    drop_other_columns(lunchr_df, lunchr_fields)
    add_prefix_to_column(lunchr_df, lunchr_id_prefix, 'id')

    remove_invalid_ids(lunchr_df)

    apply_function_to_field_overrule(lunchr_df, 'date', string_to_datetime)
    apply_function_to_field_overrule(lunchr_df, 'date', date_to_cycle, destination='cycle')
    apply_function_to_field_overrule(lunchr_df, 'bank_name', str.upper)

    add_field_with_default_value(lunchr_df, 'account', lunchr_account_name)
    add_field_with_default_value(lunchr_df, 'comment', '-')
    add_field_with_default_value(lunchr_df, 'link', '-')
    add_field_with_default_value(lunchr_df, 'pending', False)
    add_field_with_default_value(lunchr_df, "originalAmount", '')
    add_field_with_default_value(lunchr_df, 'originalCurrency', '')

    add_new_column_autofilled(lunchr_df, 'bank_name', 'name', star_fill=True)
    add_new_column_autofilled(lunchr_df, 'name', 'category')
    add_new_column_autofilled(lunchr_df, 'lunchr_type', 'type', star_fill=True)

    sort_df_by_descending_date(lunchr_df)
    drop_other_columns(lunchr_df, column_names)


def format_n26_df(df, account):
    df['bank_name'] = df.replace(NaN, '').apply(lambda row: make_bank_name(row), axis=1)
    df['originalAmount'] = df.apply(lambda row: remove_original_amount_when_euro(row), axis=1)
    df['originalCurrency'] = df.apply(lambda row: remove_original_currency_when_euro(row), axis=1)

    df['account'] = account

    apply_function_to_field_overrule(df, 'visibleTS', timestamp_to_datetime, destination='date')
    apply_function_to_field_overrule(df, 'type', string_is_AA, destination='pending')
    apply_function_to_field_overrule(df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(df, 'comment', '-')
    add_field_with_default_value(df, 'link', '-')

    add_new_column_autofilled(df, 'bank_name', 'name', star_fill=True)
    add_new_column_autofilled(df, 'name', 'category')

    return df


def format_manual_transaction(df):
    apply_function_to_field_overrule(df, 'angular_date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(df, 'timestamp', timestamp_to_datetime, destination='date')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')

    fill_with_default_values(df)

    add_new_column_autofilled(df, 'account', 'type')
    add_new_column_autofilled(df, 'name', 'category')

    set_amounts_to_numeric(df, (df["isCredit"] == "true").all())

    create_id(df)
    drop_other_columns(df, column_names)


def format_recurring_transaction(df):
    apply_function_to_field_overrule(df, 'date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(df, 'timestamp', timestamp_to_datetime, destination='date')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')
    fill_with_default_values(df)

    add_new_column_autofilled(df, 'account', 'type')
    add_new_column_autofilled(df, 'name', 'category')

    set_amounts_to_numeric(df, is_positive=False)
    add_prefix_to_column(df, 'rec', 'type')

    create_id(df)
    drop_other_columns(df, column_names)
