from numpy import NaN

from arlo.format.autofill import add_autofilled_column
from arlo.format.date_operations import lunchr_date_to_datetime, date_to_cycle, get_timestamp_now, timestamp_to_datetime
from arlo.format.df_operations import drop_other_columns, add_prefix_to_column, remove_invalid_ids, \
    apply_function_to_field_overrule, add_field_with_default_value, sort_df_by_descending_date, \
    apply_function_to_field_no_overrule
from arlo.format.formatting import make_bank_name, remove_original_amount_when_euro, remove_original_currency_when_euro
from arlo.format.types_operations import string_is_AA
from arlo.parameters.param import lunchr_dictionary, lunchr_fields, column_names, lunchr_id_prefix, lunchr_account_name


def format_lunchr_df(lunchr_df):
    lunchr_df.rename(columns=lunchr_dictionary, inplace=True)
    drop_other_columns(lunchr_df, lunchr_fields)
    add_prefix_to_column(lunchr_df, lunchr_id_prefix, 'id')

    remove_invalid_ids(lunchr_df)

    apply_function_to_field_overrule(lunchr_df, 'date', lunchr_date_to_datetime)
    apply_function_to_field_overrule(lunchr_df, 'date', date_to_cycle, destination='cycle')
    apply_function_to_field_overrule(lunchr_df, 'bank_name', str.upper)

    add_field_with_default_value(lunchr_df, 'account', lunchr_account_name)
    add_field_with_default_value(lunchr_df, 'comment', '-')
    add_field_with_default_value(lunchr_df, 'link', '-')
    add_field_with_default_value(lunchr_df, 'pending', False)
    add_field_with_default_value(lunchr_df, "originalAmount", '')
    add_field_with_default_value(lunchr_df, 'originalCurrency', '')

    add_autofilled_column(lunchr_df, 'bank_name', 'name', star_fill=True)
    add_autofilled_column(lunchr_df, 'name', 'category')
    add_autofilled_column(lunchr_df, 'lunchr_type', 'type', star_fill=True)

    sort_df_by_descending_date(lunchr_df)
    drop_other_columns(lunchr_df, column_names)


def dataframe_formatter(df, account):
    if account.endswith('N26'):
        df['bank_name'] = df.replace(NaN, '').apply(lambda row: make_bank_name(row), axis=1)
        df['originalAmount'] = df.apply(lambda row: remove_original_amount_when_euro(row), axis=1)
        df['originalCurrency'] = df.apply(lambda row: remove_original_currency_when_euro(row), axis=1)

    df['account'] = account

    calculate_universal_fields(df)

    add_autofilled_column(df, 'bank_name', 'name', star_fill=True)
    add_autofilled_column(df, 'name', 'category')

    return df


def calculate_universal_fields(df):

    if 'visibleTS' in df.columns:
        apply_function_to_field_no_overrule(df, 'visibleTS', timestamp_to_datetime, destination='date')
    elif 'date' not in df.columns:
        add_field_with_default_value(df, 'visibleTS', get_timestamp_now())

    apply_function_to_field_no_overrule(df, 'type', string_is_AA, destination='pending')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(df, 'comment', '-')
    add_field_with_default_value(df, 'link', '-')
