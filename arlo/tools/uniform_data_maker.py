from numpy import NaN

from arlo.operations.data_operations import set_amounts_to_numeric
from arlo.operations.date_operations import timestamp_to_datetime, \
    string_to_datetime, angular_string_to_timestamp
from arlo.operations.df_operations import drop_other_columns, add_prefix_to_column, remove_invalid_ids, \
    apply_function_to_field_overrule, add_field_with_default_value, sort_df_by_descending_date, \
    apply_function_to_field_no_overrule, concat_columns, assign_new_column, disable_chained_assigment_warning, \
    enable_chained_assigment_warning, assign_value_to_empty_in_existing_column
from arlo.operations.formatting import make_bank_name, remove_original_amount_when_euro, \
    remove_original_currency_when_euro
from arlo.operations.types_operations import encode_id
from arlo.parameters.param import lunchr_dictionary, lunchr_fields, lunchr_id_prefix, \
    lunchr_account_name, default_values, column_names_stored
from arlo.tools.autofill_df import add_new_column_autofilled, fill_existing_column_with_autofill
from arlo.tools.cycle_manager import date_to_cycle
from read_write.file_manager import default_value


def create_id(df):
    disable_chained_assigment_warning()
    df['id'] = df['name']
    df['id'] += '*' + (1000000*df['timestamp']).astype(int).astype(str)
    df['id'] += '*' + (100*df['amount'].fillna('0').astype(float)).astype(int).astype(str)
    df['id'] += '*' + df['account']
    apply_function_to_field_overrule(df, 'id', encode_id)
    enable_chained_assigment_warning()


def fill_columns_with_default_values(df):
    for field_name in default_values:
        assign_value_to_empty_in_existing_column(df, field_name, default_values[field_name])


def format_lunchr_df(lunchr_df):
    lunchr_df.rename(columns=lunchr_dictionary, inplace=True)
    drop_other_columns(lunchr_df, lunchr_fields)
    add_prefix_to_column(lunchr_df, lunchr_id_prefix, 'id')

    remove_invalid_ids(lunchr_df)

    apply_function_to_field_overrule(lunchr_df, 'date', string_to_datetime)
    apply_function_to_field_overrule(lunchr_df, 'date', date_to_cycle, destination='cycle')
    apply_function_to_field_overrule(lunchr_df, 'bank_name', str.upper)

    fill_columns_with_default_values(lunchr_df)

    add_field_with_default_value(lunchr_df, 'account', lunchr_account_name)
    add_field_with_default_value(lunchr_df, "originalAmount", '')
    add_field_with_default_value(lunchr_df, 'originalCurrency', '')

    add_new_column_autofilled(lunchr_df, 'bank_name', 'name', star_fill=True)
    add_new_column_autofilled(lunchr_df, 'name', 'category')
    add_new_column_autofilled(lunchr_df, 'lunchr_type', 'type', star_fill=True)

    sort_df_by_descending_date(lunchr_df)
    drop_other_columns(lunchr_df, column_names_stored)


def format_n26_df(n26_df, account):
    n26_df['bank_name'] = n26_df.replace(NaN, '').apply(lambda row: make_bank_name(row), axis=1)
    n26_df['originalAmount'] = n26_df.apply(lambda row: remove_original_amount_when_euro(row), axis=1)
    n26_df['originalCurrency'] = n26_df.apply(lambda row: remove_original_currency_when_euro(row), axis=1)

    n26_df['account'] = account

    apply_function_to_field_overrule(n26_df, 'visibleTS', timestamp_to_datetime, destination='date')
    apply_function_to_field_overrule(n26_df, 'date', date_to_cycle, destination='cycle')

    fill_columns_with_default_values(n26_df)

    add_new_column_autofilled(n26_df, 'bank_name', 'name', star_fill=True)
    add_new_column_autofilled(n26_df, 'name', 'category')

    return n26_df


def format_manual_transaction(man_df):

    apply_function_to_field_overrule(man_df, 'angular_date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(man_df, 'timestamp', timestamp_to_datetime, destination='date')
    apply_function_to_field_no_overrule(man_df, 'date', date_to_cycle, destination='cycle')


    fill_existing_column_with_autofill(man_df, 'account', 'type')
    fill_existing_column_with_autofill(man_df, 'name', 'category')

    set_amounts_to_numeric(man_df, (man_df["isCredit"] == "true").all())
    fill_columns_with_default_values(man_df)

    create_id(man_df)

    drop_other_columns(man_df, column_names_stored)


def format_recurring_transaction(rec_df):
    apply_function_to_field_overrule(rec_df, 'date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(rec_df, 'timestamp', timestamp_to_datetime, destination='date')
    apply_function_to_field_no_overrule(rec_df, 'date', date_to_cycle, destination='cycle')

    fill_existing_column_with_autofill(rec_df, 'account', 'type')
    fill_existing_column_with_autofill(rec_df, 'name', 'category')

    set_amounts_to_numeric(rec_df, is_positive=False)
    add_prefix_to_column(rec_df, 'rec', 'type')

    fill_columns_with_default_values(rec_df)

    create_id(rec_df)
    drop_other_columns(rec_df, column_names_stored)


def format_for_front(data):
    add_new_column_autofilled(data, 'type', 'method')
    add_linked_column(data)
    add_pending_column(data)
    add_manual_column(data)


def _calculate_pending_column(data):
    return concat_columns([data['type'] == 'AA', data['link'] == default_value('link')]).all(axis=1)


def _calculate_manual_column(data):
    return data['account'].isin(['T_N26', 'J_N26', 'lunchr']) == False


def add_pending_column(data):
    assign_new_column(data, 'pending', _calculate_pending_column(data))


def add_linked_column(data):
    assign_new_column(data, 'linked', data['link'] != '-')


def add_manual_column(data):
    assign_new_column(data, 'manual', _calculate_manual_column(data))
