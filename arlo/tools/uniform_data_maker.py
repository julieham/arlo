import pandas as pd
from numpy import NaN

from arlo.operations.data_operations import set_amounts_to_numeric
from arlo.operations.date_operations import timestamp_to_datetime, string_to_datetime, angular_string_to_timestamp, \
    datetime_to_timestamp, short_string_to_datetime
from arlo.operations.df_operations import drop_other_columns, remove_invalid_ids, \
    apply_function_to_field_overrule, add_field_with_default_value, sort_df_by_descending_date, \
    apply_function_to_field_no_overrule, disable_chained_assignment_warning, \
    enable_chained_assignment_warning, assign_value_to_empty_in_existing_column, both_series_are_true, \
    assign_value_to_loc
from arlo.operations.formatting import make_bank_name
from arlo.operations.types_operations import encode_id, clean_parenthesis
from arlo.parameters.param import *
from arlo.tools.autofill_df import add_new_column_autofilled, fill_existing_column_with_autofill
from arlo.tools.cycle_manager import date_to_cycle
from operations.df_operations import get_one_field, filter_df_on_bools, reverse_amount, add_prefix_to_column, \
    assign_new_column, concat_lines
from parameters.column_names import type_trans_col, category_col, deposit_name_col
from read_write.file_manager import default_value


def create_id(df):
    disable_chained_assignment_warning()
    if 'timestamp' not in df.columns:
        apply_function_to_field_overrule(df, date_col, datetime_to_timestamp, destination='timestamp')
    df[id_col] = df[name_col]
    df[id_col] += '*' + (1000000 * df['timestamp']).astype(int).astype(str)
    df[id_col] += '*' + (100 * df[amount_euro_col].fillna('0').astype(float)).astype(int).astype(str)
    df[id_col] += '*' + df[account_col]
    apply_function_to_field_overrule(df, id_col, encode_id)
    enable_chained_assignment_warning()


def fill_columns_with_default_values(df):
    for field_name in default_values:
        assign_value_to_empty_in_existing_column(df, field_name, default_values[field_name])


def add_missing_data_columns(df):
    for column_name in set(data_columns_all) - set(df.columns):
        df.insert(0, column_name, NaN)


def add_missing_deposit_columns(df):
    for column_name in set(deposit_columns_all) - set(df.columns):
        df.insert(0, column_name, NaN)


def lunchr_credit_card_amount(details):
    if type(details) != list:
        return 0
    return sum(sub_transaction['amount'] * (sub_transaction['type'] == 'CREDIT_CARD') for sub_transaction in details)


def format_lunchr_df(lunchr_df):
    lunchr_df.rename(columns=lunchr_dictionary, inplace=True)

    apply_function_to_field_overrule(lunchr_df, 'lunchr_details', lunchr_credit_card_amount,
                                     destination='credit_card_amount')
    add_missing_data_columns(lunchr_df)

    add_prefix_to_column(lunchr_df, lunchr_id_prefix, id_col)

    remove_invalid_ids(lunchr_df)

    apply_function_to_field_overrule(lunchr_df, date_col, string_to_datetime)
    apply_function_to_field_overrule(lunchr_df, date_col, date_to_cycle, destination=cycle_col)
    apply_function_to_field_overrule(lunchr_df, bank_name_col, str.upper)

    fill_columns_with_default_values(lunchr_df)

    add_field_with_default_value(lunchr_df, account_col, lunchr_account_name)
    add_field_with_default_value(lunchr_df, amount_orig_col, '')
    add_field_with_default_value(lunchr_df, currency_orig_col, '')

    add_new_column_autofilled(lunchr_df, bank_name_col, name_col, star_fill=True)
    add_new_column_autofilled(lunchr_df, name_col, category_col)
    add_new_column_autofilled(lunchr_df, 'lunchr_type', 'type', star_fill=True)

    sort_df_by_descending_date(lunchr_df)
    drop_other_columns(lunchr_df, data_columns_all + ['credit_card_amount'])


def format_n26_df(n26_df, account):
    n26_df[bank_name_col] = n26_df.replace(NaN, '').apply(lambda row: make_bank_name(row), axis=1)

    _remove_original_amount_when_euro(n26_df)

    add_field_with_default_value(n26_df, account_col, account)
    add_missing_data_columns(n26_df)

    apply_function_to_field_overrule(n26_df, 'visibleTS', timestamp_to_datetime, destination=date_col)
    apply_function_to_field_overrule(n26_df, date_col, date_to_cycle, destination=cycle_col)

    fill_columns_with_default_values(n26_df)

    add_new_column_autofilled(n26_df, bank_name_col, name_col, star_fill=True)
    add_new_column_autofilled(n26_df, name_col, category_col)
    add_new_column_autofilled(n26_df, name_col, deposit_name_col, default_value=NaN)

    return n26_df


def format_manual_transaction(man_df):
    add_missing_data_columns(man_df)

    apply_function_to_field_overrule(man_df, 'angular_date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(man_df, 'timestamp', timestamp_to_datetime, destination=date_col)
    apply_function_to_field_no_overrule(man_df, date_col, date_to_cycle, destination=cycle_col)

    fill_existing_column_with_autofill(man_df, account_col, 'type')
    fill_existing_column_with_autofill(man_df, name_col, category_col)

    set_amounts_to_numeric(man_df, (man_df["isCredit"] == "true").all())

    fill_columns_with_default_values(man_df)
    create_id(man_df)

    drop_other_columns(man_df, data_columns_all)


def format_recurring_transaction(rec_df):
    add_missing_data_columns(rec_df)
    apply_function_to_field_overrule(rec_df, date_col, angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(rec_df, 'timestamp', timestamp_to_datetime, destination=date_col)
    apply_function_to_field_no_overrule(rec_df, date_col, date_to_cycle, destination=cycle_col)

    fill_existing_column_with_autofill(rec_df, account_col, type_trans_col)
    fill_existing_column_with_autofill(rec_df, name_col, category_col)

    set_amounts_to_numeric(rec_df, is_positive=False)
    add_prefix_to_column(rec_df, 'rec', type_trans_col)

    fill_columns_with_default_values(rec_df)

    create_id(rec_df)
    drop_other_columns(rec_df, data_columns_all)


def format_deposit_df(dep_df):
    add_missing_deposit_columns(dep_df)

    add_field_with_default_value(dep_df, account_col, deposit_account)
    apply_function_to_field_overrule(dep_df, 'angular_date', angular_string_to_timestamp, destination='timestamp')
    apply_function_to_field_overrule(dep_df, 'timestamp', timestamp_to_datetime, destination=date_col)
    apply_function_to_field_no_overrule(dep_df, date_col, date_to_cycle, destination=cycle_col)
    apply_function_to_field_overrule(dep_df, name_col, clean_parenthesis, destination=deposit_name_col)

    fill_existing_column_with_autofill(dep_df, deposit_name_col, category_col, default_value='Deposit')

    create_id(dep_df)
    return dep_df[deposit_columns_all]


def format_for_front(data):
    add_new_column_autofilled(data, type_trans_col, payment_method_col)
    add_linked_column(data)
    add_pending_column(data)
    add_manual_column(data)


def _remove_original_amount_when_euro(df):
    original_amount_is_euro = get_one_field(df, currency_orig_col) == 'EUR'
    assign_value_to_loc(df, original_amount_is_euro, currency_orig_col, NaN)
    assign_value_to_loc(df, original_amount_is_euro, amount_orig_col, NaN)


def _calculate_pending_column(data):
    return both_series_are_true(data[type_trans_col] == 'AA', data[link_id_col] == default_value(link_id_col))


def _calculate_manual_column(data):
    return data[account_col].isin(auto_accounts) == False


def _calculate_refund_column(data):
    return both_series_are_true(data[type_trans_col].isin(['AE', 'AV']),
                                data[link_id_col] == default_value(link_id_col))


def add_pending_column(data):
    assign_new_column(data, pending_col, _calculate_pending_column(data))


def add_linked_column(data):
    assign_new_column(data, is_linked_col, data[link_id_col] != '-')


def add_manual_column(data):
    assign_new_column(data, is_manual_col, _calculate_manual_column(data))


def add_refund_column(data):
    assign_new_column(data, 'refund', _calculate_refund_column(data))


def turn_deposit_data_into_df(deposit_data):
    how_many = len(deposit_data) // 3
    actives = [False for _ in range(how_many)]
    amounts = [0 for _ in range(how_many)]
    names = ['' for _ in range(how_many)]
    ang_date = deposit_data['angular_date'] if deposit_data['angular_date'] else None
    for key, item in deposit_data.items():
        column_name = key.split('_')[0]
        if column_name in [name_col, amount_euro_col, 'active']:
            key_id = int(key.split('_')[1])
            if key[0] == 'n':
                names[key_id] = item if item else ""
            elif key[:2] == 'ac':
                actives[key_id] = item
            else:
                amounts[key_id] = item
    deposit_df = pd.DataFrame({amount_euro_col: amounts, name_col: names, 'angular_date': ang_date})
    return deposit_df[actives]


def bankin_name_to_type(name):
    name = name.split(' ')
    if name[0] == 'Virement':
        return 'VIR'
    if name[0] in ['Prlv', 'CB', 'Vir']:
        return name[0].upper()
    if name[0] == 'Retrait':
        return 'CASH_W'
    if name[0] == 'Rembourst':
        return 'RBST'
    return 'MISC'


def clean_bankin_name(name):
    expressions = ['Virement Faveur Tiers Vr. Permanent ', 'Virement Recu Tiers ', 'Retrait Dab ']
    for expression in expressions:
        if name[:len(expression)] == expression:
            return name.replace(expression, '')

    expression = 'Remise Cheques'
    if name[:len(expression)] == expression:
        return expression.strip()

    if 'Vir' == name[:3]:
        virement = name.split('/')
        if len(virement) < 3:
            return virement[0].strip()
        motif, origin = (virement[1], virement[2]) if virement[1][:5].lower() == 'motif' else (virement[2], virement[1])
        name = motif[6:].strip() + ' (' + origin.replace('de ', '').strip() + ')'
    else:
        expressions = ['Prlv Sepa ', 'CB ', ' Id Emetteur', ' Id', ' Paris', 'Rembourst Du ', 'Cart']
        for exp in expressions:
            name = name.replace(exp, '')

    return name.strip()


def format_bankin_df(bankin_df):
    bankin_df.rename(columns=bankin_dictionary, inplace=True)

    apply_function_to_field_overrule(bankin_df, bank_name_col, bankin_name_to_type, destination='type')

    apply_function_to_field_overrule(bankin_df, bank_name_col, clean_bankin_name)
    add_missing_data_columns(bankin_df)

    add_prefix_to_column(bankin_df, bankin_id_prefix, id_col)

    remove_invalid_ids(bankin_df)

    apply_function_to_field_overrule(bankin_df, date_col, short_string_to_datetime)
    apply_function_to_field_overrule(bankin_df, date_col, date_to_cycle, destination=cycle_col)
    apply_function_to_field_overrule(bankin_df, bank_name_col, str.upper)

    fill_columns_with_default_values(bankin_df)

    add_field_with_default_value(bankin_df, account_col, bankin_acc_name)
    add_field_with_default_value(bankin_df, amount_orig_col, '')
    add_field_with_default_value(bankin_df, currency_orig_col, '')

    add_new_column_autofilled(bankin_df, bank_name_col, name_col, star_fill=True)
    add_new_column_autofilled(bankin_df, name_col, category_col)

    sort_df_by_descending_date(bankin_df)
    drop_other_columns(bankin_df, data_columns_all)


def process_lunchr_cb_transaction(lunchr_df):
    is_split = get_one_field(lunchr_df, 'credit_card_amount') < 0
    split_transactions = filter_df_on_bools(lunchr_df, is_split)
    actual_transactions = filter_df_on_bools(lunchr_df, is_split)
    regular_transactions = filter_df_on_bools(lunchr_df, is_split, keep=False)

    refunds_transactions = filter_df_on_bools(lunchr_df, is_split)

    reverse_amount(refunds_transactions)

    add_prefix_to_column(split_transactions, 'FIC-', type_trans_col)
    add_prefix_to_column(split_transactions, 'TOTAL-', 'id')
    add_prefix_to_column(actual_transactions, 'REAL-', 'id')
    add_prefix_to_column(refunds_transactions, 'REF-', 'id')

    assign_new_column(refunds_transactions, 'type', 'TOTAL-FIC-')
    assign_new_column(actual_transactions, 'amount',
                      split_transactions['amount'] - split_transactions['credit_card_amount'])
    assign_new_column(refunds_transactions, category_col, 'Lunchr Buffer')
    assign_new_column(refunds_transactions, deposit_name_col, 'Lunchr Buffer')
    assign_new_column(actual_transactions, category_col, 'Lunchr Buffer')
    assign_new_column(actual_transactions, deposit_name_col, 'Lunchr Buffer')

    return concat_lines([refunds_transactions, split_transactions, actual_transactions, regular_transactions])
