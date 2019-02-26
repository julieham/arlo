from math import isnan
import pandas as pd
import json


from arlo.tools.autofillManager import add_new_column_autofilled
from arlo.format.date_operations import decode_cycle, get_timestamp_now
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.recap_by_category import get_categories_recap
from arlo.format.data_operations import set_amounts_to_numeric, missing_valid_amount, missing_mandatory_field
from arlo.format.df_operations import df_is_not_empty, change_field_on_single_id_to_value, get_one_field, \
    result_function_applied_to_field, how_many_rows, filter_df_several_values, \
    filter_df_on_cycle, change_field_on_several_ids_to_value, filter_df_not_this_value, vertical_concat
from arlo.format.formatting import parse_ids, create_id
from arlo.format.transaction_operations import fields_make_valid_transaction
from arlo.format.types_operations import dict_to_df, sorted_set, dict_add_value_if_not_present, dict_to_df_with_all_columns
from arlo.parameters.credentials import login_N26
from arlo.parameters.param import *
from arlo.read_write.fileManager import save_data, read_data, change_last_update_to_now, minutes_since_last_update, \
    set_field_to_value_on_ids, add_new_data
from arlo.tools.clean_n26 import get_last_transactions_as_df
from arlo.tools.recurringManager import has_default_fields, get_default_fields, get_possible_recurring
from arlo.tools.merge_data import merge_data
from arlo.tools.uniform_data_maker import dataframe_formatter, calculate_universal_fields, format_manual_transaction


# %% SERVICES

def refresh_data():
    print('REFRESHING ? ')
    if minutes_since_last_update() > delay_refresh_minutes:
        print('YES')
        print(force_refresh())
    else:
        print('NO')


def force_refresh():
    print('FORCE REFRESH')
    refresh_lunchr()

    all_valid, all_data = True, []
    for account in login_N26:
        valid, data = get_last_transactions_as_df(account, n26_max_transactions_per_user)
        all_valid = all_valid and valid
        all_data.append(dataframe_formatter(data, account))
    all_valid = True

    if not all_valid:
        print('REFRESH FAILED')
        return 'FAIL'

    save_data(merge_data(read_data(), vertical_concat(all_data)))
    change_last_update_to_now()

    return 'SUCCESS'


def list_data_json(refresh=None, hide_linked=True, cycle="now"):
    if refresh:
        refresh_data()

    data = read_data()
    # TODO recup link disappearing transactions

    data = filter_df_on_cycle(data, cycle)
    if hide_linked:
        data = filter_df_not_this_value(data, 'category', 'Link')

    add_new_column_autofilled(data, 'type', 'method')
    data['linked'] = data['link'] != '-'
    data = data[column_names_for_front]

    return data.to_json(orient="records")


def set_field_on_ids(transaction_ids, field_name, field_value):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message
    set_field_to_value_on_ids(transaction_ids, field_name, field_value)
    return 'SUCCESS'


def name(transaction_ids, transaction_name):
    return set_field_on_ids(transaction_ids, 'name', transaction_name)


def change_cycle(transaction_ids, cycle_value):
    return set_field_on_ids(transaction_ids, 'cycle', cycle_value)


def categorize(transaction_ids, category_value):
    return set_field_on_ids(transaction_ids, 'category', category_value)


def create_manual_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)

    if missing_valid_amount(df):
        return 'FAIL : no valid amount'

    if missing_mandatory_field(df):
        return 'FAIL : missing mandatory field'

    format_manual_transaction(df)
    add_new_data(df)

    return 'SUCCESS'


def link_ids(ids):
    error_message, ids = parse_ids(ids)

    if error_message:
        return error_message

    if len(ids) < 2:
        message = 'FAIL not enough transactions'
        print(message)
        return message

    data = read_data()
    change_field_on_several_ids_to_value(data, ids, "category", "Link")
    data_ids = filter_df_several_values(data, 'id', ids)

    if how_many_rows(data_ids) != len(ids):
        return 'FAIL at least one transaction missing'

    if result_function_applied_to_field(data_ids, 'amount', sum) != 0:
        return 'FAIL transactions do not cancel each other out'

    present_links = set(get_one_field(data_ids, 'link'))

    if present_links != {'-'}:
        return 'FAIL one or more transaction already linked'

    ids_link = ids[1:] + [ids[0]]
    for t_id, id_link in zip(ids, ids_link):
        change_field_on_single_id_to_value(data, t_id, 'link', id_link)

    save_data(data)
    return 'SUCCESS'


def get_recap_categories(cycle='now'):
    data = read_data()

    data = filter_df_on_cycle(data, cycle)

    # TODO make generic function with balances

    recap = pd.DataFrame()

    if df_is_not_empty(data):
        recap = get_categories_recap(data, cycle)

    recap = recap[recap['category'] != "Link"]

    return recap.to_json(orient="records")


def get_final_amount(row):
    if isnan(row['amount']):
        return round(row['originalAmount'], 2)
    return round(row['amount'], 2)


def get_final_currency(row):
    if isnan(row['amount']):
        return row['originalCurrency']
    return 'EUR'


def get_balances(cycle='now'):
    cycle = decode_cycle(cycle)
    data = read_data()
    data = data[['amount', 'account', 'cycle']]

    data_this_cycle = data[data['cycle'] == cycle]

    data = data.groupby('account').apply(lambda x: x.sum(skipna=False))
    data_this_cycle = data_this_cycle.groupby('account').apply(lambda x: x.sum(skipna=False))
    data["all_times"] = data["amount"]
    data_this_cycle["this_cycle"] = data_this_cycle[["amount"]]

    data = data[["all_times"]]
    data_this_cycle = data_this_cycle[["this_cycle"]]

    balances = pd.concat([data, data_this_cycle], axis=1, sort=False).fillna(0)
    balances["currency"] = "EUR"
    balances.index.names = ['account_name']

    return balances


def create_recurring_transaction(transaction_name, amount=None, account=None):
    if has_default_fields(transaction_name):
        default_fields = get_default_fields(transaction_name)
        if amount is None:
            amount = default_fields['amount']
        if account is None:
            account = default_fields['account']

    transaction_fields = dict({'name': transaction_name, 'amount': amount, 'account': account, 'type': 'REC'})

    dict_add_value_if_not_present(transaction_fields, 'originalAmount', '')
    dict_add_value_if_not_present(transaction_fields, 'originalCurrency', '')
    dict_add_value_if_not_present(transaction_fields, 'amount', '')
    dict_add_value_if_not_present(transaction_fields, 'visibleTS', 1000 * get_timestamp_now())
    dict_add_value_if_not_present(transaction_fields, 'type', 'MAN')
    dict_add_value_if_not_present(transaction_fields, 'id', create_id(transaction_fields))

    if not fields_make_valid_transaction(transaction_fields):
        return 'FAIL'

    transaction_df = dict_to_df(transaction_fields)

    set_amounts_to_numeric(transaction_df, False)

    calculate_universal_fields(transaction_df)

    if 'category' not in transaction_df.columns:
        add_new_column_autofilled(transaction_df, 'name', 'category')

    add_new_data(transaction_df[list(set(column_names) & set(transaction_df.columns.values))])

    return 'SUCCESS'


def all_cycles():
    data = read_data().sort_values("date", ascending=False).reset_index(drop=True)
    all_cycles_with_duplicates = list(data['cycle'])
    set_cycles = sorted_set(all_cycles_with_duplicates)
    return json.dumps(['-'] + set_cycles)


def get_list_recurring():
    return get_possible_recurring()


def refresh_lunchr():
    add_new_data(get_latest_lunchr())
