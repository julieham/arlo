import math

import pandas as pd
import numpy as np
import json

from arlo.format.date_operations import decode_cycle, angular_string_to_timestamp
from arlo.tools.clean_lunchr import get_data_lunchr_since_last_id
from arlo.tools.recap_by_category import get_categories_recap
from arlo.format.data_operations import calculate_universal_fields, set_amounts_to_numeric
from arlo.format.df_operations import df_is_not_empty, change_field_on_single_id_to_value, get_one_field, \
    result_function_applied_to_field, how_many_rows, filter_df_several_values, \
    filter_df_on_cycle, change_field_on_several_ids_to_value, get_last_id_from_account
from arlo.format.formatting import dataframe_formatter, type_to_method, parse_ids
from arlo.format.transaction_operations import add_default_values_if_absent, fields_make_valid_transaction
from arlo.format.types_operations import dict_to_df, sorted_set
from arlo.parameters.credentials import login_N26
from arlo.parameters.param import *
from arlo.read_write.crud import save_data, read_data, change_last_update_to_now, add_to_data, minutes_since_last_update, \
    set_field_to_value_on_ids
from arlo.tools.clean_n26 import get_n_last_transactions
from arlo.tools.finder import has_default_fields, get_default_fields
from arlo.tools.merge_data import merge_data

# %% PANDAS PRINT PARAMETERS

desired_width = 10000
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 100)


# %% API CALLS


def get_transactions_as_df(account, limit):
    valid, data = get_n_last_transactions(account, limit)
    if not valid:
        return False, None
    df = pd.DataFrame(data)
    return True, dataframe_formatter(df, account)


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
    #refresh_lunchr()

    all_valid, all_data = True, []
    for account in login_N26:
        valid, data = get_transactions_as_df(account, max_transactions_per_user)
        all_valid = all_valid and valid
        all_data.append(data)
    all_valid = True

    if not all_valid:
        print('REFRESH FAILED')
        return 'FAIL'

    new_data = pd.concat(all_data, sort=False).sort_values("date", ascending=False).reset_index(drop=True)

    save_data(merge_data(read_data(), new_data))
    change_last_update_to_now()

    return 'SUCCESS'


def refresh_lunchr():
    data = read_data()
    last_id = get_last_id_from_account(data, 'lunchr')
    df = get_data_lunchr_since_last_id(last_id)
    df['bank_name'].fillna('')
    data = pd.concat([data, df], join='outer', sort=False)
    save_data(data)


def list_data_json(refresh=None, hide_linked=True, cycle="now"):
    if refresh:
        refresh_data()
    data = read_data()
    #TODO recup link disappearing transactions

    if hide_linked:
        data = data[data['category'] != "Link"]

    data['method'] = data.apply(lambda row: type_to_method(row), axis=1)
    data = data[['id', 'name', 'amount', 'category', 'pending', 'originalAmount', 'originalCurrency', 'method', 'cycle']]

    cycle = decode_cycle(cycle)
    data = filter_df_on_cycle(data, cycle)

    return data.head(400).to_json(orient="records")


def categorize(transaction_ids, category_name):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message

    set_field_to_value_on_ids(transaction_ids, 'category', category_name)
    return 'SUCCESS'


def name(transaction_ids, name):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message
    set_field_to_value_on_ids(transaction_ids, 'name', name)

    return 'SUCCESS'


def change_cycle(transaction_ids, name):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message
    set_field_to_value_on_ids(transaction_ids, 'cycle', name)

    return 'SUCCESS'


def create_manual_transaction(transaction_fields):
    print(transaction_fields)

    if "angular_date" in transaction_fields:
        if transaction_fields["angular_date"] != "" and transaction_fields["angular_date"] != None:
            transaction_fields["visibleTS"] = angular_string_to_timestamp(transaction_fields["angular_date"])
        del transaction_fields["angular_date"]

    print(transaction_fields)

    add_default_values_if_absent(transaction_fields)
    if not fields_make_valid_transaction(transaction_fields):
        return 'FAIL'

    print(transaction_fields)

    transaction_df = dict_to_df(transaction_fields)

    if "isCredit" in transaction_fields:
        set_amounts_to_numeric(transaction_df, transaction_fields["isCredit"] == "true")
    else:
        set_amounts_to_numeric(transaction_df, False)
    calculate_universal_fields(transaction_df)

    add_to_data(transaction_df[list(set(column_names) & set(transaction_df.columns.values))])

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
        message = 'FAIL at least one transaction missing'
        print(message)
        return message

    if result_function_applied_to_field(data_ids, 'amount', sum) != 0:
        message = 'FAIL transactions do not cancel each other out'
        print(message)
        return message


    present_links = set(get_one_field(data_ids, 'link'))

    if present_links != {'-'}:
        message = 'FAIL one or more transaction already linked'
        print(message)
        return message

    ids_link = ids[1:]+ [ids[0]]
    for id, id_link in zip(ids, ids_link):
        change_field_on_single_id_to_value(data, id, 'link', id_link)

    save_data(data)
    return 'SUCCESS'


def get_recap_categories(cycle='now'):
    data = read_data()

    cycle = decode_cycle(cycle)
    data = filter_df_on_cycle(data, cycle)

    #TODO make generic function with balances

    recap = pd.DataFrame()

    if df_is_not_empty(data):
        recap = get_categories_recap(data, cycle)

    recap = recap[recap['category'] != "Link"]

    return recap.to_json(orient="records")


def get_final_amount(row):
    if math.isnan(row['amount']):
        return round(row['originalAmount'], 2)
    return round(row['amount'], 2)


def get_final_currency(row):
    if math.isnan(row['amount']):
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

    all = pd.concat([data, data_this_cycle], axis=1, sort=False).fillna(0)
    all["currency"] = "EUR"
    all.index.names = ['account_name']

    return(all)


def create_recurring_transaction(name, amount=None, account=None):

    if has_default_fields(name):
        default_fields = get_default_fields(name)
        if amount is None:
            amount = default_fields['amount']
        if account is None:
            account = default_fields['account']

    transaction_fields = dict({'name': name, 'amount': amount, 'account': account, 'type': 'REC'})
    result = create_manual_transaction(transaction_fields)
    return result


def all_cycles():
    data = read_data().sort_values("date", ascending=False).reset_index(drop=True)
    all_cycles_with_duplicates = list(data['cycle'])
    set_cycles = sorted_set(all_cycles_with_duplicates)
    return json.dumps(['-'] + set_cycles)


def get_budgets_from_cycle(cycle):
    cycle = decode_cycle(cycle)

    budgets = pd.read_csv('./arlo/data/' + 'budgets.csv', sep=";")
    budgets = budgets[budgets['cycle'] == cycle]

    budgets = budgets.rename(columns={"amount": "budget"})
    budgets = budgets.set_index('category')

    return budgets[['budget']]