from clean_n26 import get_n_last_transactions
from formatting import *
from credentials import *
from crud import *
from merge_tool import merge_data
from df_operations import *

import pandas as pd
import numpy as np


# %% PANDAS PRINT PARAMETERS
from recap_by_category import get_categories_recap

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
    if get_delay_since_last_update() > delay_refresh_minutes:
        print('YES')
        print(force_refresh())
    else:
        print('NO')



def force_refresh():

    print('FORCE REFRESH')
    all_valid, all_data = True, []
    for account in login:
        valid, data = get_transactions_as_df(account, max_transactions_per_user)
        all_valid = all_valid and valid
        all_data.append(data)
    all_valid = True

    if not all_valid:
        print('REFRESH FAILED')
        return 'FAIL'

    new_data = pd.concat(all_data).sort_values("date", ascending=False).reset_index(drop=True)

    #new_data = pd.read_csv("./data/data_backup_18_dec.csv")

    save_data(merge_data(read_data(), new_data))
    change_last_update_to_now()

    return 'SUCCESS'


def list_data_json(refresh=None, hide_linked=None, cycle="all"):
    if refresh:
        refresh_data()
    data = read_data().head(400)
    if hide_linked:
        data = data[data['link'] == '-']
    data['method'] = data.apply(lambda row: type_to_method(row), axis=1)
    data = data[['id', 'name', 'amount', 'category', 'pending', 'originalAmount', 'originalCurrency', 'method', 'cycle']]
    if cycle != "all":
        data = data[data['cycle'] == cycle]
    return data.to_json(orient="records")


def categorize(transaction_ids, category_name):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message

    change_one_field_on_ids(transaction_ids, 'category', category_name)
    return 'SUCCESS'


def edit_field(transaction_ids, field_name, field_value):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message

    change_one_field_on_ids(transaction_ids, field_name, field_value)
    return 'SUCCESS'


def create_manual_transaction(json_input):
    transaction_fields = json_input

    transaction_fields['link'] = '-'

    if not all(u in transaction_fields for u in mandatory_fields):
        return 'FAIL'
    if transaction_fields['amount'] == transaction_fields['originalAmount'] == '':
        return 'FAIL'

    line = make_a_csv_line(transaction_fields)
    add_data_line(line)

    return 'SUCCESS'


def link_two_ids(ids):
    if len(ids) < 2:
        return 'FAIL not enough transactions'
    if len(ids) > 2:
        return 'FAIL too many transactions'
    id1, id2 = ids
    data = read_data()
    if len({id1, id2} & set(data['id'])) != 2:
        return 'FAIL transactions not found'

    trans1, trans2 = data.loc[data['id'] == id1], data.loc[data['id'] == id2]
    if not(all(trans1['link'].isnull()) and all(trans2['link'].isnull())):
        return 'FAIL transaction already linked'
    if float(trans1['amount']) != - float(trans2['amount']):
        return 'FAIL amounts are not equal'

    data.loc[data['id'] == id1, ['link', 'pending']] = [id2, False]
    data.loc[data['id'] == id2, ['link', 'pending']] = [id1, False]
    save_data(data)
    return 'SUCCESS'


def get_recap_categories(cycle='all'):
    data = read_data()
    if cycle != 'all':
        data = data[data['cycle'] == cycle]
    if df_is_not_empty(data):
        recap = get_categories_recap(data)
    else:
        recap = pd.DataFrame()

    return recap.to_json(orient="records")


def get_final_amount(row):
    if math.isnan(row['amount']):
        return round(row['originalAmount'], 2)
    return round(row['amount'], 2)


def get_final_currency(row):
    if math.isnan(row['amount']):
        return row['originalCurrency']
    return 'EUR'


def get_balances():
    pd.set_option('mode.chained_assignment', None)

    data = read_data()
    data_other_accounts = data  # [data['account'].str.endswith('_N26') == False]
    data_other_accounts = data_other_accounts[['amount', 'originalAmount', 'account', 'originalCurrency']]
    original_curr = data_other_accounts.loc[:, 'originalCurrency'].fillna('EUR')
    data_other_accounts.loc[:, 'originalCurrency'] = original_curr

    recap = data_other_accounts.groupby(['account', 'originalCurrency']).apply(lambda x: x.sum(skipna=False))
    recap = recap[['amount', 'originalAmount']].reset_index()
    recap['finalCurrency'] = recap.apply(lambda row: get_final_currency(row), axis=1)
    recap['finalAmount'] = recap.apply(lambda row: get_final_amount(row), axis=1)

    recap = recap.groupby('account').agg({'finalAmount': "sum", 'finalCurrency': "first"})

    return recap
