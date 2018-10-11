import json
import time

from param import *
from clean_n26 import get_n_last_transactions
from formatting import *
from credentials import *
from crud import *

import pandas as pd
import numpy as np

# %% PANDAS PRINT PARAMETERS
from recap_by_category import get_categories_recap, get_trip_data

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
    if get_delay_since_last_update() > delay_refresh_minutes :
        print('YES')
        print(force_refresh())
    else:
        print('NO')


def force_refresh():
    print('FORCE REFRESH')
    t = time.time()
    all_valid, all_data = True, []
    for account in login:
        valid, data = get_transactions_as_df(account, max_transactions_per_user)
        all_valid = all_valid and valid
        all_data.append(data)
    if not all_valid:
        print('REFRESH FAILED')
        return 'FAIL'
    new_data = pd.concat(all_data).sort_values("date", ascending=False).reset_index(drop=True)
    save_data(merge_data(new_data))
    change_last_update_to_now()
    return 'SUCCESS'


def list_data_json(refresh = False, hide_linked = True):
    if refresh:
        refresh_data()
    data = read_data().head(100)
    if hide_linked:
        data = data[data['link'] == '-']
    data = data[['id','name', 'amount', 'category', 'pending', 'originalAmount', 'originalCurrency']]
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

    if not all(u in transaction_fields for u in mandatory_fields):
        return 'FAIL'
    transaction_fields['amount'] = float(transaction_fields['amount'])
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

    data.loc[data['id'] == id1, ['link']] = id2
    data.loc[data['id'] == id2, ['link']] = id1
    save_data(data)
    return 'SUCCESS'


def get_recap_categories(initial_date_str="2018-10-01"):
    this_trip_data = get_trip_data(initial_date_str)
    recap = get_categories_recap(this_trip_data)

    return recap.to_json(orient="records")


