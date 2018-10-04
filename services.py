import json

from param import *
from clean_n26 import get_n_last_transactions
from formatting import *
from credentials import *
from crud import *

import pandas as pd
import numpy as np

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
    if get_delay_since_last_update() > delay_refresh_minutes :
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
    if not all_valid:
        return 'FAIL'
    new_data = pd.concat(all_data).sort_values("date", ascending=False).reset_index(drop=True)
    save_data(merge_data(new_data))
    change_last_update_to_now()
    return 'SUCCESS'


def list_data_json():
    refresh_data()
    data = read_data().head(20)
    data['pending'] = data['type'] == 'AA'
    data['bank_name'] = data['bank_name'].apply(name_reducer)
    data = data[['id','bank_name', 'amount', 'category', 'pending']]
    return data.to_json(orient="records")


def categorize(transaction_ids, category_name):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message

    change_one_field_on_ids(transaction_ids, 'category', category_name)
    return 'SUCCESS'


def create_manual_transaction(json_input):
    transaction_fields = json.loads(json_input)

    timestamp = pd.datetime.timestamp(pd.datetime.now())
    transaction_fields['date'] = convert_timestamp_to_datetime(1000*timestamp)

    transaction_fields['id'] = create_id(transaction_fields['bank_name'], timestamp, transaction_fields['amount'])

    data = read_data().append(pd.DataFrame(transaction_fields, index=[0]), ignore_index=True, sort=False)
    save_data(data)


#%% RUN
#refresh_data()