from param import *
from clean_n26 import get_n_last_transactions
from formatting import *

import pandas as pd
import numpy as np

# %% PANDAS PRINT PARAMETERS

desired_width = 10000
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 100)


# %% CRUD

def read_data():
    data = pd.read_csv("data.csv")[column_names]
    data['date'] = pd.to_datetime(data['date'])
    return data


def save_data(data):
    data = data.sort_values("date", ascending=False).reset_index(drop=True)
    data.to_csv('data.csv', index=False)


def merge_data(new_data, old_data):
    # TODO : make this smarter
    return new_data


# %% API CALLS


def get_transactions_as_df(account, limit):
    df = pd.DataFrame(get_n_last_transactions(account, limit))
    return dataframe_formatter(df, account)


# %% SERVICES

def refresh_data():
    df = [get_transactions_as_df(account, max_transactions_per_user) for account in login]
    new_data = pd.concat(df).sort_values("date", ascending=False).reset_index(drop=True)
    save_data(merge_data(new_data, read_data()))


def list_data_json():
    data = read_data().head(10)
    data['pending'] = data['type'] == 'AA'
    data['bank_name'] = data['bank_name'].apply(name_reducer)
    data = data[['id','bank_name', 'amount', 'category', 'pending']]
    print(data)
    return data.to_json(orient="records")


def categorize(transaction_ids, category_name):
    data = read_data()
    error_message, transaction_ids = parse_ids(transaction_ids, data)
    if error_message:
        return error_message

    data.loc[data['id'].isin(transaction_ids), ['category']] = category_name
    save_data(data)
    return 'SUCCESS'


#%% RUN
#refresh_data()