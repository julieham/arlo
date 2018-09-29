from datetime import datetime as dt
from param import *

import numpy as np


def parse_ids(transaction_ids, data):
    try:
        transaction_ids = list(transaction_ids.replace(' ', '').split(','))
    except ValueError:
        return 'invalid ID', []
    ids = list(data['id'])
    if set(transaction_ids) & set(ids) != set(transaction_ids):
        return 'transaction not found', []
    return '', transaction_ids


def convert_timestamp_to_datetime(timestamp):
    return dt.fromtimestamp(timestamp // 1e3)


def make_name(row):
    if not row['merchantName'].strip() == '':
        return row['merchantName']
    if not row['referenceText'].strip() == '':
        spaces_virement = "From Main Account to "
        if spaces_virement in row['referenceText']:
            return 'Saving for ' + row['referenceText'][len(spaces_virement):]
        return row['referenceText'] + ' #VIR'
    return '#VIR to ' + row["partnerName"]


def dataframe_formatter(df, account):
    names = df[['merchantName', 'referenceText', 'partnerName']].replace(np.NaN, '').apply(lambda row: make_name(row), axis=1)
    df['bank_name'] = names
    df['date'] = df['visibleTS'].apply(convert_timestamp_to_datetime)
    df['account'] = account
    df['category'] = '-'
    df['comment'] = '-'
    return df[column_names]

def name_reducer(name):
    return name.replace(' to ',' > ').replace('From ','')

#def get_all_balances():
 #   balances = dict()
  #  for n