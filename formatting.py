import math
from collections import defaultdict

import pandas as pd

from format.data_operations import calculate_universal_fields
from format.date_operations import get_timestamp_now
from param import *
from crud import read_data
from autofill_name import *

import numpy as np
import hashlib


def parse_ids(transaction_ids):
    try:
        transaction_ids = list(transaction_ids.replace(' ', '').split(','))
    except ValueError:
        return 'invalid ID', []
    data = read_data()
    ids = list(data['id'])
    if set(transaction_ids) & set(ids) != set(transaction_ids):
        return 'transaction not found', []
    return '', transaction_ids


def make_bank_name(row):
    merchant, partner, reference, trans_type = [row[u].strip() for u in ['merchantName', 'partnerName', 'referenceText', 'type']]
    if merchant:
        return merchant
    if trans_type == 'DD':
        return '#PRLV ' + partner
    if reference:
        return row['referenceText'] + ' #VIR'
    return '#VIR ' + ['to ', 'from '][row['amount'] > 0] + partner


def remove_original_amount_when_euro(row):
    if row['originalCurrency'] == 'EUR':
        return np.NaN
    return row['originalAmount']


def remove_original_currency_when_euro(row):
    if row['originalCurrency'] == 'EUR':
        return ''
    return row['originalCurrency']




def dataframe_formatter(df, account):
    df['bank_name'] = df.replace(np.NaN, '').apply(lambda row: make_bank_name(row), axis=1)
    df['originalAmount'] = df.apply(lambda row: remove_original_amount_when_euro(row), axis=1)
    df['originalCurrency'] = df.apply(lambda row: remove_original_currency_when_euro(row), axis=1)
    df['name'] = df['bank_name'].apply(autofill_name)

    df['account'] = account

    calculate_universal_fields(df)

    return df


def type_to_method(row):
    type = row['type']
    amount = row['amount']
    account = row['account']
    if type in ['PT', 'AA', 'AE']:
        return 'card'
    if type in ['DT', 'CT']:
        return 'transfer'
    if math.isnan(amount) or account.startswith('Cash'):
        return 'cash'
    return 'card'


def create_id(fields):
    name, amount, account, timestamp = [fields[u] for u in ['name', 'amount', 'account', 'visibleTS']]

    string = '*'.join([name, str(int(timestamp) * 1000000), str(int(float('0'+str(amount)) * 100)), account])
    return hashlib.md5(string.encode()).hexdigest()

