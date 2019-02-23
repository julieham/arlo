from math import isnan
from numpy import NaN
import hashlib

from arlo.format.data_operations import calculate_universal_fields
from arlo.format.df_operations import add_autofilled_column
from arlo.read_write.fileManager import read_data


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
        return NaN
    return row['originalAmount']


def remove_original_currency_when_euro(row):
    if row['originalCurrency'] == 'EUR':
        return ''
    return row['originalCurrency']


def dataframe_formatter(df, account):
    if account.endswith('N26'):
        df['bank_name'] = df.replace(NaN, '').apply(lambda row: make_bank_name(row), axis=1)
        df['originalAmount'] = df.apply(lambda row: remove_original_amount_when_euro(row), axis=1)
        df['originalCurrency'] = df.apply(lambda row: remove_original_currency_when_euro(row), axis=1)

    df['account'] = account

    calculate_universal_fields(df)

    add_autofilled_column(df, 'bank_name', 'name')
    add_autofilled_column(df, 'name', 'category')

    return df


def type_to_method(row):
    transaction_type = row['type']
    amount = row['amount']
    account = row['account']
    if account == "SUL":
        return 'hotel'
    if transaction_type in ['PT', 'AA', 'AE']:
        return 'card'
    if transaction_type in ['DT', 'CT']:
        return 'transfer'
    if isnan(amount) or account.startswith('Cash'):
        return 'cash'
    return 'card'


def create_id(fields):
    name, amount, account, timestamp = [fields[u] for u in ['name', 'amount', 'account', 'visibleTS']]

    str_amount = str(amount)
    if str_amount == '':
        str_amount = '0'

    string = '*'.join([name, str(int(timestamp) * 1000000), str(int(float(str_amount) * 100)), account])
    return hashlib.md5(string.encode()).hexdigest()
