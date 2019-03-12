from numpy import NaN

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
