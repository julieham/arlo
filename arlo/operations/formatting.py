from arlo.read_write.file_manager import read_data
from read_write.reader import empty_data_dataframe


def parse_ids(transaction_ids):
    if not transaction_ids:
        return 'no transaction', empty_data_dataframe()
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
    return (reference + (' (' + partner + ')') if partner else '').strip()
