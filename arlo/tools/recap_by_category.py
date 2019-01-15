import math

from arlo.read_write.crud import read_dico


def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']


def get_budgets():
    return read_dico('./arlo/data/budgets.txt')


def get_exchange_rate(data):
    cash_withdrawals = data[data['type'] == 'CW']
    if cash_withdrawals.shape[0] == 0:
        print('WRONG EXCHANGE RATE : NO CASH WITHDRAWN')
        return 1
    sum_currency = data['originalAmount'].sum()
    sum_euro = data['amount'].sum()
    try:
        return sum_euro / sum_currency
    except ZeroDivisionError:
        print('WRONG EXCHANGE RATE : NO CASH WITHDRAWN')
        return 1

def summary_on_field(data, field_name, cycle='all'):
    if cycle != 'all':
        data = data[data['cycle'] == cycle]
    exchange_rate = get_exchange_rate(data)
    euro_amounts = data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    data = data.assign(euro_amount=euro_amounts)
    fields = list({'amount', 'originalAmount', 'category', 'account', field_name})
    data = data[fields]
    summary = (data.groupby([field_name]).sum().reset_index())

    budgets = get_budgets()
    summary['total_budget'] = summary['category'].map(budgets).fillna(0).astype(float)


def get_categories_recap(data):
    exchange_rate = get_exchange_rate(data)
    euro_amounts = data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    data = data.assign(euro_amount=euro_amounts)
    data = data[['date', 'euro_amount', 'category', 'pending']]

    data = data.groupby(['category']).sum().apply(abs).reset_index()

    budgets = get_budgets()
    data['total_budget'] = data['category'].map(budgets).fillna(0).astype(float)

    to_sum_data = data[data['category'].str.endswith('Input') == False]
    total = to_sum_data.sum(numeric_only=True)
    total['category'] = 'TOTAL'
    data = data.append(total, ignore_index=True)

    return data
