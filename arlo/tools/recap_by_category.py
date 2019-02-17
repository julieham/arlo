import math

from arlo.format.date_operations import decode_cycle
from arlo.format.df_operations import read_file_to_df, df_is_not_empty
from arlo.format.types_operations import series_to_dict
from arlo.parameters.param import directory

budgets_filename = directory + 'budgets.csv'


def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']


def get_budgets(cycle):
    budgets = read_file_to_df(budgets_filename, sep=';')

    if cycle != 'all':
        budgets = budgets[budgets['cycle'] == cycle]

    if df_is_not_empty(budgets):

        budgets = budgets.groupby('category').apply(sum)['amount']
        return series_to_dict(budgets)

    return dict()


def get_exchange_rate(data):
    cash_withdrawals = data[data['type'] == 'CW']
    if cash_withdrawals.shape[0] == 0:
        print('WRONG EXCHANGE RATE : NO WITHDRAWAL FOUND')
        return 1
    sum_currency = data['originalAmount'].sum()
    sum_euro = data['amount'].sum()
    try:
        return sum_euro / sum_currency
    except ZeroDivisionError:
        print('WRONG EXCHANGE RATE : TOTAL CASH WITHDRAWN = 0')
        return 1

def summary_on_field(data, field_name, cycle):
    cycle = decode_cycle(cycle)

    exchange_rate = get_exchange_rate(data)
    euro_amounts = data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    data = data.assign(euro_amount=euro_amounts)
    fields = list({'amount', 'originalAmount', 'category', 'account', field_name})
    data = data[fields]
    summary = (data.groupby([field_name]).sum().reset_index())

    budgets = get_budgets()
    summary['total_budget'] = summary['category'].map(budgets).fillna(0).astype(float)


def get_categories_recap(data, cycle):
    exchange_rate = get_exchange_rate(data)
    euro_amounts = data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    data = data.assign(euro_amount=euro_amounts)
    data = data[['date', 'euro_amount', 'category', 'pending']]

    data = data.groupby(['category']).sum().apply(abs).reset_index()

    budgets = get_budgets(cycle)
    data['total_budget'] = data['category'].map(budgets).fillna(0).astype(float)
    data = data[data['category'] != "Input"]

    return data
