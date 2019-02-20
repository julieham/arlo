import math
import pandas as pd

# from arlo.format.date_operations import decode_cycle
from arlo.format.df_operations import df_is_not_empty
from arlo.parameters.param import budgets_filename
from arlo.read_write.reader import read_df_file


def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']


def get_budgets(cycle):
    budgets = read_df_file(budgets_filename, sep=';')

    if cycle != 'all':
        budgets = budgets[budgets['cycle'] == cycle]

    if df_is_not_empty(budgets):

        budgets = budgets.groupby('category').apply(sum)['amount']
        return budgets

    return pd.Series()


def get_exchange_rate(data):
    cash_withdrawals = data[data['type'] == 'CW']
    if cash_withdrawals.shape[0] == 0:
        return 1
    sum_currency = data['originalAmount'].sum()
    sum_euro = data['amount'].sum()
    try:
        return sum_euro / sum_currency
    except ZeroDivisionError:
        return 1


"""
def summary_on_field(data, field_name, cycle):
    # cycle = decode_cycle(cycle)

    exchange_rate = get_exchange_rate(data)
    euro_amounts = data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    data = data.assign(euro_amount=euro_amounts)
    fields = list({'amount', 'originalAmount', 'category', 'account', field_name})
    data = data[fields]
    summary = (data.groupby([field_name]).sum().reset_index())

    budgets = get_budgets()
    summary['total_budget'] = summary['category'].map(budgets).fillna(0).astype(float)
"""


def get_categories_recap(cycle_data, cycle):
    exchange_rate = get_exchange_rate(cycle_data)
    euro_amounts = cycle_data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    cycle_data = cycle_data.assign(euro_amount=euro_amounts)
    cycle_data = cycle_data[['date', 'euro_amount', 'category']]

    cycle_data = cycle_data.groupby(['category']).sum().apply(abs).reset_index()

    budgets = get_budgets(cycle)
    cycle_data['total_budget'] = cycle_data['category'].map(budgets).fillna(0).astype(float)
    cycle_data = cycle_data[cycle_data['category'] != "Input"]

    return cycle_data
