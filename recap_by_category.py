import math

import pandas as pd

from crud import read_data, read_dico


def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']


def get_budgets():
    return read_dico('./data/budgets.txt')


def get_exchange_rate(data):
    cash_withdrawals = data[data['type'] == 'CW']
    if cash_withdrawals.shape[0] == 0:
        return 1
    sum_currency = data['originalAmount'].sum()
    sum_euro = data['amount'].sum()
    try:
        return sum_euro / sum_currency
    except ZeroDivisionError:
        print('WRONG EXCHANGE RATE : NO CASH WITHDRAWN')
        return 1


def get_trip_data(initial_date_str):
    initial_date = pd.datetime.strptime(initial_date_str, '%Y-%m-%d')
    data = read_data()
    this_trip_data = data[data['date'] >= initial_date]
    if this_trip_data.shape[0] == 0:
        return this_trip_data
    exchange_rate = get_exchange_rate(this_trip_data)
    euro_amounts = this_trip_data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    this_trip_data = this_trip_data.assign(euro_amount=euro_amounts)
    this_trip_data = this_trip_data[['date', 'euro_amount', 'category', 'pending']]
    return this_trip_data


def get_categories_recap(this_trip_data):
    data = this_trip_data.groupby(['category']).sum().apply(abs).reset_index()
    data = data[data['category'] != '-']

    budgets = get_budgets()
    data['total_budget'] = data['category'].map(budgets).fillna(0).astype(float)

    data = data[data['category'].str.startswith('NY')]

    to_sum_data = data[data['category'] != 'NY_Input']
    total = to_sum_data.sum(numeric_only=True)
    total['category'] = 'TOTAL'
    data = data.append(total, ignore_index=True)

    return data
