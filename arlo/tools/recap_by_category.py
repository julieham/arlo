import math

from arlo.operations.df_operations import df_is_not_empty, assign_new_column, concat_columns, empty_series, filter_df_not_this_value
from arlo.operations.series_operations import positive_part, ceil_series, floor_series
from arlo.parameters.param import budgets_filename, no_recap_categories
from arlo.read_write.reader import read_df_file
from tools.cycleManager import decode_cycle


def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']


def get_budgets(cycle):
    budgets = read_df_file(budgets_filename, sep=';')

    if cycle != 'all':
        budgets = budgets[budgets['cycle'] == decode_cycle(cycle)]

    if df_is_not_empty(budgets):

        budgets = budgets.groupby('category').apply(sum)['amount']
        return budgets

    return empty_series()


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


def get_categories_recap(cycle_data, cycle, round_it=False):
    exchange_rate = get_exchange_rate(cycle_data)
    euro_amounts = cycle_data.apply(lambda row: get_euro_amount(row, exchange_rate), axis=1)
    cycle_data = cycle_data.assign(euro_amount=euro_amounts)
    cycle_data = cycle_data[['euro_amount', 'category']]

    spent_by_category = cycle_data.groupby(['category']).sum().reset_index()
    spent_by_category.set_index('category', inplace=True)

    budgets = get_budgets(cycle).rename('total_budget')

    recap = concat_columns([spent_by_category, budgets], keep_index_name=True).round(2).fillna(0)
    recap.reset_index(inplace=True)

    over = positive_part(- recap['euro_amount'] - recap['total_budget'])
    remaining = positive_part(recap['total_budget'] + recap['euro_amount'])
    spent = positive_part(- recap['euro_amount'] - over)

    assign_new_column(recap, 'over', ceil_series(over) if round_it else over)
    assign_new_column(recap, 'remaining', floor_series(remaining) if round_it else remaining)
    assign_new_column(recap, 'spent', ceil_series(spent) if round_it else spent)

    for no_recap_cat in no_recap_categories:
        recap = filter_df_not_this_value(recap, 'category', no_recap_cat)

    return recap[['category', 'spent', 'remaining', 'over']]
