import pandas as pd

from arlo.operations.df_operations import df_is_not_empty, assign_new_column, concat_columns, empty_series, df_is_empty, \
    filter_df_not_these_values, select_columns, concat_lines, filter_df_on_bools, column_is_null, \
    add_column_with_value, reverse_amount, empty_df, drop_columns, total_amount, \
    filter_df_one_value
from arlo.operations.series_operations import positive_part, ceil_series, floor_series
from arlo.parameters.column_names import category_col, amount_euro_col, cycle_col, deposit_name_col, account_col, \
    currency_col
from arlo.parameters.param import no_recap_categories, deposit_account, default_currency
from arlo.read_write.file_manager import read_budgets, read_data
from arlo.read_write.select_data import get_data_from_cycle, get_deposit_debits_from_cycle
from arlo.tools.cycle_manager import decode_cycle, nb_days_in_cycle, cycle_overview_to_cycle_progress, \
    filter_df_on_cycle, cycle_is_finished

"""
def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']
"""

budgets_col = 'budget'


def group_amount_by(df, field_name):
    df = df[[field_name, amount_euro_col]]
    return df.groupby(field_name).apply(lambda x: x.sum(skipna=False))[amount_euro_col]


def sum_no_skip_na(x):
    return x.sum(skipna=False)


def group_by_field(data, field_name):
    if df_is_empty(data):
        return empty_series()
    data = data[[amount_euro_col, field_name, currency_col]]
    summary = (data.groupby([field_name, currency_col])).agg({amount_euro_col: sum_no_skip_na})
    summary.reset_index(level=[currency_col], inplace=True)
    return summary


def get_budgets(cycle):
    budgets = read_budgets()
    if cycle != 'all':
        budgets = budgets[budgets[cycle_col] == decode_cycle(cycle)]

    if df_is_not_empty(budgets):
        budgets = budgets.groupby(category_col).apply(sum)[amount_euro_col]
        return budgets.rename(budgets_col)

    return empty_series().rename(budgets_col)


"""
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


def recap_by_cat(cycle, round_it=True):
    data = get_data_from_cycle(cycle)
    deposit = get_deposit_debits_from_cycle(cycle)

    add_column_with_value(data, currency_col, default_currency)
    add_column_with_value(deposit, currency_col, default_currency)

    field_name = category_col
    selected_columns = [amount_euro_col, field_name, currency_col]
    data = filter_df_on_bools(data, column_is_null(data, deposit_name_col))
    data = select_columns(data, selected_columns)
    deposit = select_columns(deposit, selected_columns)

    all_output = concat_lines([data, deposit])
    if df_is_empty(all_output):
        return empty_df()

    spent = group_by_field(all_output, category_col)
    if df_is_empty(spent):
        return empty_df()

    recap = concat_columns([spent, get_budgets(cycle)], keep_index_name=True).round(2).fillna(0).reset_index()
    recap = filter_df_not_these_values(recap, category_col, no_recap_categories)

    over = positive_part(- recap[amount_euro_col] - recap[budgets_col])
    remaining = positive_part(recap[budgets_col] + recap[amount_euro_col])
    spent = positive_part(- recap[amount_euro_col] - over)

    assign_new_column(recap, 'over', ceil_series(over) if round_it else over)
    assign_new_column(recap, 'remaining', floor_series(remaining) if round_it else remaining)
    assign_new_column(recap, 'spent_from_budget', ceil_series(spent) if round_it else spent)

    return recap


def get_category_groups(cycle):
    days_in_cycle_overview = nb_days_in_cycle(cycle)
    nb_days_this_cycle = days_in_cycle_overview['all_days']
    nb_days_done_this_cycle = days_in_cycle_overview['days_done']
    this_cycle_progress = cycle_overview_to_cycle_progress(days_in_cycle_overview)

    recap = recap_by_cat(cycle)
    if df_is_empty(recap):
        return recap

    recap['progress'] = (- 100 * recap['amount']).div(recap['budget'])  # prog.clip(upper=100, lower=0)
    recap['authorized'] = recap['budget'] * this_cycle_progress / 100
    recap['delta_money'] = ceil_series(- recap['amount'] - recap['authorized']).astype(int)
    recap['delta_days'] = (recap['progress'] - this_cycle_progress) * nb_days_this_cycle / 100
    recap['delta_days'] = ceil_series(recap['delta_days']).clip(upper=nb_days_this_cycle,
                                                                lower=-nb_days_done_this_cycle).astype(int)
    recap['progress'].clip(upper=100, lower=0, inplace=True)
    recap['total_spent'] = - ceil_series(recap['amount'])
    drop_columns(recap, ['budget', 'authorized'])
    return recap


def recap_by_account(cycle):
    field_name = account_col
    selected_columns = [amount_euro_col, field_name, currency_col]

    cycle = decode_cycle(cycle)
    data = get_data_from_cycle(cycle)
    add_column_with_value(data, currency_col, default_currency)

    if cycle != 'all':
        is_deposit = column_is_null(data, deposit_name_col) == False
        deposit_transactions = filter_df_on_bools(data, is_deposit)

        add_column_with_value(deposit_transactions, account_col, deposit_account)
        reverse_amount(deposit_transactions)

        data = select_columns(concat_lines([data, deposit_transactions]), selected_columns)

        deposit = get_deposit_debits_from_cycle(cycle)
        data = concat_lines([deposit, data])

    all_outputs = select_columns(data, selected_columns)
    recap = group_by_field(all_outputs, field_name).round(decimals=2)
    return recap



def input_recap(cycle):
    overview = dict()
    data_this_cycle = filter_df_on_cycle(read_data(), cycle)
    recap_by_category = recap_by_cat(cycle, False)
    if cycle_is_finished(cycle):
        overview['Input goal'] = - round(sum(recap_by_category['amount']), 2)
        overview['(actual) Over'] = overview['Input goal'] - sum(get_budgets(cycle))
    else:
        overview['Budgets'] = round(sum(recap_by_category['budget']), 2)
        overview['Over (so far)'] = round(sum(recap_by_category['over']), 2)
        overview['Input goal'] = overview['Budgets'] + overview['Over (so far)']

    data_this_cycle_input = filter_df_one_value(data_this_cycle, 'category', 'Input')
    overview['Done'] = total_amount(data_this_cycle_input)
    remaining = round(overview['Input goal'] - overview['Done'], 2)
    if remaining > 0:
        overview['Remaining TO DO'] = remaining
    elif remaining < 0:
        overview['Margin'] = - remaining

    overview = pd.Series(overview).rename('amount', inplace=True).to_frame()
    add_column_with_value(overview, currency_col, default_currency)
    return overview
