from arlo.operations.df_operations import df_is_not_empty, assign_new_column, concat_columns, empty_series, df_is_empty, \
    filter_df_not_these_values, drop_columns, select_columns, concat_lines
from arlo.operations.series_operations import positive_part, ceil_series, floor_series
from arlo.parameters.param import budgets_filename, no_recap_categories, auto_accounts
from arlo.read_write.reader import read_df_file
from read_write.select_data import get_data_from_cycle, get_deposit_debits_from_cycle
from tools.cycle_manager import decode_cycle

"""
def get_euro_amount(row, exchange_rate):
    if math.isnan(row.loc['amount']):
        return row.loc['originalAmount'] * exchange_rate
    return row.loc['amount']
"""


def group_amount_by(df, field_name):
    df = df[[field_name, 'amount']]
    return df.groupby(field_name).apply(lambda x: x.sum(skipna=False))['amount']


def sum_no_skip_na(x):
    return x.sum(skipna=False)


def summary_on_field(data, field_name):
    if df_is_empty(data):
        return empty_series()
    data = data[["amount", field_name]]
    summary = (data.groupby([field_name])).agg({"amount": sum_no_skip_na})

    return summary


def get_budgets(cycle):
    budgets = read_df_file(budgets_filename, sep=';')

    if cycle != 'all':
        budgets = budgets[budgets['cycle'] == decode_cycle(cycle)]

    if df_is_not_empty(budgets):

        budgets = budgets.groupby('category').apply(sum)['amount']
        budgets['Input'] = - sum(budgets)
        return budgets.rename('budget')

    return empty_series().rename('budget')


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

    field_name = 'category'
    selected_columns = ['amount', field_name]

    data = select_columns(data, selected_columns)
    deposit = select_columns(deposit, selected_columns)

    all_output = concat_lines([data, deposit])

    if df_is_empty(all_output):
        return '{}'

    spent = summary_on_field(all_output, 'category')
    if df_is_empty(spent):
        return '{}'

    recap = concat_columns([spent, get_budgets(cycle)], keep_index_name=True).round(2).fillna(0).reset_index()
    recap = filter_df_not_these_values(recap, 'category', no_recap_categories)

    over = positive_part(- recap['amount'] - recap['budget'])
    remaining = positive_part(recap['budget'] + recap['amount'])
    spent = positive_part(- recap['amount'] - over)

    assign_new_column(recap, 'over', ceil_series(over) if round_it else over)
    assign_new_column(recap, 'remaining', floor_series(remaining) if round_it else remaining)
    assign_new_column(recap, 'spent', ceil_series(spent) if round_it else spent)

    drop_columns(recap, ['amount', 'budget'])

    return recap.to_json(orient="records")


def recap_by_account(cycle):
    field_name = 'account'
    selected_columns = ['amount', field_name]

    cycle = decode_cycle(cycle)
    data = get_data_from_cycle(cycle)

    all_outputs = select_columns(data, selected_columns)

    if cycle != 'all':
        deposit = get_deposit_debits_from_cycle(cycle)
        all_outputs = concat_lines([select_columns(deposit, selected_columns), all_outputs])

    return summary_on_field(all_outputs, field_name).round(decimals=2)


def recap_balances(cycle):
    this_cycle = recap_by_account(cycle).rename(columns={'amount': 'this_cycle'})
    all_times = recap_by_account('all').rename(columns={'amount': 'all_times'})

    balances = concat_columns([this_cycle, all_times])

    valid_accounts = set(this_cycle.index)
    balances = balances[balances.index.isin(valid_accounts)]

    balances['manual'] = balances.index.isin(auto_accounts) == False
    balances['currency'] = 'EUR'

    balances.reset_index(inplace=True)
    balances.rename(columns={'index': 'acc_name'}, inplace=True)
    return balances


"""

    valid_accounts = set(data_this_cycle['account'])

    data = data.groupby('account').apply(lambda x: x.sum(skipna=False))
    data_this_cycle = data_this_cycle.groupby('account').apply(lambda x: x.sum(skipna=False))

    data["all_times"] = data["amount"]
    data = data[["all_times"]]
    if df_is_not_empty(data_this_cycle):
        data_this_cycle["this_cycle"] = data_this_cycle[["amount"]]
        data_this_cycle = data_this_cycle[["this_cycle"]]
        balances = pd.concat([data[data.index.isin(valid_accounts)], data_this_cycle], axis=1, sort=False).fillna(0)
    else:
        balances = data
        add_field_with_default_value(balances, "this_cycle", 0)

    balances["currency"] = "EUR"
    balances.reset_index(inplace=True)
    balances['manual'] = balances['account'].isin(auto_accounts) == False
    balances.rename(columns={'account': 'acc_name'}, inplace=True)

    return balances.to_json(orient="records")
"""
