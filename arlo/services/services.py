import pandas as pd

from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.cycleManager import decode_cycle, filter_df_on_cycle
from arlo.tools.recap_by_category import get_categories_recap
from arlo.operations.data_operations import missing_valid_amount, missing_mandatory_field
from arlo.operations.df_operations import df_is_not_empty, concat_lines
from arlo.operations.types_operations import dict_to_df_with_all_columns
from arlo.parameters.credentials import login_N26
from arlo.parameters.param import *
from arlo.read_write.fileManager import save_data, read_data, add_new_data
from arlo.tools.clean_n26 import get_last_transactions_as_df
from arlo.tools.recurringManager import fill_missing_with_default_values
from arlo.tools.merge_data import merge_data
from arlo.tools.refresh import minutes_since_last_update, change_last_update_to_now
from arlo.tools.uniform_data_maker import format_n26_df, format_manual_transaction, \
    format_recurring_transaction


# %% SERVICES


def refresh_data():
    print('REFRESHING ? ')
    if minutes_since_last_update() > delay_refresh_minutes:
        print('YES')
        print(force_refresh())
    else:
        print('NO')


def force_refresh():
    print('FORCE REFRESH')
    refresh_lunchr()
    status_refresh_n26 = refresh_n26()

    if status_refresh_n26 == 'SUCCESS':
        change_last_update_to_now()
        return 'SUCCESS'
    return 'FAIL'


def refresh_n26():
    all_valid, all_data = True, []
    for account in login_N26:
        valid, data = get_last_transactions_as_df(account, n26_max_transactions_per_user)
        all_valid = all_valid and valid
        all_data.append(format_n26_df(data, account))
    all_valid = True

    if not all_valid:
        print('REFRESH N26 FAILED')
        return 'FAIL'

    save_data(merge_data(read_data(), concat_lines(all_data)))
    return 'SUCCESS'


def create_manual_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)

    if missing_valid_amount(df):
        return 'FAIL : no valid amount'

    if missing_mandatory_field(df):
        return 'FAIL : missing mandatory field'

    format_manual_transaction(df)
    add_new_data(df)

    return 'SUCCESS'


def get_recap_categories(cycle='now'):
    data = read_data()

    data = filter_df_on_cycle(data, cycle)

    # TODO make generic function with balances

    recap = pd.DataFrame()

    if df_is_not_empty(data):
        recap = get_categories_recap(data, cycle)

    recap = recap[recap['category'] != "Link"]

    return recap.to_json(orient="records")


def get_balances(cycle='now'):
    cycle = decode_cycle(cycle)
    data = read_data()
    data = data[['amount', 'account', 'cycle']]

    data_this_cycle = data[data['cycle'] == cycle]

    data = data.groupby('account').apply(lambda x: x.sum(skipna=False))
    data_this_cycle = data_this_cycle.groupby('account').apply(lambda x: x.sum(skipna=False))
    data["all_times"] = data["amount"]
    data_this_cycle["this_cycle"] = data_this_cycle[["amount"]]

    data = data[["all_times"]]
    data_this_cycle = data_this_cycle[["this_cycle"]]

    balances = pd.concat([data, data_this_cycle], axis=1, sort=False).fillna(0)
    balances["currency"] = "EUR"
    balances.index.names = ['account_name']

    return balances


def create_recurring_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)
    fill_missing_with_default_values(df)

    if missing_valid_amount(df):
        return 'FAIL : no valid amount'

    if missing_mandatory_field(df):
        return 'FAIL : missing mandatory field'

    format_recurring_transaction(df)
    add_new_data(df)

    print('SUCCESS')


def refresh_lunchr():
    add_new_data(get_latest_lunchr())


