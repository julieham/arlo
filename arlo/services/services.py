import pandas as pd

from arlo.operations.df_operations import df_is_not_empty, add_field_with_default_value, filter_df_not_provisions
from arlo.parameters.param import *
from arlo.read_write.file_manager import read_data, add_new_data
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.cycle_manager import decode_cycle, filter_df_on_cycle
from arlo.tools.recap_by_category import get_categories_recap
from arlo.tools.refresh import minutes_since_last_update, change_last_update_to_now
from parameters.credentials import login_N26
from tools.backup_email import send_email_backup_data
from tools.clean_n26 import get_last_transactions_as_df
from tools.logging import info, warn
from tools.merge_data import merge_with_data
from tools.split import split_transaction_if_possible
from web.status import is_successful, merge_status


def refresh_data():
    warn('REFRESHING ? ')
    if minutes_since_last_update() > delay_refresh_minutes:
        warn('YES')
        info(force_refresh())
    else:
        warn('NO')


def refresh_n26():
    all_status = []
    for account in login_N26:
        status, latest_data = get_last_transactions_as_df(account)
        merge_with_data(latest_data, account)
        all_status.append(status)
    return merge_status(all_status[0], all_status[1])


def force_refresh():
    info('Refreshing')
    send_email_backup_data()
    refresh_lunchr()
    status_n26 = refresh_n26()
    if is_successful(status_n26):
        change_last_update_to_now()
    return status_n26


def get_recap_categories(cycle='now'):
    data = read_data()

    data = filter_df_on_cycle(data, cycle)

    # TODO make generic function with balances

    recap = pd.DataFrame()

    if df_is_not_empty(data):
        recap = get_categories_recap(data, cycle)

    if df_is_not_empty(recap):
        recap = recap[recap['category'] != "Link"]

    return recap.to_json(orient="records")


def get_balances(cycle='now'):
    cycle = decode_cycle(cycle)
    data = read_data()
    data = data[['amount', 'account', 'cycle', 'type']]

    data_this_cycle = filter_df_on_cycle(data, cycle)

    valid_accounts = set(data_this_cycle['account'])
    data = filter_df_not_provisions(data)

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


def refresh_lunchr():
    add_new_data(get_latest_lunchr())


def split_transaction(fields):
    return split_transaction_if_possible(fields)
