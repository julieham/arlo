from arlo.parameters.param import *
from arlo.read_write.file_manager import add_new_data, set_field_to_value_on_ids
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.refresh import minutes_since_last_update, change_last_update_to_now
from operations.df_operations import select_columns, drop_columns
from parameters.credentials import login_N26
from read_write.select_data import get_deposit_input_and_output
from tools.backup_email import send_email_backup_data
from tools.clean_bankin import get_latest_bankin
from tools.clean_n26 import get_latest_n26
from tools.logging import info, warn
from tools.merge_data import merge_with_data
from tools.split import split_transaction_if_possible
from tools.summary_by_field import recap_by_cat, group_by_field, recap_by_account
from tools.transfers import balances_to_transfers, get_end_of_cycle_balances
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
        status, latest_data = get_latest_n26(account)
        merge_with_data(latest_data, account)
        all_status.append(status)
    return merge_status(all_status[0], all_status[1])


def force_refresh():
    info('Refreshing')
    send_email_backup_data()
    refresh_lunchr()
    refresh_bankin()
    status_n26 = refresh_n26()
    if is_successful(status_n26):
        change_last_update_to_now()
    return status_n26


def get_transfers_to_do(cycle):
    balances = get_end_of_cycle_balances(cycle)
    return balances_to_transfers(balances)


def get_recap_categories(cycle='now'):
    recap = recap_by_cat(cycle, False)
    drop_columns(recap, ['amount', 'budget'])
    return recap.to_json(orient="records")


def cycle_balances(cycle):
    return recap_by_account(cycle)


def bank_balances(cycle):
    # TODO select interesting accounts
    return recap_by_account('all')


def refresh_lunchr():
    add_new_data(get_latest_lunchr())


def refresh_bankin():
    add_new_data(get_latest_bankin())


def split_transaction(fields):
    return split_transaction_if_possible(fields)


def create_deposit_debit(id_tr, deposit_name):
    set_field_to_value_on_ids([id_tr], deposit_name_col, deposit_name)


def get_state_deposit():
    selected_columns = [deposit_name_col, amount_euro_col]

    deposit = select_columns(get_deposit_input_and_output(), selected_columns)
    return group_by_field(deposit, deposit_name_col)
