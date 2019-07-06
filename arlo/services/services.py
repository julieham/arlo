from arlo.parameters.param import *
from arlo.read_write.file_manager import add_new_data, set_field_to_value_on_ids
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.refresh import minutes_since_last_update, change_last_update_to_now
from operations.df_operations import select_columns
from parameters.credentials import login_N26
from read_write.select_data import get_deposit_input_and_output
from tools.backup_email import send_email_backup_data
from tools.clean_n26 import get_last_transactions_as_df
from tools.logging import info, warn
from tools.merge_data import merge_with_data
from tools.split import split_transaction_if_possible
from tools.summary_by_field import recap_by_cat, recap_balances, summary_on_field, recap_by_account
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
    return recap_by_cat(cycle, False).drop(columns=['amount', 'budget']).to_json(orient="records")


def get_balances(cycle='now'):
    # TODO clean this out
    return recap_balances(cycle).to_json(orient="records")


def cycle_balances(cycle):
    return recap_by_account(cycle)


def bank_balances(cycle):
    # TODO select interesting accounts
    return recap_by_account('all')


def refresh_lunchr():
    add_new_data(get_latest_lunchr())


def split_transaction(fields):
    return split_transaction_if_possible(fields)


def create_deposit_debit(id_tr, deposit_name):
    set_field_to_value_on_ids([id_tr], deposit_name_col, deposit_name)
    set_field_to_value_on_ids([id_tr], cycle_col, deposit_name_col)


def get_state_deposit():
    selected_columns = [deposit_name_col, amount_euro_col]

    deposit = select_columns(get_deposit_input_and_output(), selected_columns)
    return summary_on_field(deposit, deposit_name_col)
