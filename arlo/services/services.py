from arlo.parameters.param import *
from arlo.read_write.file_manager import add_new_data
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.refresh import minutes_since_last_update, change_last_update_to_now
from parameters.credentials import login_N26
from tools.backup_email import send_email_backup_data
from tools.clean_n26 import get_last_transactions_as_df
from tools.logging import info, warn
from tools.merge_data import merge_with_data
from tools.split import split_transaction_if_possible
from tools.summary_by_field import recap_by_cat, recap_balances
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
    return recap_balances(cycle).to_json(orient="records")


def refresh_lunchr():
    add_new_data(get_latest_lunchr())


def split_transaction(fields):
    return split_transaction_if_possible(fields)
