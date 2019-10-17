from arlo.operations.date_operations import get_calendar_around, now
from arlo.operations.df_operations import select_columns, add_column_with_value
from arlo.operations.types_operations import json_to_df
from arlo.parameters.credentials import login_N26
from arlo.parameters.param import *
from arlo.read_write.file_manager import add_new_data, set_field_to_value_on_ids
from arlo.read_write.select_data import get_deposit_input_and_output
from arlo.tools.autofill_manager import read_autofill_dictionary, make_dictioname
from arlo.tools.backup_email import save_backup_with_data
from arlo.tools.budgets_manager import edit_budgets_cycle
from arlo.tools.clean_bankin import get_latest_bankin, force_refresh_bankin
from arlo.tools.clean_lunchr import get_latest_lunchr
from arlo.tools.clean_n26 import get_latest_n26
from arlo.tools.cycle_manager import set_dates_to_cycle
from arlo.tools.logging import info, error
from arlo.tools.merge_data import merge_with_data
from arlo.tools.refresh import change_last_update_to_now
from arlo.tools.split import split_transaction_if_possible
from arlo.tools.summary_by_field import group_by_field, recap_by_account, get_category_groups, input_recap
from arlo.tools.transfers import balances_to_transfers, get_end_of_cycle_balances
from arlo.web.status import is_successful, merge_status
from tools.errors import N26TokenError


def refresh_n26():
    all_status = []
    for name in login_N26:
        try:
            latest_data = get_latest_n26(name)
            merge_with_data(latest_data, name)
        except ValueError:
            print('youpla')
            error('Could not refresh ' + name)
    return merge_status(all_status)


def force_refresh():
    info('Refreshing')
    save_backup_with_data()
    refresh_lunchr()
    refresh_bankin()
    status_n26 = refresh_n26()
    if is_successful(status_n26):
        change_last_update_to_now()
    return status_n26


def force_api_refresh():
    force_refresh_bankin()


def get_transfers_to_do(cycle):
    balances = get_end_of_cycle_balances(cycle)
    return balances_to_transfers(balances)


def get_recap_categories(cycle='now'):
    recap = get_category_groups(cycle).sort_values(category_col)
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
    # TODO Take into account category input deposit fluff ?????
    set_field_to_value_on_ids([id_tr], deposit_name_col, deposit_name)
    set_field_to_value_on_ids([id_tr], category_col, deposit_name_col.title())


def delete_deposit_debit(id_tr):
    set_field_to_value_on_ids([id_tr], deposit_name_col, '')
    set_field_to_value_on_ids([id_tr], category_col, default_values[category_col])


def get_state_deposit(filter_null):
    selected_columns = [deposit_name_col, amount_euro_col]
    deposit = select_columns(get_deposit_input_and_output(), selected_columns)
    add_column_with_value(deposit, currency_col, default_currency)
    deposit_state = group_by_field(deposit, deposit_name_col).round(decimals=2)
    if filter_null:
        deposit_state = deposit_state[deposit_state != 0]
    return deposit_state


def edit_budgets(budgets, cycle):
    budgets_df = json_to_df(budgets, orient='records')
    add_column_with_value(budgets_df, cycle_col, cycle)
    return edit_budgets_cycle(budgets_df)


def cycle_calendar():
    date = now()
    calendar = read_autofill_dictionary(make_dictioname(date_col, cycle_col))
    return get_calendar_around(calendar, date)


def edit_calendar(dates, cycle):
    return set_dates_to_cycle(dates, cycle)


def input_overview(cycle):
    return input_recap(cycle)
