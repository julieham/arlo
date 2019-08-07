import json

from operations.df_operations import select_columns
from operations.series_operations import filter_series_on_value
from parameters.column_names import deposit_name_col
from parameters.param import data_columns_front
from read_write.file_manager import read_recurring_deposit
from read_write.select_data import get_data_this_cycle_not_deposit, get_deposit_input_and_output
from services.services import refresh_data
from tools.autofill_manager import read_autofill_dictionary
from tools.cycle_manager import cycle_now, cycles_before_after
from tools.recurring_manager import get_possible_recurring
from tools.summary_by_field import get_budgets
from tools.uniform_data_maker import format_for_front


def all_categories(cycle='all'):
    budgets = get_budgets(cycle)
    return json.dumps(sorted(list(budgets.index) + ['Input']))


def all_accounts():
    accounts = filter_series_on_value(read_autofill_dictionary('account-to-active'), True)
    return json.dumps(list(accounts.index))


def all_cycles():
    cycle_today = cycle_now()
    surrounding_cycles = cycles_before_after(cycle_today)
    cycles = {'all_cycles': surrounding_cycles,
              'current_cycle': cycle_today}
    return json.dumps(cycles)


def local_cycles(cycle):
    return json.dumps(cycles_before_after(cycle, exclude=True))


def all_recurring():
    return list(get_possible_recurring().index)


def data(refresh=None, cycle="now"):
    if refresh:
        refresh_data()

    this_cycle_data = get_data_this_cycle_not_deposit(cycle)
    format_for_front(this_cycle_data)

    return select_columns(this_cycle_data, data_columns_front).to_json(orient="records")


def all_recurring_deposit():
    return read_recurring_deposit().to_json(orient="records")


def all_deposit_names():
    return sorted(set(get_deposit_input_and_output()[deposit_name_col]))


def cycle_budgets(cycle):
    return get_budgets(cycle)
