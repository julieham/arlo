import json

from arlo.operations.df_operations import select_columns
from arlo.operations.series_operations import filter_series_on_value
from arlo.parameters.column_names import deposit_name_col
from arlo.parameters.param import data_columns_front
from arlo.read_write.file_manager import read_recurring_deposit
from arlo.read_write.select_data import get_data_this_cycle_not_deposit, get_deposit_input_and_output
from arlo.tools.autofill_manager import read_autofill_dictionary
from arlo.tools.cycle_manager import cycle_now, cycles_before_after
from arlo.tools.recurring_manager import get_possible_recurring
from arlo.tools.summary_by_field import get_budgets
from arlo.tools.uniform_data_maker import format_for_front


def all_categories(cycle='all'):
    budgets = get_budgets(cycle)
    return json.dumps(sorted(list(budgets.index) + ['Input']))


def all_accounts():
    accounts = filter_series_on_value(read_autofill_dictionary('account-to-active'), True)
    return json.dumps(list(accounts.index))


def all_currencies():
    currencies = filter_series_on_value(read_autofill_dictionary('currency-to-active'), True)
    return json.dumps(list(currencies.index))


def all_cycles():
    cycle_today = cycle_now()
    surrounding_cycles = cycles_before_after(cycle_today)
    cycles = {'all_cycles': surrounding_cycles,
              'current_cycle': cycle_today}
    return json.dumps(cycles)


def local_cycles(cycle, long=False):
    return json.dumps(cycles_before_after(cycle, exclude=True, long=long))


def all_recurring():
    return list(get_possible_recurring().index)


def data(cycle="now"):
    this_cycle_data = get_data_this_cycle_not_deposit(cycle)
    format_for_front(this_cycle_data)

    return select_columns(this_cycle_data, data_columns_front).to_json(orient="records")


def all_recurring_deposit():
    return read_recurring_deposit().to_json(orient="records")


def all_deposit_names():
    return sorted(set(get_deposit_input_and_output()[deposit_name_col]))


def cycle_budgets(cycle):
    return get_budgets(cycle)
