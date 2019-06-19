import json

from operations.date_operations import two_next_cycles
from operations.df_operations import select_columns
from operations.series_operations import filter_series_on_value
from operations.types_operations import sorted_set
from parameters.param import column_names_for_front
from read_write.file_manager import read_data, read_recurring_deposit
from read_write.select_data import get_data_from_cycle
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
    data = read_data().sort_values("date", ascending=False).reset_index(drop=True)
    all_cycles_with_duplicates = list(data['cycle'])
    set_cycles = sorted_set(all_cycles_with_duplicates[::-1])
    now_index = set_cycles.index(cycle_now())
    selected_cycles = set_cycles[now_index - 3:now_index + 2] + two_next_cycles()
    cycles = {'all_cycles': ['Lagos19', 'Feb19', 'Mar19', 'Apr19', 'Pyr19', 'May19', 'Jun19', 'DK19', 'Cali19'],
              'current_cycle': 'May19'}
    return json.dumps(cycles)


def local_cycles(cycle):
    return json.dumps(cycles_before_after(cycle))


def all_recurring():
    return list(get_possible_recurring().index)


def data(refresh=None, cycle="now"):
    if refresh:
        refresh_data()

    this_cycle_data = get_data_from_cycle(cycle)
    format_for_front(this_cycle_data)

    return select_columns(this_cycle_data, column_names_for_front).to_json(orient="records")


def all_deposit():
    return read_recurring_deposit().to_json(orient="records")
