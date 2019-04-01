import json

from operations.date_operations import two_next_cycles
from operations.series_operations import filter_series_on_value
from operations.types_operations import sorted_set
from parameters.param import column_names_for_front
from read_write.file_manager import read_data
from services.services import refresh_data
from tools.autofill_manager import read_autofill_dictionary
from tools.cycle_manager import cycle_now, filter_df_on_cycle
from tools.recap_by_category import get_budgets
from tools.recurring_manager import get_possible_recurring
from tools.uniform_data_maker import format_for_front


def all_categories(cycle='now'):
    budgets = get_budgets(cycle)
    return json.dumps(list(budgets.index) + ['Input'])


def all_accounts():
    accounts = filter_series_on_value(read_autofill_dictionary('account-to-active'), True)
    return json.dumps(list(accounts.index))


def all_cycles():
    data = read_data().sort_values("date", ascending=False).reset_index(drop=True)
    all_cycles_with_duplicates = list(data['cycle'])
    set_cycles = sorted_set(all_cycles_with_duplicates[::-1] + two_next_cycles())
    now_index = set_cycles.index(cycle_now())
    return json.dumps(set_cycles[now_index-3:now_index+3])


def all_recurring():
    return get_possible_recurring()


def data(refresh=None, cycle="now"):
    if refresh:
        refresh_data()

    data = read_data()
    # TODO recup link disappearing transactions

    data = filter_df_on_cycle(data, cycle)
    format_for_front(data)

    return data[column_names_for_front].to_json(orient="records")
