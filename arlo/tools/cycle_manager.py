import datetime

from arlo.operations.date_operations import date_today, string_date_now
from arlo.tools.autofill_manager import autofill_single_value, read_autofill_dictionary, make_dictioname, edit_reference
from operations.df_operations import concat_columns, apply_function_to_field_overrule, filter_df_on_bools, get_one_field
from operations.series_operations import series_swap_index_values
from parameters.column_names import cycle_col, date_col
from web.status import success_response

source = 'date'
destination = 'cycle'


def date_to_cycle(date):
    auto_cycle = autofill_single_value(date, source, destination)
    return auto_cycle if auto_cycle != '-' else date.strftime('%B')[:3] + date.strftime('%y')


def cycle_now():
    return date_to_cycle(date_today())


def decode_cycle(cycle):
    if cycle == 'now':
        return cycle_now()
    return cycle


def filter_df_on_cycle(df, cycle):
    cycle = decode_cycle(cycle)
    if cycle == 'all':
        return df
    return df[df['cycle'] == cycle]


def get_first_last_day_cycles():
    d = read_autofill_dictionary(make_dictioname('date', 'cycle'))
    start_cycles = series_swap_index_values(d.drop_duplicates(keep='first')).rename('first')
    end_cycles = series_swap_index_values(d.drop_duplicates(keep='last')).rename('last')
    return concat_columns([start_cycles, end_cycles])


def get_names_cycles_ordered():
    d = read_autofill_dictionary(make_dictioname('date', 'cycle'))
    start_cycles = series_swap_index_values(d.drop_duplicates(keep='last')).rename('last')
    return start_cycles.reset_index()[['cycle']]


def cycle_is_travel(cycle):
    month_names_3_letters = [datetime.date(1900, j + 1, 1).strftime('%b') for j in range(12)]
    cycle = decode_cycle(cycle)
    return (len(cycle) != 5) or cycle[:3] not in month_names_3_letters or not cycle[-2:].isnumeric()


def cycles_before_after(cycle, exclude=False, long=False):
    num_life_past = -2 if long else 3
    num_life_future = 8 if long else 3

    cycle = decode_cycle(cycle)
    cycle_names = get_names_cycles_ordered()
    apply_function_to_field_overrule(cycle_names, cycle_col, cycle_is_travel, destination='travel')
    travel_cycles = filter_df_on_bools(cycle_names, cycle_names['travel']).index.tolist()
    life_cycles = filter_df_on_bools(cycle_names, cycle_names['travel'], keep=False).index.tolist()

    try:
        index_cycle = min(cycle_names[cycle_names[cycle_col] == cycle].index)

        travel_past = [u for u in travel_cycles if u < index_cycle]
        travel_future = [u for u in travel_cycles if u >= index_cycle]
        life_past = [u for u in life_cycles if u < index_cycle]
        life_future = [u for u in life_cycles if u >= index_cycle]

        selected_indexes = sorted(
            travel_past[-2:] + travel_future + life_past[num_life_past:] + life_future[:num_life_future])
        selected_cycles = get_one_field(cycle_names.iloc[selected_indexes], cycle_col).tolist()
        if exclude:
            selected_cycles = [u for u in selected_cycles if u != cycle]
        return selected_cycles

    except ValueError:
        return []


def nb_days_in_cycle(cycle='now'):
    all_days = all_days_in_cycle(cycle)
    days_done = [date for date in all_days if date <= string_date_now()]
    return dict({'days_done': len(days_done), 'all_days': len(all_days)})


def cycle_overview_to_cycle_progress(overview):
    if overview['all_days'] == 0:
        return 100
    return 100 * overview['days_done'] / overview['all_days']


def all_days_in_cycle(cycle):
    d = read_autofill_dictionary(make_dictioname('date', 'cycle'))
    return d[d == decode_cycle(cycle)].index.tolist()


def cycle_is_finished(cycle):
    days = nb_days_in_cycle(cycle)
    return days['days_done'] == days['all_days']


def progress(cycle):
    days_in_cycle_overview = nb_days_in_cycle(cycle)
    return cycle_overview_to_cycle_progress(days_in_cycle_overview)


def set_dates_to_cycle(dates, cycle):
    for date in dates:
        edit_reference(date_col, cycle_col, date, cycle)
    return success_response()
