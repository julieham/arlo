from arlo.operations.date_operations import date_today, string_date_now
from arlo.tools.autofill_manager import autofill_single_value, read_autofill_dictionary, make_dictioname
from operations.df_operations import concat_columns
from operations.series_operations import series_swap_index_values

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
    start_cycles = series_swap_index_values(d.drop_duplicates(keep='first')).rename('first')
    return start_cycles.index.tolist()


def cycles_before_after(cycle):
    cycle = decode_cycle(cycle)
    cycle_names = get_names_cycles_ordered()
    try:
        index_cycle = cycle_names.index(cycle)
    except ValueError:
        return []
    before_after = cycle_names[index_cycle - 1:index_cycle] + cycle_names[index_cycle + 1:index_cycle + 7]
    return before_after


def nb_days_in_cycle(cycle='now'):
    all_days = all_days_in_cycle(cycle)
    days_done = [date for date in all_days if date <= string_date_now()]
    return dict({'days_done': len(days_done), 'all_days': len(all_days)})


def all_days_in_cycle(cycle):
    d = read_autofill_dictionary(make_dictioname('date', 'cycle'))
    return d[d == decode_cycle(cycle)].index.tolist()


def cycle_is_finished(cycle):
    days = nb_days_in_cycle(cycle)
    return days['days_done'] == days['all_days']
