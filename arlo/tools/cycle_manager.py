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
    dates = read_autofill_dictionary(make_dictioname('date', 'cycle'))
    cycle = decode_cycle(cycle)
    dates_this_cycle = dates[dates == cycle]
    total = dates_this_cycle.shape[0]
    return sum(dates_this_cycle.index <= string_date_now()), total
