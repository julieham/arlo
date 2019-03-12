from arlo.operations.date_operations import date_today
from arlo.tools.autofillManager import autofill_single_value


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