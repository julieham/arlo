from arlo.operations.df_operations import sort_df_by_descending_date, change_field_on_several_ids_to_value, \
    concat_lines, null_value, df_is_not_empty, disable_chained_assignment_warning, filter_df_not_this_value
from arlo.parameters.param import data_columns_all, data_directory, default_values, deposit_columns_all
from arlo.read_write.reader import read_df_file
from arlo.read_write.writer import write_df_to_csv
from operations.date_operations import date_parser_for_reading
from tools.logging import info, warn, info_df
from web.status import success_response, failure_response

transactions_file = data_directory + "data.csv"
provisions_file = data_directory + "provisions.csv"
recurring_deposit_file = data_directory + "recurring_deposit.csv"
last_update_file = data_directory + "last_update.txt"


def read_data():
    return read_data_from_file(transactions_file)


def save_data(data):
    save_data_in_file(data, transactions_file)


def read_deposit_input():
    return read_data_from_file(provisions_file)


def save_deposit(data):
    save_deposit_in_file(data, provisions_file)


def read_data_from_file(filename):
    data = read_df_file(filename, parse_dates=['date'], date_parser=date_parser_for_reading)
    return data.dropna(how='all')


def save_data_in_file(data, filename):
    save_df_in_file(data[data_columns_all], filename)


def save_deposit_in_file(data, filename):
    save_df_in_file(data[deposit_columns_all], filename)


def read_recurring_deposit():
    return read_df_file(recurring_deposit_file)


def save_df_in_file(df, filename):
    disable_chained_assignment_warning()
    df.dropna(how='all', inplace=True)
    df.drop_duplicates(inplace=True)
    sort_df_by_descending_date(df)
    write_df_to_csv(df, filename, index=False)


def set_field_to_value_on_ids(ids, field_name, field_value):
    if field_name == 'id':
        return failure_response('Impossible to change id')
    data = read_data()
    change_field_on_several_ids_to_value(data, ids, field_name, field_value)
    save_data(data)
    return success_response()


def default_value(field):
    if field in default_values:
        return default_values[field]
    return null_value()


def reset_field_on_ids(ids, field_name):
    field_value = default_value(field_name)
    if field_value == null_value():
        return False
    set_field_to_value_on_ids(ids, field_name, field_value)


def change_last_update_to_this_date(date):
    with open(last_update_file, mode='w') as file:
        file.write("%s" % date)


def get_last_update_string():
    try:
        with open(last_update_file, mode='r') as file:
            return file.read()
    except FileNotFoundError:
        return "1900-01-01 01:00:00.0"


def add_new_data(new_data):
    if df_is_not_empty(new_data):
        warn('#add_data ------- Adding : -------')
        info_df(new_data)
        info('#add_data -----------------------------\n')
        data = concat_lines([read_data(), new_data])
        save_data(data)


def add_new_deposit(new_dep):
    if df_is_not_empty(new_dep):
        warn('#add_deposit ------- Adding DEPOSIT: -------')
        info_df(new_dep)
        info('#add_deposit -----------------------------\n')
        deposit = concat_lines([read_deposit_input(), new_dep])
        save_deposit(deposit)


def read_series(filename, parse_dates=False):
    return read_df_file(filename, sep=';', index_col=0, squeeze=True, parse_dates=parse_dates)


def write_dictionary_to_file(dictionary, filename):
    write_df_to_csv(dictionary, filename)


def remove_data_on_id(id_to_remove):
    data = filter_df_not_this_value(read_data(), 'id', id_to_remove)
    save_data(data)
