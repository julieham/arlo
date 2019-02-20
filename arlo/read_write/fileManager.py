import pandas as pd

from arlo.format.date_operations import string_to_datetime, time_since
from arlo.format.df_operations import apply_function_to_field_overrule, sort_df_by_descending_date,\
    change_field_on_several_ids_to_value
from arlo.parameters.param import column_names, directory
from arlo.read_write.reader import read_df_file

data_file = directory + "data.csv"
last_update_file = directory + "last_update.txt"


def read_data():
    return read_data_from_file(data_file)


def read_data_from_file(filename):
    data = read_df_file(filename, sep=',')
    apply_function_to_field_overrule(data, 'date', string_to_datetime)
    return data


def save_data(data):
    save_data_in_file(data, data_file)


def save_data_in_file(data, filename):
    data = sort_df_by_descending_date(data)[column_names]
    data.to_csv(filename, index=False)


def set_field_to_value_on_ids(ids, field_name, field_value):
    data = read_data()
    change_field_on_several_ids_to_value(data, ids, field_name, field_value)
    save_data(data)


def change_last_update_to_now():
    with open(last_update_file, mode='w') as file:
        file.write("%s" % pd.datetime.now())


def get_last_update_string():
    try:
        with open(last_update_file, mode='r') as file:
            return file.read()
    except FileNotFoundError:
        return "1900-01-01 01:00:00.0"


def minutes_since_last_update():
    last_update = string_to_datetime(get_last_update_string())
    return time_since(last_update).total_seconds()//60


def add_to_data(transaction_df):
    data = read_data()
    data = pd.concat([transaction_df, data], sort=False).sort_values("date", ascending=False).reset_index(drop=True)
    save_data(data[column_names])
