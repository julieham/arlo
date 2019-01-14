import codecs
import json
from format.df_operations import *
from format.date_operations import *
from param import *

data_file = "./data/data.csv"
last_update_file = "last_update.txt"

def read_data():
    return read_data_from_file(data_file)


def read_data_from_file(filename):
    data = pd.read_csv(filename, na_values=' ')
    apply_function_to_field_overrule(data, 'date', pd.to_datetime)
    return data


def read_dico(filename):
    dictionary = dict()
    f = codecs.open(filename,'r',encoding='utf8')
    for line in f:
        try:
            line = line.replace('\n', '')
            a, b = line.split(',')
            dictionary[a] = b
        except ValueError:
            pass
    f.close()
    return dictionary


def write_sorted_dico(dico, filename):
    f = open(filename, 'w')
    for d in sorted(dico, key=lambda k: dico[k][1].lower() + dico[k][0].lower()):
        f.write(d + "," + ','.join(dico[d]) + '\n')
    f.close()


def save_data(data):
    save_data_in_file(data, data_file)


def save_data_in_file(data, filename):
    data = sort_df_by_descending_date(data)
    data.to_csv(filename, index=False)


def set_field_to_value_on_ids(ids, field_name, field_value):
    data = read_data()
    change_field_on_ids_to_value(data, ids, field_name, field_value)
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


def write_json_dict(filename, dico):
    with open(filename, 'w') as f:
        json.dump(dico, f, separators=[",\n", ":"])


def read_json_dict(filename):
    with open(filename, "r") as read_file:
        data = json.load(read_file)
    return data


def add_to_data(transaction_df):
    data = read_data()
    data = pd.concat([transaction_df, data], sort=False).sort_values("date", ascending=False).reset_index(drop=True)
    save_data(data[column_names])
