import codecs
import json
from df_operations import *
from date_operations import *


data_file = "./data/data.csv"


def read_data():
    return read_data_from_file(data_file)


def read_data_from_file(filename):
    data = pd.read_csv(filename, na_values=' ')
    apply_function_to_field(data, 'date', pd.to_datetime)
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
    with open("last_update.txt", mode='w') as file:
        file.write("%s" % pd.datetime.now())


def minutes_since_last_update():
    try:
        with open("last_update.txt", mode='r') as file:
            last_update = to_datetime(file.read())
            return time_since(last_update).total_seconds()//60
    except FileNotFoundError:
        return 1000000


def add_data_line(line):
    with open(data_file, 'r+') as f:
        content = f.readlines()
        content.insert(1, line + "\n")
        f.seek(0)
        f.write("".join(content))


def write_json_dict(filename, dico):
    with open(filename, 'w') as f:
        json.dump(dico, f, separators=[",\n", ":"])


def read_json_dict(filename):
    with open(filename, "r") as read_file:
        data = json.load(read_file)
    return data

