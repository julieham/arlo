import pandas as pd

from formatting import is_pending

data_file = "./data/data.csv"


def read_data():
    data = pd.read_csv(data_file)
    data['date'] = pd.to_datetime(data['date'])
    data['pending'] = data.apply(lambda row: is_pending(row), axis=1)
    return data


def read_dico(filename):
    dictionary = dict()
    f = open(filename, 'r')
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
    data = data.sort_values("date", ascending=False).reset_index(drop=True)
    data.to_csv(data_file, index=False)


def merge_data(new_data):
    # old_data = read_data()
    return new_data
    # new = new_data[new_data['id'].isin(list(old_data['id'])) == False]
    # gone = old_data[old_data['id'].isin(list(new_data['id'])) is False]
    # if gone.shape[0] > 0:
    #     print('-'*40 + '\n' + 'DISAPPEARED TRANSACTIONS' + '\n' + '-'*40)
    #     print(gone)
    # TODO : make this smarter
    # return new_data


def change_one_field_on_ids(transaction_ids, field_name, field_value):
    data = read_data()
    data.loc[data['id'].isin(transaction_ids), [field_name]] = field_value
    save_data(data)


def change_last_update_to_now():
    with open("last_update.txt", mode='w') as file:
        file.write("%s" % pd.datetime.now())


def get_delay_since_last_update():
    try:
        with open("last_update.txt", mode='r') as file:
            last_update = file.read()
            delta = pd.datetime.now() - pd.datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S.%f')
            print(delta)
            return delta.total_seconds()//60
    except FileNotFoundError:
        return 1000000


def add_data_line(line):
    with open(data_file, 'r+') as f:
        content = f.readlines()
        content.insert(1, line + "\n")
        f.seek(0)
        f.write("".join(content))
