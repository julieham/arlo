import pandas as pd

recurring_filename = './arlo/data/recurring.txt'


def has_default_fields(name):
    return name in pd.read_csv(recurring_filename, sep=";", index_col=0).index


def get_default_fields(name):
    default = pd.read_csv(recurring_filename, sep=";", index_col=0)
    if name in default.index:
        return dict({'amount': default.at[name, 'amount'], 'account': default.at[name, 'account']})


def get_possible_recurring(cycle):
    return list(pd.read_csv(recurring_filename, sep=";", index_col=0).index)