from arlo.parameters.param import data_directory
from arlo.read_write.reader import read_df_file

recurring_filename = data_directory + 'recurring.txt'


def get_default_recurring():
    return read_df_file(recurring_filename, sep=';', index_col=0)


def has_default_fields(name):
    return name in get_default_recurring().index


def get_default_fields(name):
    default = get_default_recurring()
    if name in default.index:
        return dict({column_name: default.at[name, column_name] for column_name in default.columns})
        # return dict({'amount': default.at[name, 'amount'], 'account': default.at[name, 'account']})


def get_possible_recurring():
    return get_default_recurring()
