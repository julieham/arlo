from crud import read_dictionary

recurring_filename = './data/recurring.txt'

default_fields = read_dictionary(recurring_filename)


def has_default_fields(name):
    return name in default_fields


def get_default_fields(name):
    if name in default_fields:
        amount_str, account = default_fields[name]
        return dict({'amount': amount_str, 'account': account})
