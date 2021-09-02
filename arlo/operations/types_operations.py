import hashlib

import pandas as pd

from arlo.operations.df_operations import concat_lines, new_dataframe


def string_to_float(string):
    try:
        return float(string)
    except ValueError:
        return 0


def dict_to_dfreadable_dict(dictionary):
    return dict({d: [dictionary[d]] for d in dictionary})


def string_is_AA(string):
    return string == 'AA'


def dict_to_df(dictionary):
    dictionary = dict_to_dfreadable_dict(dictionary)
    df = new_dataframe(dictionary)
    return df.mask(df == '')


def list_of_dict_to_df(dict_list):
    return pd.DataFrame(dict_list)


def dict_add_value_if_not_present(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = value


def df_field_to_numeric_with_sign(df, field, is_credit=True):
    df[field] = pd.to_numeric(df[field])
    if not is_credit:
        df[field] = - df[field]


def sorted_set(list_to_sort):
    seen = set()
    return [x for x in list_to_sort if not (x in seen or seen.add(x))]


def add_prefix_to_dict_keys(prefix, dictionary):
    return dict({prefix+key: item for key, item in dictionary.items()})


def unnest_dictionary_layers(dictionary):
    new_dictionary = dict()
    for key, item in dictionary.items():
        if type(item) == dict:
            new_dictionary.update(unnest_dictionary_layers(add_prefix_to_dict_keys(key + '_', item)))
        else:
            new_dictionary[key] = item
    return new_dictionary


def layered_dict_to_df(payments):
    payments = [dict_to_df(unnest_dictionary_layers(transaction)) for transaction in payments]
    return concat_lines(payments)


def encode_id(id_value):
    return hashlib.md5(id_value.encode()).hexdigest()


def clean_parenthesis(name):
    name = name.split(' (')
    return name[0]


def json_to_df(json_input, orient):
    return pd.read_json(json_input, orient=orient)


def string_to_bool(b):
    if type(b) is not str:
        return False
    return b.lower() == 'true'


def value_is_nan(value):
    return pd.isnull(value)
