import pandas as pd

from arlo.format.df_operations import vertical_concat


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
    df = pd.DataFrame(dictionary)
    return df


def dict_add_value_if_not_present(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = value


def df_field_to_numeric_with_sign(df, field, is_positive):
    df[field] = pd.to_numeric(df[field])
    if not is_positive:
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
    return vertical_concat(payments)
