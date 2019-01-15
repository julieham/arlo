import pandas as pd


def string_to_float(string):
    return float('0' + str(string))


def dict_to_dfreadable_dict(dictionary):
    return dict({d:[dictionary[d]] for d in dictionary})


def string_is_AA(string):
    return string == 'AA'


def dict_to_df(dictionary):
    dictionary = dict_to_dfreadable_dict(dictionary)
    df = pd.DataFrame(dictionary)
    return df


def dict_add_value_if_not_present(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = value


def df_field_to_numeric(df, field):
    df[field] = pd.to_numeric(df[field])


def series_to_dict(serie):
    return dict({a: serie[a] for a in serie.index})
