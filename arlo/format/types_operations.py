import pandas as pd


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
