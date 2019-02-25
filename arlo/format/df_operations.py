import pandas as pd

from arlo.format.date_operations import decode_cycle


def set_pandas_print_parameters():
    desired_width = 10000
    pd.set_option('display.width', desired_width)
    pd.np.set_printoptions(linewidth=desired_width)
    pd.set_option("display.max_columns", 100)


def enable_chained_assigment_warning():
    pd.set_option('mode.chained_assignment', 'warn')


def disable_chained_assigment_warning():
    pd.set_option('mode.chained_assignment', None)


def sort_df_by_descending_date(df):
    df.sort_values("date", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)


def make_empty_dataframe_based_on_this(df):
    empty_df = pd.DataFrame(index=None)
    for col, dtype in zip(df.columns, df.dtypes):
        empty_df[col] = pd.Series(dtype=dtype)
    return empty_df


def get_pending_transactions(df):
    return df[df['pending'] == True]


def get_refund_transactions(df):
    refunds = df[df['type'].isin(['AV', 'AE'])]
    refunds = refunds[refunds['link'] == '-']
    return refunds


def apply_function_to_field_no_overrule(df, field, function_to_apply, destination=''):
    if not destination:
        destination = field

    calculated_values = df[field].apply(function_to_apply)

    if destination not in df.columns.values:
        df[destination] = calculated_values
    else:
        df.loc[(pd.isnull(df[destination]), destination)] = calculated_values


def set_field_value_no_overrule(df, field, value):
    df.loc[(pd.isnull(df[field]))] = value


def apply_function_to_field_overrule(df, field, function_to_apply, destination=None):
    disable_chained_assigment_warning()
    if destination:
        df[destination] = df[field].apply(function_to_apply)
    else:
        df[field] = df[field].apply(function_to_apply)
    enable_chained_assigment_warning()


def add_field_with_default_value(df, field, value):
    disable_chained_assigment_warning()
    df[field] = value
    enable_chained_assigment_warning()


def add_prefix_to_column(df, prefix, column):
    disable_chained_assigment_warning()
    df.loc[:, column] = prefix + df[column]
    enable_chained_assigment_warning()


def get_ids(df):
    return set(df['id'])


def filter_df_one_value(df, field_name, field_value):
    return df[df[field_name] == field_value]


def filter_df_not_this_value(df, field_name, field_value):
    return df[df[field_name] != field_value]


def filter_df_several_values(df, field_name, field_values):
    return df[df[field_name].isin(field_values)]


def df_is_not_empty(df):
    return df.shape[0] > 0


def extract_line_from_df(index, df):
    line = df.loc[index]
    df.drop(index, inplace=True)
    return line


def change_field_on_several_ids_to_value(df, ids, field_name, field_value):
    df.loc[df['id'].isin(ids), [field_name]] = field_value


def change_field_on_single_id_to_value(df, id_value, field_name, field_value):
    df.loc[df['id'] == id_value, [field_name]] = field_value


def result_function_applied_to_field(df, field_name, function_to_apply):
    return function_to_apply(df[field_name])


def get_one_field(df, field_name):
    return df[field_name]


def how_many_rows(df):
    return df.shape[0]


def filter_df_on_cycle(df, cycle):
    cycle = decode_cycle(cycle)
    if cycle == 'all':
        return df

    return df[df['cycle'] == cycle]


def horizontal_concat(df1, df2):
    return pd.concat([df1, df2], axis=1, sort=False).fillna(0)


def vertical_concat(df_list):
    return pd.concat(df_list, axis=0, sort=False, ignore_index=True)


def make_a_df_from_dict(dictionary):
    return pd.DataFrame(data=dictionary).drop_duplicates()


def assign_new_column(df, column_name, column_content):
    disable_chained_assigment_warning()
    df[column_name] = column_content
    enable_chained_assigment_warning()


def drop_other_columns(df, fields):
    columns_to_remove = [column_name for column_name in df.columns.tolist() if column_name not in set(fields)]
    df.drop(columns=columns_to_remove, inplace=True)


def remove_invalid_ids(df):
    invalid_ids = pd.isnull(df['id'])
    invalid_ids = invalid_ids[invalid_ids == True]
    df.drop(list(invalid_ids.index), inplace=True)


def series_dictioname(dictionary):
    source = dictionary.index.name
    destination = dictionary.name
    return source + '-to-' + destination
