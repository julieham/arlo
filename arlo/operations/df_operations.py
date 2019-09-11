from logging import info

import pandas as pd

# %% PANDAS STUFF
from arlo.operations.series_operations import get_first_value_from_series
from arlo.parameters.column_names import date_col
from arlo.parameters.param import immutable_values


def set_pandas_print_parameters():
    desired_width = 100000
    pd.np.set_printoptions(linewidth=desired_width)
    pd.set_option("display.max_columns", 100)
    pd.set_option('display.width', 2000)


def enable_chained_assignment_warning():
    pd.set_option('mode.chained_assignment', 'warn')


def disable_chained_assignment_warning():
    pd.set_option('mode.chained_assignment', None)


#%% SORT & FILTER

def field_is(df, field, value):
    return df[field] == value


def field_is_not(df, field, value):
    return df[field] != value


def sort_df_by_descending_date(df):
    df.sort_values(date_col, ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)


def filter_df_one_value(df, field_name, field_value):
    return df[field_is(df, field_name, field_value)]


def filter_df_several_values(df, field_name, field_values):
    return df[df[field_name].isin(field_values)]


def filter_df_not_this_value(df, field_name, field_value):
    return df[field_is_not(df, field_name, field_value)]


def filter_df_not_these_values(df, field_name, values):
    return df[df[field_name].isin(values) == False]


def df_is_not_empty(df):
    return df.shape[0] > 0


def drop_other_columns(df, fields):
    columns_to_remove = [column_name for column_name in df.columns.tolist() if column_name not in set(fields)]
    df.drop(columns=columns_to_remove, inplace=True)


def drop_columns(df, fields):
    disable_chained_assignment_warning()
    df.drop(columns=list(set(fields) & set(df.columns)), inplace=True)
    enable_chained_assignment_warning()


def rename_columns(df, dict_names):
    df.rename(columns=dict_names, inplace=True)


def remove_invalid_ids(df):
    invalid_ids = series_is_null(df['id'])
    invalid_ids = invalid_ids[invalid_ids == True]
    df.drop(list(invalid_ids.index), inplace=True)


#%% LOC


def add_field_with_default_value(df, field, value):
    if df_is_not_empty(df):
        disable_chained_assignment_warning()
        df[field] = value
        enable_chained_assignment_warning()


def add_prefix_to_column(df, prefix, column):
    if df_is_not_empty(df):
        disable_chained_assignment_warning()
        df.loc[:, column] = prefix + df[column].astype(str)
        enable_chained_assignment_warning()


def change_field_on_several_ids_to_value(df, ids, field_name, field_value, force_code=None):
    if force_code:
        info(
            'Editing immutable field ' + field_name + ' for reason : ' + str(force_code) + ' on id :' + ' ; '.join(ids))
    if force_code not in ['unlink_ok', 'clean_ok']:
        ids = remove_immutable_ids(df, ids, field_name)
    df.loc[df['id'].isin(ids), [field_name]] = field_value


def change_field_on_several_indexes_to_value(df, indexes, field_name, field_value, force_code=None):
    if df_is_not_empty(df) and len(indexes) > 0:
        disable_chained_assignment_warning()
        if force_code == None:
            indexes = remove_immutable_indexes(df, indexes, field_name)
        df.loc[indexes, field_name] = field_value
        enable_chained_assignment_warning()


def change_field_on_single_id_to_value(df, id_value, field_name, field_value):
    if df_is_not_empty(df):
        disable_chained_assignment_warning()
        df.loc[df['id'] == id_value, [field_name]] = field_value
        enable_chained_assignment_warning()


def extract_line_from_df(index, df):
    line = df.loc[index]
    df.drop(index, inplace=True)
    return line


def assign_new_column(df, column_name, column_content):
    disable_chained_assignment_warning()
    if df_is_not_empty(df):
        df.loc[:, column_name] = column_content[:]
    else:
        df.assign(column_name=None)
    enable_chained_assignment_warning()


def assign_content_to_existing_column(df, column_name, column_content, overrule=False):
    disable_chained_assignment_warning()
    indexes = df.index if overrule else series_is_null(get_one_field(df, column_name))
    df.loc[indexes, column_name] = column_content[indexes]


def assign_value_to_empty_in_existing_column(df, column_name, column_value):
    disable_chained_assignment_warning()
    indexes = series_is_null(get_one_field(df, column_name))
    df.loc[indexes, column_name] = column_value
    enable_chained_assignment_warning()


def assign_value_to_bool_rows(df, bool_series, column_name, value):
    disable_chained_assignment_warning()
    if df_is_empty(df):
        df.assign(column_name=None)
    else:
        df.loc[bool_series, column_name] = value
    enable_chained_assignment_warning()


#%% ACCESSING GENERAL

def get_one_field(df, field_name):
    return df[field_name]


def how_many_rows(df):
    return df.shape[0]


def select_columns(df, columns):
    columns = list(set(columns) & set(df.columns.tolist()))
    return df[columns]


#%% ACCESSING SPECIFIC

def get_ids(df):
    ids = get_one_field(df, 'id')
    return set(ids)


#%% FUNCTIONS

def result_function_applied_to_field(df, field_name, function_to_apply):
    return function_to_apply(df[field_name])


def concat_lines(df_list, sort=False, join='outer'):
    return pd.concat(df_list, axis=0, sort=sort, ignore_index=True, join=join)


def concat_columns(df_list, sort=False, keep_index_name=False):
    index_names = [df.index.names for df in df_list]
    df = pd.concat(df_list, axis=1, sort=sort)
    if keep_index_name:
        df.index.names = index_names.pop(0)
    return df


#%% SET NEW COLUMNS / MODIFY EXISTING COLUMNS

def apply_function_to_field_overrule(df, field, function_to_apply, destination=None):
    destination = field if not destination else destination
    column_content = df[field].apply(function_to_apply)
    assign_new_column(df, destination, column_content)


def apply_function_to_field_no_overrule(df, field, function_to_apply, destination=None):
    destination = field if not destination else destination
    column_content = df[field].apply(function_to_apply)
    assign_content_to_existing_column(df, destination, column_content, overrule=False)


def empty_series():
    return pd.Series()


def empty_df(columns=None):
    return pd.DataFrame(columns=columns)


def null_value():
    return pd.np.NaN


def series_is_null(series):
    return pd.isnull(series)


def not_series(series):
    return series == False


def filter_df_on_id(data, id):
    return filter_df_one_value(data, 'id', id)


def filter_df_on_bools(df, bools, keep=True):
    if keep is not True:
        keep = False
    return df[bools == keep]


def column_is_null(df, field_name):
    return series_is_null(get_one_field(df, field_name))


def get_this_field_from_this_id(data, id, field):
    transaction = filter_df_on_id(data, id)
    field = get_first_value_from_series(get_one_field(transaction, field))
    return field


def get_this_field_from_this_index(data, index, field):
    return data.loc[index, field]


def remove_immutable_ids(df, ids, field_name):
    if field_name in immutable_values:
        forbidden_values = immutable_values[field_name]
        return [the_id for the_id in ids if get_this_field_from_this_id(df, the_id, field_name) not in forbidden_values]
    return ids


def remove_immutable_indexes(df, indexes, field_name):
    if field_name in immutable_values:
        forbidden_values = immutable_values[field_name]
        return [index for index in indexes if get_this_field_from_this_index(df, index, field_name) not in forbidden_values]
    return indexes


def reverse_amount(df):
    if df_is_not_empty(df):
        assign_new_column(df, 'amount', - df['amount'])


def set_value_to_column(df, column_name, column_value):
    disable_chained_assignment_warning()
    df.loc[:, column_name] = column_value
    enable_chained_assignment_warning()


def both_series_are_true(serie1, serie2):
    return concat_columns([serie1, serie2]).all(axis=1)


def assign_value_to_loc(df, index_loc, column_loc, value):
    disable_chained_assignment_warning()
    df.loc[index_loc, column_loc] = value
    enable_chained_assignment_warning()


def get_loc_df(df, index, column_name):
    return df.loc[index, column_name]


def drop_line_with_index(df, index):
    disable_chained_assignment_warning()
    df.drop(index, inplace=True)
    enable_chained_assignment_warning()


def add_column_with_value(df, column_name, column_value):
    disable_chained_assignment_warning()
    df[column_name] = column_value
    enable_chained_assignment_warning()


def df_is_empty(df):
    return df.shape[0] == 0


def sum_no_skip_na(x):
    return x.sum(skipna=False)


def total_amount(df):
    return sum_no_skip_na(df['amount'])


def new_dataframe(dictionary):
    return pd.DataFrame(dictionary)
