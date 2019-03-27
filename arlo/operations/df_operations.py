import pandas as pd

# %% PANDAS STUFF
from operations.series_operations import get_first_value_from_series
from parameters.param import immutable_values


def set_pandas_print_parameters():
    desired_width = 10000
    pd.set_option('display.width', desired_width)
    pd.np.set_printoptions(linewidth=desired_width)
    pd.set_option("display.max_columns", 100)


def enable_chained_assigment_warning():
    pd.set_option('mode.chained_assignment', 'warn')


def disable_chained_assigment_warning():
    pd.set_option('mode.chained_assignment', None)


#%% SORT & FILTER

def sort_df_by_descending_date(df):
    df.sort_values("date", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)


def filter_df_one_value(df, field_name, field_value):
    return df[df[field_name] == field_value]


def filter_df_several_values(df, field_name, field_values):
    return df[df[field_name].isin(field_values)]


def filter_df_not_this_value(df, field_name, field_value):
    return df[df[field_name] != field_value]


def df_is_not_empty(df):
    return df.shape[0] > 0


def drop_other_columns(df, fields):
    columns_to_remove = [column_name for column_name in df.columns.tolist() if column_name not in set(fields)]
    df.drop(columns=columns_to_remove, inplace=True)


def drop_columns(df, fields):
    df.drop(columns=fields, inplace=True)


def remove_invalid_ids(df):
    invalid_ids = pd.isnull(df['id'])
    invalid_ids = invalid_ids[invalid_ids == True]
    df.drop(list(invalid_ids.index), inplace=True)


#%% LOC


def add_field_with_default_value(df, field, value):
    disable_chained_assigment_warning()
    df[field] = value
    enable_chained_assigment_warning()


def add_prefix_to_column(df, prefix, column):
    disable_chained_assigment_warning()
    df.loc[:, column] = prefix + df[column]
    enable_chained_assigment_warning()


def change_field_on_several_ids_to_value(df, ids, field_name, field_value, force_code=None):
    if force_code != 'unlink_ok':
        ids = remove_immutable_ids(df, ids, field_name)
    df.loc[df['id'].isin(ids), [field_name]] = field_value


def change_field_on_several_indexes_to_value(df, indexes, field_name, field_value, force_code=None):
    disable_chained_assigment_warning()
    if force_code == None:
        indexes = remove_immutable_indexes(df, indexes, field_name)
    df.loc[indexes, field_name] = field_value
    enable_chained_assigment_warning()


def change_field_on_single_id_to_value(df, id_value, field_name, field_value):
    df.loc[df['id'] == id_value, [field_name]] = field_value


def extract_line_from_df(index, df):
    line = df.loc[index]
    df.drop(index, inplace=True)
    return line


def assign_new_column(df, column_name, column_content):
    disable_chained_assigment_warning()
    df.loc[:, column_name] = column_content[:]
    enable_chained_assigment_warning()


def assign_content_to_existing_column(df, column_name, column_content, overrule=False):
    disable_chained_assigment_warning()
    indexes = df.index if overrule else pd.isnull(df[column_name])
    df.loc[indexes, column_name] = column_content[indexes]
    enable_chained_assigment_warning()


#%% ACCESSING GENERAL

def get_one_field(df, field_name):
    return df[field_name]


def how_many_rows(df):
    return df.shape[0]


def select_columns(df, columns):
    return df[columns]


#%% ACCESSING SPECIFIC

def get_ids(df):
    ids = get_one_field(df, 'id')
    return set(ids)


#%% FUNCTIONS

def result_function_applied_to_field(df, field_name, function_to_apply):
    return function_to_apply(df[field_name])


def concat_lines(df_list, sort=False):
    return pd.concat(df_list, axis=0, sort=sort, ignore_index=True)


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


def null_value():
    return pd.np.NaN


def get_transaction_with_id(data, id):
    return filter_df_one_value(data, 'id', id)


def get_this_field_from_this_id(data, id, field):
    transaction = get_transaction_with_id(data, id)
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
