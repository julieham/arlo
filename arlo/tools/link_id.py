#%% PARAMETERS

from arlo.operations.df_operations import add_field_with_default_value, get_one_field, change_field_on_several_indexes_to_value, \
    assign_new_column, select_columns, drop_columns
from arlo.operations.series_operations import apply_function

sign_name, amount_name = 'link_sign', 'link_amount'

name_field, account_field, amount_field = 'bank_name', 'account', 'amount'

sep_link_ids = "__"


#%% CALCULATED PARAMTERS

fields_link_ids = dict({'link_id': [sign_name, amount_name, name_field, account_field],
                        'link_id_no_name': [sign_name, amount_name, account_field],
                        'link_id_no_amount': [sign_name, name_field, account_field]})

link_id_columns = [field_name for field_name in fields_link_ids]


#%% COLUMNS TOOLS

def add_the_sign_to_df(df):
    def is_negative(series):
        return series < 0

    add_field_with_default_value(df, sign_name, '+')
    negative_amounts = is_negative(get_one_field(df, amount_field))
    change_field_on_several_indexes_to_value(df, negative_amounts, sign_name, '-')


def add_the_amount_to_df(df):
    def turn_amount_to_string(series):
        return (100*series.fillna(0)).abs().astype(int).astype(str)
    the_amounts = turn_amount_to_string(get_one_field(df, amount_field))
    assign_new_column(df, amount_name, the_amounts)


def add_link_fields(df):
    def join_str(x):
        return sep_link_ids.join(x)

    for field_name, fields in fields_link_ids.items():
        values = apply_function(select_columns(df, fields), join_str).astype(str)
        assign_new_column(df, field_name, values)


def opposite_link_id(link_id):
    replacement_sign = dict({'-': '+', '+': '-'})
    return replacement_sign[link_id[0]] + link_id[1:]


#%%
def add_link_ids(df):
    add_the_sign_to_df(df)
    add_the_amount_to_df(df)
    add_link_fields(df)
    drop_columns(df, [sign_name, amount_name])


def remove_link_ids(df):
    drop_columns(df, link_id_columns)
