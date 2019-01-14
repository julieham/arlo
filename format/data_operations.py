from autofill_name import autofill_cat
from format.date_operations import timestamp_to_datetime, date_to_cycle
from format.df_operations import apply_function_to_field_no_overrule, add_field_with_default_value
from format.types_operations import string_is_AA, df_field_to_numeric
from param import column_names


def calculate_universal_fields(df):

    apply_function_to_field_no_overrule(df, 'visibleTS', timestamp_to_datetime, destination='date')
    apply_function_to_field_no_overrule(df, 'name', autofill_cat, destination='category')
    apply_function_to_field_no_overrule(df, 'type', string_is_AA, destination='pending')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(df, 'comment', '-')
    add_field_with_default_value(df, 'link', '-')


def set_amounts_to_numeric(df):
    fields = ['amount', 'originalAmount']

    for field in fields:
        df_field_to_numeric(df, field)