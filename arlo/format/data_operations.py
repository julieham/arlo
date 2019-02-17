from arlo.format.date_operations import timestamp_to_datetime, date_to_cycle, get_timestamp_now
from arlo.format.df_operations import apply_function_to_field_no_overrule, add_field_with_default_value
from arlo.format.types_operations import df_field_to_numeric_with_sign, string_is_AA
from arlo.tools.autofill import autofill_cat


def calculate_universal_fields(df):

    if 'visibleTS' in df.columns:
        apply_function_to_field_no_overrule(df, 'visibleTS', timestamp_to_datetime, destination='date')
    elif 'date' not in df.columns:
        add_field_with_default_value(df, 'visibleTS', get_timestamp_now())

    apply_function_to_field_no_overrule(df, 'name', autofill_cat, destination='category')
    apply_function_to_field_no_overrule(df, 'type', string_is_AA, destination='pending')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(df, 'comment', '-')
    add_field_with_default_value(df, 'link', '-')


def set_amounts_to_numeric(df, isPositive):
    fields = ['amount', 'originalAmount']

    for field in fields:
        df_field_to_numeric_with_sign(df, field, isPositive)