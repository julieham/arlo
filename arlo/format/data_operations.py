from arlo.format.date_operations import timestamp_to_datetime, date_to_cycle, get_timestamp_now
from arlo.format.df_operations import apply_function_to_field_no_overrule, add_field_with_default_value
from arlo.format.types_operations import df_field_to_numeric_with_sign, string_is_AA
from arlo.read_write.fileManager import read_data


def calculate_universal_fields(df):

    if 'visibleTS' in df.columns:
        apply_function_to_field_no_overrule(df, 'visibleTS', timestamp_to_datetime, destination='date')
    elif 'date' not in df.columns:
        add_field_with_default_value(df, 'visibleTS', get_timestamp_now())

    apply_function_to_field_no_overrule(df, 'type', string_is_AA, destination='pending')
    apply_function_to_field_no_overrule(df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(df, 'comment', '-')
    add_field_with_default_value(df, 'link', '-')


def set_amounts_to_numeric(df, isPositive):
    fields = ['amount', 'originalAmount']

    for field in fields:
        df_field_to_numeric_with_sign(df, field, isPositive)


def remove_already_present_id(df, account, limit=None):
    data_account = read_data()
    data_account = data_account[data_account['account'] == account]
    if limit:
        data_account = data_account.head(limit)
    present_ids = data_account['id']
    return df[df['id'].isin(present_ids) == False]
