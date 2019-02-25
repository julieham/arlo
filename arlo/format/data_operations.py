from arlo.format.types_operations import df_field_to_numeric_with_sign
from arlo.read_write.fileManager import read_data


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
