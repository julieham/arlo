from arlo.format.date_operations import lunchr_date_to_datetime, date_to_cycle
from arlo.format.df_operations import drop_other_columns, add_prefix_to_column, remove_invalid_ids, \
    apply_function_to_field_overrule, add_field_with_default_value, add_autofilled_column, sort_df_by_descending_date
from arlo.parameters.param import lunchr_dictionary, lunchr_fields, column_names, lunchr_id_prefix, lunchr_account_name


def format_lunchr_df(lunchr_df):
    lunchr_df.rename(columns=lunchr_dictionary, inplace=True)
    drop_other_columns(lunchr_df, lunchr_fields)
    add_prefix_to_column(lunchr_df, lunchr_id_prefix, 'id')

    remove_invalid_ids(lunchr_df)

    apply_function_to_field_overrule(lunchr_df, 'date', lunchr_date_to_datetime)
    apply_function_to_field_overrule(lunchr_df, 'date', date_to_cycle, destination='cycle')

    add_field_with_default_value(lunchr_df, 'account', lunchr_account_name)
    add_field_with_default_value(lunchr_df, 'comment', '-')
    add_field_with_default_value(lunchr_df, 'link', '-')
    add_field_with_default_value(lunchr_df, 'pending', False)
    add_field_with_default_value(lunchr_df, "originalAmount", '')
    add_field_with_default_value(lunchr_df, 'originalCurrency', '')

    add_autofilled_column(lunchr_df, 'bank_name', 'name', star_fill=True)
    add_autofilled_column(lunchr_df, 'name', 'category')
    add_autofilled_column(lunchr_df, 'lunchr_type', 'type', star_fill=True)

    sort_df_by_descending_date(lunchr_df)
    drop_other_columns(lunchr_df, column_names)
