from arlo.operations.df_operations import filter_df_not_this_value, column_is_null, filter_df_on_bools, concat_lines, \
    filter_df_one_value, sort_df_by_descending_date, add_column_with_value, set_pandas_print_parameters
from arlo.parameters.column_names import deposit_name_col, account_col, currency_col
from arlo.read_write.file_manager import read_data, read_deposit_input
from arlo.tools.cycle_manager import filter_df_on_cycle
from parameters.param import default_currency


def get_data_from_cycle(cycle):
    return filter_df_on_cycle(read_data(), cycle)


def get_deposit_debits_from_cycle(cycle):
    deposit = filter_df_on_cycle(read_deposit_input(), cycle)
    deposit['amount'] = - deposit['amount']
    add_column_with_value(deposit, currency_col, default_currency)
    return deposit


def get_field_data(field_name):
    data = read_data()
    return data[field_name]


def filtered_data_not_this_value(field_name, field_value):
    return filter_df_not_this_value(read_data(), field_name, field_value)


def get_transaction_with_id(id_to_find):
    data = read_data()
    return data[data['id'] == id_to_find]


def get_deposit_output():
    data = read_data()
    is_not_deposit = column_is_null(data, deposit_name_col)
    return filter_df_on_bools(data, is_not_deposit, keep=False)


def get_deposit_input_and_output():
    deposit_input = read_deposit_input()
    deposit_output = get_deposit_output()
    all_deposit = concat_lines([deposit_input, deposit_output], join='inner')
    set_pandas_print_parameters()
    sort_df_by_descending_date(all_deposit)
    return all_deposit


def get_data_this_cycle_not_deposit(cycle):
    data = get_data_from_cycle(cycle)
    is_not_deposit = column_is_null(data, deposit_name_col)
    return filter_df_on_bools(data, is_not_deposit, keep=True)


def get_data_this_account(account):
    return filter_df_one_value(read_data(), account_col, account)
