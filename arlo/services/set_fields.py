from operations.df_operations import change_field_on_several_ids_to_value, filter_df_several_values, how_many_rows, \
    result_function_applied_to_field, get_one_field, change_field_on_single_id_to_value
from operations.formatting import parse_ids
from read_write.fileManager import read_data, save_data, set_field_to_value_on_ids


def name(transaction_ids, transaction_name):
    return set_field_on_ids(transaction_ids, 'name', transaction_name)


def change_cycle(transaction_ids, cycle_value):
    return set_field_on_ids(transaction_ids, 'cycle', cycle_value)


def categorize(transaction_ids, category_value):
    return set_field_on_ids(transaction_ids, 'category', category_value)


def link_ids(ids):
    error_message, ids = parse_ids(ids)

    if error_message:
        return error_message

    if len(ids) < 2:
        message = 'FAIL not enough transactions'
        print(message)
        return message

    data = read_data()
    change_field_on_several_ids_to_value(data, ids, "category", "Link")
    data_ids = filter_df_several_values(data, 'id', ids)

    if how_many_rows(data_ids) != len(ids):
        return 'FAIL at least one transaction missing'

    if result_function_applied_to_field(data_ids, 'amount', sum) != 0:
        return 'FAIL transactions do not cancel each other out'

    present_links = set(get_one_field(data_ids, 'link'))

    if present_links != {'-'}:
        return 'FAIL one or more transaction already linked'

    ids_link = ids[1:] + [ids[0]]
    for t_id, id_link in zip(ids, ids_link):
        change_field_on_single_id_to_value(data, t_id, 'link', id_link)

    save_data(data)
    return 'SUCCESS'


def set_field_on_ids(transaction_ids, field_name, field_value):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return error_message
    set_field_to_value_on_ids(transaction_ids, field_name, field_value)
    return 'SUCCESS'