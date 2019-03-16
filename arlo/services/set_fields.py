from operations.df_operations import change_field_on_several_ids_to_value, filter_df_several_values, how_many_rows, \
    result_function_applied_to_field, get_one_field, change_field_on_single_id_to_value, get_this_field_from_this_id
from operations.formatting import parse_ids
from read_write.fileManager import read_data, save_data, set_field_to_value_on_ids, set_field_to_default_value_on_ids, \
    default_value
from web.status import my_response, success_response


def rename(transaction_ids, transaction_name):
    return set_field_on_ids(transaction_ids, 'name', transaction_name)


def change_cycle(transaction_ids, cycle_value):
    return set_field_on_ids(transaction_ids, 'cycle', cycle_value)


def categorize(transaction_ids, category_value):
    return set_field_on_ids(transaction_ids, 'category', category_value)


def present_links_is_not_empty(data):
    return set(get_one_field(data, 'link')) != {'-'}


def link_ids_if_possible(ids):
    error_message, ids = parse_ids(ids)

    if error_message:
        return error_message

    if len(ids) < 2:
        return 'FAIL not enough transactions'

    data = read_data()
    data_ids = filter_df_several_values(data, 'id', ids)

    if how_many_rows(data_ids) != len(ids):
        return 'FAIL at least one transaction missing'

    if result_function_applied_to_field(data_ids, 'amount', sum) != 0:
        return 'FAIL transactions do not cancel each other out'

    if present_links_is_not_empty(data_ids):
        return 'FAIL one or more transaction already linked'

    _link_ids(ids)
    return 'SUCCESS'


def link_is_not_null(link):
    return link is not default_value('link')


def all_transactions_linked_to_this(data, id):
    ids = [id]
    link = get_this_field_from_this_id(data, id, 'link')
    while link not in ids and link_is_not_null(link):
        ids.append(link)
        link = get_this_field_from_this_id(data, link, 'link')
    return ids


def _unlink_id(id):
    all_ids = all_transactions_linked_to_this(read_data(), id)
    set_field_to_default_value_on_ids(all_ids, 'link')
    set_field_to_value_on_ids(all_ids, 'category', '-')


def unlink_ids_if_possible(ids):
    error_message, ids = parse_ids(ids)

    if error_message:
        return error_message

    for id in ids:
        _unlink_id(id)
    return 'SUCCESS'


def _link_ids(ids):
    data = read_data()
    ids_link = ids[1:] + [ids[0]]
    for transaction_id, transaction_link in zip(ids, ids_link):
        change_field_on_single_id_to_value(data, transaction_id, 'link', transaction_link)
    change_field_on_several_ids_to_value(data, ids, "category", "Link")

    save_data(data)


def set_field_on_ids(transaction_ids, field_name, field_value):
    error_message, transaction_ids = parse_ids(transaction_ids)
    if error_message:
        return my_response(False, error_message)
    set_field_to_value_on_ids(transaction_ids, field_name, field_value)
    return success_response()