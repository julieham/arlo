from operations.df_operations import change_field_on_several_ids_to_value, filter_df_several_values, how_many_rows, \
    result_function_applied_to_field, get_one_field, change_field_on_single_id_to_value, get_this_field_from_this_id
from operations.formatting import parse_ids
from read_write.file_manager import read_data, save_data, set_field_to_value_on_ids, default_value
from web.status import success_response, failure_response


def rename(transaction_id, transaction_name):
    return set_field_to_value_on_ids([transaction_id], 'name', transaction_name)


def categorize(transaction_id, category_value):
    return set_field_to_value_on_ids([transaction_id], 'category', category_value)


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

    if round(result_function_applied_to_field(data_ids, 'amount', sum), 2) != 0:
        return 'FAIL transactions do not cancel each other out'

    if present_links_is_not_empty(data_ids):
        return 'FAIL one or more transaction already linked'

    _link_ids(ids)
    return 'SUCCESS'


def link_is_not_null(link):
    return link is not default_value('link')


def _link_ids(ids):
    data = read_data()
    ids_link = ids[1:] + [ids[0]]
    for transaction_id, transaction_link in zip(ids, ids_link):
        change_field_on_single_id_to_value(data, transaction_id, 'link', transaction_link)
        change_field_on_single_id_to_value(data, transaction_id, "category", "Link")
    save_data(data)


def unlink_ids_if_possible(ids):
    error_message, ids = parse_ids(ids)

    if error_message:
        return error_message

    _unlink_ids(id)
    return 'SUCCESS'


def all_transactions_linked_to_this(data, this_id):
    ids = [this_id]
    link = get_this_field_from_this_id(data, this_id, 'link')
    while link not in ids and link_is_not_null(link):
        ids.append(link)
        link = get_this_field_from_this_id(data, link, 'link')
    return ids


def _unlink_ids(ids):
    data = read_data()
    fields = ['link', 'category']
    force_code = "unlink_ok"
    for this_id in ids:
        all_ids = all_transactions_linked_to_this(data, this_id)
        for field in fields:
            change_field_on_several_ids_to_value(data, all_ids, field, default_value(field), force_code=force_code)
    save_data(data)


def edit_transaction(fields):
    if 'id' not in fields:
        return failure_response('invalid id')
    transaction_id = fields['id']
    response = success_response()
    for field in set(fields) - {'id'}:
        if field == 'cycle':
            response = change_cycle_on_id(transaction_id, fields[field])
        else:
            response = set_field_to_value_on_ids([transaction_id], field, fields[field])
    return response


def change_cycle_on_id(transaction_id, cycle_value):
    print('CHANGE CYCLE')
    ids = all_transactions_linked_to_this(read_data(), transaction_id)
    print(ids)
    return set_field_to_value_on_ids(ids, 'cycle', cycle_value)
