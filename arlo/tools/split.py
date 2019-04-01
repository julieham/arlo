from copy import deepcopy

from operations.date_operations import datetime_to_timestamp, get_timestamp_now
from operations.df_operations import get_transaction_with_id, concat_lines, reverse_amount, assign_new_column, \
    apply_function_to_field_overrule, set_value_to_column
from operations.series_operations import get_first_value_from_series
from read_write.file_manager import read_data, add_new_data
from services.set_fields import link_ids_if_possible
from tools.uniform_data_maker import create_id
from web.status import failure_response, success_response


def make_refund_split_transaction(transaction):
    reverse_amount(transaction)
    assign_new_column(transaction, 'type', 'FIC')
    apply_function_to_field_overrule(transaction, 'date', datetime_to_timestamp, destination='timestamp')
    create_id(transaction)
    return transaction


def amounts_valid_split(transaction, amount1, amount2):
    return get_first_value_from_series(transaction['amount']) == amount1 + amount2


def all_split_fields_present(the_input):
    fields = ['amount', 'category', 'cycle', 'name']
    suffixes = '12'
    return all(field + suffix in the_input for field in fields for suffix in suffixes)


def make_split_transactions(transaction, the_input):
    transaction_split_1 = deepcopy(transaction)
    set_value_to_column(transaction_split_1, 'timestamp', get_timestamp_now())

    transaction_split_2 = deepcopy(transaction)
    set_value_to_column(transaction_split_2, 'timestamp', get_timestamp_now())

    for field in ['amount', 'category', 'cycle', 'name']:
        set_value_to_column(transaction_split_1, field, the_input[field + '1'])
        set_value_to_column(transaction_split_2, field, the_input[field + '2'])

    both = concat_lines([transaction_split_2, transaction_split_1])
    set_value_to_column(both, 'type', 'FIC')

    create_id(both)

    return both


def split_transaction_if_possible(the_input):
    data = read_data()
    transaction = get_transaction_with_id(data, the_input['id'])

    if transaction.shape[0] != 1:
        return failure_response('Invalid id')

    if not amounts_valid_split(transaction, the_input['amount1'], the_input['amount2']):
        return failure_response('Invalid amounts')

    if not all_split_fields_present(the_input):
        return failure_response('Missing fields')

    _split_transaction(the_input, transaction)
    return success_response()


def _split_transaction(the_input, refund_transaction):
    split = make_split_transactions(refund_transaction, the_input)
    make_refund_split_transaction(refund_transaction)
    refund_id = get_first_value_from_series(refund_transaction['id'])

    all_transactions = concat_lines([split, refund_transaction])
    set_value_to_column(all_transactions, 'comment', refund_id)

    add_new_data(all_transactions)
    link_ids_if_possible(refund_id + ',' + the_input['id'])
