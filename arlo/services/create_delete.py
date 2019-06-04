from operations.data_operations import missing_valid_amount, missing_mandatory_field, get_bank_name_from_id
from operations.df_operations import add_field_with_default_value, reverse_amount, set_pandas_print_parameters
from operations.types_operations import dict_to_df
from parameters.param import auto_accounts
from read_write.file_manager import add_new_data, remove_data_on_id
from read_write.select_data import get_transaction_with_id
from services.set_fields import rename, categorize, link_ids_if_possible
from tools.autofill_manager import add_reference
from tools.logging import warn, info
from tools.recurring_manager import get_possible_recurring
from tools.uniform_data_maker import format_manual_transaction, format_recurring_transaction, create_id
from web.status import success_response, is_successful, failure_response, merge_status


def create_manual_transaction(transaction_fields):
    warn('#create_manual ---------------')
    df = dict_to_df(transaction_fields)
    set_pandas_print_parameters()
    info('\n' + str(df))
    valid_response = is_valid_transaction_df(df)
    if is_successful(valid_response):
        format_manual_transaction(df)
        add_new_data(df)
    info('Response : ' + str(valid_response))
    return valid_response


def create_single_recurring(name, number=None):
    rec = get_possible_recurring()
    df = rec[rec.index == name].reset_index()
    if df.shape[0] != 1:
        return failure_response('Invalid name')
    valid_response = is_valid_transaction_df(df)
    print(valid_response)
    if is_successful(valid_response):
        format_recurring_transaction(df)
        if number:
            df['name'] = (str(number) + ' ' + df['name'] + 's') if number > 1 else df['name']
            df['amount'] = int(number) * df['amount']
        add_new_data(df)
    return valid_response


def create_several_recurring(how_many_recurring):
    response = success_response()
    for name in how_many_recurring:
        if how_many_recurring[name] > 0:
            response = create_single_recurring(name, how_many_recurring[name])
    return response


def is_valid_transaction_df(df):
    if missing_valid_amount(df):
        return failure_response('no valid amount')

    if missing_mandatory_field(df):
        return failure_response('missing mandatory field')

    return success_response()


def create_name_references_if_possible(this_id, name, category):
    if not name:
        return failure_response("name cannot be empty")
    bank_name = get_bank_name_from_id(this_id)
    status_name = status_field_not_empty('name', name)
    if is_successful(status_name):
        name_ref_added = add_reference('bank_name', 'name', bank_name, name)
        if is_successful(name_ref_added):
            rename(this_id, name)
    else:
        return status_name

    status_category = status_field_not_empty('category', category)
    if is_successful(status_category):
        cat_ref_added = add_reference('name', 'category', name, category)
        if is_successful(cat_ref_added):
            categorize(this_id, category)
    else:
        return status_category

    return merge_status(name_ref_added, cat_ref_added)


def status_field_not_empty(field_name, field_value):
    if field_value is None:
        return failure_response('No '+field_name+' Entered')
    return success_response()


def remove_data_on_id_if_possible(id_to_remove):
    transaction_to_delete = get_transaction_with_id(id_to_remove)
    if transaction_to_delete.shape[0] == 0:
        return failure_response('ID not found, nothing to delete')

    if set(transaction_to_delete['account'].tolist()) & set(auto_accounts):
        return failure_response('Impossible to remove automatic transaction')

    remove_data_on_id(id_to_remove)
    return success_response()


def create_transfer_if_possible(id_one_way, account_destination):
    transaction_to_copy = get_transaction_with_id(id_one_way)
    if transaction_to_copy.shape[0] != 1:
        return failure_response('invalid id')

    return _create_transfer(transaction_to_copy, account_destination)


def _create_transfer(transaction, account_destination):
    ids_to_link = list(transaction['id'])
    add_field_with_default_value(transaction, 'account', account_destination)
    reverse_amount(transaction)
    create_id(transaction)
    ids_to_link += list(transaction['id'])
    add_new_data(transaction)
    u = link_ids_if_possible(','.join(ids_to_link))
    print(u)
    return u
