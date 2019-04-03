from operations.data_operations import missing_valid_amount, missing_mandatory_field, get_bank_name_from_id
from operations.types_operations import dict_to_df_with_all_columns
from parameters.param import auto_accounts
from read_write.file_manager import add_new_data, remove_data_on_id, get_transaction_with_id
from services.set_fields import rename, categorize
from tools.autofill_manager import add_reference
from tools.recurring_manager import fill_missing_with_default_values
from tools.uniform_data_maker import format_manual_transaction, format_recurring_transaction
from web.status import success_response, is_successful, failure_response, merge_status


def create_manual_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)

    valid_response = is_valid_transaction_df(df)
    if is_successful(valid_response):
        format_manual_transaction(df)
        add_new_data(df)
    return valid_response


def create_recurring_transaction(transaction_fields):
    df = dict_to_df_with_all_columns(transaction_fields)
    fill_missing_with_default_values(df)

    valid_response = is_valid_transaction_df(df)
    if is_successful(valid_response):
        format_recurring_transaction(df)
        add_new_data(df)
    return valid_response


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
