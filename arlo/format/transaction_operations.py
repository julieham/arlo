from arlo.format.date_operations import get_timestamp_now
from arlo.format.types_operations import string_to_float, dict_add_value_if_not_present
from arlo.format.formatting import create_id
from arlo.parameters.param import mandatory_fields


def has_at_least_one_valid_amount(fields):
    amount = string_to_float(fields['amount'])
    originalAmount = string_to_float(fields['originalAmount'])
    originalCurrency = fields['originalCurrency']

    return (originalCurrency == '' and originalAmount == 0 and amount != 0) or \
           (originalCurrency != '' and originalAmount != 0)


def fields_make_valid_transaction(fields):
    if not all(u in fields for u in mandatory_fields):
        return False
    return has_at_least_one_valid_amount(fields)


def add_default_values_if_absent(fields):

    dict_add_value_if_not_present(fields, 'originalAmount', '')
    dict_add_value_if_not_present(fields, 'originalCurrency', '')
    dict_add_value_if_not_present(fields, 'amount', '')
    dict_add_value_if_not_present(fields, 'visibleTS', 1000*get_timestamp_now())
    dict_add_value_if_not_present(fields, 'type', 'MAN')
    dict_add_value_if_not_present(fields, 'id', create_id(fields))
