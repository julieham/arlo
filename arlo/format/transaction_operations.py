from arlo.format.types_operations import string_to_float
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
