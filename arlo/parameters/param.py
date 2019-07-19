from parameters.column_names import *

delay_refresh_minutes = 10

## CATEGORIES
link_category = 'Link'
input_category = 'Input'

data_columns_mandatory_fields = [name_col, account_col]

data_columns_all = [name_col, amount_euro_col, category_col, account_col, cycle_col, date_col, type_trans_col,
                    split_id_col,
                    bank_name_col, amount_orig_col, currency_orig_col, id_col, link_id_col, deposit_name_col]

data_columns_front = [id_col, name_col, amount_euro_col, category_col, pending_col, amount_orig_col, account_col,
                      currency_orig_col, payment_method_col, cycle_col, is_linked_col, bank_name_col, date_col,
                      is_manual_col]

data_columns_to_recover = [name_col, category_col, split_id_col, cycle_col, deposit_name_col]

deposit_columns_all = [name_col, amount_euro_col, account_col, category_col, cycle_col, date_col, deposit_name_col,
                       id_col]

typed_columns = dict({amount_euro_col: float, amount_orig_col: float, date_col: 'datetime64[ns]'})

default_values = dict({link_id_col: '-',
                       split_id_col: '-',
                       category_col: '-'})

immutable_values = dict({category_col: {link_category}})

# %% FILES

directory = '/Users/julie/PycharmProjects/arlo/arlo/'
data_directory = directory + 'data/'
log_directory = directory + 'log/'

budgets_filename = data_directory + 'budgets.csv'

# %% DEPOSIT

deposit_account = 'HB'

#%% N26

julie_account = 'J_N26'
thomus_account = 'T_N26'

n26_url = 'https://api.tech26.de'
n26_max_transactions_per_user = 200


#%% LUNCHR

lunchr_url = "https://api.lunchr.fr"
lunchr_id_prefix = 'lunchr-T-'
lunchr_account_name = 'lunchr'

lunchr_dictionary = dict({'created_at': date_col,
                          'amount_value': amount_euro_col,
                          'transaction_number': id_col,
                          'name': bank_name_col,
                          'type': 'lunchr_type',
                          'details': 'lunchr_details'})

# %% BANKIN

bankin_acc_name = 'Hello'
hello_acc_id = '15661423'
bankin_id_prefix = 'hello-' + hello_acc_id + '-'
hello_type_trans = 'HB'

bankin_dictionary = dict({'date': date_col,
                          'description': bank_name_col,
                          'id': id_col,
                          'amount': amount_euro_col})



# %% VALUES

auto_accounts = [thomus_account, julie_account, lunchr_account_name]
no_recap_categories = [link_category, input_category]
minutes_valid_token = 60
