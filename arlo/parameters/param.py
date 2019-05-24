delay_refresh_minutes = 10

mandatory_fields = ['name', 'account']
no_recap_categories = ['link', 'Input']

column_names_stored = 'name,amount,category,account,cycle,date,type,comment,' \
               'bank_name,originalAmount,originalCurrency,id,link'.split(',')

typed_columns = dict({'amount': float, 'originalAmount': float, 'date': 'datetime64[ns]'})

default_values = dict({'link': '-',
                       'comment': '-',
                       'category': '-'})

immutable_values = dict({'category': {'Link'}})

column_names_for_front = 'id,name,amount,category,pending,originalAmount,account' \
                         ',originalCurrency,method,cycle,linked,bank_name,date,manual'.split(',')

directory = '/Users/julie/PycharmProjects/arlo/arlo/'
data_directory = directory + 'data/'
log_directory = directory + 'log/'

budgets_filename = data_directory + 'budgets.csv'

auto_accounts = ['T_N26', 'J_N26', 'lunchr']

editable_fields_to_recover = ['name', 'category', 'comment', 'cycle']

provisions_type = 'PROV'
deprovisions_type = 'UNPROV'


#%% N26

n26_url = 'https://api.tech26.de'
n26_max_transactions_per_user = 200


#%% LUNCHR

lunchr_url = "https://api.lunchr.fr"
lunchr_id_prefix = 'lunchr-T-'
lunchr_account_name = 'lunchr'

lunchr_dictionary = dict({'created_at': 'date',
                          'amount_value': 'amount',
                          'transaction_number': 'id',
                          'name': 'bank_name',
                          'type':  'lunchr_type'})
