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

directory = './arlo/data/'
budgets_filename = directory + 'budgets.csv'

#%% N26

n26_url = 'https://api.tech26.de'
n26_max_transactions_per_user = 1500


#%% LUNCHR

lunchr_url = "https://api.lunchr.fr"
lunchr_id_prefix = 'lunchr-T-'
lunchr_account_name = 'lunchr'

lunchr_dictionary = dict({'create'
                          'd_at': 'date',
                          'amount_value': 'amount',
                          'transaction_number': 'id',
                          'name': 'bank_name',
                          'type':  'lunchr_type'})

lunchr_fields = ['id', 'amount', 'bank_name', 'date', 'lunchr_type']
