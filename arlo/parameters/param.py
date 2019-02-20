delay_refresh_minutes = 10

mandatory_fields = ['name', 'account']
dash_fields = ['category', 'link', 'comment']

column_names = 'name,amount,category,account,cycle,date,type,comment,' \
               'pending,bank_name,originalAmount,originalCurrency,id,link'.split(',')
directory = './arlo/data/'

max_transactions_per_user = 1500

n26_url = 'https://api.tech26.de'
lunchr_url = "https://api.lunchr.fr"

budgets_filename = directory + 'budgets.csv'
