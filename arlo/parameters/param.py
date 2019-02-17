delay_refresh_minutes = 10
virement_charges = ['ENGIE']
mandatory_fields = ['name', 'account']
dash_fields = ['category', 'link', 'comment']
column_names = 'name,bank_name,amount,originalAmount,originalCurrency,date,category,account,type,id,comment,link,pending,cycle'.split(',')
directory = './arlo/data/'

max_transactions_per_user = 1500

n26_url = 'https://api.tech26.de'
lunchr_url = "https://api.lunchr.fr"
