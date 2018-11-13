import n26.api as n26api
import datetime
import pandas as pd
import numpy as np
from n26.config import Config
import os
import matplotlib.pyplot as plt


desired_width = 10000
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 30)


start_date = datetime.datetime(year=2018, month=9, day=3)
column_names = ['amount', 'name', 'original_amount', 'createdDate', 'account', 'category', 'type', 'id', 'comment']
user_ids = 'JT'
change_rate = 7.28

initial_balance_bank = dict({'T N26':825.12, 'J N26':688.48+33.98})
initial_balance_cash = 668.5


def get_balances():
    spent_euros = data.groupby(["account"]).sum()["amount"].round(2)
    spent_original = data.groupby(["account"]).sum()["original_amount"].round(2)
    balances = dict()
    for account_id in initial_balance_bank:
        balances[account_id] = initial_balance_bank[account_id] + spent_euros[account_id]
    balances['Cash'] = initial_balance_cash + spent_original["Cash"]
    for acc in balances:
        print(acc, ':',balances[acc])


def get_balance(person_id):
    name_account = person_id + ' N26'
    username, password, card_id = [os.environ.get(e + '_' + person_id) for e in
                                   ["N26_USER", "N26_PASSWORD", "N26_CARD_ID"]]
    account_info = n26api.Api(cfg=Config(username, password, card_id)).get_balance()

    balance = round(float(account_info['availableBalance']),2)
    pending_operations = round(float(account_info['availableBalance'] - account_info['bankBalance']),2)
    print(name_account)
    print('balance :', balance)
    print('pending :', pending_operations)


def get_current_data(person_id):
    name_account = person_id + ' N26'
    username, password, card_id = [os.environ.get(e+'_'+person_id) for e in ["N26_USER", "N26_PASSWORD", "N26_CARD_ID"]]
    account_info = n26api.Api(cfg=Config(username, password, card_id))

    transactions = account_info.get_transactions_limited(limit=1000)
    """for t in transactions:
        print('###')
        for u in t:
            print(u, t[u])
        print('---')"""
    this_data = pd.DataFrame(columns=column_names)
    for i, transaction in enumerate(transactions):
        created_date = datetime.datetime.fromtimestamp(transaction['visibleTS'] / 1e3)
        if created_date < start_date:
            continue

        amount = float(transaction['amount'])
        try:
            name = transaction['merchantName']
        except KeyError:
            try:
                name = transaction['referenceText']
                name += (' ' if name else '') + '#VIR'
            except KeyError:
                name = '#VIR to ' + transaction["partnerName"]
        try:
            original_amount = transaction['originalAmount']
        except:
            original_amount = np.NaN
        trans_type = transaction['type']
        trans_id = transaction['id']

        this_data.loc[this_data.shape[0]] = [amount, name, original_amount, created_date, name_account,  '-', trans_type, trans_id, '-']

    return this_data


def upupdate_data(person_id):
    global data
    current_data = get_current_data(person_id)
    previous_data_this_account = data[data['account'] == person_id + ' N26']

    for _, line in current_data.iterrows():
        if line["id"] not in list(previous_data_this_account["id"]):
            data.loc[data.shape[0]] = line

    for _, line in previous_data_this_account.iterrows():
        if line["id"] not in list(current_data["id"]):
            data = data[data["id"] != line["id"]]

def update_data(person_id, data):
    current_data = get_current_data(person_id)
    previous_data_this_account = data[data['account'] == person_id + ' N26']

    for _, line in current_data.iterrows():
        if line["id"] not in list(previous_data_this_account["id"]):
            data.loc[data.shape[0]] = line

    for _, line in previous_data_this_account.iterrows():
        if line["id"] not in list(current_data["id"]):
            data = data[data["id"] != line["id"]]

    return data


def rerefresh_all_accounts():
    global data
    for user in user_ids:
        upupdate_data(user)
    data = data.sort_values("createdDate", ascending=False).reset_index(drop=True)


def refresh_all_accounts():
    data = read_data()
    for user in user_ids:
        data = update_data(user,data)
    data = data.sort_values("createdDate", ascending=False).reset_index(drop=True)
    persist_data(data)


def set_category(idd, cat_name):
    data.ix[idd, 'category'] = cat_name


def set_comment(idd, comment):
    data.ix[idd, 'comment'] = comment


def parse_ids(transaction_ids, data):
    try:
        transaction_ids = list(map(int, transaction_ids.replace(' ', '').split(',')))
    except ValueError:
        return False, 'invalid number as ID', []
    indexes = list(data.index)
    if set(transaction_ids) & set(indexes) != set(transaction_ids):
        return False, 'transaction not found', []
    return True, '', transaction_ids


def categorize(transaction_ids,category_name):
    data = read_data()
    success, error_message, transaction_ids = parse_ids(transaction_ids, data)
    if not success:
        return error_message

    for transaction_id in transaction_ids:
        data.ix[transaction_id, 'category'] = category_name
    persist_data(data)
    return 'SUCCESS'


def cacategorize():
    global data
    valid_rows = data['category'] == '-'
    print(True in valid_rows)
    print(set(valid_rows))
    if len(set(valid_rows)) == 1 and False in valid_rows:
        print(data[["name", 'amount', 'category']])
    else:
        print(data.loc[valid_rows, ["name", 'amount', 'category']])
    success, error_message, transaction_ids = parse_ids(input('Transaction ID : '), data)
    if not success:
        print(error_message)
    category_name = input('Category name : ')
    for id_transaction in transaction_ids:
        set_category(int(id_transaction), category_name)


def add_cash_transaction():
    global data, change_rate
    name = input('Name')
    try:
        amount = round(float(input('Original Amount')), 2)
    except:
        print('INVALID AMOUNT')
        return
    created_date = datetime.datetime.now()
    category = input('Category')
    add_data(-round(-amount/change_rate,2), name, -amount, created_date, 'Cash', category, 'manual', '0', '-')


def make_percentages(values):
    def make_percentage(pct):
        return '{p:.0f}%  ({v:d}\u20ac)'.format(p=pct,v=int(round(pct*sum(values)/100.0)))
    return make_percentage


def get_and_save_recap():
    global data
    valid_data = data[data["category"] != "Unrelated"]
    valid_data = valid_data[valid_data["category"] != "Input"]
    spendings = valid_data.groupby(["category"]).sum()["amount"].round(2).abs()
    print(spendings)
    spendings.to_csv('recap.csv')
    f = plt.figure(figsize=(4,4))
    spendings.plot.pie(autopct=make_percentages(spendings))
    f.show()


def add_comment():
    global data
    valid_rows = data['comment'] == '-'
    print(data.loc[valid_rows, ["name", 'amount', 'category', 'createdDate']])
    try:
        id_transactions = list(map(int, input('Transaction ID : ').replace(' ', '').split(',')))
    except ValueError:
        print('INVALID ID PROVIDED')
        return
    comment = input('Comment  : ')
    if 0 <= min(id_transactions) and max(id_transactions) < data.shape[0]:
        for id_transaction in id_transactions:
            set_comment(int(id_transaction), comment)
    else:
        print('INVALID Transaction ID')


def add_data(amount, name, original_amount, created_date, account, category, type, idd, comment):
    global data
    data.loc[data.shape[0]] = [-amount, name, -original_amount, created_date, account, category, type, idd, comment]


def add_new_transaction():
    global data
    name = input('Name')
    try:
        amount = round(float(input('Amount')),2)
    except:
        print('INVALID AMOUNT')
        return
    account = input('Account')
    created_date = datetime.datetime.now()
    category = input('Category')
    original_amount = input('original amount')
    try:
        original_amount = round(float(original_amount),2)
    except:
        original_amount = None
    add_data(-amount, name, -original_amount, created_date, account, category, 'manual', '0', '-')


def list_data():
    display_columns = ['amount', 'name', 'original_amount', 'createdDate', 'account', 'category','comment']
    return read_data()[display_columns]


def run():
    while True:
        type_request = input('\nTYPE:\n 0 to quit \n 1 to categorize \n 2 for cash transaction \n 3 to refresh \n 4 for recap \n 5 to display \n 6 to save \n 7 to add comment \n 8 for manual transaction')
        if type_request == '0':
            break
        elif type_request == '1':
            cacategorize()
        elif type_request == '2':
            add_cash_transaction()
        elif type_request == '3':
            rerefresh_all_accounts()
        elif type_request == '4':
            get_and_save_recap()
        elif type_request == '5':
            list_data()
        elif type_request == '6':
            save_data()
        elif type_request == '7':
            add_comment()
        elif type_request == '8':
            add_new_transaction()
        elif type_request == '9':
            get_balances()


def read_data():
    dataa = pd.read_csv("data.csv")
    dataa['createdDate'] = dataa['createdDate'].astype('datetime64[ns]')
    return dataa


def save_data():
    global data
    data = data.sort_values("createdDate", ascending=False).reset_index(drop=True)
    data.to_csv('data.csv', index=False)
    get_and_save_recap()

def persist_data(data):
    data = data.sort_values("createdDate", ascending=False).reset_index(drop=True)
    data.to_csv('data.csv', index=False)


global data
data = read_data()

"""


run()

save_data()





# PT = card transaction
# DT = direct transfer (outgoing)
# CT = transfer (incoming)
# AA = authorisation
# AE/AV = authorisation refund


"""

#TODO Check Balance
#TODO Update existing transactions






