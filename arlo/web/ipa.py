from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from arlo.web.WebServices import *
from classbot.WebServices import GetClassbotVenues, GetClassbotUsers, LoginClasspassUser, \
    ClassPassBookNow, ClassPassBookLater, GetClasspassCalendar, GetClasspassUpcoming
from tools.clean_n26 import refresh_all_tokens
from tools.scheduler import start_scheduler

app = Flask(__name__)
CORS(app)
ipa = Api(app)

ipa.add_resource(Login, "/login")

ipa.add_resource(SetUpN26, "/setup/n26")
ipa.add_resource(N26State, "/state/n26")

ipa.add_resource(GetRecurring, "/list/recurring")
ipa.add_resource(GetAllCycles, "/list/cycle")
ipa.add_resource(GetLocalCycles, "/list/local_cycle")
ipa.add_resource(GetAccounts, "/list/account")
ipa.add_resource(GetCurrencies, "/list/currency")
ipa.add_resource(GetCategories, "/list/category")
ipa.add_resource(GetRecurringDeposit, "/list/recurring_deposit")
ipa.add_resource(GetDepositNames, "/list/deposit")
ipa.add_resource(GetDepositTransactions, "/transactions/deposit")
ipa.add_resource(GetBudgets, "/list/budgets")
ipa.add_resource(GetCyclesCalendar, "/cycle/calendar")

ipa.add_resource(ListOperations, "/transactions")
ipa.add_resource(GetRecap, "/recap")
ipa.add_resource(CycleProgress, "/cycle/progress")

ipa.add_resource(AmountsDeposit, "/amounts/deposit")
ipa.add_resource(AmountsBank, "/amounts/bank")
ipa.add_resource(AmountsCycle, "/amounts/cycle")
ipa.add_resource(AmountsInput, "/amounts/input")

ipa.add_resource(RefreshOperations, "/refresh")
ipa.add_resource(ForceApiRefresh, "/force_refresh_api")

ipa.add_resource(Transfers, '/transfers')

ipa.add_resource(CreateManualTransaction, "/create/manual")
ipa.add_resource(CreateSingleRecurring, "/create/recurring/single")
ipa.add_resource(CreateSeveralRecurring, "/create/recurring/several")
ipa.add_resource(AddNameReference, "/create/name_ref")
ipa.add_resource(AddDepositReference, "/create/deposit_ref")
ipa.add_resource(CreateDeposit, "/create/deposit")
ipa.add_resource(CreateDepositDebit, "/create/deposit_debit")
ipa.add_resource(DeleteDepositDebit, "/delete/deposit_debit")
ipa.add_resource(DeleteTransaction, "/delete/transaction")

ipa.add_resource(LinkTransactions, "/set-fields/link")
ipa.add_resource(UnlinkTransactions, "/set-fields/unlink")

ipa.add_resource(EditTransaction, "/edit/transaction")
ipa.add_resource(SplitTransaction, "/edit/split")
ipa.add_resource(TransferTransaction, "/edit/transfer")
ipa.add_resource(EditBudget, "/edit/budget")
ipa.add_resource(EditCalendar, "/edit/calendar")


ipa.add_resource(GetClassbotVenues, "/classbot/venues")
ipa.add_resource(GetClassbotUsers, "/classbot/users")
ipa.add_resource(LoginClasspassUser, "/classbot/login")
ipa.add_resource(GetClasspassCalendar, "/classbot/calendar")
ipa.add_resource(GetClasspassUpcoming, "/classbot/upcoming")
ipa.add_resource(ClassPassBookNow, "/classbot/book_now")
ipa.add_resource(ClassPassBookLater, "/classbot/book_later")

start_scheduler(refresh_all_tokens)

if __name__ == '__main__':
    app.run(debug=True)
