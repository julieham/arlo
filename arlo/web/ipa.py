from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from arlo.web.WebServices import *

app = Flask(__name__)
CORS(app)
ipa = Api(app)

ipa.add_resource(GetRecurring, "/list/recurring")
ipa.add_resource(GetAllCycles, "/list/cycle")
ipa.add_resource(GetAccounts, "/list/account")
ipa.add_resource(GetCategories, "/list/category")

ipa.add_resource(ListOperations, "/transactions")
ipa.add_resource(GetRecap, "/recap")
ipa.add_resource(GetBalances, "/balances")

ipa.add_resource(RefreshOperations, "/refresh")

ipa.add_resource(CreateManualTransaction, "/create/manual")
ipa.add_resource(MakeRecurring, "/create/recurring")
ipa.add_resource(AddNameReference, "/create/name_ref")

ipa.add_resource(LinkTransactions, "/set-fields/link")
ipa.add_resource(UnlinkTransactions, "/set-fields/unlink")

ipa.add_resource(EditTransaction, "/edit/transaction")


if __name__ == '__main__':
    app.run(debug=True)
