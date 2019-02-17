from flask import Flask
from flask_restful import Api
from flask_cors import CORS

from arlo.web.WebServices import *

app = Flask(__name__)
CORS(app)
ipa = Api(app)

ipa.add_resource(ListOperations, "/list")
ipa.add_resource(CategorizeOperations, "/categorize")
ipa.add_resource(RefreshOperations, "/refresh")
ipa.add_resource(CreateManualTransaction, "/create")
ipa.add_resource(LinkTwoTransactions, "/link")
ipa.add_resource(GetRecap, "/recap")
ipa.add_resource(GetBalances, "/balances")
ipa.add_resource(MakeRecurring, "/recurring")
ipa.add_resource(GetRecurring, "/list/recurring")
ipa.add_resource(NameOperations, "/name")
ipa.add_resource(changeCycle, "/cycle")
ipa.add_resource(GetAllCycles, "/cycles")


if __name__ == '__main__':
    app.run(debug=True)
