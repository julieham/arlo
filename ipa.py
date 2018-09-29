from flask import Flask
from flask_restful import Api
from flask_cors import CORS

from WebServices import *

app = Flask(__name__)
CORS(app)
ipa = Api(app)

ipa.add_resource(ListOperations, "/list")
ipa.add_resource(CategorizeOperations, "/categorize")
ipa.add_resource(FetchOperations, "/fetch")

if __name__ == '__main__':
    app.run(debug=True)
