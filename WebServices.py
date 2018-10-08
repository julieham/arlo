from flask import json, request
from flask_restful import Resource
from services import *


class ListOperations (Resource):

    @staticmethod
    def get():
        operations = list_data_json()
        return json.loads(operations)


class CategorizeOperations(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        category = request.json['category']
        result = categorize(ids, category)
        return {"status": result}


class CreateManualTransaction(Resource):

    @staticmethod
    def post():
        json_input = request.json
        result = create_manual_transaction(json_input)
        return {"status": result}


class RefreshOperations(Resource):

    @staticmethod
    def get():
        result = force_refresh()
        return {"status": result}
