from flask import json, request
from flask_restful import Resource
from services import *


class ListOperations (Resource):

    @staticmethod
    def get():
        #refresh = request.???
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


class LinkTwoTransactions(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        result = link_two_ids(ids)
        return {"status": result}


class GetRecap(Resource):

    @staticmethod
    def get():
        recap = get_recap_categories()
        return json.loads(recap)


class GetBalances(Resource):

    @staticmethod
    def get():
        recap = get_balances().reset_index()
        return json.loads(recap.to_json(orient="records"))
