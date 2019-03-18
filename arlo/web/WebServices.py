from flask import json, request
from flask_restful import Resource

from operations.data_operations import get_bank_name_from_id
from services.list import all_categories, all_accounts, all_cycles, all_recurring, data
from services.services import force_refresh, get_recap_categories, get_balances
from services.create import create_manual_transaction, create_recurring_transaction, create_name_references_if_possible

from services.set_fields import link_ids_if_possible, rename, change_cycle, categorize, unlink_ids_if_possible
from web.status import is_fail


class ListOperations (Resource):

    @staticmethod
    def get():
        refresh = request.args.get('refresh')
        cycle = request.args.get('cycle')
        hide_linked = request.args.get("hide_linked")
        operations = data(cycle=cycle, refresh=refresh, hide_linked=hide_linked)
        return json.loads(operations)


class CategorizeOperations(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        category = request.json['field_value']
        return categorize(ids, category)


class AddNameReference(Resource):
    @staticmethod
    def post():
        this_id = request.args.get('id')
        this_name = request.json['name']
        category = request.json['category']
        bank_name = get_bank_name_from_id(this_id)
        result = rename(this_id, this_name)
        if is_fail(result):
            return result
        if category is not '':
            result = categorize(this_id, category)
            if is_fail(result):
                return result
        return create_name_references_if_possible(bank_name, this_name, category)


class NameOperations(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        category = request.json['field_value']
        return rename(ids, category)


class ChangeCycle(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        category = request.json['field_value']
        return change_cycle(ids, category)


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


class LinkTransactions(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        result = link_ids_if_possible(ids)
        return {"status": result}


class UnlinkTransactions(Resource):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        result = unlink_ids_if_possible(ids)
        return {"status": result}


class GetRecap(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        recap = get_recap_categories(cycle=cycle)
        return json.loads(recap)


class GetBalances(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        recap = get_balances(cycle=cycle).reset_index()
        return json.loads(recap.to_json(orient="records"))


class MakeRecurring(Resource):
    @staticmethod
    def post():
        result = create_recurring_transaction(request.json)
        return {"status": result}


class GetRecurring(Resource):
    @staticmethod
    def get():
        return json.loads(json.dumps(all_recurring()))


class GetAllCycles(Resource):
    @staticmethod
    def get():
        return json.loads(all_cycles())


class GetAccounts(Resource):
    @staticmethod
    def get():
        return json.loads(all_accounts())


class GetCategories(Resource):
    @staticmethod
    def get():
        return json.loads(all_categories())
