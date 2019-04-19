from flask import json, request
from flask_restful import Resource

from services.create_delete import create_manual_transaction, create_single_recurring, \
    create_name_references_if_possible, remove_data_on_id_if_possible, create_several_recurring, \
    create_transfer_if_possible
from services.list import all_categories, all_accounts, all_cycles, all_recurring, data
from services.services import force_refresh, get_recap_categories, get_balances, split_transaction
from services.set_fields import link_ids_if_possible, unlink_ids_if_possible, edit_transaction
# %% CREATE
from web.status import success_response


class AddNameReference(Resource):
    @staticmethod
    def post():
        this_id = request.args.get('id')
        this_name = request.json['name']
        category = request.json['category']
        return create_name_references_if_possible(this_id, this_name, category)


class CreateManualTransaction(Resource):

    @staticmethod
    def post():
        json_input = request.json
        response = create_manual_transaction(json_input)
        return response


class CreateSingleRecurring(Resource):
    @staticmethod
    def post():
        this_name = request.json['name']
        response = create_single_recurring(this_name)
        return response


class CreateSeveralRecurring(Resource):
    @staticmethod
    def post():
        response = create_several_recurring(request.json)
        return response


#%% LIST

class ListOperations (Resource):

    @staticmethod
    def get():
        refresh = request.args.get('refresh')
        cycle = request.args.get('cycle')
        operations = data(cycle=cycle, refresh=refresh)
        return json.loads(operations)


class GetRecurring(Resource):
    @staticmethod
    def get():
        return all_recurring()


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


#%% SERVICE

class RefreshOperations(Resource):

    @staticmethod
    def get():
        result = force_refresh()
        return {"status": result}


class GetRecap(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        recap = get_recap_categories(cycle=cycle)
        return json.loads(recap)


class TransferTransaction(Resource):

    @staticmethod
    def post():
        id_one_way = request.args.get('id')
        account = request.args.get('account')
        create_transfer_if_possible(id_one_way, account)
        return success_response()


class GetBalances(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        balances = get_balances(cycle=cycle)
        return json.loads(balances)


#%% SET FIELDS


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


# %% EDIT

class EditTransaction(Resource):
    @staticmethod
    def post():
        json_input = request.json
        result = edit_transaction(json_input)
        return result


class SplitTransaction(Resource):
    @staticmethod
    def post():
        json_input = request.json
        result = split_transaction(json_input)
        return result


class DeleteTransaction(Resource):

    @staticmethod
    def post():
        id_to_delete = request.json['transaction_ids']
        result = remove_data_on_id_if_possible(id_to_delete)
        return {"status": result}
