from flask import json, request, make_response, jsonify
from flask_restful import Resource

from parameters.param import deposit_type
from read_write.select_data import get_deposit_input_and_output
from services.create_delete import create_manual_transaction, create_single_recurring, \
    create_name_references_if_possible, remove_data_on_id_if_possible, create_several_recurring, \
    create_transfer_if_possible, create_deposit, remove_deposit_input_on_id_if_possible
from services.list import all_categories, all_accounts, all_cycles, all_recurring, data, local_cycles, \
    all_recurring_deposit, all_deposit_names
from services.services import force_refresh, get_recap_categories, split_transaction, \
    create_deposit_debit, get_state_deposit, bank_balances, cycle_balances, get_transfers_to_do
from services.set_fields import link_ids_if_possible, unlink_ids_if_possible, edit_transaction
from tools.cycle_manager import filter_df_on_cycle
from tools.logging import warn
from web.authentication import generate_new_token, login_is_valid


def make_this_amount_item(series):
    return json.loads(series.rename_axis('description').reset_index().to_json(orient='records'))


from web.status import success_response


# %% LOGIN


class Login(Resource):
    @staticmethod
    def post():
        user = request.json['username']
        password = request.json['password']
        if login_is_valid(user, password):
            token = generate_new_token()
            return json.loads(json.dumps({'username': user, 'token': token}))
        return make_response(jsonify({'message': ' - authentication failed'}), 400)


# %% CREATE


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


class CreateDeposit(Resource):
    @staticmethod
    def post():
        deposit_data = request.json
        # TODO LESS DISGUSTING SHIT
        return create_deposit(deposit_data)


class CreateDepositDebit(Resource):
    @staticmethod
    def post():
        the_id = request.args.get('id')
        the_deposit_name = request.args.get('deposit')
        create_deposit_debit(the_id, the_deposit_name)
        return success_response()


#%% LIST

class ListOperations (Resource):

    @staticmethod
    def get():
        refresh = request.args.get('refresh')
        cycle = request.args.get('cycle')
        operations = data(cycle=cycle, refresh=refresh)
        return json.loads(operations)


class AmountsDeposit(Resource):

    @staticmethod
    def get():
        return make_this_amount_item(get_state_deposit())


class AmountsBank(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return make_this_amount_item(bank_balances(cycle))


class AmountsCycle(Resource):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return make_this_amount_item(cycle_balances(cycle))


class GetRecurring(Resource):
    @staticmethod
    def get():
        return all_recurring()


class GetAllCycles(Resource):
    @staticmethod
    def get():
        return json.loads(all_cycles())


class GetLocalCycles(Resource):
    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return json.loads(local_cycles(cycle))


class GetAccounts(Resource):
    @staticmethod
    def get():
        return json.loads(all_accounts())


class GetCategories(Resource):
    @staticmethod
    def get():
        return json.loads(all_categories())


class GetRecurringDeposit(Resource):
    @staticmethod
    def get():
        return json.loads(all_recurring_deposit())


class GetDepositNames(Resource):
    @staticmethod
    def get():
        return all_deposit_names()


class GetDepositTransactions(Resource):
    @staticmethod
    def get():
        u = get_deposit_input_and_output()
        u = filter_df_on_cycle(u, 'Jul19')
        return json.loads(u.to_json(orient="records"))


#%% SERVICE

class RefreshOperations(Resource):

    @staticmethod
    def get():
        result = force_refresh()
        return {"status": result}


class Transfers(Resource):
    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return json.loads(json.dumps(get_transfers_to_do(cycle)))


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
        warn('#edit_transaction\n' + str(json_input))
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
        id_to_delete = request.args.get('id')
        type_to_delete = request.args.get('type')
        if type_to_delete == deposit_type:
            result = remove_deposit_input_on_id_if_possible(id_to_delete)
        else:
            result = remove_data_on_id_if_possible(id_to_delete)

        return {"status": result}
