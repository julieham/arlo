from flask import json, request, make_response, jsonify
from flask_restful import Resource

from arlo.operations.types_operations import string_to_bool
from arlo.parameters.param import deposit_type
from arlo.read_write.select_data import get_deposit_input_and_output
from arlo.services.create_delete import create_manual_transaction, create_single_recurring, \
    create_name_references_if_possible, remove_data_on_id_if_possible, create_several_recurring, \
    create_transfer_if_possible, create_deposit, remove_deposit_input_on_id_if_possible, \
    create_deposit_references_if_possible
from arlo.services.list import all_categories, all_accounts, all_cycles, all_recurring, data, local_cycles, \
    all_recurring_deposit, all_deposit_names, cycle_budgets
from arlo.services.services import force_refresh, get_recap_categories, split_transaction, \
    create_deposit_debit, get_state_deposit, bank_balances, cycle_balances, get_transfers_to_do, delete_deposit_debit, \
    edit_budgets, cycle_calendar, edit_calendar, input_overview, force_api_refresh
from arlo.services.set_fields import link_ids_if_possible, unlink_ids_if_possible, edit_transaction
from arlo.tools.cycle_manager import progress
from arlo.tools.logging import warn
from arlo.web.authentication import generate_new_token, login_is_valid, ResourceWithAuth
from arlo.web.status import success_response, failure_response


def make_this_amount_item(df):
    df.index.names = ['description']
    df.reset_index(inplace=True)
    return json.loads(df.to_json(orient='records'))


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


class AddNameReference(ResourceWithAuth):
    @staticmethod
    def post():
        this_id = request.args.get('id')
        this_name = request.json['name']
        category = request.json['category']
        return create_name_references_if_possible(this_id, this_name, category)


class AddDepositReference(ResourceWithAuth):
    @staticmethod
    def post():
        this_id = request.args.get('id')
        category = request.json['category']
        return create_deposit_references_if_possible(this_id, category)


class CreateManualTransaction(ResourceWithAuth):

    @staticmethod
    def post():
        json_input = request.json
        response = create_manual_transaction(json_input)
        return response


class CreateSingleRecurring(ResourceWithAuth):
    @staticmethod
    def post():
        this_name = request.json['name']
        response = create_single_recurring(this_name)
        return response


class CreateSeveralRecurring(ResourceWithAuth):
    @staticmethod
    def post():
        response = create_several_recurring(request.json)
        return response


class CreateDeposit(ResourceWithAuth):
    @staticmethod
    def post():
        deposit_data = request.json
        # TODO LESS DISGUSTING SHIT
        return create_deposit(deposit_data)


class CreateDepositDebit(ResourceWithAuth):
    @staticmethod
    def post():
        the_id = request.args.get('id')
        the_deposit_name = request.args.get('deposit')
        create_deposit_debit(the_id, the_deposit_name)
        return success_response()


class DeleteDepositDebit(ResourceWithAuth):
    @staticmethod
    def post():
        the_id = request.args.get('id')
        delete_deposit_debit(the_id)
        return success_response()


#%% LIST

class ListOperations (ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        operations = data(cycle=cycle)
        return json.loads(operations)


class AmountsDeposit(ResourceWithAuth):

    @staticmethod
    def get():
        filter_null = request.args.get('hide_null')
        return make_this_amount_item(get_state_deposit(filter_null=filter_null))


class AmountsBank(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return make_this_amount_item(bank_balances(cycle))


class AmountsCycle(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return make_this_amount_item(cycle_balances(cycle))


class GetRecurring(ResourceWithAuth):
    @staticmethod
    def get():
        return all_recurring()


class GetAllCycles(ResourceWithAuth):
    @staticmethod
    def get():
        return json.loads(all_cycles())


class GetLocalCycles(ResourceWithAuth):
    @staticmethod
    def get():
        long = request.args.get('long')
        cycle = request.args.get('cycle')
        return json.loads(local_cycles(cycle, long=string_to_bool(long) if long != None else False))


class GetAccounts(ResourceWithAuth):
    @staticmethod
    def get():
        return json.loads(all_accounts())


class GetCategories(ResourceWithAuth):
    @staticmethod
    def get():
        return json.loads(all_categories())


class GetRecurringDeposit(ResourceWithAuth):
    @staticmethod
    def get():
        return json.loads(all_recurring_deposit())


class GetDepositNames(ResourceWithAuth):
    @staticmethod
    def get():
        return all_deposit_names()


class GetDepositTransactions(ResourceWithAuth):
    @staticmethod
    def get():
        return json.loads(get_deposit_input_and_output().to_json(orient="records"))


#%% SERVICE

class RefreshOperations(ResourceWithAuth):

    @staticmethod
    def get():
        result = force_refresh()
        return {"status": result}


class ForceApiRefresh(ResourceWithAuth):

    @staticmethod
    def post():
        result = force_api_refresh()
        return {"status": result}


class Transfers(ResourceWithAuth):
    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return json.loads(json.dumps(get_transfers_to_do(cycle)))


class GetRecap(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        recap = get_recap_categories(cycle=cycle)
        return json.loads(recap)


class TransferTransaction(ResourceWithAuth):

    @staticmethod
    def post():
        id_one_way = request.args.get('id')
        account = request.args.get('account')
        create_transfer_if_possible(id_one_way, account)
        return success_response()


class EditBudget(ResourceWithAuth):

    @staticmethod
    def post():
        cycle = request.args.get('cycle')
        budgets = json.dumps(request.json['budgets'])
        print(budgets)
        print(cycle)
        edit_budgets(budgets, cycle)


#%% SET FIELDS


class LinkTransactions(ResourceWithAuth):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        result = link_ids_if_possible(ids)
        return {"status": result}


class UnlinkTransactions(ResourceWithAuth):

    @staticmethod
    def post():
        ids = request.json['transaction_ids']
        result = unlink_ids_if_possible(ids)
        return {"status": result}


# %% EDIT

class EditTransaction(ResourceWithAuth):
    @staticmethod
    def post():
        json_input = request.json
        warn('#edit_transaction\n' + str(json_input))
        result = edit_transaction(json_input)
        return result


class SplitTransaction(ResourceWithAuth):
    @staticmethod
    def post():
        json_input = request.json
        result = split_transaction(json_input)
        return result


class DeleteTransaction(ResourceWithAuth):

    @staticmethod
    def post():
        id_to_delete = request.args.get('id')
        type_to_delete = request.args.get('type')
        if type_to_delete == deposit_type:
            result = remove_deposit_input_on_id_if_possible(id_to_delete)
        else:
            result = remove_data_on_id_if_possible(id_to_delete)

        return {"status": result}


class CycleProgress(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return progress(cycle)


class GetBudgets(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        return make_this_amount_item(cycle_budgets(cycle))


class GetCyclesCalendar(ResourceWithAuth):

    @staticmethod
    def get():
        return json.loads(json.dumps(cycle_calendar()))


class EditCalendar(ResourceWithAuth):

    @staticmethod
    def post():
        json_input = request.json
        try:
            dates = json_input['dates']
            cycle = json_input['cycle']
            edit_calendar(dates, cycle)
        except:
            return failure_response('invalid data')


class AmountsInput(ResourceWithAuth):

    @staticmethod
    def get():
        cycle = request.args.get('cycle')
        u = input_overview(cycle)
        return make_this_amount_item(u)
