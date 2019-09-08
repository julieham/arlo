from arlo.tools.summary_by_field import recap_by_account
from parameters.column_names import amount_euro_col

TRANSFER = '>>'


def get_richest(balances):
    return min(name for name in balances if balances[name] == max(balances.values()))


def get_poorest(balances):
    return min(name for name in balances if balances[name] == min(balances.values()))


def create_transfer(amount, src, dest):
    return [{'source': src, 'destination': dest, 'amount': round(amount, ndigits=2)}]


def get_transfer_from_eco(balances):
    print(balances)
    total_balance = sum(balances.values())
    if total_balance > 0:
        richest = get_richest(balances)
        remove_from_balance(balances, richest, total_balance)
        return create_transfer(total_balance, richest, '')

    if total_balance < 0:
        poorest = get_poorest(balances)
        add_to_balance(balances, poorest, -total_balance)
        return create_transfer(-total_balance, '', poorest)

    return []


def remove_from_balance(balances, name, amount):
    balances[name] = round(balances[name] - amount, ndigits=2)


def add_to_balance(balances, name, amount):
    balances[name] = round(balances[name] + amount, ndigits=2)


def not_all_balances_are_zero(balances):
    print('#not_all_balances')
    print(balances)
    values = [round(bal, ndigits=2) for bal in balances.values()]
    return set(values) != {0}


def make_internal_transfer(balances):
    richest = get_richest(balances)
    poorest = get_poorest(balances)
    amount = min(abs(balances[richest]), abs(balances[poorest]))
    remove_from_balance(balances, richest, amount)
    add_to_balance(balances, poorest, amount)
    return create_transfer(amount, richest, poorest)


def balances_to_transfers(balances):
    transfers = []
    if not_all_balances_are_zero(balances):
        transfers = transfers + get_transfer_from_eco(balances)

    while not_all_balances_are_zero(balances):
        transfers = transfers + make_internal_transfer(balances)

    return transfers


def get_end_of_cycle_balances(cycle):
    bilan_this_cycle = recap_by_account(cycle)
    other_accounts = [acc for acc in bilan_this_cycle.index.tolist() if acc.endswith('N26') == False]
    bilan_this_cycle.at['Hello & Co', amount_euro_col] = sum(bilan_this_cycle.loc[other_accounts, amount_euro_col])
    bilan_this_cycle = bilan_this_cycle[amount_euro_col]
    return bilan_this_cycle.drop(labels=other_accounts).to_dict()
