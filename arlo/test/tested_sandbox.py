import numpy as np
from arlo.format.df_operations import *


def add_link_id(df):
    pd.options.mode.chained_assignment = None

    df.loc[:, 'theSign'] = np.where(df['amount'] >= 0, '+', '-')
    df.loc[:, 'theAmount'] = (100*np.where(pd.isnull(df['originalAmount']), df['amount'].abs(),df['originalAmount'].abs())).astype(int).astype(str)
    df.loc[:, 'theCurrency'] = np.where(pd.isnull(df['originalAmount']), 'EUR', df['originalCurrency'])

    df.loc[:, 'linkID'] = df['theSign'].str.cat(df[['theAmount', 'theCurrency', 'bank_name', 'account']], sep='*')
    df.loc[:, 'linkIDnoName'] = df['theSign'].str.cat(df[['theAmount', 'theCurrency', 'account']], sep='*')
    df.loc[:, 'linkIDnoAmount'] = df['theSign'].str.cat(df[['theCurrency', 'bank_name', 'account']], sep='*')

    df.drop(['theAmount', 'theCurrency', 'theSign'], inplace=True, axis=1)


def match_gone_transactions(gone, candidates, processed_transactions, id_field_name):
    recup_columns = ['name', 'category', 'comment']

    not_found_pending_gone = make_empty_dataframe_based_on_this(gone)

    for _, row in gone.iterrows():
        replacement_transactions = filter_df_one_value(candidates, id_field_name, row[id_field_name])

        if df_is_not_empty(replacement_transactions):
            chosen_index = min(replacement_transactions.index)
            the_transaction = extract_line_from_df(chosen_index, candidates)
            the_transaction[recup_columns] = row[recup_columns]
            add_line_to_df(the_transaction, processed_transactions)
        else:
            add_line_to_df(row, not_found_pending_gone)

    return not_found_pending_gone


def identify_old_new_data(old_data, new_data):
    old_ids = get_ids(old_data)
    new_ids = get_ids(new_data)

    unchanged = filter_df_several_values(old_data, 'id', old_ids & new_ids)
    new = filter_df_several_values(new_data, 'id', new_ids - old_ids)
    gone = filter_df_several_values(old_data, 'id', old_ids - new_ids)

    return (gone, unchanged, new)


def associate_pending_gone(old_data, new_data):
    (gone, old, new) = identify_old_new_data(old_data, new_data)

    add_link_id(new)
    add_link_id(gone)

    gone = match_gone_transactions(gone, new, old, 'linkID')
    gone = match_gone_transactions(gone, new, old, 'linkIDnoAmount')
    gone = match_gone_transactions(gone, new, old, 'linkIDnoName')

    if len(gone):
        print('LOST TRANSACTIONS : ', len(gone))

    return (old, new)


def opposite_sign_linkID(linkID):
    replacement_sign = dict({'-': '+', '+': '-'})
    return replacement_sign[linkID[0]] + linkID[1:]


def find_the_associated_refund(pending_transactions, pending_source, refund_transactions, refunds_source):
    for index_pending, pending_trans in pending_transactions.iterrows():
        candidates = filter_df_one_value(refunds_source, 'linkID', opposite_sign_linkID(pending_trans['linkID']))
        if df_is_not_empty(candidates):
            chosen_index = min(candidates.index)
            pending_source.loc[index_pending, ['link', 'pending']] = [refunds_source.loc[chosen_index, 'id'], False]
            refunds_source.loc[chosen_index, ['link', 'pending']] = [pending_source.loc[index_pending, 'id'], False]
            extract_line_from_df(chosen_index, refund_transactions)


def associate_pending_with_refund(old_data, new_data):
    old_pending = get_pending_transactions(old_data)
    new_pending = get_pending_transactions(new_data)

    refunds = get_refund_transactions(new_data)

    add_link_id(old_pending)
    add_link_id(new_pending)
    add_link_id(refunds)

    find_the_associated_refund(old_pending, old_data, refunds, new_data)
    find_the_associated_refund(new_pending, new_data, refunds, new_data)

    return pd.concat([old_data, new_data], join='inner')


def merge_n26_data(old_data, new_data):
    (old_data, new_data) = associate_pending_gone(old_data, new_data)

    return associate_pending_with_refund(old_data, new_data)
