from arlo.operations.df_operations import get_ids, filter_df_several_values, filter_df_one_value, df_is_not_empty, \
    extract_line_from_df, how_many_rows, concat_lines, filter_df_not_this_value
from arlo.read_write.reader import empty_data_dataframe
from arlo.tools.link_id import opposite_link_id, add_link_ids


def get_pending_transactions(df):
    return filter_df_one_value(df, 'pending', True)


def get_refund_transactions(df):
    refunds = filter_df_several_values(df, 'type', ['AV', 'AE'])
    return filter_df_one_value(refunds, 'link', '-')


def match_gone_transactions(gone, candidates, processed_transactions, id_field_name):
    recup_columns = ['name', 'category', 'comment', 'link']

    not_found = empty_data_dataframe()
    for _, row in gone.iterrows():
        replacement_transactions = filter_df_one_value(candidates, id_field_name, row[id_field_name])

        if df_is_not_empty(replacement_transactions):
            chosen_index = min(replacement_transactions.index)
            the_transaction = extract_line_from_df(chosen_index, candidates)
            the_transaction[recup_columns] = row[recup_columns]
            the_transaction = the_transaction.drop(labels=['link_id', 'link_id_no_amount', 'link_id_no_name'])
            processed_transactions = processed_transactions.append(the_transaction)
        else:
            not_found = not_found.append(row)

    return not_found, processed_transactions


def identify_old_new_data(old_data, new_data):
    old_ids = get_ids(old_data)
    new_ids = get_ids(new_data)

    unchanged = filter_df_several_values(old_data, 'id', old_ids & new_ids)
    new = filter_df_several_values(new_data, 'id', new_ids - old_ids)
    gone = filter_df_several_values(old_data, 'id', old_ids - new_ids)

    return gone, unchanged, new


def associate_pending_gone(old_data, new_data):
    (gone, old, new) = identify_old_new_data(old_data, new_data)
    add_link_ids(new)
    add_link_ids(gone)

    gone, old = match_gone_transactions(gone, new, old, 'link_id')
    gone, old = match_gone_transactions(gone, new, old, 'link_id_no_amount')
    gone, old = match_gone_transactions(gone, new, old, 'link_id_no_name')

    if len(gone):
        print('LOST TRANSACTIONS : ', len(gone))

    return old, new


def find_the_associated_refund(pending_transactions, pending_source, refund_transactions, refunds_source):
    # TODO use link function

    print('starting refund process')
    links_names = ['link_id', 'link_id_no_amount', 'link_id_no_name']

    while pending_transactions.shape[0] > 0 and links_names:
        not_found = []
        link_name = links_names.pop(0)
        print(pending_transactions.shape)
        for index_pending, pending_trans in pending_transactions.iterrows():
            candidates = filter_df_one_value(refund_transactions, link_name, opposite_link_id(pending_trans[link_name]))

            if df_is_not_empty(candidates):
                chosen_index = min(candidates.index)
                print('refund', index_pending, chosen_index)
                pending_source.loc[index_pending, ['link', 'pending']] = [refunds_source.loc[chosen_index, 'id'], False]
                refunds_source.loc[chosen_index, ['link', 'pending']] = [pending_source.loc[index_pending, 'id'], False]
                extract_line_from_df(chosen_index, refund_transactions)
            else:
                not_found.append(index_pending)

        pending_transactions = pending_transactions.loc[not_found]

    if df_is_not_empty(pending_transactions):
        print('PENDING NO REFUND FOUND :')
        print(how_many_rows(pending_transactions))
    if df_is_not_empty(refund_transactions):
        print('REFUNDS remaining')
        print(how_many_rows(refund_transactions))


def associate_pending_with_refund(old_data, new_data):
    old_pending = get_pending_transactions(old_data)
    new_pending = get_pending_transactions(new_data)

    refunds = get_refund_transactions(new_data)

    add_link_ids(old_pending)
    add_link_ids(new_pending)
    add_link_ids(refunds)

    find_the_associated_refund(old_pending, old_data, refunds, new_data)
    find_the_associated_refund(new_pending, new_data, refunds, new_data)

    return concat_lines([old_data, new_data])


def merge_n26_data(old_data, new_data):
    (old_data, new_data) = associate_pending_gone(old_data, new_data)
    old_data = old_data.sort_values("date", ascending=True).reset_index(drop=True)
    new_data = new_data.sort_values("date", ascending=True).reset_index(drop=True)

    return associate_pending_with_refund(old_data, new_data)


def merge_data(old_data, new_data):
    other_accounts = old_data[old_data['account'].str.endswith('_N26') == False]

    old_n26 = old_data[old_data['account'].str.endswith('N26')]
    old_n26_real = filter_df_not_this_value(old_n26, 'type', 'FIC')
    old_n26_fic = filter_df_one_value(old_n26, 'type', 'FIC')

    keep = concat_lines([other_accounts, old_n26_fic])

    all_n26 = merge_n26_data(old_n26_real, new_data)
    all_n26['pending'] = all_n26.apply(lambda row: row['type'] == 'AA' and row['link'] == '-', axis=1)

    return concat_lines([all_n26, keep]).sort_values("date", ascending=False).reset_index(drop=True)
