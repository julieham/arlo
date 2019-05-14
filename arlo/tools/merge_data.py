from arlo.operations.df_operations import filter_df_one_value, df_is_not_empty, concat_lines, both_series_are_true, \
    get_loc_df, drop_line_with_index, assign_value_to_loc, add_column_with_value, set_pandas_print_parameters, is_empty
from arlo.tools.link_id import add_link_ids, fields_link_ids
from parameters.param import editable_fields_to_recover
from read_write.file_manager import read_data, save_data, default_value, add_new_data
from read_write.reader import empty_data_dataframe
from services.set_fields import link_ids_if_possible, all_transactions_linked_to_this, unlink_ids_if_possible
from tools.uniform_data_maker import add_pending_column, add_refund_column
from web.status import is_successful


def get_pending_transactions(df):
    add_pending_column(df)
    return filter_df_one_value(df, 'pending', True)


def get_refund_transactions(df):
    add_refund_column(df)
    return filter_df_one_value(df, 'refund', True)


def find_matches_pending_refunds(pending, refunds, link_name):
    for index_refund in refunds.index.tolist()[::-1]:
        link_value = get_loc_df(refunds, index_refund, link_name)
        match = filter_df_one_value(pending, link_name, link_value)

        if df_is_not_empty(match):
            index_pending = max(match.index)
            id_refund = get_loc_df(refunds, index_refund, 'id')
            id_pending = get_loc_df(pending, index_pending, 'id')
            if is_successful(link_ids_if_possible(id_refund + ',' + id_pending)):
                drop_line_with_index(pending, index_pending)


def link_the_refunds():
    for link_name in fields_link_ids:
        data = read_data()
        pending = get_pending_transactions(data)
        refunds = get_refund_transactions(data)

        if df_is_not_empty(pending) & df_is_not_empty(refunds):
            add_link_ids(pending, '-', '+')
            add_link_ids(refunds, '+', '-')

            find_matches_pending_refunds(pending, refunds, link_name)


def find_matches_gone_newsettled(new, gone, link_name, links_to_add):
    data = read_data()
    free_new = new[new['replaces_a_pending'] == False]
    for index_gone in gone.index.tolist():
        link_value = get_loc_df(gone, index_gone, link_name)
        match = filter_df_one_value(free_new, link_name, link_value)

        if df_is_not_empty(match):
            index_new_settled = max(match.index)
            gone_transaction = gone.loc[index_gone]
            settled_transaction = new.loc[index_new_settled]
            set_pandas_print_parameters()
            print('#merge_data ------- Identified : -------')
            print(concat_lines([gone_transaction.to_frame().T, settled_transaction.to_frame().T]))
            print('merge_data -----------------------------')

            to_relink_after_gone_replaced_with_settled(data, gone_transaction, settled_transaction, links_to_add)
            data = read_data()
            recover_editable_fields(new, index_new_settled, gone, index_gone)
            drop_line_with_index(gone, index_gone)
            drop_line_with_index(data, index_gone)
            assign_value_to_loc(new, index_new_settled, 'replaces_a_pending', True)
            save_data(data)


def recover_editable_fields(appeared_data, index_appeared, gone_data, index_gone):
    for field in editable_fields_to_recover:
        assign_value_to_loc(appeared_data, index_appeared, field, get_loc_df(gone_data, index_gone, field))


def to_relink_after_gone_replaced_with_settled(data, gone_transaction, settled_transaction, links_to_add):
    link_gone = gone_transaction['link']
    id_gone = gone_transaction['id']
    id_settled = settled_transaction['id']
    if link_gone != default_value('link'):
        all_links = ','.join(all_transactions_linked_to_this(data, id_gone))
        unlink_ids_if_possible(id_gone)
        links_to_add.append(all_links.replace(id_gone, id_settled))


def identify_new_and_gone(data, latest_data, account):
    if df_is_not_empty(latest_data):
        add_link_ids(latest_data, '+', '-')
        condition1 = data['account'] == account
        condition2 = data['type'] != 'FIC'
        previous_data = data[both_series_are_true(condition1, condition2)]
        new_data = latest_data[latest_data['id'].isin(previous_data['id']) == False]
        previous_in_range = previous_data[previous_data['date'] >= min(latest_data['date'])]
        gone_data = previous_in_range[previous_in_range['id'].isin(latest_data['id']) == False]
        if df_is_not_empty(gone_data):
            add_link_ids(gone_data, '+', '-')
            add_column_with_value(new_data, 'replaces_a_pending', False)
            return new_data, gone_data
        return new_data, empty_data_dataframe()
    return empty_data_dataframe(), empty_data_dataframe()


def delete_gone_from_data(data, gone):
    if df_is_not_empty(gone):
        print('#merge_data NOT FOUND GONE TRANSACTIONS :')
        set_pandas_print_parameters()
        print(gone)
        for index in gone.index:
            drop_line_with_index(data, index)
        save_data(data)


def process_gone_transactions(latest, account):
    data = read_data()
    new_data, gone_data = identify_new_and_gone(data, latest, account)
    if is_empty(new_data):
        delete_gone_from_data(data, gone_data)
        return

    if is_empty(gone_data):
        add_new_data(new_data)
        return

    links_to_add = []
    for link_name in fields_link_ids:
        find_matches_gone_newsettled(new_data, gone_data, link_name, links_to_add)
    add_new_data(new_data)
    delete_gone_from_data(data, gone_data)
    for link_ids in links_to_add:
        link_ids_if_possible(link_ids)


def merge_with_data(latest_data, account):
    process_gone_transactions(latest_data, account)
    link_the_refunds()
