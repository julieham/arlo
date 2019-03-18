from unittest import TestCase

import pandas as pd

from operations.df_operations import set_pandas_print_parameters
from read_write.file_manager import read_data_from_file
from tools.merge_data import merge_data_from_single_account



class MergeDataFromSingleAccount(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assert_equals_df(self, expected_merged, effective_merged):
        expected_merged = expected_merged.sort_values('id').reset_index(drop=True)
        effective_merged = effective_merged.sort_values('id').reset_index(drop=True)
        pd.testing.assert_frame_equal(expected_merged,effective_merged, check_like=True, check_dtype=False)

    def test_no_old_no_new(self):
        old_data = pd.DataFrame({'id':[]})
        new_data = pd.DataFrame({'id':[]})
        expected_merged = pd.DataFrame({'id':[]})
        effective_merged = merge_data_from_single_account(old_data, new_data, 'acc')
        self.assert_equals_df(expected_merged, effective_merged)

    def test_no_new(self):
        old_data = read_data_from_file('./test_files/one_transaction.csv')
        new_data = pd.DataFrame({'id':[]})
        expected_merged = old_data
        effective_merged = merge_data_from_single_account(old_data, new_data, 'acc')
        self.assert_equals_df(expected_merged, effective_merged)

    def test_no_old_one_new(self):
        old_data = pd.DataFrame({'id':[]})
        new_data = read_data_from_file('./test_files/one_transaction.csv')
        expected_merged = new_data
        effective_merged = merge_data_from_single_account(old_data, new_data, 'acc')
        self.assert_equals_df(expected_merged, effective_merged)

    def test_two_old_one_new(self):
        old_data = read_data_from_file('./test_files/two_transactions.csv')
        new_data = read_data_from_file('./test_files/one_transaction.csv')
        expected_merged = read_data_from_file('./test_files/merged_one_and_two_transactions.csv')
        effective_merged = merge_data_from_single_account(old_data, new_data, 'acc')
        self.assert_equals_df(expected_merged, effective_merged)
