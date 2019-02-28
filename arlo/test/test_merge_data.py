from unittest import TestCase
from arlo.tools.merge_data import merge_data

import pandas as pd
import numpy as np

desired_width = 10000
pd.set_option('display.width', desired_width)
pd.np.set_printoptions(linewidth=desired_width)
pd.set_option("display.max_columns", 30)


class MergeDataTest(TestCase):

    def merge_with_pattern(self, test_name):
        old_data = read_test_data_prefix(test_name, 'old_data')
        new_data = read_test_data_prefix(test_name, 'new_data')
        expected_data = read_test_data_prefix(test_name, 'expected').sort_values("date", ascending=False).reset_index(drop=True)
        effective_data = merge_data(old_data, new_data).sort_values("date", ascending=False).reset_index(drop=True)
        result = effective_data.equals(expected_data)
        if not result:
            print('TEST NAME :', test_name)
            print('EXP')
            print(expected_data)
            print('EFF')
            print(effective_data)

        return result

    def test_empty_file(self):
        old_data = read_test_data_prefix('empty', 'data')
        new_data = read_test_data_prefix('empty', 'data')

        self.assertTrue(merge_data(old_data, new_data).empty)

    def test_nothing_to_merge(self):
        test_name = 'nothing_to_merge'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_one_transaction_pending_gone(self):
        test_name = 'one_transaction_pending_gone'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_one_transaction_amount_changed(self):
        test_name = 'one_transaction_amount_changed'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_just_new_data(self):
        test_name = 'just_new_data'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_new_pending_new_refund(self):
        test_name = 'new_pending_new_refund'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_old_pending_new_refund(self):
        test_name = 'old_pending_new_refund'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_one_pending_two_refunds(self):
        test_name = 'one_pending_two_refunds'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)

    def test_refund_name_change(self):
        test_name = 'refund_name_change'
        result = self.merge_with_pattern(test_name)

        self.assertTrue(result)


def read_test_data_prefix(file_name_case, prefix):
    return pd.read_csv('./test_files/' + file_name_case + '_' + prefix + ".csv", dtype={'pending': np.bool})