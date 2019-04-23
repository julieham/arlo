from unittest import TestCase

import pandas as pd

from read_write.file_manager import read_data_from_file, read_data, save_data
from read_write.reader import empty_data_dataframe
from tools.merge_data import merge_with_data


class MergeDataFromSingleAccount(TestCase):
    saved_current_data = empty_data_dataframe()
    directory = './test_files/'
    effective_merged = None

    def setUp(self):
        self.saved_current_data = read_data()
        self.effective_merged = None

    def tearDown(self):
        save_data(self.saved_current_data)

    def assert_equals_df(self, expected_merged, effective_merged):
        expected_merged = expected_merged.sort_values('id').reset_index(drop=True)
        effective_merged = effective_merged.sort_values('id').reset_index(drop=True)
        pd.testing.assert_frame_equal(expected_merged,effective_merged, check_like=True, check_dtype=False)

    def test_split_AA_to_PT(self):
        directory = 'split_AA_to_PT/'

        self.given_data_file(directory)

        self.when_merge_data(directory)

        self.then_merged_data_file_is(directory)

    def test_linked_AA_to_PT(self):
        directory = 'linked_AA_to_PT/'

        self.given_data_file(directory)

        self.when_merge_data(directory)

        self.then_merged_data_file_is(directory)

    # TOOLS

    def given_data_file(self, directory_name):
        save_data(read_data_from_file(self.directory + directory_name + 'data.csv'))

    def when_merge_data(self, latest_directory):
        self.latest = read_data_from_file(self.directory + latest_directory + 'latest.csv')
        merge_with_data(self.latest, 'acc')
        self.effective_merged = read_data()

    def then_merged_data_file_is(self, merged_directory):
        expected_merged = read_data_from_file(self.directory + merged_directory + 'expected.csv')

        self.assert_equals_df(expected_merged, self.effective_merged)
