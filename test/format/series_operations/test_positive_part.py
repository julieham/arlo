from unittest import TestCase

import pandas as pd

from operations.series_operations import positive_part


class PositivePart(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_empty_series(self):
        effective_positive_part = positive_part(pd.Series())
        expected_positive_part = pd.Series()
        self.assertTrue(expected_positive_part.equals(effective_positive_part))

    def test_all_positive(self):
        effective_positive_part = positive_part(pd.Series([1, 2, 3]))
        expected_positive_part = pd.Series([1, 2, 3])
        self.assertTrue(expected_positive_part.equals(effective_positive_part))

    def test_all_negative(self):
        effective_positive_part = positive_part(pd.Series([-9, -100, -0.5]))
        expected_positive_part = pd.Series([0.0, 0.0, 0.0])
        self.assertTrue(expected_positive_part.equals(effective_positive_part))
