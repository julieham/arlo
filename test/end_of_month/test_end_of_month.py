import unittest

from tools.transfers import balances_to_transfers


class EndOfMonth(unittest.TestCase):

    def assertListContentAreEqual(self, expected, effective):
        self.assertEqual(len(expected), len(effective))
        for el in expected:
            self.assertTrue(el in effective)

    def test_one_is_positive_one_sup_total(self):
        given = {'J': -115, 'T': 233, 'H': -695}
        expected = [{'source': '', 'destination': 'H', 'amount': 577},
                    {'source': 'T', 'destination': 'J', 'amount': 115},
                    {'source': 'T', 'destination': 'H', 'amount': 118}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_one_is_positive_one_inf_total(self):
        given = dict({'J': -300, 'T': 200, 'H': -300})
        expected = [{'source': '', 'destination': 'H', 'amount': 400},
                    {'source': 'T', 'destination': 'J', 'amount': 200},
                    {'source': 'H', 'destination': 'J', 'amount': 100}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_all_negative(self):
        given = dict({'J': -50, 'T': -5, 'H': -100})
        expected = [{'source': '', 'destination': 'H', 'amount': 155},
                    {'source': 'H', 'destination': 'J', 'amount': 50}, {'source': 'H', 'destination': 'T', 'amount': 5}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_one_is_zero(self):
        given = dict({'J': -20, 'T': -10, 'H': 0})
        expected = [{'source': '', 'destination': 'J', 'amount': 30}, {'source': 'J', 'destination': 'T', 'amount': 10}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_two_is_zero(self):
        given = dict({'J': 0, 'T': -10, 'H': 0})
        expected = [{'source': '', 'destination': 'T', 'amount': 10}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_all_is_zero(self):
        given = dict({'J': 0, 'T': 0, 'H': 0})
        expected = []
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_total_positive(self):
        given = dict({'J': -300, 'T': 1000, 'H': -100})
        expected = [{'source': 'T', 'destination': '', 'amount': 600},
                    {'source': 'T', 'destination': 'H', 'amount': 100},
                    {'source': 'T', 'destination': 'J', 'amount': 300}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_total_positive_but_several_account(self):
        given = dict({'J': 300, 'T': 200, 'H': 100})
        expected = [{'source': 'J', 'destination': '', 'amount': 600},
                    {'source': 'H', 'destination': 'J', 'amount': 100},
                    {'source': 'T', 'destination': 'J', 'amount': 200}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))

    def test_total_is_zero(self):
        given = dict({'J': -3, 'T': 2, 'H': 1})
        expected = [{'source': 'H', 'destination': 'J', 'amount': 1}, {'source': 'T', 'destination': 'J', 'amount': 2}]
        self.assertListContentAreEqual(expected, balances_to_transfers(given))


if __name__ == '__main__':
    unittest.main()
