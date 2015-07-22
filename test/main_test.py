__author__ = 'zephyre'

import unittest


class UtilsTest(unittest.TestCase):
    def testHaversine(self):
        from utils.utils import haversine

        lon1, lat1 = 40, 50
        lon2, lat2 = 30, 40
        calc_dist = haversine(lon1, lat1, lon2, lat2)
        actual_dist = 1358.4011247032208
        self.assertAlmostEqual(calc_dist, actual_dist)

        lat1 = -91
        valid_param = True
        try:
            haversine(lon1, lat1, lon2, lat2)
        except ValueError:
            valid_param = False

        self.assertFalse(valid_param, "Failed in arguments validation")