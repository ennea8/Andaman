__author__ = 'zephyre'

import unittest


def fun(x):
    return x + 1


class MainTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(4), 5)

    def testAnother(self):
        self.assertEqual(fun(8), 9)


class FakeTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 5)