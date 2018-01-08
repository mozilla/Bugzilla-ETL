# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# from __future__ import unicode_literals
import random
import unittest
from math import floor

from mo_math import Math
from mo_math.randoms import Random


class TestMath(unittest.TestCase):
    def test_isnumber(self):
        assert Math.is_number(9999999999000)

    def test_mod(self):
        self.assertEqual(Math.mod(12, 12), 0)
        self.assertEqual(Math.mod(11, 12), 11)
        self.assertEqual(Math.mod(2, 12), 2)
        self.assertEqual(Math.mod(1, 12), 1)
        self.assertEqual(Math.mod(-0, 12), 0)
        self.assertEqual(Math.mod(-1, 12), 11)
        self.assertEqual(Math.mod(-2, 12), 10)
        self.assertEqual(Math.mod(-12, 12), 0)

    def test_floor(self):
        self.assertEqual(Math.floor(0, 1), 0)
        self.assertEqual(Math.floor(1, 1), 1)
        self.assertEqual(Math.floor(-1, 1), -1)
        self.assertEqual(Math.floor(0.1, 1), 0)
        self.assertEqual(Math.floor(1.1, 1), 1)
        self.assertEqual(Math.floor(-1.1, 1), -2)

        self.assertEqual(Math.floor(0, 2), 0)
        self.assertEqual(Math.floor(1, 2), 0)
        self.assertEqual(Math.floor(-1, 2), -2)
        self.assertEqual(Math.floor(0.1, 2), 0)
        self.assertEqual(Math.floor(1.1, 2), 0)
        self.assertEqual(Math.floor(-1.1, 2), -2)
        self.assertEqual(Math.floor(-10, 2), -10)

    def test_floor_mod_identity(self):
        for i in range(100):
            x = Random.float()*200 - 100.0
            m = abs(random.gauss(0, 5))

            self.assertAlmostEqual(Math.floor(x, m)+Math.mod(x, m), x, places=7)

    def test_floor_mod_identity_w_ints(self):
        for i in range(100):
            x = Random.float()*200 - 100.0
            m = floor(abs(random.gauss(0, 5)))

            if m == 0:
                self.assertEqual(Math.floor(x, m), None)
                self.assertEqual(Math.mod(x, m), None)
            else:
                self.assertAlmostEqual(Math.floor(x, m)+Math.mod(x, m), x, places=7)
