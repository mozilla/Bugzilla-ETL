# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_math import stats

from mo_math.stats import ZeroMoment, ZeroMoment2Stats
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestStats(FuzzyTestCase):

    def setUp(self):
        stats.DEBUG = True
        stats.DEBUG_STRANGMAN = True

    def test_convert01(self):
        z_m = ZeroMoment(5, 3389.3216783216785, 2297521.2992811385, 1557436224.6382546, 1055759415011.5643)
        stats = ZeroMoment2Stats(z_m)
        self.assertAlmostEqual(stats, {
            "count": 5,
            "kurtosis": -1.6291215900707667,
            "mean": 677.8643356643357,
            "skew": -0.24151691954619345,
            "variance": 4.202290576475207
        }, places=12)
