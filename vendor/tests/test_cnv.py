# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import datetime
import unittest

from mo_future import text_type
from pyLibrary import convert


class TestConvert(unittest.TestCase):
    def test_datetime(self):

        result = convert.datetime2milli(datetime.datetime(2012, 7, 24))
        expected = 1343088000000
        assert result == expected

        result = convert.datetime2milli(datetime.date(2012, 7, 24))
        expected = 1343088000000
        assert result == expected

        result = convert.datetime2milli(datetime.datetime(2014, 1, 7, 10, 21, 0))
        expected = 1389090060000
        assert result == expected

        result = text_type(convert.datetime2milli(datetime.datetime(2014, 1, 7, 10, 21, 0)))
        expected = u"1389090060000"
        assert result == expected

