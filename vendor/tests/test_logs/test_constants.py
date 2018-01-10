# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

from mo_logs import constants
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestConstants(FuzzyTestCase):


    def test_setting(self):
        constants.set({"mo_logs": {"constants": {"DEBUG": False}}})
        self.assertEqual(constants.DEBUG, False, "expecting change")

        constants.set({"mo_logs": {"constants": {"DEBUG": True}}})
        self.assertEqual(constants.DEBUG, True, "expecting change")
