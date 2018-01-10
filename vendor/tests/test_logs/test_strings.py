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
from __future__ import division
from __future__ import absolute_import
from mo_logs import strings

from mo_logs.strings import expand_template
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestStrings(FuzzyTestCase):
    def setUp(self):
        pass

    def test_right_align(self):
        total = 123.45
        some_list = [10, 11, 14, 80]
        details = {"person": {"name": "Kyle Lahnakoski", "age": 40}}

        result = expand_template("it is currently {{now|datetime}}", {"now": 1420119241000})
        self.assertEqual(result, 'it is currently 2015-01-01 13:34:01')

        result = expand_template("Total: {{total|right_align(20)}}", {"total": total})
        self.assertEqual(result, 'Total:               123.45')

        result = expand_template("Summary:\n{{list|json|indent}}", {"list": some_list})
        self.assertEqual(result, 'Summary:\n\t[10, 11, 14, 80]')

        result = expand_template("Summary:\n{{list|indent}}", {"list": some_list})
        self.assertEqual(result, 'Summary:\n\t[10, 11, 14, 80]')

        result = expand_template("{{person.name}} is {{person.age}} years old", details)
        self.assertEqual(result, "Kyle Lahnakoski is 40 years old")


    def test_percent(self):

        self.assertEqual(strings.percent(.123, digits=1), "10%")
        self.assertEqual(strings.percent(.123, digits=2), "12%")
        self.assertEqual(strings.percent(.123, digits=3), "12.3%")
        self.assertEqual(strings.percent(.120, digits=3), "12.0%")

        self.assertEqual(strings.percent(.0123, digits=1), "1%")
        self.assertEqual(strings.percent(.0123, digits=2), "1.2%")
        self.assertEqual(strings.percent(.0123, digits=3), "1.23%")
        self.assertEqual(strings.percent(.0120, digits=3), "1.20%")

        self.assertEqual(strings.percent(0.5), "50%")
