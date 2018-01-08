# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from mo_collections.unique_index import UniqueIndex
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestUniqueIndex(FuzzyTestCase):
    def test_single_key(self):
        data = [
            {"a": 1, "b": "w"},
            {"a": 2, "b": "x"},
            {"a": 3, "b": "y"},
            {"a": 4, "b": "z"}
        ]

        i = UniqueIndex(["a"], data=data)
        s = UniqueIndex(["a"])

        s.add({"a": 4, "b": "x"})

        self.assertEqual(i - s, [
            {"a": 1, "b": "w"},
            {"a": 2, "b": "x"},
            {"a": 3, "b": "y"}
        ])

        self.assertEqual(i | s, data)
        self.assertEqual(s | i, [
            {"a": 1, "b": "w"},
            {"a": 2, "b": "x"},
            {"a": 3, "b": "y"},
            {"a": 4, "b": "x"}
        ])

        self.assertEqual(i & s, [{"a": 4, "b": "z"}])

    def test_double_key(self):
        data = [
            {"a": 1, "b": "w"},
            {"a": 2, "b": "x"},
            {"a": 3, "b": "y"},
            {"a": 4, "b": "z"}
        ]

        i = UniqueIndex(["a", "b"], data=data)
        s = UniqueIndex(["a", "b"])

        s.add({"a": 4, "b": "x"})

        self.assertEqual(i - s, data)

        self.assertEqual(i | s, i |s)

        self.assertEqual(i & s, [])


