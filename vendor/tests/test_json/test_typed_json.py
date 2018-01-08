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

from mo_dots import wrap
from mo_json.typed_encoder import EXISTS_TYPE, NUMBER_TYPE, STRING_TYPE, BOOLEAN_TYPE, NESTED_TYPE
from mo_logs.strings import quote
from pyLibrary.env.typed_inserter import TypedInserter


def typed_encode(value):
    return TypedInserter().typed_encode({"value": value})['json']


class TestJSON(unittest.TestCase):
    def test_date(self):
        value = {"test": datetime.date(2013, 11, 13)}
        test1 = typed_encode(value)
        expected = u'{"test":{' + quote(NUMBER_TYPE) + u':1384318800},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_unicode1(self):
        value = {"comment": u"Open all links in the current tab, except the pages opened from external apps â€” open these ones in new windows"}
        test1 = typed_encode(value)
        expected = u'{"comment":{' + quote(STRING_TYPE) + u':"Open all links in the current tab, except the pages opened from external apps â€” open these ones in new windows"},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_unicode2(self):
        value = {"comment": "testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"}
        test1 = typed_encode(value)
        expected = u'{"comment":{' + quote(STRING_TYPE) + u':"testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_unicode3(self):
        value = {"comment": u"testing accented char ŕáâăäĺćçčéęëěíîďđńňóôőö÷řůúűüýţ˙"}
        test1 = typed_encode(value)
        expected = u'{"comment":{' + quote(STRING_TYPE) + u':"testing accented char ŕáâăäĺćçčéęëěíîďđńňóôőö÷řůúűüýţ˙"},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_double(self):
        value = {"value": 5.2025595183536973e-07}
        test1 = typed_encode(value)
        expected = u'{"value":{' + quote(NUMBER_TYPE) + u':5.202559518353697e-7},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_empty_list(self):
        value = {"value": []}
        test1 = typed_encode(value)
        expected = u'{"value":{' + quote(NESTED_TYPE) + u':[]},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_nested(self):
        value = {"a": {}, "b": {}}
        test1 = typed_encode(value)
        expected = u'{"a":{' + quote(EXISTS_TYPE) + u':0},"b":{' + quote(EXISTS_TYPE) + u':0},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_list_of_objects(self):
        value = {"a": [{}, "b"]}
        test1 = typed_encode(value)
        expected = u'{"a":{"'+NESTED_TYPE+u'":[{' + quote(EXISTS_TYPE) + u':0},{' + quote(STRING_TYPE) + u':"b"}],' + quote(EXISTS_TYPE) + u':2},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_empty_list_value(self):
        value = []
        test1 = typed_encode(value)
        expected = u'{'+quote(NESTED_TYPE)+':[]}'
        self.assertEqual(test1, expected)

    def test_list_value(self):
        value = [42]
        test1 = typed_encode(value)
        expected = u'{' + quote(NUMBER_TYPE) + u':42}'
        self.assertEqual(test1, expected)

    def test_list(self):
        value = {"value": [23, 42]}
        test1 = typed_encode(value)
        expected = u'{"value":{' + quote(NUMBER_TYPE) + u':[23,42]},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_number_value(self):
        value = 42
        test1 = typed_encode(value)
        expected = '{' + quote(NUMBER_TYPE) + u':42}'
        self.assertEqual(test1, expected)

    def test_empty_string_value(self):
        value = u""
        test1 = typed_encode(value)
        expected = '{' + quote(STRING_TYPE) + u':""}'
        self.assertEqual(test1, expected)

    def test_string_value(self):
        value = u"42"
        test1 = typed_encode(value)
        expected = '{' + quote(STRING_TYPE) + u':"42"}'
        self.assertEqual(test1, expected)

    def test_escaped_string_value(self):
        value = "\""
        test1 = typed_encode(value)
        expected = '{' + quote(STRING_TYPE) + u':"\\""}'
        self.assertEqual(test1, expected)

    def test_bad_key(self):
        test = {24: "value"}
        self.assertRaises(Exception, typed_encode, *[test])

    def test_false(self):
        value = False
        test1 = typed_encode(value)
        expected = '{' + quote(BOOLEAN_TYPE) + u':false}'
        self.assertEqual(test1, expected)

    def test_true(self):
        value = True
        test1 = typed_encode(value)
        expected = '{' + quote(BOOLEAN_TYPE) + u':true}'
        self.assertEqual(test1, expected)

    def test_null(self):
        def encode_null():
            value = None
            typed_encode(value)

        self.assertRaises(Exception, encode_null)

    def test_empty_dict(self):
        value = wrap({"match_all": wrap({})})
        test1 = typed_encode(value)
        expected = u'{"match_all":{' + quote(EXISTS_TYPE) + u':0},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_complex_object(self):
        value = wrap({"s": 0, "r": 5})
        test1 = typed_encode(value)
        expected = u'{"r":{' + quote(NUMBER_TYPE) + u':5},"s":{' + quote(NUMBER_TYPE) + u':0},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_empty_list1(self):
        value = wrap({"a": []})
        test1 = typed_encode(value)
        expected = u'{"a":{' + quote(NESTED_TYPE) + u':[]},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)

    def test_empty_list2(self):
        value = wrap({"a": [], "b": 1})
        test1 = typed_encode(value)
        expected = u'{"a":{' + quote(NESTED_TYPE) + ':[]},"b":{' + quote(NUMBER_TYPE) + u':1},' + quote(EXISTS_TYPE) + u':1}'
        self.assertEqual(test1, expected)
