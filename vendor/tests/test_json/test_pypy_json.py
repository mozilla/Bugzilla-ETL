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

import datetime
import unittest

from mo_dots import Data, wrap
from mo_future import text_type
from mo_json import json2value
from mo_logs import Log
from pyLibrary import convert

import mo_json
from mo_json.encoder import pypy_json_encode, cPythonJSONEncoder, pretty_json
from mo_times.dates import Date


def value2json(value):
    return pypy_json_encode(value)


class TestPyPyJSON(unittest.TestCase):

    def test_date(self):
        output = value2json({"test": datetime.date(2013, 11, 13)})
        Log.note("JSON = {{json}}", json= output)


    def test_unicode1(self):
        output = value2json({"comment": u"Open all links in the current tab, except the pages opened from external apps â€” open these ones in new windows"})
        assert output == u'{"comment":"Open all links in the current tab, except the pages opened from external apps â€” open these ones in new windows"}'

        if not isinstance(output, text_type):
            Log.error("expecting unicode json")

    def test_unicode2(self):
        output = value2json({"comment": "testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"})

        assert output == u'{"comment":"testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"}'
        if not isinstance(output, text_type):
            Log.error("expecting text_type json")

    def test_unicode3(self):
        output = value2json({"comment": u"testing accented char ŕáâăäĺćçčéęëěíîďđńňóôőö÷řůúűüýţ˙"})
        assert output == u'{"comment":"testing accented char ŕáâăäĺćçčéęëěíîďđńňóôőö÷řůúűüýţ˙"}'
        if not isinstance(output, text_type):
            Log.error("expecting unicode json")

    def test_double1(self):
        test = {"value":5.2025595183536973e-07}
        output = value2json(test)
        if output != u'{"value":5.202559518353697e-7}':
            Log.error("expecting correct value")

    def test_double2(self):
        test = {"value": 52}
        output = value2json(test)
        if output != u'{"value":52}':
            Log.error("expecting correct value")

    def test_double3(self):
        test = {"value": .52}
        output = value2json(test)
        if output != u'{"value":0.52}':
            Log.error("expecting correct value")

    def test_generator(self):
        test = {"value": (x for x in [])}
        output = value2json(test)
        if output != u'{"value":[]}':
            Log.error("expecting correct value")

    def test_bad_key(self):
        test = {24: "value"}
        self.assertRaises(Exception, value2json, *[test])

    def test_bad_long_json(self):
        test = value2json({"values": [i for i in range(1000)]})
        test = test[:1000] + "|" + test[1000:]
        expected = u"Can not decode JSON at:\n\t..., 216, 217, 218, 219|, 220, 221, 222, 22...\n\t                       ^\n"
        # expected = u'Can not decode JSON at:\n\t...9,270,271,272,273,27|4,275,276,277,278,2...\n\t                       ^\n'
        try:
            output = json2value(test)
            Log.error("Expecting error")
        except Exception as e:
            if "Can not decode JSON" in e:
                return  # GOOD ENOUGH
            if e.message != expected:
                Log.error("Expecting good error message", cause=e)

    def test_whitespace_prefix(self):
        hex = "00 00 00 00 7B 22 74 68 72 65 61 64 22 3A 20 22 4D 61 69 6E 54 68 72 65 61 64 22 2C 20 22 6C 65 76 65 6C 22 3A 20 22 49 4E 46 4F 22 2C 20 22 70 69 64 22 3A 20 31 32 39 33 2C 20 22 63 6F 6D 70 6F 6E 65 6E 74 22 3A 20 22 77 70 74 73 65 72 76 65 22 2C 20 22 73 6F 75 72 63 65 22 3A 20 22 77 65 62 2D 70 6C 61 74 66 6F 72 6D 2D 74 65 73 74 73 22 2C 20 22 74 69 6D 65 22 3A 20 31 34 32 34 31 39 35 30 31 33 35 39 33 2C 20 22 61 63 74 69 6F 6E 22 3A 20 22 6C 6F 67 22 2C 20 22 6D 65 73 73 61 67 65 22 3A 20 22 53 74 61 72 74 69 6E 67 20 68 74 74 70 20 73 65 72 76 65 72 20 6F 6E 20 31 32 37 2E 30 2E 30 2E 31 3A 38 34 34 33 22 7D 0A"
        json = convert.utf82unicode(convert.hex2bytes("".join(hex.split(" "))))
        self.assertRaises(Exception, json2value, *[json])

    def test_default_python(self):

        test = {"add": Data(start="".join([" ", "â€"]))}
        output = value2json(test)

        expecting = u'{"add":{"start":" â€"}}'
        self.assertEqual(expecting, output, "expecting correct json")

    def test_false(self):
        test = value2json(wrap({"value": False}))
        expecting = u'{"value":false}'
        self.assertEqual(test, expecting, "expecting False to serialize as 'false'")

    def test_empty_dict(self):
        test = value2json(wrap({"match_all": wrap({})}))
        expecting = u'{"match_all":{}}'
        self.assertEqual(test, expecting, "expecting empty dict to serialize")

    def test_empty_list1(self):
        test = value2json(wrap({"a": []}))
        expecting = u'{"a":[]}'
        self.assertEqual(test, expecting, "expecting empty list to serialize")

    def test_empty_list2(self):
        test = value2json(wrap({"a": [], "b": 1}))
        expecting = u'{"a":[],"b":1}'
        self.assertEqual(test, expecting, "expecting empty list to serialize")

    def test_deep_empty_dict(self):
        test = value2json(wrap({"query": {"match_all": {}}, "size": 20000}))
        expecting = u'{"query":{"match_all":{}},"size":20000}'
        self.assertEqual(test, expecting, "expecting empty dict to serialize")

    def test_pretty_json(self):
        j = wrap({"not": {"match_all": wrap({})}})
        test = pretty_json(j)
        expecting = u'{"not": {"match_all": {}}}'
        self.assertEqual(test, expecting, "expecting empty dict to serialize")

    def test_Date(self):
        test = Date(1430983248.0)
        output = value2json(test)
        expecting = '1430983248'
        self.assertEqual(output, expecting, "expecting integer")

    def test_float(self):
        test = float(10.0)
        output = value2json(test)
        expecting = '10'
        self.assertEqual(output, expecting, "expecting integer")

    def test_nan(self):
        test = float("nan")
        output = value2json(test)
        expecting = cPythonJSONEncoder().encode(mo_json.scrub(test))
        self.assertEqual(output, expecting, "expecting " + expecting)

    def test_inf(self):
        test = float("+inf")
        output = value2json(test)
        expecting = cPythonJSONEncoder().encode(mo_json.scrub(test))
        self.assertEqual(output, expecting, "expecting " + expecting)

    def test_minus_inf(self):
        test = float("-inf")
        output = value2json(test)
        expecting = cPythonJSONEncoder().encode(mo_json.scrub(test))
        self.assertEqual(output, expecting, "expecting " + expecting)

    def test_string_stripper(self):
        test = {"hello": " world"}
        mo_json.FIND_LOOPS = True
        self.assertEqual(value2json(test), '{"hello":" world"}')

    def test_json_is_unicode(self):
        self.assertIsInstance(value2json({}), text_type)

    def test_json_encode_slash(self):
        self.assertEqual(value2json("/"), '"/"')


if __name__ == '__main__':
    try:
        Log.start()
        unittest.main()
    finally:
        Log.stop()
