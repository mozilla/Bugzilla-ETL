# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from io import BytesIO

from mo_future import text_type

from mo_dots import Null
from mo_testing.fuzzytestcase import FuzzyTestCase

from mo_json import stream


class TestJsonStream(FuzzyTestCase):
    def test_select_from_list(self):
        json = slow_stream('{"1":[{"a":"b"}]}')
        result = list(stream.parse(json, "1", ["1.a"]))
        expected = [{"1":{"a": "b"}}]
        self.assertEqual(result, expected)

    def test_select_nothing_from_many_list(self):
        json = slow_stream('{"1":[{"a":"b"}, {"a":"c"}]}')

        result = list(stream.parse(json, "1"))
        expected = [
            {},
            {}
        ]
        self.assertEqual(result, expected)

    def test_select_from_many_list(self):
        json = slow_stream('{"1":[{"a":"b"}, {"a":"c"}]}')

        result = list(stream.parse(json, "1", ["1.a"]))
        expected = [
            {"1": {"a": "b"}},
            {"1": {"a": "c"}}
        ]
        self.assertEqual(result, expected)

    def test_select_from_diverse_list(self):
        json = slow_stream('{"1":["test", {"a":"c"}]}')

        result = list(stream.parse(json, "1", ["1.a"]))
        expected = [
            {"1": {}},
            {"1": {"a": "c"}}
        ]
        self.assertEqual(result[0]["1"], None)
        self.assertEqual(result, expected)

    def test_select_from_deep_many_list(self):
        #                   0123456789012345678901234567890123
        json = slow_stream('{"1":{"2":[{"a":"b"}, {"a":"c"}]}}')

        result = list(stream.parse(json, "1.2", ["1.2.a"]))
        expected = [
            {"1": {"2": {"a": "b"}}},
            {"1": {"2": {"a": "c"}}}
        ]
        self.assertEqual(result, expected)

    def test_post_properties_error(self):
        json = slow_stream('{"0":"v", "1":[{"a":"b"}, {"a":"c"}], "2":[{"a":"d"}, {"a":"e"}]}')

        def test():
            result = list(stream.parse(json, "1", ["0", "1.a", "2"]))
        self.assertRaises(Exception, test)

    def test_select_objects(self):
        json = slow_stream('{"b":[{"a":1, "p":{"b":2, "c":{"d":3}}}, {"a":4, "p":{"b":5, "c":{"d":6}}}]}')

        result = list(stream.parse(json, "b", ["b.a", "b.p.c"]))
        expected = [
            {"b": {"a": 1, "p": {"c": {"d": 3}}}},
            {"b": {"a": 4, "p": {"c": {"d": 6}}}}
        ]
        self.assertEqual(result, expected)

    def test_select_all(self):
        json = slow_stream('{"b":[{"a":1, "p":{"b":2, "c":{"d":3}}}, {"a":4, "p":{"b":5, "c":{"d":6}}}]}')

        result = list(stream.parse(json, "b", ["b"]))
        expected = [
            {"b": {"a": 1, "p": {"b": 2, "c": {"d": 3}}}},
            {"b": {"a": 4, "p": {"b": 5, "c": {"d": 6}}}}
        ]
        self.assertEqual(result, expected)

    def test_big_baddy(self):
        source = """
        {
          "builds": [
            {
              "builder_id": 367155,
              "buildnumber": 460,
              "endtime": 1444699317,
              "id": 77269739,
              "master_id": 161,
              "properties": {
                "appName": "Firefox",
                "appVersion": "44.0a1",
                "basedir": "/c/builds/moz2_slave/m-in-w64-pgo-00000000000000000",
                "branch": "mozilla-inbound",
                "buildername": "WINNT 6.1 x86-64 mozilla-inbound pgo-build",
                "buildid": "20151012133004",
                "buildnumber": 460,
                "builduid": "2794c8ed62f642aeae5cd3f6cd72bdfd",
                "got_revision": "001f7d3139ce",
                "jsshellUrl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/jsshell-win64.zip",
                "log_url": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/mozilla-inbound-win64-pgo-bm84-build1-build460.txt.gz",
                "master": "http://buildbot-master84.bb.releng.scl3.mozilla.com:8001/",
                "packageFilename": "firefox-44.0a1.en-US.win64.zip",
                "packageUrl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/firefox-44.0a1.en-US.win64.zip",
                "platform": "win64",
                "product": "firefox",
                "project": "",
                "repo_path": "integration/mozilla-inbound",
                "repository": "",
                "request_ids": [
                  83875568
                ],
                "request_times": {
                  "83875568": 1444681804
                },
                "revision": "001f7d3139ce06e63075cb46bc4c6cbb607e4be4",
                "scheduler": "mozilla-inbound periodic",
                "script_repo_revision": "production",
                "script_repo_url": "https://hg.mozilla.org/build/mozharness",
                "slavename": "b-2008-ix-0099",
                "sourcestamp": "001f7d3139ce06e63075cb46bc4c6cbb607e4be4",
                "stage_platform": "win64-pgo",
                "symbolsUrl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/firefox-44.0a1.en-US.win64.crashreporter-symbols.zip",
                "testPackagesUrl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/test_packages.json",
                "testsUrl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/firefox-44.0a1.en-US.win64.web-platform.tests.zip",
                "toolsdir": "/c/builds/moz2_slave/m-in-w64-pgo-00000000000000000/scripts",
                "uploadFiles": "[u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\install\\\\sea\\\\firefox-44.0a1.en-US.win64.installer.exe', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\win64\\\\xpi\\\\firefox-44.0a1.en-US.langpack.xpi', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\mozharness.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.common.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.cppunittest.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.xpcshell.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.mochitest.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.talos.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.reftest.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.web-platform.tests.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.crashreporter-symbols.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.txt', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.json', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.mozinfo.json', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\test_packages.json', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\jsshell-win64.zip', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\host\\\\bin\\\\mar.exe', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\host\\\\bin\\\\mbsdiff.exe', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.checksums', u'c:\\\\builds\\\\moz2_slave\\\\m-in-w64-pgo-00000000000000000\\\\build\\\\src\\\\obj-firefox\\\\dist\\\\firefox-44.0a1.en-US.win64.checksums.asc']"
              },
              "reason": "The Nightly scheduler named 'mozilla-inbound periodic' triggered this build",
              "request_ids": [
                83875568
              ],
              "requesttime": 1444681804,
              "result": 0,
              "slave_id": 8812,
              "starttime": 1444681806
            }
        ]}
        """
        json = slow_stream(source)
        expected = [{"builds": {
            "requesttime": 1444681804,
            "starttime": 1444681806,
            "endtime": 1444699317,
            "reason": "The Nightly scheduler named 'mozilla-inbound periodic' triggered this build",
            "properties": {
                "request_times": {
                    "83875568": 1444681804
                },
                "slavename": "b-2008-ix-0099",
                "log_url": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1444681804/mozilla-inbound-win64-pgo-bm84-build1-build460.txt.gz",
                "buildername": "WINNT 6.1 x86-64 mozilla-inbound pgo-build"
            }
        }}]

        result = list(stream.parse(
            json,
            "builds",
            [
                "builds.starttime",
                "builds.endtime",
                "builds.requesttime",
                "builds.reason",
                "builds.properties.request_times",
                "builds.properties.slavename",
                "builds.properties.log_url",
                "builds.properties.buildername"
            ]
        ))
        self.assertEqual(result, expected)

    def test_constants(self):
        #                    01234567890123456789012345678901234567890123456789012345678901234567890123456789
        json = slow_stream(u'[true, false, null, 42, 3.14, "hello world", "àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"]')

        result = list(stream.parse(json, None, ["."]))
        expected = [True, False, None, 42, 3.14, u"hello world", u"àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"]
        self.assertEqual(result, expected)

    def test_object_items(self):
        json = slow_stream('{"a": 1, "b": 2, "c": 3}')
        result = list(stream.parse(json, {"items": "."}, expected_vars={"name", "value"}))
        expected = [
            {"name": "a", "value": 1},
            {"name": "b", "value": 2},
            {"name": "c", "value": 3}
        ]
        self.assertEqual(result, expected)

    def test_nested_primitives(self):
        json = slow_stream('{"u": "a", "t": [1, 2, 3]}')
        result = list(stream.parse(json, "t", expected_vars={"t", "u"}))
        expected = [
            {"u": "a", "t": 1},
            {"u": "a", "t": 2},
            {"u": "a", "t": 3}
        ]
        self.assertEqual(result, expected)

    def test_select_no_items(self):
        json = slow_stream('{"a": 1, "b": 2, "c": 3}')
        result = list(stream.parse(json, {"items": "."}, expected_vars={}))
        expected = [
            {},
            {},
            {}
        ]
        self.assertEqual(result, expected)

    def test_array_object_items(self):
        json = slow_stream('[{"a": 1}, {"b": 2}, {"c": 3}]')
        result = list(stream.parse(json, {"items": "."}, expected_vars={"name", "value"}))
        expected = [
            {"name": "a", "value": 1},
            {"name": "b", "value": 2},
            {"name": "c", "value": 3}
        ]
        self.assertEqual(result, expected)

    def test_nested_items(self):
        json = slow_stream('{"u": "a", "t": [{"a": 1}, {"b": 2}, {"c": 3}]}')
        result = list(stream.parse(json, {"items": "t"}, expected_vars={"u", "t.name", "t.value"}))
        expected = [
            {"u": "a", "t": {"name": "a", "value": 1}},
            {"u": "a", "t": {"name": "b", "value": 2}},
            {"u": "a", "t": {"name": "c", "value": 3}}
        ]
        self.assertEqual(result, expected)

    def test_empty(self):
        json = slow_stream('{"u": "a", "t": []}')
        result = list(stream.parse(json, "t", expected_vars={"u", "t"}))
        expected = [
            {"u": "a", "t": Null}
        ]
        self.assertEqual(result, expected)

    def test_miss_item_in_list(self):
        json = slow_stream('{"u": "a", "t": ["k", null, "m"]}')
        result = list(stream.parse(json, "t", expected_vars={"u", "t"}))
        expected = [
            {"u": "a", "t": "k"},
            {"u": "a", "t": Null},
            {"u": "a", "t": "m"}
        ]
        self.assertEqual(result, expected)

    def test_ignore_elements_of_list(self):
        json = slow_stream('{"u": "a", "t": ["k", null, "m"]}')
        result = list(stream.parse(json, "t", expected_vars={"u"}))
        expected = [
            {"u": "a"},
            {"u": "a"},
            {"u": "a"}
        ]
        self.assertEqual(result, expected)

    def test_bad_item_in_list(self):
        json = slow_stream('{"u": "a", "t": ["k", None, "m"]}')

        def output():
            list(stream.parse(json, "t", expected_vars={"u", "t"}))
        self.assertRaises(Exception, output)

    def test_object_instead_of_list(self):
        json = slow_stream('{"u": "a", "t": "k"}')

        result = list(stream.parse(json, "t", expected_vars={"u", "t"}))
        expected = [
            {"u": "a", "t": "k"}
        ]
        self.assertEqual(result, expected)

    def test_simple(self):
        json = slow_stream('{"u": "a"}')

        result = list(stream.parse(json, ".", expected_vars={"."}))
        expected = [
            {"u": "a"}
        ]
        self.assertEqual(result, expected)

    def test_missing_array(self):
        json = slow_stream('{"u": "a"}')

        result = list(stream.parse(json, "t", expected_vars={"u"}))
        expected = [
            {"u": "a"}
        ]
        self.assertEqual(result, expected)

    def test_not_used_array(self):
        json = slow_stream('{"u": "a", "t": ["k", null, "m"]}')

        def output():
            list(stream.parse(json, "t", expected_vars={"."}))

        self.assertRaises(Exception, output)

    def test_nested_items_w_error(self):
        json = slow_stream('{"u": "a", "t": [{"a": 1}, {"b": 2}, {"c": 3}], "v":3}')
        def test():
            result = list(stream.parse(json, {"items": "t"}, expected_vars={"u", "t.name", "v"}))
        self.assertRaises(Exception, test)

    def test_values_are_arrays(self):
        json = slow_stream('{"AUTHORS": ["mozilla.org", "Licensing"], "CLOBBER": ["Core", "Build Config"]}')
        result = list(stream.parse(json, {"items": "."}, expected_vars={"name", "value"}))
        expected = [
            {"name": "AUTHORS", "value": ["mozilla.org", "Licensing"]},
            {"name": "CLOBBER", "value": ["Core", "Build Config"]}
        ]
        self.assertEqual(result, expected)


def slow_stream(bytes):
    if isinstance(bytes, text_type):
        bytes = bytes.encode("utf8")

    r = BytesIO(bytes).read
    def output():
        return r(1)
    return output
