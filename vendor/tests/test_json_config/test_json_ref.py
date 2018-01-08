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

import os

import mo_json_config
from mo_files import File
from mo_json_config import URL
from mo_logs.exceptions import extract_stack
from mo_logs.strings import expand_template
from mo_dots import Data
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestRef(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)
        stack = extract_stack(0)
        this_file = stack[0]["file"]
        self.resources = "file:///"+File.new_instance(this_file, "../resources").abspath

    def test_doc1(self):
        os.environ["test_variable"] = "abc"

        doc = mo_json_config.get(self.resources+"/test_ref1.json")

        self.assertEqual(doc.env_variable, "abc")
        self.assertEqual(doc.relative_file1, "*_ts")
        self.assertEqual(doc.relative_file2, "*_ts")
        self.assertEqual(doc.relative_doc, "value")
        self.assertEqual(doc.absolute_doc, "another value")
        self.assertEqual(doc.env_variable, "abc")
        self.assertEqual(doc.relative_object_doc, {"key": "new value", "another_key": "another value"})

    def test_doc2(self):
        # BETTER TEST OF RECURSION
        doc = mo_json_config.get(self.resources+"/test_ref2.json")

        self.assertEqual(doc, {
            "a": "some_value",
            "test_key": "test_value",
            "b": {
                "test_key": "test_value"
            }
        })

    def test_empty_object_as_json_parameter(self):
        url = URL(self.resources+"/test_ref_w_parameters.json")
        url.query = {"metadata": Data()}
        result = mo_json_config.get(url)
        self.assertEqual(result, {}, "expecting proper parameter expansion")

    def test_json_parameter(self):
        url = URL(self.resources+"/test_ref_w_parameters.json")
        url.query = {"metadata": ["a", "b"]}
        result = mo_json_config.get(url)
        self.assertEqual(result, {"a": ["a", "b"]}, "expecting proper parameter expansion")

    def test_url_parameter_list(self):
        url = self.resources+"/test_ref_w_parameters.json?test1=a&test1=b&test2=c&test1=d"
        self.assertEqual(URL(url).query, {"test1": ["a", "b", "d"], "test2": "c"}, "expecting test1 to be an array")

    def test_leaves(self):
        url = self.resources + "/test_ref_w_deep_parameters.json?&value.one.two=42"
        result = mo_json_config.get(url)
        self.assertEqual(result, {"a": {"two": 42}, "b": 42}, "expecting proper parameter expansion")

    def test_leaves_w_array(self):
        url = URL(self.resources + "/test_ref_w_deep_parameters.json")
        url.query = {"value": {"one": {"two": [{"test": 1}, {"test": 2}, "3"]}}}
        result = mo_json_config.get(url)
        expected = {
            "a": {"two": [
                {"test": 1},
                {"test": 2},
                "3"
            ]},
            "b": [
                {"test": 1},
                {"test": 2},
                "3"
            ]
        }
        self.assertEqual(result, expected, "expecting proper parameter expansion")

    def test_inner_doc(self):
        doc = mo_json_config.get(self.resources+"/inner.json")

        self.assertEqual(doc, {
            "area": {
                "color": {"description": "css color"},
                "border": {"properties": {"color": {"description": "css color"}}}
            },
            "definitions": {
                "object_style": {
                    "color": {"description": "css color"},
                    "border": {"properties": {"color": {"description": "css color"}}}
                },
                "style": {"properties": {"color": {"description": "css color"}}}
            }
        }, "expecting proper expansion")

    def test_read_home(self):
        file = "~/___test_file.json"
        source = File.new_instance(extract_stack(0)[0]["file"], "../resources/simple.json")
        File.copy(File(source), File(file))
        content = mo_json_config.get("file:///"+file)

        try:
            self.assertEqual(
                content,
                {"test_key": "test_value"}
            )
        finally:
            File(file).delete()

    def test_array_expansion(self):
        # BETTER TEST OF RECURSION
        doc = mo_json_config.get(self.resources+"/test_array.json")

        self.assertEqual(doc, {
            "a": "some_value",
            "list": {"deep": [
                {
                    "a": "a",
                    "test_key": "test_value"
                },
                {
                    "a": "b",
                    "test_key": "test_value"
                },
                {
                    "a": "c",
                    "test_key": "test_value"
                },
                {
                    "a": "d",
                    "test_key": "test_value"
                },
                {
                    "a": "e",
                    "test_key": "test_value"
                }
            ]}
        })

    def test_grandparent_reference(self):
        doc = mo_json_config.get(self.resources+"/child/grandchild/simple.json")

        self.assertEqual(doc, {"test_key": "test_value"})

    def test_params_simple(self):
        doc = {"a": {"$ref": "param://value"}}
        doc_url = "http://example.com/"
        result = mo_json_config.expand(doc, doc_url, {"value": "hello"})
        self.assertEqual(result, {"a": "hello"})

    def test_params_deep(self):
        doc = {"a": {"$ref": "param://value.name"}}
        doc_url = "http://example.com/"
        result = mo_json_config.expand(doc, doc_url, {"value": {"name": "hello"}})
        self.assertEqual(result, {"a": "hello"})

    def test_params_object(self):
        doc = {"a": {"$ref": "param://value"}}
        doc_url = "http://example.com/"
        result = mo_json_config.expand(doc, doc_url, {"value": {"name": "hello"}})
        self.assertEqual(result, {"a": {"name": "hello"}})
