# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

import unittest
from traceback import extract_tb

from mo_future import text_type

import jx_elasticsearch
from mo_files import File
from mo_json import json2value
from mo_logs import Log
from mo_logs import strings
from mo_logs.exceptions import Except, ERROR
from pyLibrary import convert


class TestFromES(unittest.TestCase):
    # THE COMPLICATION WITH THIS TEST IS KNOWING IF
    # THE NESTED TERMS ARE andED TOGETHER ON EACH
    # NESTED DOCUMENT, OR *ANY* OF THE NESTED DOCUMENTS
    # "and" IS AMBIGUOUS, AND THE CONTEXT DB JOIN (OR ES "nested")
    # IS REQUIRED FOR DISAMBIGUATION.
    # USUALLY I WOULD SIMPLY FORCE THE QUERY TO APPLY TO THE NESTED
    # DOCUMENTS ONLY.  RETURNING THE PARENT DOCUMENT IS WHAT'S
    # AMBIGUOUS

    def setUp(self):
        self.esq=FromESTester("private_bugs")

    def not_done_test1(self):
        esquery = self.esq.query({
            "from": "private_bugs",
            "select": "*",
            "where": {"and": [
                {"range": {"expires_on": {"gte": 1393804800000}}},
                {"range": {"modified_ts": {"lte": 1394074529000}}},
                {"term": {"changes.field_name": "assigned_to"}},
                {"term": {"changes.new_value": "klahnakoski"}}
            ]},
            "limit": 10
        })

        expecting = {}

        assert convert.value2json(esquery, pretty=True) == convert.value2json(expecting, pretty=True)


class FromESTester(object):
    def __init__(self, index):
        self.es = FakeES({
            "host":"example.com",
            "index":"index"
        })
        self.esq = jx_elasticsearch.new_instance(self.es)

    def query(self, query):
        try:
            with self.esq:
                self.esq.query(query)
                return None
        except Exception as e:
            f = Except(ERROR, text_type(e), trace=extract_tb(1))
            try:
                details = str(f)
                query = json2value(strings.between(details, ">>>>", "<<<<"))
                return query
            except Exception as g:
                Log.error("problem", f)



class FakeES(object):

    def __init__(self, settings):
        self.settings = settings
        pass

    def search(self, query):
        Log.error("<<<<\n{{query}}\n>>>>", {"query": convert.value2json(query)})

    def get_schema(self):
        return json2value(File("tests/resources/bug_version.json").read()).mappings.bug_version




# 4 - select None/single/list(1)/list(2)
# 4 - select aggregates/setop/aggop/count(no value)
# 5 - deep select: gparent/parent/self/child/gchild
# 3 - edges simple/deep(1)/deep(2)
# 4 - 0, 1, 2, 3 edges
# 4 - 0, 1, 2, 3 group by
# n - aggregates min/sum/count/max/etc...
# 3 - from memory/es/database sources
# where
# sort
# having
# window

#data
# 0, 1, 2, 3 properties
# 0, 1, 2, 3 children
# 0, 1, 2, 3 depth
# numeric/string/boolean values


