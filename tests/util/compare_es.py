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

import jx_elasticsearch
from jx_python.containers.list_usingPythonList import ListContainer
from mo_logs import Log
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.env import elasticsearch
from pyLibrary.testing.elasticsearch import FakeES


def get_esq(es):
    if isinstance(es, elasticsearch.Index):
        return jx_elasticsearch.new_instance(index=es.settings.alias, alias=None, kwargs=es.settings)
    elif isinstance(es, FakeES):
        return ListContainer(name="bugs", data=es.data.values())
    else:
        raise Log.error("unknown container")


def get_all_bug_versions(es, bug_id, max_time=None, esq=None):
    if esq is None:
        esq = get_esq(es)

    response = esq.query({
        "from": esq.name,
        "where": {"and": [
            {"eq": {"bug_id": bug_id}}
        ]},
        "format": "list",
        "limit": 100000
    })
    return response.data


def get_private_bugs(es):
    """
    FIND THE BUGS WE DO NOT EXPECT TO BE FOUND IN PUBLIC
    """
    data = es.search({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and": [
                {"script": {"script": "true"}},
                {"and": [{"exists": {"field": "bug_group"}}]}
            ]}
        }},
        "from": 0,
        "size": 200000,
        "sort": [],
        "facets": {},
        "fields": ["bug_id", "blocked", "dependson", "dupe_of", "dupe_by"]
    })

    with Timer("aggregate es results on private bugs"):
        output = set([])
        for bug in data.hits.hits:
            output.add(bug.fields.bug_id)
            output |= set(convert.value2intlist(bug.fields.blocked))
            output |= set(convert.value2intlist(bug.fields.dependson))
            output |= set(convert.value2intlist(bug.fields.dupe_of))
            output |= set(convert.value2intlist(bug.fields.dupe_by))

    output.add(551988, 636964)
    return output


