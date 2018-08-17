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
from bzETL import transform_bugzilla
from bzETL.extract_bugzilla import MAX_TIMESTAMP
from jx_python import jx
from jx_python.containers.list_usingPythonList import ListContainer
from mo_dots import coalesce, unwrap, listwrap
from mo_future import long
from mo_json import json2value, value2json
from mo_logs import Log
from mo_math import Math
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


def old2new(bug, max_date):
    """
    CONVERT THE OLD ES FORMAT TO THE NEW
    THESE ARE KNOWN CHANGES THAT SHOULD BE MADE TO THE PRODUCTION VERSION
    """
    # if bug.everconfirmed != None:
    #     if bug.everconfirmed == "":
    #         bug.everconfirmed = None
    #     else:
    #         bug.everconfirmed = int(bug.everconfirmed)

    # bug = json2value(value2json(bug).replace("bugzilla: other b.m.o issues ", "bugzilla: other b.m.o issues"))

    # if bug.expires_on > max_date:
    #     bug.expires_on = MAX_TIMESTAMP
    # if bug.votes != None:
    #     bug.votes = int(bug.votes)
    # bug.dupe_by = convert.value2intlist(bug.dupe_by)
    # if bug.votes == 0:
    #     del bug["votes"]
        # if Math.is_integer(bug.remaining_time) and int(bug.remaining_time) == 0:
    #     bug.remaining_time = 0
    # if bug.cf_due_date != None and not Math.is_number(bug.cf_due_date):
    #     bug.cf_due_date = convert.datetime2milli(
    #         convert.string2datetime(bug.cf_due_date, "%Y-%m-%d")
    #     )
    # bug.changes = jx.sort(listwrap(bug.changes), "field_name")

    # if bug.everconfirmed == 0:
    #     del bug["everconfirmed"]
    # if bug.id == "692436_1336314345":
    #     bug.votes = 3

    # try:
    #     if bug.cf_last_resolved == None:
    #         pass
    #     elif Math.is_number(bug.cf_last_resolved):
    #         bug.cf_last_resolved = long(bug.cf_last_resolved)
    #     else:
    #         bug.cf_last_resolved = convert.datetime2milli(convert.string2datetime(bug.cf_last_resolved, "%Y-%m-%d %H:%M:%S"))
    # except Exception as e:
    #     pass

    for c in listwrap(bug.changes):
        if c.attach_id == '':
            c.attach_id = None
        else:
            c.attach_id = convert.value2int(c.attach_id)

    bug.attachments = jx.sort(listwrap(bug.attachments), "attach_id")
    for a in bug.attachments:
        a.attach_id = convert.value2int(a.attach_id)
        for k, v in list(a.items()):
            if k.startswith('attachments') and k.endswith("isobsolete") or k.endswith("ispatch") or k.endswith("isprivate"):
                del a[k]
                k = k.replace('attachments.', '').replace('attachments_', '')
                a[k] = convert.value2int(v)
            elif k in ('attachments_mimetype','attachments.mimetype'):
                del a[k]
                k = k.replace('attachments.', '').replace('attachments_', '')
                a[k] = v

    bug = transform_bugzilla.normalize(bug)
    return bug
