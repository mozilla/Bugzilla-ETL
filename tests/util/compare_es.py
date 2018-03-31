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

from datetime import datetime

from mo_future import long
from mo_logs import Log

import jx_elasticsearch
import jx_python
from bzETL import transform_bugzilla, parse_bug_history
from jx_python import jx
from mo_dots import coalesce, unwrap
from mo_json import json2value, value2json
from mo_math import Math
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.env import elasticsearch
from pyLibrary.testing.elasticsearch import FakeES


def get_all_bug_versions(es, bug_id, max_time=None):
    max_time = coalesce(max_time, datetime.max)

    if isinstance(es, elasticsearch.Index):
        esq = jx_elasticsearch.new_instance(es.settings)
    elif isinstance(es, FakeES):
        esq = jx_python.wrap_from(es.data.values())
    else:
        raise Log.error("unknown container")

    response = esq.query({
        "where": {"and": [
            {"eq": {"bug_id": bug_id}},
            {"lte": {"modified_ts": convert.datetime2milli(max_time)}}
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
            output |= set(coalesce(convert.value2intlist(bug.fields.blocked), []))
            output |= set(coalesce(convert.value2intlist(bug.fields.dependson), []))
            output |= set(coalesce(convert.value2intlist(bug.fields.dupe_of), []))
            output |= set(coalesce(convert.value2intlist(bug.fields.dupe_by), []))

    output.add(551988, 636964)
    return output


def old2new(bug, max_date):
    """
    CONVERT THE OLD ES FORMAT TO THE NEW
    THESE ARE KNOWN CHANGES THAT SHOULD BE MADE TO THE PRODUCTION VERSION
    """
    if bug.everconfirmed != None:
        if bug.everconfirmed == "":
            bug.everconfirmed = None
        else:
            bug.everconfirmed = int(bug.everconfirmed)

    bug = json2value(value2json(bug).replace("bugzilla: other b.m.o issues ", "bugzilla: other b.m.o issues"))

    if bug.expires_on > max_date:
        bug.expires_on = parse_bug_history.MAX_TIME
    if bug.votes != None:
        bug.votes = int(bug.votes)
    bug.dupe_by = convert.value2intlist(bug.dupe_by)
    if bug.votes == 0:
        del bug["votes"]
        # if Math.is_integer(bug.remaining_time) and int(bug.remaining_time) == 0:
    #     bug.remaining_time = 0
    if bug.cf_due_date != None and not Math.is_number(bug.cf_due_date):
        bug.cf_due_date = convert.datetime2milli(
            convert.string2datetime(bug.cf_due_date, "%Y-%m-%d")
        )
    bug.changes = json2value(
        value2json(jx.sort(bug.changes, "field_name")) \
            .replace("\"field_value_removed\":", "\"old_value\":") \
            .replace("\"field_value\":", "\"new_value\":")
    )

    if bug.everconfirmed == 0:
        del bug["everconfirmed"]
    if bug.id == "692436_1336314345":
        bug.votes = 3

    try:
        if Math.is_number(bug.cf_last_resolved):
            bug.cf_last_resolved = long(bug.cf_last_resolved)
        else:
            bug.cf_last_resolved = convert.datetime2milli(convert.string2datetime(bug.cf_last_resolved, "%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        pass

    bug = transform_bugzilla.rename_attachments(bug)
    for c in bug.changes:
        c.field_name = c.field_name.replace("attachments.", "attachments_")
        if c.attach_id == '':
            c.attach_id = None
        else:
            c.attach_id = convert.value2int(c.attach_id)

    bug.attachments = jx.sort(bug.attachments, "attach_id")
    for a in bug.attachments:
        a.attach_id = convert.value2int(a.attach_id)
        for k, v in list(a.items()):
            if k.endswith("isobsolete") or k.endswith("ispatch") or k.endswith("isprivate"):
                unwrap(a)[k] = convert.value2int(v) # PREVENT dot (.) INTERPRETATION
                a[k.split(".")[-1].split("_")[-1]] = convert.value2int(v)

    bug = transform_bugzilla.normalize(bug)
    return bug
