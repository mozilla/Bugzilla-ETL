################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

from bzETL import transform_bugzilla
from bzETL.util.basic import nvl
from bzETL.util.cnv import CNV
from bzETL.util.maths import Math
from bzETL.util.query import Q



#PULL ALL BUG DOCS FROM ONE ES
from bzETL.util.timer import Timer

def get_all_bug_versions(es, bug_id, max_time):

    data=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"and":[
                {"term":{"bug_id":bug_id}},
                {"range":{"modified_ts":{"lte":CNV.datetime2milli(max_time)}}}
            ]}
        }},
        "from":0,
        "size":200000,
        "sort":[]
    })

    return Q.select(data.hits.hits, "_source")


def get_private_bugs(es):
    """
    FIND THE BUGS WE DO NOT EXPECT TO BE FOUND IN PUBLIC
    """
    data=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"and":[
                {"script":{"script":"true"}},
                {"and":[{"exists":{"field":"bug_group"}}]}
            ]}
        }},
        "from":0,
        "size":200000,
        "sort":[],
        "facets":{},
        "fields":["bug_id","blocked","dependson","dupe_of","dupe_by"]
    })

    with Timer("aggregate es results on private bugs"):
        output=set([])
        for bug in data.hits.hits:
            output.add(bug.fields.bug_id)
            output|=set(nvl(CNV.value2intlist(bug.fields.blocked), []))
            output|=set(nvl(CNV.value2intlist(bug.fields.dependson), []))
            output|=set(nvl(CNV.value2intlist(bug.fields.dupe_of), []))
            output|=set(nvl(CNV.value2intlist(bug.fields.dupe_by), []))

    output.add(551988,636964)
    return output



def old2new(bug, max_date):
    """
    CONVERT THE OLD ES FORMAT TO THE NEW
    THESE ARE KNOWN CHANGES THAT SHOULD BE MADE TO THE PRODUCTION VERSION
    """
    bug.id=bug._id.replace(".", "_")[:-3]
    bug._id=None

    if bug.everconfirmed is not None:
        if bug.everconfirmed=="":
            bug.everconfirmed=None
        else:
            bug.everconfirmed=int(bug.everconfirmed)

    bug=CNV.JSON2object(CNV.object2JSON(bug).replace("bugzilla: other b.m.o issues ", "bugzilla: other b.m.o issues"))

    if bug.expires_on is not None and bug.expires_on >= max_date:
        bug.expires_on = None
    if bug.votes is not None:
        bug.votes = int(bug.votes)
    bug.dupe_by = CNV.value2intlist(bug.dupe_by)
    if bug.votes == 0:
        del bug["votes"]
    if Math.is_integer(bug.remaining_time) and int(bug.remaining_time) == 0:
        del bug["remaining_time"]
    if bug.cf_due_date is not None:
        bug.cf_due_date = CNV.datetime2milli(
            CNV.string2datetime(bug.cf_due_date, "%Y-%m-%d")
        )
    bug.changes = CNV.JSON2object(
        CNV.object2JSON(Q.sort(bug.changes, "field_name"))\
        .replace("\"field_value_removed\":", "\"old_value\":")\
        .replace("\"field_value\":", "\"new_value\":")
    )

    if bug.everconfirmed == 0:
        del bug["everconfirmed"]
    if bug.id=="692436_1336314345":
        bug.votes=3



    try:
        bug.cf_last_resolved=CNV.datetime2milli(CNV.string2datetime(bug.cf_last_resolved, "%Y-%m-%d %H:%M:%S"))
    except Exception, e:
        pass


    bug=transform_bugzilla.rename_attachments(bug)
    for c in nvl(bug.changes, []):
        c.field_name=c.field_name.replace("attachments.", "attachments_")

    bug=transform_bugzilla.scrub(bug)
    return bug
