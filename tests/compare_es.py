################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

import transform_bugzilla
from util.basic import nvl
from util.cnv import CNV
from util.maths import Math
from util.query import Q



#PULL ALL BUG DOCS FROM ONE ES
from util.timer import Timer

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


#FIND THE BUGS WE DO NOT EXPECT TO BE FOUND IN PUBLIC
def get_private_bugs(es):
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


    return output



#CONVERT THE OLD ES FORMAT TO THE NEW
def old2new(bug):
    #THESE ARE KNOWN CHANGES THAT SHOULD BE MADE TO THE PRODUCTION VERSION
    bug.id=bug.id.replace(".", "_")[:-3]
    if bug.everconfirmed is not None: bug.everconfirmed=int(bug.everconfirmed)
    if bug.votes is not None: bug.votes=int(bug.votes)
    bug.dupe_by=CNV.value2intlist(bug.dupe_by)
    if bug.votes==0: del bug["votes"]
    if Math.is_integer(bug.remaining_time) and int(bug.remaining_time)==0: del bug["remaining_time"]
    if bug.cf_due_date is not None: bug.cf_due_date=CNV.datetime2milli(CNV.string2datetime(bug.cf_due_date, "%Y-%m-%d"))
    if bug.everconfirmed==0: del bug["everconfirmed"]


    try:
        bug.cf_last_resolved=CNV.datetime2milli(CNV.string2datetime(bug.cf_last_resolved, "%Y-%m-%d %H:%M:%S"))
    except Exception, e:
        pass


    bug=transform_bugzilla.rename_attachments(bug)

    bug=transform_bugzilla.scrub(bug)
    return bug