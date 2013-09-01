################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
import transform_bugzilla
from util.cnv import CNV
from util.debug import D
from util.query import Q



#PULL ALL BUG DOCS FROM ONE ES
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


#CONVERT THE OLD ES FORMAT TO THE NEW
def old2new(bug):
    #THESE ARE KNOWN CHANGES THAT SHOULD BE MADE TO THE PRODUCTION VERSION
    bug._id=bug._id.replace(".", "_")[:-3]
    if bug.everconfirmed is not None: bug.everconfirmed=int(bug.everconfirmed)
    if bug.votes is not None: bug.votes=int(bug.votes)
    bug.dupe_by=CNV.value2intlist(bug.dupe_by)
    if bug.votes==0: del bug["votes"]
    if bug.remaining_time==0: del bug["remaining_time"]
    if bug.cf_due_date is not None: bug.cf_due_date=CNV.datetime2milli(CNV.string2datetime(bug.cf_due_date, "%Y-%m-%d"))


    try:
        bug.cf_last_resolved=CNV.datetime2milli(CNV.string2datetime(bug.cf_last_resolved, "%Y-%m-%d %H:%M:%S"))
    except Exception, e:
        pass

    #WE ARE RENAMING THE ATTACHMENTS FIELDS TO CAUSE LESS PROBLEMS IN ES QUERIES
    bug=CNV.JSON2object(CNV.object2JSON(bug).replace("attachments.", "attachments_"))

    bug=transform_bugzilla.scrub(bug)
    return bug