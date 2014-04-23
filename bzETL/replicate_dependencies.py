# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from datetime import datetime, timedelta
from bzETL.util.collections import MIN
from bzETL.util.struct import nvl, Struct
from bzETL.util.thread.threads import ThreadedQueue
from bzETL.util.times.timer import Timer
import transform_bugzilla
from bzETL.util.cnv import CNV
from bzETL.util.env.logs import Log
from bzETL.util.queries import Q
from bzETL.util.env import startup
from bzETL.util.env.files import File
from bzETL.util.collections.multiset import Multiset
from bzETL.util.env.elasticsearch import ElasticSearch


# REPLICATION
#
# Replication has a few benefits:
# 1) The slave can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity increases the probability of reduced latency
# 3) The slave can be configured with better hardware
# 4) The slave's exclusivity increases availability (Mozilla's public cluster my have time of high load)


far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 50000


def get_last_updated(es):
    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {
                    "range": {
                    "modified_ts": {"gte": CNV.datetime2milli(far_back)}}}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"modified_ts": {"statistical": {"field": "modified_ts"}}}
        })

        if results.facets.modified_ts.count == 0:
            return CNV.milli2datetime(0)
        return CNV.milli2datetime(results.facets.modified_ts.max)
    except Exception, e:
        return CNV.milli2datetime(0)


def get_pending(es, since):
    result = es.search({
        "query": {"match_all": {}},
        "from": 0,
        "size": 0,
        "sort": [],
        "facets": {"default": {"statistical": {"field": "bug_id"}}}
    })

    max_bug = int(result.facets.default.max)
    pending_bugs = None

    for s, e in Q.intervals(0, max_bug+1, 100000):
        Log.note("Collect history for bugs from {{start}}..{{end}}", {"start":s, "end":e})
        result = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and":[
                    {"range": {"modified_ts": {"gte": CNV.datetime2milli(since)}}},
                    {"range": {"bug_id": {"gte": s, "lte": e}}}
                ]}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"default": {"terms": {"field": "bug_id", "size": 200000}}}
        })

        temp = Multiset(
            result.facets.default.terms,
            key_field="term",
            count_field="count"
        )

        if pending_bugs is None:
            pending_bugs = temp
        else:
            pending_bugs = pending_bugs + temp



    Log.note("Source has {{num}} bug versions for updating", {
        "num": len(pending_bugs)
    })
    return pending_bugs


# USE THE source TO GET THE INDEX SCHEMA
def get_or_create_index(destination_settings, source):
    #CHECK IF INDEX, OR ALIAS, EXISTS
    es = ElasticSearch(destination_settings)
    aliases = es.get_aliases()

    indexes = [a for a in aliases if a.alias == destination_settings.index or a.index == destination_settings.index]
    if not indexes:
        #CREATE INDEX
        Log.error("Expecting an index")
    elif len(indexes) > 1:
        Log.error("do not know how to replicate to more than one index")
    elif indexes[0].alias != None:
        destination_settings.alias = indexes[0].alias
        destination_settings.index = indexes[0].index

    return ElasticSearch(destination_settings)


def replicate(source, destination, pending, last_updated):
    """
    COPY source RECORDS TO destination
    """
    for g, bugs in Q.groupby(pending, max_size=BATCH_SIZE):
        with Timer("Replicate {{num_bugs}} bug versions", {"num_bugs": len(bugs)}):
            data = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"terms": {"bug_id": set(bugs)}},
                        {"range": {"expires_on":
                            {"gte": CNV.datetime2milli(last_updated)}
                        }},
                        {"exists":{"field":"dependson"}}
                    ]}
                }},
                "from": 0,
                "size": 200000,
                "sort": [],
                "fields":["bug_id", "modified_ts", "expires_on", "dependson"]
            })

            d2 = [{"id": str(x.bug_id)+"_"+str(x.modified_ts)[:-3], "value": x} for x in data.hits.hits.fields if x.dependson]
            destination.extend(d2)


def main(settings):
    current_time = datetime.utcnow()
    time_file = File(settings.param.last_replication_time)

    # SYNCH WITH source ES INDEX
    source=ElasticSearch(settings.source)
    destination=get_or_create_index(settings["destination"], source)

    # GET LAST UPDATED
    from_file = None
    if time_file.exists:
        from_file = CNV.milli2datetime(CNV.value2int(time_file.read()))
    from_es = get_last_updated(destination) - timedelta(hours=1)
    last_updated = MIN(nvl(from_file, CNV.milli2datetime(0)), from_es)
    Log.note("updating records with modified_ts>={{last_updated}}", {"last_updated":last_updated})

    pending = get_pending(source, last_updated)
    with ThreadedQueue(destination, size=1000) as data_sink:
        replicate(source, data_sink, pending, last_updated)

    # RECORD LAST UPDATED
    time_file.write(unicode(CNV.datetime2milli(current_time)))


def start():
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__=="__main__":
    start()
