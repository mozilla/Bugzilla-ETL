# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

#
# REPLICATION
#
# Replication has a few benefits:
# 1) The slave can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity reduces latency
# 3) The slave can be configured with better hardware
# 4) The slave's exclusivity increases availability (Mozilla's public cluster may have high load)

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

from datetime import datetime, timedelta
from bzETL import transform_bugzilla
from pyLibrary import convert
from pyLibrary.collections import MIN, Multiset
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, Dict
from pyLibrary.env import elasticsearch
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.files import File
from pyLibrary.queries import qb
from pyLibrary.thread.threads import ThreadedQueue
from pyLibrary.times.timer import Timer


far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 1000


def extract_from_file(source_settings, destination):
    file = File(source_settings.filename)
    for g, d in qb.groupby(file, size=BATCH_SIZE):
        try:
            d2 = map(
                lambda (x): {"id": x.id, "value": x},
                map(
                    lambda(x): transform_bugzilla.normalize(convert.json2value(x)),
                    d
                )
            )
            Log.note("add {{num}} records", {"num":len(d2)})
            destination.extend(d2)
        except Exception, e:
            filename = "Error_" + unicode(g) + ".txt"
            File(filename).write(d)
            Log.warning("Can not convert block {{block}} (file={{host}})", {
                "block": g,
                "filename": filename
            }, e)


def get_last_updated(es):

    if not isinstance(es, elasticsearch.Index):
        return convert.milli2datetime(0)

    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {
                    "range": {
                    "modified_ts": {"gte": convert.datetime2milli(far_back)}}}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"modified_ts": {"statistical": {"field": "modified_ts"}}}
        })

        if results.facets.modified_ts.count == 0:
            return convert.milli2datetime(0)
        return convert.milli2datetime(results.facets.modified_ts.max)
    except Exception, e:
        Log.error("Can not get_last_updated from {{host}}/{{index}}",{
            "host": es.settings.host,
            "index": es.settings.index
        }, e)


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

    for s, e in qb.intervals(0, max_bug+1, 100000):
        Log.note("Collect history for bugs from {{start}}..{{end}}", {"start":s, "end":e})
        result = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and":[
                    {"range": {"modified_ts": {"gte": convert.datetime2milli(since)}}},
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
    es = elasticsearch.Index(destination_settings)
    aliases = es.get_aliases()

    indexes = [a for a in aliases if a.alias == destination_settings.index or a.index == destination_settings.index]
    if not indexes:
        #CREATE INDEX
        schema = convert.json2value(File(destination_settings.schema_file).read(), paths=True)
        assert schema.settings
        assert schema.mappings
        Cluster(destination_settings).create_index(destination_settings, schema, limit_replicas=True)
    elif len(indexes) > 1:
        Log.error("do not know how to replicate to more than one index")
    elif indexes[0].alias != None:
        destination_settings.alias = indexes[0].alias
        destination_settings.index = indexes[0].index

    return elasticsearch.Index(destination_settings)


def replicate(source, destination, pending, last_updated):
    """
    COPY source RECORDS TO destination
    """
    for g, bugs in qb.groupby(pending, max_size=BATCH_SIZE):
        with Timer("Replicate {{num_bugs}} bug versions", {"num_bugs": len(bugs)}):
            data = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"terms": {"bug_id": set(bugs)}},
                        {"range": {"expires_on":
                            {"gte": convert.datetime2milli(last_updated)}
                        }}
                    ]}
                }},
                "from": 0,
                "size": 200000,
                "sort": []
            })

            d2 = map(
                lambda(x): {"id": x.id, "value": x},
                map(
                    lambda(x): transform_bugzilla.normalize(transform_bugzilla.rename_attachments(x._source), old_school=True),
                    data.hits.hits
                )
            )
            destination.extend(d2)


def main(settings):
    current_time = datetime.utcnow()
    time_file = File(settings.param.last_replication_time)

    #USE A SOURCE FILE
    if settings.source.filename != None:
        settings.destination.alias = settings.destination.index
        settings.destination.index = Cluster.proto_name(settings.destination.alias)
        schema = convert.json2value(File(settings.destination.schema_file).read(), paths=True, flexible=True)
        if transform_bugzilla.USE_ATTACHMENTS_DOT:
            schema = convert.json2value(convert.value2json(schema).replace("attachments_", "attachments."))

        dest = Cluster(settings.destination).create_index(settings.destination, schema, limit_replicas=True)
        dest.set_refresh_interval(-1)
        extract_from_file(settings.source, dest)
        dest.set_refresh_interval(1)

        dest.delete_all_but(settings.destination.alias, settings.destination.index)
        dest.add_alias(settings.destination.alias)

    else:
        # SYNCH WITH source ES INDEX
        source=elasticsearch.Index(settings.source)


        # USE A DESTINATION FILE
        if settings.destination.filename:
            Log.note("Sending records to file: {{filename}}", {"filename":settings.destination.filename})
            file = File(settings.destination.filename)
            destination = Dict(
                extend=lambda x: file.extend([convert.value2json(v["value"]) for v in x]),
                file=file
            )
        else:
            destination=get_or_create_index(settings["destination"], source)

        # GET LAST UPDATED
        from_file = None
        if time_file.exists:
            from_file = convert.milli2datetime(convert.value2int(time_file.read()))
        from_es = get_last_updated(destination) - timedelta(hours=1)
        last_updated = MIN(coalesce(from_file, convert.milli2datetime(0)), from_es)
        Log.note("updating records with modified_ts>={{last_updated}}", {"last_updated":last_updated})

        pending = get_pending(source, last_updated)
        with ThreadedQueue(destination, max_size=1000) as data_sink:
            replicate(source, data_sink, pending, last_updated)

    # RECORD LAST UPDATED
    time_file.write(unicode(convert.datetime2milli(current_time)))


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
