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
from bzETL.util.maths import Math
from bzETL.util.timer import Timer
import transform_bugzilla
from bzETL.util.cnv import CNV
from bzETL.util.logs import Log
from bzETL.util.queries import Q
from bzETL.util import startup
from bzETL.util.files import File
from bzETL.util.multiset import Multiset
from bzETL.util.elasticsearch import ElasticSearch


far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 10000


def fix_json(json):
    json = json.replace("attachments.", "attachments_")
    return json.decode('iso-8859-1').encode('utf8')


def extract_from_file(source_settings, destination):
    with File(source_settings.filename).iter() as handle:
        for g, d in Q.groupby(handle, size=BATCH_SIZE):
            try:
                d2 = map(
                    lambda (x): {"id": x.id, "value": x},
                    map(
                        lambda(x): transform_bugzilla.normalize(CNV.JSON2object(fix_json(x))),
                        d
                    )
                )
                destination.add(d2)
            except Exception, e:
                filename = "Error_" + unicode(g) + ".txt"
                File(filename).write(d)
                Log.warning("Can not convert block {{block}} (file={{host}})", {
                    "block": g,
                    "filename": filename
                }, e)


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
            "facets": {"0": {"statistical": {"field": "modified_ts"}}}
        })

        if results.facets["0"].count == 0:
            return datetime.min
        return CNV.milli2datetime(results.facets["0"].max)
    except Exception, e:
        Log.error("Can not get_last_updated from {{host}}/{{index}}",{
            "host": es.settings.host,
            "index": es.settings.index
        }, e)


def get_pending(es, since):
    result = es.search({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {
            "range": {"modified_ts": {"gte": CNV.datetime2milli(since)}}}
        }},
        "from": 0,
        "size": 0,
        "sort": [],
        "facets": {"default": {"terms": {"field": "bug_id", "size": 200000}}}
    })

    if len(result.facets.default.terms) >= 200000:
        Log.error("Can not handle more than 200K bugs changed")

    pending_bugs = Multiset(
        result.facets.default.terms,
        key_field="term",
        count_field="count"
    )
    Log.note("Source has {{num}} bug versions for updating", {
        "num": len(pending_bugs)
    })
    return pending_bugs


# USE THE source TO GET THE INDEX SCHEMA
def get_or_create_index(destination_settings, source):
    #CHECK IF INDEX, OR ALIAS, EXISTS
    es = ElasticSearch(destination_settings)
    aliases = es.get_aliases()

    indexes = [a for a in aliases if a.alias == destination_settings.index]
    if not indexes:
        #CREATE INDEX
        schema = source.get_schema()
        assert schema.settings
        assert schema.mappings
        ElasticSearch.create_index(destination_settings, schema)
    elif len(indexes) > 1:
        Log.error("do not know how to replicate to more than one index")
    elif indexes[0].alias != None:
        destination_settings.alias = destination_settings.index
        destination_settings.index = indexes[0].index

    return ElasticSearch(destination_settings)


def replicate(source, destination, pending, last_updated):
    """
    COPY source RECORDS TO destination
    """
    for g, bugs in Q.groupby(pending, max_size=BATCH_SIZE):
        with Timer("Replicate {{num_bugs}} bugs...", {"num_bugs": len(bugs)}):
            data = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"terms": {"bug_id": bugs}},
                        {"range": {"modified_ts":
                            {"gte": CNV.datetime2milli(last_updated)}
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
                    lambda(x): transform_bugzilla.normalize(
                        transform_bugzilla.rename_attachments(x._source)),
                    data.hits.hits
                )
            )
            destination.add(d2)


def main(settings):
    #USE A FILE
    if settings.source.filename != None:
        settings.destination.alias = settings.destination.index
        settings.destination.index = ElasticSearch.proto_name(settings.destination.alias)
        schema = CNV.JSON2object(File(settings.source.schema_filename).read())
        if transform_bugzilla.USE_ATTACHMENTS_DOT:
            schema = CNV.JSON2object(CNV.object2JSON(schema).replace("attachments_", "attachments."))

        dest = ElasticSearch.create_index(settings.destination, schema)
        dest.set_refresh_interval(-1)
        extract_from_file(settings.source, dest)
        dest.set_refresh_interval(1)

        dest.delete_all_but(settings.destination.alias,
                            settings.destination.index)
        dest.add_alias(settings.destination.alias)
        return

    # SYNCH WITH source ES INDEX
    source=ElasticSearch(settings.source)
    destination=get_or_create_index(settings["destination"], source)

    # GET LAST UPDATED
    time_file = File(settings.param.last_replication_time)
    from_file = None
    if time_file.exists:
        from_file = CNV.milli2datetime(CNV.value2int(time_file.read()))
    from_es = get_last_updated(destination)
    last_updated = Math.min(from_file, from_es)
    current_time = datetime.utcnow()

    pending = get_pending(source, last_updated)
    replicate(source, destination, pending, last_updated)

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