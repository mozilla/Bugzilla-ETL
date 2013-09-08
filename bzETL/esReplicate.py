################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


## REPLICATE SOME OTHER ES INDEX
##



from datetime import datetime, timedelta
import transform_bugzilla
from .util.randoms import Random
from .util.cnv import CNV
from .util.debug import D
from .util.query import Q
from .util.startup import startup
from .util.files import File
from .util.multiset import multiset
from .util.elasticsearch import ElasticSearch


far_back=datetime.utcnow()-timedelta(weeks=52)
BATCH_SIZE=10000



def fix_json(json):
    json=json.replace("attachments.", "attachments_")
    return json.decode('iso-8859-1').encode('utf8')






def extract_from_file(source_settings, destination):
    with File(source_settings.filename).iter() as handle:
        for g, d in Q.groupby(handle, size=BATCH_SIZE):
            try:
                d2=map(lambda(x): {"id":x.id, "value":x}, map(lambda(x): transform_bugzilla.normalize(CNV.JSON2object(fix_json(x))), d))
                destination.add(d2)
            except Exception, e:
                filename="Error_"+Random.hex(20)+".txt"
                File(filename).write(d)
                D.warning("Can not convert block {{block}} (file={{host}})", {"block":g, "filename":filename}, e)



def get_last_updated(es):
    try:
        results=es.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"range":{"modified_ts":{"gte":CNV.datetime2milli(far_back)}}}
            }},
            "from":0,
            "size":0,
            "sort":[],
            "facets":{"0":{"statistical":{"field":"modified_ts"}}}
        })

        if results.facets["0"].count==0: return datetime.min;
        return CNV.milli2datetime(results.facets["0"].max)
    except Exception, e:
        D.error("Can not get_last_updated from {{host}}/{{index}}", {"host":es.settings.host, "index":es.settings.index}, e)


def get_pending(es, since):

    result=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"range":{"modified_ts":{"gte":CNV.datetime2milli(since)}}}
        }},
        "from":0,
        "size":0,
        "sort":[],
        "facets":{"default":{"terms":{"field":"bug_id","size":200000}}}
    })

    if len(result.facets.default.terms)>=200000: D.error("Can not handle more than 200K bugs changed")

    pending_bugs=multiset(result.facets.default.terms, key_field="term", count_field="count")
    return pending_bugs



# USE THE source TO GET THE INDEX SCHEMA
def get_or_create_index(destination_settings, source):
    #CHECK IF INDEX, OR ALIAS, EXISTS
    es=ElasticSearch(destination_settings)
    aliases=es.get_aliases()

    indexes=[a for a in aliases if a.alias==destination_settings.index]
    if len(indexes)==0:
        #CREATE INDEX
        schema=source.get_schema()
        assert schema.settings is not None
        assert schema.mappings is not None
        ElasticSearch.create_index(settings, schema)
    elif len(indexes)>1:
        D.error("do not know how to replicate to more than one index")
    elif indexes[0].alias is not None:
        destination_settings.alias=destination_settings.index
        destination_settings.index=indexes[0].index

    return ElasticSearch(destination_settings)





def main(settings):
    #USE A FILE
    if settings.source.filename is not None:
        settings.destination.alias=settings.destination.index
        settings.destination.index=settings.destination.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
        schema=CNV.JSON2object(File(settings.source.schema_filename).read())

        dest=ElasticSearch.create_index(settings.destination, schema)
        dest.set_refresh_interval(-1)
        extract_from_file(settings.source, dest)
        dest.set_refresh_interval(1)

        dest.delete_all_but(settings.destination.alias, settings.destination.index)
        dest.add_alias(settings.destination.alias)
        return

    # SYNCH WITH source ES INDEX
    source=ElasticSearch(settings.source)
    destination=get_or_create_index(settings["destination"], source)
    last_updated=get_last_updated(destination)-timedelta(days=7)
    pending=get_pending(source, last_updated)

    # pending IS IN {"bug_id":b, "count":c} FORM
    # MAIN ETL LOOP
    for g, bugs in Q.groupby(pending, max_size=BATCH_SIZE):
        data=source.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"and":[
                    {"terms":{"bug_id":bugs}},
                    {"range":{"modified_ts":{"gte":CNV.datetime2milli(last_updated)}}}
                ]}
            }},
            "from":0,
            "size":200000,
            "sort":[]
        })

        d2=map(
            lambda(x): {"id":x.id, "value":x},
            map(
                lambda(x): transform_bugzilla.normalize(transform_bugzilla.rename_attachments(x)),
                data.hits.hits
            )
        )
        destination.add(d2)

if __name__=="__main__":
#    import profile
#    profile.run("""
    try:
        settings=startup.read_settings()
        D.start(settings.debug)
        main(settings)
    except Exception, e:
        D.error("Problems exist", e)
    finally:
        D.stop()
#    """)


