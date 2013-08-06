from datetime import datetime, timedelta
import itertools
from util.cnv import CNV
from util.debug import D
from util.query import Q
from util.startup import startup
from util.multiset import multiset
from util.elasticsearch import ElasticSearch


far_back=datetime.utcnow()-timedelta(weeks=52)
BATCH_SIZE=10000


def transform(data):
    data._id=str(data.bug_id)+"."+str(data.modified_ts)
    return data


def load_from_file(source_settings, destination):
    with open(source_settings.filename, "r") as handle:
        for g, d in Q.groupby(handle, size=BATCH_SIZE):
            d2=map(transform, map(CNV.JSON2object, d))
            destination.load(d2, "_id")


def get_last_updated(es):
    try:
        results=es.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(far_back)}}}
            }},
            "from":0,
            "size":0,
            "sort":[],
            "facets":{"0":{"statistical":{"field":"modified_ts"}}}
        })

        if results.facets["0"].count==0: return datetime.min;
        return CNV.unixmilli2datetime(results.facets["0"].max)
    except Exception, e:
        D.error("Can not get_last_updated from ${host}/${index}", {"host":es.settings.host, "index":es.settings.index}, e)

#USING CUBES
#    result=Q({
#        "from":es,
#        "select":{"name":"max_date", "value":"modified_ts", "aggregate":"max"},
#        "esfilter":{"range":{"modified_ts":{"gte":str(CNV.datetime2unixmilli(far_back))}}}
#    })
#
#    return CNV.unix2datetime(result.cube/1000)

def get_pending(es, since):

    result=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"and":[
                {"script":{"script":"true"}},
                {"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(since)}}}
            ]}
        }},
        "from":0,
        "size":0,
        "sort":[],
        "facets":{"default":{"terms":{"field":"bug_id","size":200000}}}
    })

    if len(result.facets.default.terms)>=200000: D.error("Can not handle more than 200K bugs changed")

    pending_bugs=multiset(result.facets.default.terms, key_field="term", count_field="count")
    return pending_bugs
#USING CUBES
#    pending_bugs=Q({
#        "from":es,
#        "select":{"name":"count", "value":"bug_id", "aggregate":"count"},
#        "edges":[
#            "bug_id"
#        ],
#        "esfilter":{"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(since)}}}
#    })

#    return Q.stack(pending_bugs)
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
        with open (settings.source.schema_filename, "r") as file:
            settings.destination.alias=settings.destination.index
            settings.destination.index=settings.destination.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
            schema=CNV.JSON2object("".join(file.readlines()))

        dest=ElasticSearch.create_index(settings.destination, schema)
        dest.set_refresh_interval(-1)
        load_from_file(settings.source, dest)
        dest.set_refresh_interval(1)
        return

    #SYNCH WITH source ES INDEX
    source=ElasticSearch(settings.source)
    destination=get_or_create_index(settings["destination"], source)
    last_updated=get_last_updated(destination)
    pending=get_pending(source, last_updated)

    
    # pending IS IN {"bug_id":b, "count":c} FORM
    for g, bugs in Q.groupby(pending, min_size=BATCH_SIZE):
        data=source.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"and":[
                    {"terms":{"bug_id":bugs}},
                    {"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(last_updated)}}}
                ]}
            }},
            "from":0,
            "size":200000,
            "sort":[]
        })

        destination.load(map(transform, Q.select(data.hits.hits, "_source")), "_id")



settings=startup.read_settings()
main(settings)

