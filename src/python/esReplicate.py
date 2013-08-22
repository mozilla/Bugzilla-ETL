from datetime import datetime, timedelta
from util.randoms import Random
from util.cnv import CNV
from util.debug import D
from util.query import Q
from util.startup import startup
from util.files import File
from util.struct import StructList
from util.multiset import multiset
from util.elasticsearch import ElasticSearch


far_back=datetime.utcnow()-timedelta(weeks=52)
BATCH_SIZE=10000



#ALL ETL HAS A TRANSFORM STEP
def transform(data):
    data._id=str(data.bug_id)+"."+str(data.modified_ts)

    data.dependson=CNV.value2intlist(data.dependson)
    data.blocked=CNV.value2intlist(data.blocked)
    data.dupe_by=CNV.value2intlist(data.dupe_by)
    return data


def fix_json(json):
    #return json.decode(encoding='UTF-8',errors='backslashreplace')
    return json.decode('iso-8859-1').encode('utf8')
#    try:
#        json.decode('ascii')
#        return json
#    except UnicodeDecodeError:
#        pass
#
#    #JSON HAS SOME BAD BYTE SEQUENCES
#    output=[]
#    for i, c in enumerate(json):
#        a=CNV.char2ascii(c)
#        if a>0x80:
#            hex=CNV.int2hex(a, 2)
#            output.append("\\u00"+hex)
#        else:
#            output.append(c)
#    return "".join(output)


def extract_from_file(source_settings, destination):
    with File(source_settings.filename).iter() as handle:
        for g, d in Q.groupby(handle, size=BATCH_SIZE):
            try:
                d2=map(transform, map(lambda(x): CNV.JSON2object(fix_json(x)), d))
                destination.load(d2, "_id")
            except Exception, e:
                filename=Random.hex(20)+".txt"
                File(filename).write(d)
                D.warning("Can not convert block ${block} (file=${filename})", {"block":g, "filename":filename}, e)



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


def get_pending(es, since):

    result=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(since)}}}
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
                    {"range":{"modified_ts":{"gte":CNV.datetime2unixmilli(last_updated)}}}
                ]}
            }},
            "from":0,
            "size":200000,
            "sort":[]
        })

        destination.load(map(transform, Q.select(data.hits.hits, "_source")), "_id")


settings=startup.read_settings()
D.settings(settings.debug)
main(settings)

