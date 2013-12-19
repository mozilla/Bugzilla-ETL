from datetime import datetime
from bzETL.util import startup
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log
from bzETL.util.queries import Q


def main(settings, public, private):

    # max_bug_id = private.query({
    # 	"from":"private_bugs",
    # 	"select":{"value":"bug_id","aggregate":"maximum"}
    # })

    max_bug_id = private.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"and":[{"match_all":{}}]}
        }},
        "from":0,
        "size":0,
        "sort":[],
        "facets":{"0":{"statistical":{"field":"bug_id"}}}
    }).facets["0"].max


    # FOR ALL BUG BLOCKS
    for min_id, max_id in reversed(list(Q.range(0, max_bug_id, settings.param.increment))):
    # FIND ALL PRIVATE BUGS
    # {
    #     "from":"private_bugs",
    #     "esfilter":{"and":[
    #         {"range":{"bug_id":{"lt":900000,"gte":800000}}},
    #         {"exists":{"field":"bug_group"}},
    #         {"range":{"expires_on":{"gte":2000000000000}}}
    #     ]},
    #     "select":["bug_id","bug_group"]
    # }
        results = private.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"and":[
                    {"match_all":{}},
                    {"and":[
                        {"range":{"bug_id":{"gte":min_id, "lt":max_id}}},
                        {"exists":{"field":"bug_group"}},
                        {"missing":{"field":"expires_on", "null_value":True}}
                    ]}
                ]}
            }},
            "from":0,
            "size":200000,
            "sort":[],
            "facets":{},
            "fields":["bug_id","bug_group"]
        })

        private_ids=set(Q.select(results.hits.hits, "fields.bug_id"))


        Log.note("Ensure {{num}} bugs did not leak\n{{bugs}}", {
            "num": len(private_ids),
            "bugs": private_ids
        })

        # VERIFY NONE IN PUBLIC
        results = public.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"and":[{"match_all":{}},{"terms":{"bug_id":private_ids}}]}
            }},
            "from":0,
            "size":200000,
            "sort":[],
            "facets":{},
            "fields":["bug_id"]
        })

        leaked_bugs = set(Q.select(results.hits.hits, "bug_id"))

        if leaked_bugs:
            Log.error("Bugs have leaked!\n{{bugs|indent}}", {"bugs":leaked_bugs})





    # FIND ALL PRIVATE ATTACHMENTS

    #VERIFY NONE IN PUBLIC

    #FIND ALL PRIVATE COMMENTS

    #VERIFY NONE IN PUBLIC




if __name__=="__main__":
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        private = ElasticSearch(settings.private)
        public =  ElasticSearch(settings.public)
        main(settings, public, private)
    except Exception, e:
        Log.error("Can not start", e)
    finally:
        Log.note("Done Leak Check")
        Log.stop()



