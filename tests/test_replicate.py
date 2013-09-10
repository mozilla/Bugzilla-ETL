
from bzETL import bzReplicate
from bzETL.util.startup import startup
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log


#def get_bug_versions(es, bug_id):
#    try:
#        results=es.search({
#            "query":{"filtered":{
#                "query":{"match_all":{}},
#                "filter":{"term":{"bug_id":bug_id}}
#            }},
#            "from":0,
#            "size":200000,
#            "sort":[]
#        })
#
#        if len(results.hits.hits)==0: return [];
#        return [b._source for b in results.hits.hits]
#    except Exception, e:
#        Log.error("Can not {{bug_id}}", {"bug_id":bug_id}, e)
#
#
#


def test_replication():
    try:
        settings=startup.read_settings(filename="replication_settings.json")
        Log.start(settings.debug)

        source=ElasticSearch(settings.source)
        destination=bzReplicate.get_or_create_index(settings["destination"], source)

        bzReplicate.replicate(source, destination, [537285], CNV.string2datetime("19900101", "%Y%m%d"))
    finally:
        Log.stop()










if __name__=="__main__":
    test_replication()