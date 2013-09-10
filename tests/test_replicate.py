
from bzETL import bzReplicate
from bzETL.util.startup import startup
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log


def test_replication():
    try:
        settings=startup.read_settings(filename="replication_settings.json")
        Log.start(settings.debug)

        source=ElasticSearch(settings.source)
        destination=bzReplicate.get_or_create_index(settings["destination"], source)

        bzReplicate.replicate(source, destination, [537285], CNV.string2datetime("19900101", "%Y%m%d"))
    finally:
        Log.stop()



test_replication()