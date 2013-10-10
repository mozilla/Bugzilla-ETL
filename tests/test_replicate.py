
from bzETL import replicate
from bzETL.util.startup import startup
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log


def test_replication():
    try:
        settings=startup.read_settings(filename="replication_settings.json")
        Log.start(settings.debug)

        source=ElasticSearch(settings.source)
        destination=replicate.get_or_create_index(settings["destination"], source)

        replicate.replicate(source, destination, [537285], CNV.string2datetime("19900101", "%Y%m%d"))
    finally:
        Log.stop()


if __name__=="__main__":
    test_replication()
