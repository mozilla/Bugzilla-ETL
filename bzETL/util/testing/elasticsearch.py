# encoding: utf-8
#
from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.env.elasticsearch import ElasticSearch
from bzETL.util.env.logs import Log
from bzETL.util.env.files import File
from bzETL.util.queries import Q
from bzETL.util.struct import Struct, wrap, unwrap


def make_test_instance(name, settings):
    if settings.filename:
        File(settings.filename).delete()
    return open_test_instance(name, settings)


def open_test_instance(name, settings):
    if settings.filename:
        Log.note("Using {{filename}} as {{type}}", {
            "filename": settings.filename,
            "type": name
        })
        return Fake_ES(settings)
    else:
        Log.note("Using ES cluster at {{host}} as {{type}}", {
            "host": settings.host,
            "type": name
        })
        return ElasticSearch(settings)




class Fake_ES():
    def __init__(self, settings):
        self.settings = wrap({"host":"fake", "index":"fake"})
        self.filename = settings.filename
        try:
            self.data = CNV.JSON2object(File(self.filename).read())
        except IOError:
            self.data = Struct()


    def search(self, query):
        query=wrap(query)
        f = CNV.esfilter2where(query.query.filtered.filter)
        filtered=wrap([{"_id": i, "_source": d} for i, d in self.data.items() if f(d)])
        if query.fields:
            return wrap({"hits": {"total":len(filtered), "hits": [{"_id":d._id, "fields":unwrap(Q.select([unwrap(d._source)], query.fields)[0])} for d in filtered]}})
        else:
            return wrap({"hits": {"total":len(filtered), "hits": filtered}})

    def extend(self, records):
        """
        JUST SO WE MODEL A Queue
        """
        records = {v["id"]: v["value"] for v in records}

        struct.unwrap(self.data).update(records)

        data_as_json = CNV.object2JSON(self.data, pretty=True)

        File(self.filename).write(data_as_json)
        Log.note("{{num}} items added", {"num": len(records)})

    def add(self, record):
        if isinstance(record, list):
            Log.error("no longer accepting lists, use extend()")
        return self.extend([record])

    def delete_record(self, filter):
        f = CNV.esfilter2where(filter)
        self.data = wrap({k: v for k, v in self.data.items() if not f(v)})

    def set_refresh_interval(self, seconds):
        pass

