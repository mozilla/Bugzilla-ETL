# encoding: utf-8
#
from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log
from bzETL.util.files import File
from bzETL.util.queries import Q
from bzETL.util.struct import Struct


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
        self.filename=settings.filename
        try:
            self.data=CNV.JSON2object(File(self.filename).read())
        except IOError:
            self.data=Struct()


    def search(self, query):
        f=CNV.esfilter2where(struct.wrap(query).query.filtered.filter)
        return struct.wrap({"hits":{"hits":[{"_id":i, "_source":d} for i,d in self.data.items() if f(d)]}})

    def extend(self, records):
        """
        JUST SO WE MODEL A Queue
        """
        records={v["id"]:v["value"] for v in records}

        self.data.dict.update(records)

        data_as_json=CNV.object2JSON(self.data, pretty=True)
        File(self.filename).write(data_as_json)
        Log.note("{{num}} items added", {"num":len(records)})

    def add(self, record):
        if isinstance(record, list):
            Log.error("no longer accepting lists, use extend()")
        return self.extend([record])

    def delete_record(self, filter):
        f = CNV.esfilter2where(filter)
        self.data = struct.wrap({k: v for k, v in self.data.items() if not f(v)})

    def set_refresh_interval(self, seconds):
        pass

