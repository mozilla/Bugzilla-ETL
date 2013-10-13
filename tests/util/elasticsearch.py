from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log
from bzETL.util.files import File
from bzETL.util.struct import Struct, Null


def make_test_instance(name, settings):
    if settings.filename != Null:
        File(settings.filename).delete()
    return open_test_instance(name, settings)

def open_test_instance(name, settings):
    if settings.filename != Null:
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
        f=parse_filter(struct.wrap(query).query.filtered.filter)
        return struct.wrap({"hits":{"hits":[{"_id":i, "_source":d} for i,d in self.data.items() if f(d)]}})


    def add(self, records):
        records={v["id"]:v["value"] for v in records}

        self.data.dict.update(records)
        File(self.filename).write(CNV.object2JSON(self.data))
        Log.note("{{num}} items added", {"num":len(records)})

    def delete_record(self, filter):
        f = parse_filter(filter)
        self.data = struct.wrap({k: v for k, v in self.data if not f(v)})




def parse_filter(filter):
    (type, value)=filter.items()[0]
    if type=="and":
        return _and([parse_filter(v) for v in value])
    elif type=="term":
        return _term(value)
    elif type=="terms":
        return _terms(value)
    elif type=="range":
        (field, limits)=value.items()[0]
        parts=[_range(field, type, v) for type, v in limits.items()]
        return _and(parts)
    else:
        Log.error("{{type}} filter not supported by fake_es yet", {"type":type})


def _and(args):
    def output(data):
        for a in args:
            if not a(data):
                return False
        return True
    return output

def _term(arg):
    (field, value)=arg.items()[0]
    def output(data):
        return data[field]==value
    return output

def _terms(arg):
    (field, values)=arg.items()[0]
    def output(data):
        return data[field] in values
    return output

def _range(field, type, value):
    if type=="lte":
        def output(data):
            return data[field]<=value
    elif type=="gte":
        def output(data):
            return data[field]<=value
    else:
        Log.error("Range type {{type}} not supported yet", {"type":type})
    return output
