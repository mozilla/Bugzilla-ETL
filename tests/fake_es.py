from util.cnv import CNV
from util.debug import D
from util.files import File
from util.struct import Struct, wrap


class Fake_ES():


    def __init__(self, settings):
        self.filename=settings.filename
        try:
            self.data=CNV.JSON2object(File(self.filename).read())
        except IOError:
            self.data={}

    def search(self, query):
        filter=parse_filter(wrap(query).query.filtered.filter)
        return wrap({"hits":{"hits":[{"_id":i, "_source":d} for i,d in self.data.items() if filter(d)]}})


    def add(self, records):
        records={v["id"]:v["value"] for v in records}

        self.data.dict.update(records)
        File(self.filename).write(CNV.object2JSON(self.data))
        D.println("{{num}} items added", {"num":len(records)})



def parse_filter(filter):
    (type, value)=filter.items()[0]
    if type=="and":
        return _and([parse_filter(v) for v in value])
    elif type=="term":
        return _term(value)
    elif type=="range":
        (field, limits)=value.items()[0]
        parts=[_range(field, type, v) for type, v in limits.items()]
        return _and(parts)
    else:
        D.error("{{type}} filter not supported by fake_es yet", {"type":type})


def _and(args):
    def output(data):
        for a in args:
            if not a(data): return False
        return True
    return output

def _term(arg):
    (field, value)=arg.items()[0]
    def output(data):
        return data[field]==value
    return output

def _range(field, type, value):
    if type=="lte":
        def output(data):
            return data[field]<=value
    elif type=="gte":
        def output(data):
            return data[field]<=value
    else:
        D.error("Range type {{type}} not supported yet", {"type":type})
    return output
