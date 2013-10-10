from datetime import datetime, time
from decimal import Decimal
import json
from threading import Lock
from .struct import Null, Struct, StructList


class NewJSONEncoder(json.JSONEncoder):

    def __init__(self):
        json.JSONEncoder.__init__(self, sort_keys=True)

    def default(self, obj):
        if obj == Null:
            return None
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, Struct):
            return obj.dict
        elif isinstance(obj, StructList):
            return obj.list
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return int(time.mktime(obj.timetuple())*1000)
        return json.JSONEncoder.default(self, obj)

# OH HUM, cPython with uJSON, OR pypy WITH BUILTIN JSON?
# http://liangnuren.wordpress.com/2012/08/13/python-json-performance/
# http://morepypy.blogspot.ca/2011/10/speeding-up-json-encoding-in-pypy.html
json_lock=Lock()  #NOT SURE IF INSTANCE CAN HANDLE MORE THAN ONE INSTANCE
json_encoder=NewJSONEncoder()
json_decoder=json._default_decoder

def toString(val):
    with json_lock:
        if isinstance(val, Struct):
            return json_encoder.encode(val.dict)
        elif isinstance(val, dict) or isinstance(val, list) or isinstance(val, set):
            val=json_encoder.encode(val)
            return val
    return unicode(val)

#REMOVE VALUES THAT CAN NOT BE JSON-IZED
def json_scrub(r):
    return _scrub(r)


def _scrub(r):
    if r == Null:
        return Null
    elif isinstance(r, dict):
        output = {}
        for k, v in r.items():
            v = _scrub(v)
            output[k] = v
        return output
    elif hasattr(r, '__iter__'):
        output = []
        for v in r:
            v = _scrub(v)
            output.append(v)
        return output
    else:
        try:
            with json_lock:
                json_encoder.encode(r)
                return r
        except Exception, e:
            return None


def value2json(value):
    buffer=[]
    _value2json(value, buffer)
    return "".join(buffer)



def _value2json(value, buffer):
    if value == Null or value is None:
        buffer.append("null")
    elif value is True:
        buffer.append('true')
    elif value is False:
        buffer.append('false')
    elif isinstance(value, basestring):
        return repr(value)
    elif isinstance(value, (int, long, Decimal)):
        buffer.append(str(value))
    elif isinstance(value, float):
        buffer.append(repr(value))
    elif isinstance(value, datetime.datetime):
        return int(time.mktime(value.timetuple())*1000)
    elif isinstance(value, dict):
        _dict2json(value, buffer)
    elif hasattr(value, '__iter__'):
        _list2json(value, buffer)
    else:
        raise Exception("Can not jsonize "+repr(value))


def _list2json(value, buffer):
    buffer.append("[")
    first = True
    for v in value:
        if not first:
            buffer.append(", ")
        first = False
        value2json(v, buffer)
    buffer.append("]")

def _dict2json(value, buffer):
    buffer.append("{")
    first = False
    for k, v in value.items():
        if not first:
            buffer.append(", ")
        first = False
        buffer.append(unicode(k))
        buffer.append(": ")
        value2json(v, buffer)
    buffer.append("}")
