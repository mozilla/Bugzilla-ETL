from StringIO import StringIO
from datetime import datetime, time
from decimal import Decimal
import json
from .struct import Null


class NewJSONEncoder(object):

    def __init__(self):
        object.__init__(self)

    def encode(self, value):
        buffer = []
        _value2json(value, buffer)
        output = u"".join(buffer)
        return output


# OH HUM, cPython with uJSON, OR pypy WITH BUILTIN JSON?
# http://liangnuren.wordpress.com/2012/08/13/python-json-performance/
# http://morepypy.blogspot.ca/2011/10/speeding-up-json-encoding-in-pypy.html
json_encoder=NewJSONEncoder()
json_decoder=json._default_decoder



def _value2json(value, _buffer):
    if value == Null or value is None:
        _buffer.append("null")
    elif value is True:
        _buffer.append('true')
    elif value is False:
        _buffer.append('false')
    elif isinstance(value, basestring):
        _string2json(value, _buffer)
    elif isinstance(value, (int, long, Decimal)):
        _buffer.append(str(value))
    elif isinstance(value, float):
        _buffer.append(repr(value))
    elif isinstance(value, datetime):
        _buffer.append(unicode(long(time.mktime(value.timetuple())*1000)))
    elif isinstance(value, dict):
        _dict2json(value, _buffer)
    elif hasattr(value, '__iter__'):
        _list2json(value, _buffer)
    else:
        raise Exception(repr(value)+" is not JSON serializable")


def _list2json(value, _buffer):
    _buffer.append("[")
    first = True
    for v in value:
        if not first:
            _buffer.append(", ")
        first = False
        _value2json(v, _buffer)
    _buffer.append("]")

def _dict2json(value, _buffer):
    _buffer.append("{")
    first = True
    for k, v in value.items():
        if not first:
            _buffer.append(", ")
        first = False
        _string2json(k, _buffer)
        _buffer.append(": ")
        _value2json(v, _buffer)
    _buffer.append("}")


special = u"\\\"\t\n\r"
replacement = [u"\\\\", u"\\\"", u"\\t", u"\\n", u"\\r"]


def _string2json(value, _buffer):
    """
    SLOW IN cPython, FAST IN pypy
    """
    _buffer.append("\"")
    for c in value:
        i=special.find(c)
        if i>=0:
            _buffer.append(replacement[i])
        else:
            _buffer.append(c)
    _buffer.append("\"")



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
            json_encoder.encode(r)
            return r
        except Exception, e:
            return None



