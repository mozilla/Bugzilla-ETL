# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from datetime import datetime
import time
from decimal import Decimal
import json
import re


try:
    # StringBuilder IS ABOUT 2x FASTER THAN list()
    from __pypy__.builders import StringBuilder

    use_pypy = True
except Exception, e:
    use_pypy = False
    class StringBuilder(list):
        def __init__(self, length=None):
            list.__init__(self)

        def build(self):
            return "".join(self)



class PyPyJSONEncoder(object):
    """
    pypy DOES NOT OPTIMIZE GENERATOR CODE WELL
    """
    def __init__(self):
        object.__init__(self)

    def encode(self, value, pretty=False):
        if pretty:
            return json.dumps(json_scrub(value), indent=4, sort_keys=True, separators=(',', ': '))

        _buffer = StringBuilder(1024)
        _value2json(value, _buffer.append)
        output = _buffer.build()
        return output


class cPythonJSONEncoder(object):
    def __init__(self):
        object.__init__(self)

    def encode(self, value, pretty=False):
        if pretty:
            return json.dumps(json_scrub(value), indent=4, sort_keys=True, separators=(',', ': '))

        return json.dumps(json_scrub(value))


# OH HUM, cPython with uJSON, OR pypy WITH BUILTIN JSON?
# http://liangnuren.wordpress.com/2012/08/13/python-json-performance/
# http://morepypy.blogspot.ca/2011/10/speeding-up-json-encoding-in-pypy.html
if use_pypy:
    json_encoder = PyPyJSONEncoder()
    json_decoder = json._default_decoder
else:
    json_encoder = cPythonJSONEncoder()
    json_decoder = json._default_decoder






def _value2json(value, appender):
    if isinstance(value, basestring):
        _string2json(value, appender)
    elif value == None:
        appender("null")
    elif value is True:
        appender('true')
    elif value is False:
        appender('false')
    elif isinstance(value, (int, long, Decimal)):
        appender(str(value))
    elif isinstance(value, float):
        appender(repr(value))
    elif isinstance(value, datetime):
        appender(unicode(long(time.mktime(value.timetuple())*1000)))
    elif isinstance(value, dict):
        _dict2json(value, appender)
    elif hasattr(value, '__iter__'):
        _list2json(value, appender)
    else:
        raise Exception(repr(value)+" is not JSON serializable")


def _list2json(value, appender):
    appender("[")
    first = True
    for v in value:
        if first:
            first = False
        else:
            appender(", ")
        _value2json(v, appender)
    appender("]")


def _dict2json(value, appender):
    items = value.iteritems()

    appender("{")
    first = True
    for k, v in value.iteritems():
        if first:
            first = False
        else:
            appender(", ")
        _string2json(unicode(k), appender)
        appender(": ")
        _value2json(v, appender)
    appender("}")


special_find = u"\\\"\t\n\r".find
replacement = [u"\\\\", u"\\\"", u"\\t", u"\\n", u"\\r"]

ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))


def _string2json(value, appender):
    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    appender("\"")
    appender(ESCAPE.sub(replace, value))
    appender("\"")



#REMOVE VALUES THAT CAN NOT BE JSON-IZED
def json_scrub(value):
    return _scrub(value)


def _scrub(value):
    if value == None:
        return None
    elif isinstance(value, datetime):
        return long(time.mktime(value.timetuple())*1000)
    elif isinstance(value, dict):
        output = {}
        for k, v in value.iteritems():
            v = _scrub(v)
            output[k] = v
        return output
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = _scrub(v)
            output.append(v)
        return output
    elif isinstance(value, Decimal):
        return float(value)
    else:
        return value




