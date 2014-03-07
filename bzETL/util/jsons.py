# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

import json
from math import floor
import re
import time
from datetime import datetime, date, timedelta
from decimal import Decimal
import sys

from .collections import AND, MAX
from .struct import Struct


# THIS FILE EXISTS TO SERVE AS A FAST REPLACEMENT FOR JSON ENCODING
# THE DEFAULT JSON ENCODERS CAN NOT HANDLE A DIVERSITY OF TYPES *AND* BE FAST
#
# 1) WHEN USING cPython, WE HAVE NO COMPILER OPTIMIZATIONS: THE BEST STRATEGY IS TO
#    CONVERT THE MEMORY STRUCTURE TO STANDARD TYPES AND SEND TO THE INSANELY FAST
#    DEFAULT JSON ENCODER
# 2) WHEN USING PYPY, WE USE CLEAR AND SIMPLE PROGRAMMING SO THE OPTIMIZER CAN DO
#    ITS JOB.  ALONG WITH THE UnicodeBuilder WE GET NEAR C SPEEDS


use_pypy = False
try:
    # UnicodeBuilder IS ABOUT 2x FASTER THAN list()
    # use_pypy = True
    from __pypy__.builders import UnicodeBuilder

    use_pypy = True
except Exception, e:
    if use_pypy:
        sys.stdout.write("The PyPy JSON serializer is in use!  Currently running CPython, not a good mix.")

    class UnicodeBuilder(list):
        def __init__(self, length=None):
            list.__init__(self)

        def build(self):
            return u"".join(self)

append = UnicodeBuilder.append


class PyPyJSONEncoder(object):
    """
    pypy DOES NOT OPTIMIZE GENERATOR CODE WELL
    """

    def __init__(self):
        object.__init__(self)

    def encode(self, value, pretty=False):
        if pretty:
            return pretty_json(value)

        try:
            _buffer = UnicodeBuilder(1024)
            _value2json(value, _buffer)
            output = _buffer.build()
            return output
        except Exception, e:
            #THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
            from .env.logs import Log

            try:
                pretty_json(value)
            except Exception, f:
                Log.error("problem serializing object", f)


class cPythonJSONEncoder(object):
    def __init__(self):
        object.__init__(self)

        self.encoder = json.JSONEncoder(
            skipkeys=False,
            ensure_ascii=False,  # DIFF FROM DEFAULTS
            check_circular=True,
            allow_nan=True,
            indent=None,
            separators=None,
            encoding='utf-8',
            default=None,
            sort_keys=False
        )

    def encode(self, value, pretty=False):
        if value == None:
            return "null"

        if pretty:
            return pretty_json(value)

        return unicode(self.encoder.encode(json_scrub(value)))


# OH HUM, cPython with uJSON, OR pypy WITH BUILTIN JSON?
# http://liangnuren.wordpress.com/2012/08/13/python-json-performance/
# http://morepypy.blogspot.ca/2011/10/speeding-up-json-encoding-in-pypy.html
if use_pypy:
    json_encoder = PyPyJSONEncoder()
    json_decoder = json._default_decoder
else:
    json_encoder = cPythonJSONEncoder()
    json_decoder = json._default_decoder


def _value2json(value, _buffer):
    if value == None:
        append(_buffer, u"null")
        return
    elif value is True:
        append(_buffer, u"true")
        return
    elif value is False:
        append(_buffer, u"false")
        return

    type = value.__class__
    if type is dict:
        if value:
            _dict2json(value, _buffer)
        else:
            append(_buffer, u"{}")
    elif type is str:
        append(_buffer, u"\"")
        v = value.decode("utf8")
        v = ESCAPE.sub(replace, v)
        append(_buffer, v)  # ASSUME ALREADY utf-8 ENCODED
        append(_buffer, u"\"")
    elif type is unicode:
        try:
            append(_buffer, u"\"")
            v = ESCAPE.sub(replace, value)
            append(_buffer, v)
            append(_buffer, u"\"")
        except Exception, e:
            from .env.logs import Log

            Log.error(value, e)
    elif type in (int, long, Decimal):
        append(_buffer, unicode(value))
    elif type is float:
        append(_buffer, unicode(repr(value)))
    elif type in (set, list, tuple):
        _list2json(value, _buffer)
    elif type is date:
        append(_buffer, unicode(long(time.mktime(value.timetuple()) * 1000)))
    elif type is datetime:
        append(_buffer, unicode(long(time.mktime(value.timetuple()) * 1000)))
    elif type is timedelta:
        append(_buffer, unicode(value.total_seconds())+"second")
    elif hasattr(value, '__iter__'):
        _iter2json(value, _buffer)
    elif hasattr(value, '__json__'):
        append(value.__json__(), _buffer)
    else:
        raise Exception(repr(value) + " is not JSON serializable")


def _list2json(value, _buffer):
    if not value:
        append(_buffer, u"[]")
    else:
        sep = u"["
        for v in value:
            append(_buffer, sep)
            sep = u", "
            _value2json(v, _buffer)
        append(_buffer, u"]")


def _iter2json(value, _buffer):
    append(_buffer, u"[")
    sep = u""
    for v in value:
        append(_buffer, sep)
        sep = u", "
        _value2json(v, _buffer)
    append(_buffer, u"]")


def _dict2json(value, _buffer):
    prefix = u"{\""
    for k, v in value.iteritems():
        append(_buffer, prefix)
        prefix = u", \""
        if isinstance(k, str):
            k = k.decode("utf8")
        append(_buffer, ESCAPE.sub(replace, unicode(k)))
        append(_buffer, u"\": ")
        _value2json(v, _buffer)
    append(_buffer, u"}")


ESCAPE = re.compile(ur'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_DCT = {
    u"\\": u"\\\\",
    u"\"": u"\\\"",
    u"\b": u"\\b",
    u"\f": u"\\f",
    u"\n": u"\\n",
    u"\r": u"\\r",
    u"\t": u"\\t",
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), u'\\u{0:04x}'.format(i))


def replace(match):
    return ESCAPE_DCT[match.group(0)]


#REMOVE VALUES THAT CAN NOT BE JSON-IZED
def json_scrub(value):
    return _scrub(value)


def _scrub(value):
    if value == None:
        return None

    type = value.__class__
    if type in (date, datetime):
        return datetime2milli(value)
    elif type is timedelta:
        return unicode(value.total_seconds())+"second"
    elif type is str:
        return unicode(value.decode("utf8"))
    elif type is dict:
        output = {}
        for k, v in value.iteritems():
            v = _scrub(v)
            output[k] = v
        return output
    elif type is Decimal:
        return float(value)
    elif type is list:
        output = []
        for v in value:
            v = _scrub(v)
            output.append(v)
        return output
    elif hasattr(value, '__json__'):
        return json._default_decoder.decode(value.__json__())
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = _scrub(v)
            output.append(v)
        return output
    else:
        return value


def expand_dot(value):
    """
    JSON CAN HAVE ATTRIBUTE NAMES WITH DOTS
    """
    if value == None:
        return None
    elif isinstance(value, (basestring, int, float)):
        return value
    elif isinstance(value, dict):
        output = Struct()
        for k, v in value.iteritems():
            output[k] = expand_dot(v)
        return output
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = expand_dot(v)
            output.append(v)
        return output
    else:
        return value


ARRAY_ROW_LENGTH = 80
ARRAY_ITEM_MAX_LENGTH = 30
ARRAY_MAX_COLUMNS = 10
INDENT = "    "

def pretty_json(value):
    try:
        if isinstance(value, dict):
            try:
                if not value:
                    return "{}"
                items = list(value.items())
                if len(items) == 1:
                    return "{\"" + items[0][0] + "\": " + pretty_json(items[0][1]).strip() + "}"

                items = sorted(items, lambda a, b: value_compare(a[0], b[0]))
                values = ["\"" + ESCAPE.sub(replace, unicode(k)) + "\": " + indent(pretty_json(v)).strip() for k, v in items if v != None]
                return "{\n" + INDENT + (",\n"+INDENT).join(values) + "\n}"
            except Exception, e:
                from .env.logs import Log
                from .collections import OR

                if OR(not isinstance(k, basestring) for k in value.keys()):
                    Log.error("JSON must have string keys: {{keys}}:", {
                        "keys": [k for k in value.keys()]
                    }, e)

                Log.error("problem making dict pretty: keys={{keys}}:", {
                    "keys": [k for k in value.keys()]
                }, e)
        elif isinstance(value, list):
            if not value:
                return "[]"

            if ARRAY_MAX_COLUMNS==1:
                return "[\n" + ",\n".join([indent(pretty_json(v)) for v in value]) + "\n]"

            if len(value) == 1:
                j = pretty_json(value[0])
                if j.find("\n") >= 0:
                    return "[\n" + indent(j) + "\n]"
                else:
                    return "[" + j + "]"

            js = [pretty_json(v) for v in value]
            max_len = MAX(len(j) for j in js)
            if max_len<=ARRAY_ITEM_MAX_LENGTH and AND(j.find("\n")==-1 for j in js):
                #ALL TINY VALUES
                num_columns = max(1, min(ARRAY_MAX_COLUMNS, int(floor((ARRAY_ROW_LENGTH + 2.0)/float(max_len+2))))) # +2 TO COMPENSATE FOR COMMAS
                if len(js)<=num_columns:  # DO NOT ADD \n IF ONLY ONE ROW
                    return "[" + ", ".join(js) + "]"
                if num_columns == 1:  # DO NOT rjust IF THERE IS ONLY ONE COLUMN
                    return "[\n" + ",\n".join([indent(pretty_json(v)) for v in value]) + "\n]"

                content = ",\n".join(
                    ", ".join(j.rjust(max_len) for j in js[r:r+num_columns])
                    for r in xrange(0, len(js), num_columns)
                )
                return "[\n" + indent(content) + "\n]"

            return "[\n" + ",\n".join([indent(pretty_json(v)) for v in value]) + "\n]"
        elif hasattr(value, '__json__'):
            if value.__json__ == None:
                Log.debug()
            j = value.__json__()
            return pretty_json(json_decoder.decode(j))
        elif hasattr(value, '__iter__'):
            return pretty_json(list(value))
        else:
            return json_encoder.encode(value)

    except Exception, e:
        from .env.logs import Log

        Log.error("Problem turning value to json", e)


def indent(value, prefix=INDENT):
    try:
        content = value.rstrip()
        suffix = value[len(content):]
        lines = content.splitlines()
        return prefix + (u"\n" + prefix).join(lines) + suffix
    except Exception, e:
        raise Exception(u"Problem with indent of value (" + e.message + u")\n" + value)


def value_compare(a, b):
    if a == None:
        if b == None:
            return 0
        return -1
    elif b == None:
        return 1

    if a > b:
        return 1
    elif a < b:
        return -1
    else:
        return 0


def datetime2milli(d):
    try:
        if d == None:
            return None
        elif isinstance(d, datetime.datetime):
            epoch = datetime.datetime(1970, 1, 1)
        elif isinstance(d, datetime.date):
            epoch = datetime.date(1970, 1, 1)
        else:
            raise Exception("Can not convert "+repr(d)+" to json")

        diff = d - epoch
        return long(diff.total_seconds()) * 1000L + long(diff.microseconds / 1000)
    except Exception, e:
        raise Exception("Can not convert "+repr(d)+" to json")
