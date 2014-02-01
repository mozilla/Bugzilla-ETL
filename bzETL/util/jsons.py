# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# THIS FILE EXISTS TO SERVE AS A FAST REPLACMENT FOR JSON ENCODING
# THE DEFAULT JSON ENCODERS CAN NOT HANDLE A DIVERSITY OF TYPES *AND* BE FAST
#
# 1) WHEN USING CPython, WE HAVE NO COMPILER OPTIMIZATIONS: THE BEST STRATEGY IS TO
#    CONVERT THE MEMORY STRUCTURE TO STANDARD TYPES AND SEND TO THE INSANELY FAST
#    DEFAULT JSON ENCODER
# 2) WHEN USING PYPY, WE USE CLEAR AND SIMPLE PROGRAMMING SO THE OPTIMIZER CAN DO
#    ITS JOB.  ALONG WITH THE UnicodeBuilder WE GET NEAR C SPEEDS


from __future__ import unicode_literals
import json
import re
import time
from datetime import datetime, date
from decimal import Decimal
import sys

from .struct import Struct

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
            return unicode(json.dumps(json_scrub(value), indent=4, sort_keys=True, separators=(',', ': ')))

        _buffer = UnicodeBuilder(1024)
        _value2json(value, _buffer)
        output = _buffer.build()
        return output


class cPythonJSONEncoder(object):
    def __init__(self):
        object.__init__(self)

    def encode(self, value, pretty=False):
        if pretty:
            return unicode(json.dumps(json_scrub(value), ensure_ascii=False, indent=4, sort_keys=True, separators=(',', ': ')))

        return unicode(json.dumps(json_scrub(value), ensure_ascii=False))


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
            from util.logs import Log

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
    elif hasattr(value, '__iter__'):
        _iter2json(value, _buffer)
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
            k = unicode(k.decode("utf8"))
        append(_buffer, ESCAPE.sub(replace, k))
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
    if type is date:
        return long(time.mktime(value.timetuple()) * 1000)
    elif type is datetime:
        return long(time.mktime(value.timetuple()) * 1000)
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
            output[k]=expand_dot(v)
        return output
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = expand_dot(v)
            output.append(v)
        return output
    else:
        return value







