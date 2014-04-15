# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
import StringIO
import base64
import datetime
import json
import re
import time
from . import jsons
from .collections.multiset import Multiset
from .env.profiles import Profiler
from .jsons import json_encoder, replace, ESCAPE
from .env.logs import Log
from . import struct
from .strings import expand_template
from .struct import wrap


json_decoder = json.JSONDecoder().decode


class CNV:
    """
    DUE TO MY POOR MEMORY, THIS IS A LIST OF ALL CONVERSION ROUTINES
    """

    @staticmethod
    def object2JSON(obj, pretty=False):
        try:
            json = json_encoder(obj, pretty=pretty)
            if json == None:
                Log.note(str(type(obj))+ " is not valid{{type}}JSON", {"type": " (pretty) " if pretty else " "})
                Log.error("Not valid JSON: "+str(obj)+ " of type "+str(type(obj)))
            return json
        except Exception, e:
            Log.error("Can not encode into JSON: {{value}}", {"value": repr(obj)}, e)

    @staticmethod
    def JSON2object(json_string, params=None, flexible=False, paths=False):
        with Profiler("JSON2Object"):
            try:
                #REMOVE """COMMENTS""", #COMMENTS, //COMMENTS, AND \n \r
                if flexible:
                    #DERIVED FROM https://github.com/jeads/datasource/blob/master/datasource/bases/BaseHub.py#L58
                    json_string = re.sub(r"\"\"\".*?\"\"\"|[ \t]+//.*\n|^//.*\n|#.*?\n", r"\n", json_string)
                    json_string = re.sub(r"\n//.*\n", r"\n\n", json_string)
                if params:
                    params = dict([(k, CNV.value2quote(v)) for k, v in params.items()])
                    json_string = expand_template(json_string, params)
                if isinstance(json_string, str):
                    Log.error("only unicode json accepted")

                value = wrap(json_decoder(json_string))

                if paths:
                    value = jsons.expand_dot(value)

                return value

            except Exception, e:
                Log.error("Can not decode JSON:\n\t" + str(json_string), e)


    @staticmethod
    def string2datetime(value, format):
        ## http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
        if value == None:
            return None
        try:
            return datetime.datetime.strptime(value, format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value": value, "format": format}, e)


    @staticmethod
    def datetime2string(value, format="%Y-%m-%d %H:%M:%S"):
        try:
            return value.strftime(format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value": value, "format": format}, e)


    @staticmethod
    def datetime2unix(d):
        if d == None:
            return None
        return long(time.mktime(d.timetuple()))


    @staticmethod
    def datetime2milli(d):
        try:
            if d == None:
                return None
            elif isinstance(d, datetime.datetime):
                epoch = datetime.datetime(1970, 1, 1)
            elif isinstance(d, datetime.date):
                epoch = datetime.date(1970, 1, 1)
            else:
                Log.error("Can not convert {{value}} of type {{type}}", {"value": d, "type":d.__class__})

            diff = d - epoch
            return long(diff.total_seconds()) * 1000L + long(diff.microseconds / 1000)
        except Exception, e:
            Log.error("Can not convert {{value}}", {"value": d}, e)

    @staticmethod
    def timedelta2milli(v):
        return v.total_seconds()

    @staticmethod
    def unix2datetime(u):
        try:
            if u == None:
                return None
            if u == 9999999999: # PYPY BUG https://bugs.pypy.org/issue1697
                return datetime.datetime(2286, 11, 20, 17, 46, 39)
            return datetime.datetime.utcfromtimestamp(u)
        except Exception, e:
            Log.error("Can not convert {{value}} to datetime", {"value": u}, e)

    @staticmethod
    def milli2datetime(u):
        return CNV.unix2datetime(u/1000.0)

    @staticmethod
    def dict2Multiset(dic):
        if dic == None:
            return None

        output = Multiset()
        output.dic = struct.unwrap(dic).copy()
        return output

    @staticmethod
    def multiset2dict(value):
        """
        CONVERT MULTISET TO dict THAT MAPS KEYS TO MAPS KEYS TO KEY-COUNT
        """
        if value == None:
            return None
        return dict(value.dic)

    @staticmethod
    def table2list(
            column_names, #tuple of columns names
            rows          #list of tuples
    ):
        return wrap([dict(zip(column_names, r)) for r in rows])

    @staticmethod
    def list2tab(rows):
        columns = set()
        for r in rows:
            columns |= set(r.keys())
        keys = list(columns)

        output = []
        for r in rows:
            output.append("\t".join(CNV.object2JSON(r[k]) for k in keys))

        return "\t".join(keys)+"\n"+"\n".join(output)


    #PROPER NULL HANDLING
    @staticmethod
    def value2string(value):
        if value == None:
            return None
        return unicode(value)


    #RETURN PRETTY PYTHON CODE FOR THE SAME
    @staticmethod
    def value2quote(value):
        if isinstance(value, basestring):
            return CNV.string2quote(value)
        else:
            return repr(value)

    @staticmethod
    def string2quote(value):
        return "\""+ESCAPE.sub(replace, value)+"\""

    @staticmethod
    def quote2string(value):
        if value[0] == "\"" and value[-1] == "\"":
            value = value[1:-1]

        return value.replace("\\\\", "\\").replace("\\\"", "\"").replace("\\'", "'").replace("\\\n", "\n").replace("\\\t", "\t")

    #RETURN PYTHON CODE FOR THE SAME
    @staticmethod
    def value2code(value):
        return repr(value)


    @staticmethod
    def DataFrame2string(df, columns=None):
        output = StringIO.StringIO()
        try:
            df.to_csv(output, sep="\t", header=True, cols=columns, engine='python')
            return output.getvalue()
        finally:
            output.close()

    @staticmethod
    def ascii2char(ascii):
        return chr(ascii)

    @staticmethod
    def char2ascii(char):
        return ord(char)

    @staticmethod
    def latin12hex(value):
        return value.encode("hex")

    @staticmethod
    def int2hex(value, size):
        return (("0" * size) + hex(value)[2:])[-size:]

    @staticmethod
    def hex2bytearray(value):
        return bytearray(value.decode("hex"))

    @staticmethod
    def bytearray2hex(value):
        return value.decode("latin1").encode("hex")

    @staticmethod
    def base642bytearray(value):
        return bytearray(base64.b64decode(value))

    @staticmethod
    def bytearray2base64(value):
        return base64.b64encode(value)

    @staticmethod
    def value2intlist(value):
        if value == None:
            return None
        elif hasattr(value, '__iter__'):
            output = [int(d) for d in value if d != "" and d != None]
            return output
        elif value.strip() == "":
            return None
        else:
            return [int(value)]


    @staticmethod
    def value2int(value):
        if value == None:
            return None
        else:
            return int(value)


    @staticmethod
    def value2number(v):
        try:
            if isinstance(v, float) and round(v, 0) != v:
                return v
                #IF LOOKS LIKE AN INT, RETURN AN INT
            return int(v)
        except Exception:
            try:
                return float(v)
            except Exception, e:
                Log.error("Not a number ({{value}})", {"value": v}, e)

    @staticmethod
    def utf82unicode(value):
        return unicode(value.decode('utf8'))

    @staticmethod
    def unicode2utf8(value):
        return value.encode('utf8')

    @staticmethod
    def latin12unicode(value):
        return unicode(value.decode('iso-8859-1'))

    @staticmethod
    def esfilter2where(esfilter):
        """
        CONVERT esfilter TO FUNCTION THAT WILL PERFORM THE FILTER
        WILL ADD row, rownum, AND rows AS CONTEXT VARIABLES FOR {"script":} IF NEEDED
        """

        def output(row, rownum=None, rows=None):
            return _filter(esfilter, row, rownum, rows)

        return output


    @staticmethod
    def pipe2value(value):
        type = value[0]
        if type == '0':
            return None
        if type == 'n':
            return CNV.value2number(value[1::])

        if type != 's' and type != 'a':
            Log.error("unknown pipe type ({{type}}) in {{value}}", {"type": type, "value": value})

        # EXPECTING MOST STRINGS TO NOT HAVE ESCAPED CHARS
        output = unPipe(value)
        if type == 's':
            return output

        return [CNV.pipe2value(v) for v in output.split("|")]


def unPipe(value):
    s = value.find("\\", 1)
    if s < 0:
        return value[1::]

    result = ""
    e = 1
    while True:
        c = value[s + 1]
        if c == 'p':
            result = result + value[e:s] + '|'
            s += 2
            e = s
        elif c == '\\':
            result = result + value[e:s] + '\\'
            s += 2
            e = s
        else:
            s += 1

        s = value.find("\\", s)
        if s < 0:
            break
    return result + value[e::]


def _filter(esfilter, row, rownum, rows):
    esfilter = wrap(esfilter)

    if esfilter[u"and"]:
        for a in esfilter[u"and"]:
            if not _filter(a, row, rownum, rows):
                return False
        return True
    elif esfilter[u"or"]:
        for a in esfilter[u"and"]:
            if _filter(a, row, rownum, rows):
                return True
        return False
    elif esfilter[u"not"]:
        return not _filter(esfilter[u"not"], row, rownum, rows)
    elif esfilter.term:
        for col, val in esfilter.term.items():
            if row[col] != val:
                return False
        return True
    elif esfilter.terms:
        for col, vals in esfilter.terms.items():
            if not row[col] in vals:
                return False
        return True
    elif esfilter.range:
        for col, ranges in esfilter.range.items():
            for sign, val in ranges.items():
                if sign in ("gt", ">") and row[col] <= val:
                    return False
                if sign == "gte" and row[col] < val:
                    return False
                if sign == "lte" and row[col] > val:
                    return False
                if sign == "lt" and row[col] >= val:
                    return False
        return True
    elif esfilter.missing:
        if isinstance(esfilter.missing, basestring):
            field = esfilter.missing
        else:
            field = esfilter.missing.field

        if row[field] == None:
            return True
        return False

    elif esfilter.exists:
        if isinstance(esfilter.missing, basestring):
            field = esfilter.missing
        else:
            field = esfilter.missing.field

        if row[field] != None:
            return True
        return False
    else:
        Log.error(u"Can not convert esfilter to SQL: {{esfilter}}", {u"esfilter": esfilter})

