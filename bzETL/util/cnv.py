# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import StringIO
import datetime
import re
import time
from .multiset import Multiset
from .jsons import json_decoder, json_encoder
from .logs import Log
import struct
from .strings import expand_template, indent
from .struct import StructList, Null


class CNV:
    """
    DUE TO MY POOR MEMORY, THIS IS A LIST OF ALL CONVERSION ROUTINES
    """

    @staticmethod
    def object2JSON(obj, pretty=False):
        try:
            return json_encoder.encode(obj, pretty=pretty)
        except Exception, e:
            Log.error("Can not encode into JSON: {{value}}", {"value": repr(obj)}, e)

    @staticmethod
    def JSON2object(json_string, params=None, flexible=False):
        try:
            #REMOVE """COMMENTS""", #COMMENTS, //COMMENTS, AND \n \r
            if flexible: json_string = re.sub(r"\"\"\".*?\"\"\"|\s+//.*\n|#.*?\n|\n|\r", r" ",
                                              json_string)  #DERIVED FROM https://github.com/jeads/datasource/blob/master/datasource/bases/BaseHub.py#L58

            if params:
                params = dict([(k, CNV.value2quote(v)) for k, v in params.items()])
                json_string = expand_template(json_string, params)

            obj = json_decoder.decode(json_string)
            if isinstance(obj, list): return StructList(obj)
            return struct.wrap(obj)
        except Exception, e:
            Log.error("Can not decode JSON:\n\t" + json_string, e)


    @staticmethod
    def string2datetime(value, format):
        ## http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
        try:
            return datetime.datetime.strptime(value, format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value": value, "format": format}, e)


    @staticmethod
    def datetime2string(value, format):
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
            epoch = datetime.datetime(1970, 1, 1)
            diff = d - epoch
            return (diff.days * 86400000) + \
                   (diff.seconds * 1000) + \
                   (diff.microseconds / 1000)  # 86400000=24*3600*1000
        except Exception, e:
            Log.error("Can not convert {{value}}", {"value": d})

    @staticmethod
    def unix2datetime(u):
        return datetime.datetime.utcfromtimestamp(u)

    @staticmethod
    def milli2datetime(u):
        return datetime.datetime.utcfromtimestamp(u / 1000)


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
        return StructList([dict(zip(column_names, r)) for r in rows])


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
        # return repr(value)
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

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

def _filter(esfilter, row, rownum, rows):
    esfilter=struct.wrap(esfilter)

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

