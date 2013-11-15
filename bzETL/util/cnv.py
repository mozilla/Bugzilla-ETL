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
from .strings import expand_template
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
            Log.error("Can not encode into JSON: {{value}}", {"value":repr(obj)}, e)

    @staticmethod
    def JSON2object(json_string, params=None, flexible=False):
        try:
            #REMOVE """COMMENTS""", #COMMENTS, //COMMENTS, AND \n \r
            if flexible: json_string=re.sub(r"\"\"\".*?\"\"\"|\s+//.*\n|#.*?\n|\n|\r", r" ", json_string)  #DERIVED FROM https://github.com/jeads/datasource/blob/master/datasource/bases/BaseHub.py#L58

            if params:
                params=dict([(k,CNV.value2quote(v)) for k,v in params.items()])
                json_string=expand_template(json_string, params)

            obj=json_decoder.decode(json_string)
            if isinstance(obj, list): return StructList(obj)
            return struct.wrap(obj)
        except Exception, e:
            Log.error("Can not decode JSON:\n\t"+json_string, e)


    @staticmethod
    def string2datetime(value, format):
        ## http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
        try:
            return datetime.datetime.strptime(value, format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value":value, "format":format}, e)


    @staticmethod
    def datetime2string(value, format):
        try:
            return value.strftime(format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value":value, "format":format}, e)



    @staticmethod
    def datetime2unix(d):
        if d == None:
            return None
        return long(time.mktime(d.timetuple()))


    @staticmethod
    def datetime2milli(d):
        try:
            epoch = datetime.datetime(1970, 1, 1)
            diff = d-epoch
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
        return datetime.datetime.utcfromtimestamp(u/1000)



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
        return "\""+value.replace("\\", "\\\\").replace("\"", "\\\"")+"\""

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
        return (("0"*size)+hex(value)[2:])[-size:]

    @staticmethod
    def value2intlist(value):
        if value == None:
            return None
        elif hasattr(value, '__iter__'):
            output=[int(d) for d in value if d!=""]
            return output
        elif value.strip()=="":
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
            if isinstance(v, float) and round(v,0)!=v:
                return v
            #IF LOOKS LIKE AN INT, RETURN AN INT
            return int(v)
        except Exception:
            try:
                return float(v)
            except Exception, e:
                Log.error("Not a number ({{value}})", {"value":v}, e)

    @staticmethod
    def utf82unicode(value):
        return unicode(value.decode('utf8'))

    @staticmethod
    def latin12unicode(value):
        return unicode(value.decode('iso-8859-1'))
