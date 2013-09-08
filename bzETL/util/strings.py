################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
import datetime
from decimal import Decimal
import json
import re
from threading import Lock
import time
import struct

from .struct import Struct, StructList

import sys
reload(sys)
sys.setdefaultencoding("utf-8")



def indent(value, prefix=u"\t", indent=None):
    if indent is not None:
        prefix=prefix*indent
        
    try:
        content=value.rstrip()
        suffix=value[len(content):]
        lines=content.splitlines()
        return prefix+(u"\n"+prefix).join(lines)+suffix
    except Exception, e:
        raise Exception(u"Problem with indent of value ("+e.message+u")\n"+unicode(value))


def outdent(value):
    num=100
    lines=value.splitlines()
    for l in lines:
        trim=len(l.lstrip())
        if trim>0: num=min(num, len(l)-len(l.lstrip()))
    return u"\n".join([l[num:] for l in lines])

def between(value, prefix, suffix):
    s = value.find(prefix)
    if s==-1: return None
    s+=len(prefix)

    e=value.find(suffix, s)
    if e==-1:
        return None

    s=value.rfind(prefix, 0, e)+len(prefix) #WE KNOW THIS EXISTS, BUT THERE MAY BE A RIGHT-MORE ONE
    return value[s:e]


def right(value, len):
    if len<=0: return u""
    return value[-len:]

def find_first(value, find_arr, start=0):
    i=len(value)
    for f in find_arr:
        temp=value.find(f, start)
        if temp==-1: continue
        i=min(i, temp)
    if i==len(value): return -1
    return i

#TURNS OUT PYSTACHE MANGLES CHARS FOR HTML
#def expand_template(template, values):
#    if values is None: values={}
#    return pystache.render(template, values)

pattern=re.compile(r"(\{\{[\w_\.]+\}\})")
def expand_template(template, values):
    if values is None: values={}
    values=struct.wrap(values)

    def replacer(found):
        var=found.group(1)
        try:
            val=values[var[2:-2]]
            val=toString(val)
            return val
        except Exception, e:
            try:
                if e.message.find(u"is not JSON serializable"):
                    #WORK HARDER
                    val=json_scrub(val)
                    val=toString(val)
                    return val
            except Exception:
                raise Exception(u"Can not find "+var[2:-2]+u" in template:\n"+indent(template))

    return pattern.sub(replacer, template)



class NewJSONEncoder(json.JSONEncoder):

    def __init__(self):
        json.JSONEncoder.__init__(self, sort_keys=True)

    def default(self, obj):
        if isinstance(obj, set):
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

#OH HUM, cPython with uJSON, OR pypy WITH BUILTIN JSON?
#http://liangnuren.wordpress.com/2012/08/13/python-json-performance/

#import ujson

#class json_encoder():
#    @classmethod
#    def encode(self, value):
#        return ujson.dumps(value)

#class json_decoder():
#    @classmethod
#    def decode(cls, value):
#        return ujson.loads(value)

json_lock=Lock()
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
    if r is None:# or type(r).__name__=="long" or type(r).__name__ in ["str", "bool", "int", "basestring", "float", "boolean"]:
        return r
    elif isinstance(r, dict):
        output={}
        for k, v in r.items():
            v=_scrub(v)
            output[k]=v
        return output
    elif hasattr(r, '__iter__'):
        output=[]
        for v in r:
            v=_scrub(v)
            output.append(v)
        return output
    else:
        try:
            with json_lock:
                json_encoder.encode(r)
                return r
        except Exception, e:
            return None
