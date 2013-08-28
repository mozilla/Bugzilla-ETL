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
import time
import pystache
from util.struct import Struct


def indent(value, prefix="\t"):
    return prefix+("\n"+prefix).join(value.rstrip().splitlines())


def outdent(value):
    num=100
    lines=value.splitlines()
    for l in lines:
        trim=len(l.lstrip())
        if trim>0: num=min(num, len(l)-len(l.lstrip()))
    return "\n".join([l[num:] for l in lines])

def between(value, prefix, suffix):
    s = value.find(prefix)
    if s==-1: return None
    s+=len(prefix)

    e=value.find(suffix, s)
    if e==-1: raise Exception("can not find '"+suffix+"'")

    s=value.rfind(prefix, 0, e)+len(prefix) #WE KNOW THIS EXISTS, BUT THERE MAY BE A RIGHT-MORE ONE
    return value[s:e]


def right(value, len):
    if len<=0: return ""
    return value[-len:]

def find_first(value, find_arr, start=0):
    i=len(value)
    for f in find_arr:
        temp=value.find(f, start)
        if temp==-1: continue
        i=min(i, temp)
    if i==len(value): return -1
    return i


def expand_template(template, values):
    return pystache.render(template, values)



class NewJSONEncoder(json.JSONEncoder):

    def __init__(self):
        json.JSONEncoder.__init__(self, sort_keys=True)

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return int(time.mktime(obj.timetuple())*1000)
        return json.JSONEncoder.default(self, obj)


import ujson

class json_encoder():
    @classmethod
    def encode(self, value):
        return ujson.dumps(value)

class json_decoder():
    @classmethod
    def decode(cls, value):
        return ujson.loads(value)


#json_encoder=NewJSONEncoder()
#json_decoder=json._default_decoder

def toString(val):
    if isinstance(val, Struct):
        return json_encoder.encode(val.dict)
    elif isinstance(val, dict) or isinstance(val, list) or isinstance(val, set):
        val=json_encoder.encode(val)
        return val
    return str(val)

#REMOVE VALUES THAT CAN NOT BE JSON-IZED
def scrub(r):
    return Struct(**_scrub(r))

def _scrub(r):
    if isinstance(r, dict):
        output={}
        for k, v in r.items():
            v=_scrub(v)
            output[k]=v
        if len(output)==0: return None
        return output
    elif hasattr(r, '__iter__'):
        output=[]
        for v in r:
            v=_scrub(v)
            output.append(v)
        if len(output)==0: return None
        return output
    elif r is None:
        return None
    else:
        try:
            json_encoder.encode(r)
            return r
        except Exception, e:
            return None
