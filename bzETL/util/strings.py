################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

import re
from .jsons import json_encoder
import struct

from .struct import Null, Struct

import sys
reload(sys)
sys.setdefaultencoding("utf-8")



def indent(value, prefix=u"\t", indent=Null):
    if indent != Null:
        prefix=prefix*indent
        
    try:
        content=value.rstrip()
        suffix=value[len(content):]
        lines=content.splitlines()
        return prefix+(u"\n"+prefix).join(lines)+suffix
    except Exception, e:
        raise Exception(u"Problem with indent of value ("+e.message+u")\n"+unicode(value))


def outdent(value):
    try:
        num=100
        lines=value.splitlines()
        for l in lines:
            trim=len(l.lstrip())
            if trim>0: num=min(num, len(l)-len(l.lstrip()))
        return u"\n".join([l[num:] for l in lines])
    except Exception, e:
        from .logs import Log
        Log.error("can not outdent value", e)

def between(value, prefix, suffix):
    s = value.find(prefix)
    if s==-1: return Null
    s+=len(prefix)

    e=value.find(suffix, s)
    if e==-1:
        return Null

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
#    if values == Null: values={}
#    return pystache.render(template, values)

pattern=re.compile(r"(\{\{[\w_\.]+\}\})")
def expand_template(template, values):
    if values == Null: values={}
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
                    val=toString(val)
                    return val
            except Exception:
                raise Exception(u"Can not find "+var[2:-2]+u" in template:\n"+indent(template))

    return pattern.sub(replacer, template)


def toString(val):
    if isinstance(val, Struct):
        return json_encoder.encode(val.dict)
    elif isinstance(val, dict) or isinstance(val, list) or isinstance(val, set):
        val=json_encoder.encode(val)
        return val
    return unicode(val)