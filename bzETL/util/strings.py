# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import re
from .jsons import json_encoder
import struct

from .struct import Struct

import sys
reload(sys)
sys.setdefaultencoding("utf-8")



def indent(value, prefix=u"\t", indent=None):
    if indent != None:
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




pattern=re.compile(r"\{\{([\w_\.]+(\|[\w_]+)*)\}\}")
def expand_template(template, values):
    values=struct.wrap(values)

    def replacer(found):
        seq=found.group(1).split("|")

        var=seq[0]
        try:
            val=values[var]
            val=toString(val)
            for filter in seq[1:]:
                val=eval(filter+"(val)")
            return val
        except Exception, e:
            try:
                if e.message.find(u"is not JSON serializable"):
                    #WORK HARDER
                    val=toString(val)
                    return val
            except Exception:
                raise Exception(u"Can not expand "+"|".join(seq)+u" in template:\n"+indent(template), e)

    return pattern.sub(replacer, template)


def toString(val):
    if isinstance(val, Struct):
        return json_encoder.encode(val.dict, pretty=True)
    elif isinstance(val, dict) or isinstance(val, list) or isinstance(val, set):
        val=json_encoder.encode(val, pretty=True)
        return val
    return unicode(val)



def edit_distance(s1, s2):
    """
    FROM http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python
    LICENCE http://creativecommons.org/licenses/by-sa/3.0/
    """
    if len(s1) < len(s2):
        return edit_distance(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return 1.0

    previous_row = xrange(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1 # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return float(previous_row[-1])/len(s1)