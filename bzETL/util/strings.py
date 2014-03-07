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
from datetime import timedelta, date
from datetime import datetime as builtin_datetime
import re

from . import struct
from .struct import wrap


def datetime(value):
    from .cnv import CNV

    if isinstance(value, (date, builtin_datetime)):
        CNV.datetime2string(value, "%Y-%m-%d %H:%M:%S")

    if value < 10000000000:
        value = CNV.unix2datetime(value)
    else:
        value = CNV.milli2datetime(value)

    return CNV.datetime2string(value, "%Y-%m-%d %H:%M:%S")


def upper(value):
    return value.upper()


def newline(value):
    """
    ADD NEWLINE, IF SOMETHING
    """
    return "\n" + toString(value).lstrip("\n")


def indent(value, prefix=u"\t", indent=None):
    if indent != None:
        prefix = prefix * indent

    value = toString(value)
    try:
        content = value.rstrip()
        suffix = value[len(content):]
        lines = content.splitlines()
        return prefix + (u"\n" + prefix).join(lines) + suffix
    except Exception, e:
        raise Exception(u"Problem with indent of value (" + e.message + u")\n" + unicode(toString(value)))


def outdent(value):
    try:
        num = 100
        lines = toString(value).splitlines()
        for l in lines:
            trim = len(l.lstrip())
            if trim > 0:
                num = min(num, len(l) - len(l.lstrip()))
        return u"\n".join([l[num:] for l in lines])
    except Exception, e:
        from ...env.logs import Log

        Log.error("can not outdent value", e)


def between(value, prefix, suffix):
    value = toString(value)
    s = value.find(prefix)
    if s == -1: return None
    s += len(prefix)

    e = value.find(suffix, s)
    if e == -1:
        return None

    s = value.rfind(prefix, 0, e) + len(prefix) #WE KNOW THIS EXISTS, BUT THERE MAY BE A RIGHT-MORE ONE
    return value[s:e]


def right(value, len):
    if len <= 0: return u""
    return value[-len:]


def find_first(value, find_arr, start=0):
    i = len(value)
    for f in find_arr:
        temp = value.find(f, start)
        if temp == -1: continue
        i = min(i, temp)
    if i == len(value): return -1
    return i


pattern = re.compile(r"\{\{([\w_\.]+(\|[\w_]+)*)\}\}")


def expand_template(template, value):
    """
    template IS A STRING WITH {{variable_name}} INSTANCES, WHICH WILL
    BE EXPANDED TO WHAT IS IS IN THE value dict
    """
    value = wrap(value)
    if isinstance(template, basestring):
        return _simple_expand(template, (value,))

    return _expand(template, (value,))


def _expand(template, seq):
    """
    seq IS TUPLE OF OBJECTS IN PATH ORDER INTO THE DATA TREE
    """
    if isinstance(template, basestring):
        return _simple_expand(template, seq)
    elif isinstance(template, dict):
        template = wrap(template)
        assert template["from"], "Expecting template to have 'from' attribute"
        assert template.template, "Expecting template to have 'template' attribute"

        data = seq[-1][template["from"]]
        output = []
        for d in data:
            s = seq + (d,)
            output.append(_expand(template.template, s))
        return struct.nvl(template.separator, "").join(output)
    elif isinstance(template, list):
        return "".join(_expand(t, seq) for t in template)
    else:
        from ...env.logs import Log

        Log.error("can not handle")


def _simple_expand(template, seq):
    """
    seq IS TUPLE OF OBJECTS IN PATH ORDER INTO THE DATA TREE
    seq[-1] IS THE CURRENT CONTEXT
    """

    def replacer(found):
        ops = found.group(1).split("|")

        path = ops[0]
        var = path.lstrip(".")
        depth = min(len(seq), max(1, len(path) - len(var)))
        try:
            val = seq[-depth][var]
            for filter in ops[1:]:
                val = eval(filter + "(val)")
            val = toString(val)
            return val
        except Exception, e:
            try:
                if e.message.find("is not JSON serializable"):
                    #WORK HARDER
                    val = toString(val)
                    return val
            except Exception, f:
                from .env.logs import Log
                val = toString(val)
                Log.error(u"Can not expand " + "|".join(ops) + u" in template:\n" + indent(template), e)

    return pattern.sub(replacer, template)


def toString(val):
    if val == None:
        return u""
    elif isinstance(val, (dict, list, set)):
        from .jsons import json_encoder

        return json_encoder.encode(val, pretty=True)
    elif isinstance(val, timedelta):
        duration = val.total_seconds()
        return unicode(round(duration, 3))+" seconds"

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

    return float(previous_row[-1]) / len(s1)


DIFF_PREFIX = re.compile(r"@@ -(\d+(?:\s*,\d+)?) \+(\d+(?:\s*,\d+)?) @@")
def apply_diff(text, diff, reverse=False):
    """
    SOME EXAMPLES OF diff
    #@@ -1 +1 @@
    #-before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    #+before china goes live (end January developer release, June general audience release) , the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    @@ -0,0 +1,3 @@
    +before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    +
    +kward has the details.
    @@ -1 +1 @@
    -before china goes live (end January developer release, June general audience release), the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    +before china goes live , the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    @@ -3 +3 ,6 @@
    -kward has the details.+kward has the details.
    +
    +Target Release Dates :
    +https://mana.mozilla.org/wiki/display/PM/Firefox+OS+Wave+Launch+Cross+Functional+View
    +
    +Content Team Engagement & Tasks : https://appreview.etherpad.mozilla.org/40
    """
    if not diff:
        return text
    if diff[0].strip() == "":
        return text

    matches = DIFF_PREFIX.match(diff[0].strip())
    if not matches:
        from .env.logs import Log

        Log.error("Can not handle {{diff}}\n", {"diff": diff[0]})

    remove = [int(i.strip()) for i in matches.group(1).split(",")]
    if len(remove) == 1:
        remove = [remove[0], 1]  # DEFAULT 1
    add = [int(i.strip()) for i in matches.group(2).split(",")]
    if len(add) == 1:
        add = [add[0], 1]

    # UNUSUAL CASE WHERE @@ -x +x, n @@ AND FIRST LINE HAS NOT CHANGED
    half = len(diff[1]) / 2
    first_half = diff[1][:half]
    last_half = diff[1][half:half * 2]
    if remove[1] == 1 and add[0] == remove[0] and first_half[1:] == last_half[1:]:
        diff[1] = first_half
        diff.insert(2, last_half)

    if not reverse:
        if remove[1] != 0:
            text = text[:remove[0] - 1] + text[remove[0] + remove[1] - 1:]
        text = text[:add[0] - 1] + [d[1:] for d in diff[1 + remove[1]:1 + remove[1] + add[1]]] + text[add[0] - 1:]
        text = apply_diff(text, diff[add[1]+remove[1]+1:], reverse=reverse)
    else:
        text = apply_diff(text, diff[add[1]+remove[1]+1:], reverse=reverse)
        if add[1] != 0:
            text = text[:add[0] - 1] + text[add[0] + add[1] - 1:]
        text = text[:remove[0] - 1] + [d[1:] for d in diff[1:1 + remove[1]]] + text[remove[0] - 1:]

    return text


