# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from ..struct import wrap, StructList, unwrap


TRUE_FILTER = True
FALSE_FILTER = False


def simplify(esfilter):
    output = normalize(esfilter)
    if output is TRUE_FILTER:
        return {"match_all": {}}
    output.isNormal = None
    return output


def removeOr(esfilter):
    if esfilter["not"]:
        return {"not": removeOr(esfilter["not"])}

    if esfilter["and"]:
        return {"and": [removeOr(v) for v in esfilter["and"]]}

    if esfilter["or"]:  # CONVERT OR TO NOT.AND.NOT
        return {"not": {"and": [{"not": removeOr(v)} for v in esfilter["or"]]}}

    return esfilter

def normalize(esfilter):
    """
    SIMPLFY THE LOGIC EXPRESSION
    """
    return wrap(_normalize(wrap(esfilter)))



def _normalize(esfilter):
    """
    DO NOT USE Structs, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING and AND or FILTERS
    """
    if esfilter is TRUE_FILTER or esfilter is FALSE_FILTER or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + CNV.object2JSON(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if esfilter["and"]:
            output = []
            for a in esfilter["and"]:
                a = _normalize(a)
                if a == TRUE_FILTER:
                    isDiff = True
                    continue
                if a == FALSE_FILTER:
                    isDiff = True
                    output = None
                    break
                if a.get("and", None):
                    isDiff = True
                    a.isNormal = None
                    output.extend(a.get("and", None))
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return TRUE_FILTER
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter["and"] = output
            continue

        if esfilter["or"]:
            output = []
            for a in esfilter["or"]:
                a = _normalize(a)
                if a == TRUE_FILTER:
                    isDiff = True
                    output = None
                    break
                if a == FALSE_FILTER:
                    isDiff = True
                    continue
                if a.get("or", None):
                    a.isNormal = None
                    isDiff = True
                    output.extend(a["or"])
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return FALSE_FILTER
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter["or"] = output
            continue

    esfilter.isNormal = True
    return esfilter
