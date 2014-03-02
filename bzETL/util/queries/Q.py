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
import __builtin__

from . import group_by
from ..collections import UNION
from ..queries import flat_list, query
from ..queries.filters import TRUE_FILTER, FALSE_FILTER
from ..queries.query import Query, _normalize_selects
from ..queries.cube import Cube
from .index import UniqueIndex, Index
from .flat_list import FlatList
from ..maths import Math
from ..env.logs import Log
from ..struct import nvl, listwrap, EmptyList, split_field, unwrap, wrap
from .. import struct
from ..struct import Struct, Null, StructList


# A COLLECTION OF DATABASE OPERATORS (RELATIONAL ALGEBRA OPERATORS)


def run(query):
    query = Query(query)
    frum = query["from"]
    if isinstance(frum, list):
        pass
    elif isinstance(frum, Cube):
        pass
    elif isinstance(frum, Query):
        frum = run(frum)
    else:
        Log.error("Do ont know how to handle")

    if query.edges:
        Log.error("not implemented yet")

    try:
        if query.filter != None or query.esfilter != None:
            Log.error("use 'where' clause")
    except Exception, e:
        pass

    if query.window:
        if isinstance(frum, Cube):
            frum = StructList(list(frum))  # TRY TO CAST TO LIST OF RECORDS

        for param in query.window:
            window(frum, param)

    if query.where is not TRUE_FILTER:
        frum = filter(frum, query.where)

    if query.sort:
        frum = sort(frum, query.sort)

    if query.select:
        frum = select(frum, query.select)

    return frum


groupby = group_by.groupby


def index(data, keys=None):
#return dict that uses keys to index data
    o = Index(listwrap(keys))
    for d in data:
        o.add(d)
    return o


def unique_index(data, keys=None):
    """
    RETURN dict THAT USES KEYS TO INDEX DATA
    ONLY ONE VALUE ALLOWED PER UNIQUE KEY
    """
    o = UniqueIndex(listwrap(keys))

    for d in data:
        try:
            o.add(d)
        except Exception, e:
            Log.error("index {{index}} is not unique {{key}} maps to both {{value1}} and {{value2}}", {
                "index": keys,
                "key": select([d], keys)[0],
                "value1": o[d],
                "value2": d
            }, e)
    return o


def map2set(data, relation):
    """
    EXPECTING A dict THAT MAPS VALUES TO lists
    THE LISTS ARE EXPECTED TO POINT TO MEMBERS OF A SET
    A set() IS RETURNED
    """
    if data == None:
        return Null
    if isinstance(relation, Struct):
        Log.error("Does not accept a Struct")

    if isinstance(relation, dict):
        try:
            #relation[d] is expected to be a list
            # return set(cod for d in data for cod in relation[d])
            output = set()
            for d in data:
                for cod in relation.get(d, []):
                    output.add(cod)
            return output
        except Exception, e:
            Log.error("Expecting a dict with lists in codomain", e)
    else:
        try:
            #relation[d] is expected to be a list
            # return set(cod for d in data for cod in relation[d])
            output = set()
            for d in data:
                cod = relation(d)
                if cod == None:
                    continue
                output.add(cod)
            return output
        except Exception, e:
            Log.error("Expecting a dict with lists in codomain", e)
    return Null


def tuple(data, field_name):
    """
    RETURN LIST  OF TUPLES
    """
    if isinstance(data, Cube):
        Log.error("not supported yet")

    if isinstance(data, FlatList):
        Log.error("not supported yet")

    if isinstance(field_name, dict) and "value" in field_name:
        # SIMPLIFY {"value":value} AS STRING
        field_name = field_name["value"]

    # SIMPLE PYTHON ITERABLE ASSUMED
    if isinstance(field_name, basestring):
        if len(split_field(field_name)) == 1:
            return [(d[field_name], ) for d in data]
        else:
            path = split_field(field_name)
            output = []
            flat_list._tuple1(data, path, 0, output)
            return output
    elif isinstance(field_name, list):
        paths = [_select_a_field(f) for f in field_name]
        output = []
        _tuple((), unwrap(data), paths, 0, output)
        return output
    else:
        paths = [_select_a_field(field_name)]
        output = []
        _tuple((), data, paths, 0, output)
        return output


def _tuple(template, data, fields, depth, output):
    deep_path = None
    deep_fields = []
    for d in data:
        record = template
        for f in fields:
            index, children, record = _tuple_deep(d, f, depth, record)
            if index:
                path = f.value[0:index:]
                deep_fields.append(f)
                if deep_path and path != deep_path:
                    Log.error("Dangerous to select into more than one branch at time")
        if not children:
            output.append(record)
        else:
            _tuple(record, children, deep_fields, depth + 1, output)

    return output


def _tuple_deep(v, field, depth, record):
    """
    field = {"name":name, "value":["attribute", "path"]}
    r[field.name]=v[field.value], BUT WE MUST DEAL WITH POSSIBLE LIST IN field.value PATH
    """
    if hasattr(field.value, '__call__'):
        return 0, None, record + (field.value(v), )

    for i, f in enumerate(field.value[depth:len(field.value) - 1:]):
        v = v.get(f, None)
        if isinstance(v, list):
            return depth + i + 1, v, record

    f = field.value.last()
    return 0, None, record + (v.get(f, None), )




def select(data, field_name):
#return list with values from field_name
    if isinstance(data, Cube):
        return data._select(_normalize_selects(field_name))

    if isinstance(data, FlatList):
        return data.select(field_name)

    if isinstance(field_name, dict) and "value" in field_name:
        # SIMPLIFY {"value":value} AS STRING
        field_name = field_name["value"]

    # SIMPLE PYTHON ITERABLE ASSUMED
    if isinstance(field_name, basestring):
        if len(split_field(field_name)) == 1:
            return StructList([d[field_name] for d in data])
        else:
            keys = split_field(field_name)
            output = []
            flat_list._select1(data, keys, 0, output)
            return output
    elif isinstance(field_name, list):
        keys = [_select_a_field(f) for f in field_name]
        return _select(Struct(), unwrap(data), keys, 0)
    else:
        keys = [_select_a_field(field_name)]
        return _select(Struct(), unwrap(data), keys, 0)


def _select_a_field(field):
    if isinstance(field, basestring):
        return wrap({"name": field, "value": split_field(field)})
    elif isinstance(wrap(field).value, basestring):
        field = wrap(field)
        return wrap({"name": field.name, "value": split_field(field.value)})
    else:
        return wrap({"name": field.name, "value": field.value})


def _select(template, data, fields, depth):
    output = []
    deep_path = None
    deep_fields = []
    for d in data:
        record = template.copy()
        for f in fields:
            index, children = _select_deep(d, f, depth, record)
            if index:
                path = f.value[0:index:]
                deep_fields.append(f)
                if deep_path and path != deep_path:
                    Log.error("Dangerous to select into more than one branch at time")
        if not children:
            output.append(record)
        else:
            output.extend(_select(record, children, deep_fields, depth + 1))

    return output


def _select_deep(v, field, depth, record):
    """
    field = {"name":name, "value":["attribute", "path"]}
    r[field.name]=v[field.value], BUT WE MUST DEAL WITH POSSIBLE LIST IN field.value PATH
    """
    if hasattr(field.value, '__call__'):
        record[field.name]=field.value(v)
        return 0, None

    for i, f in enumerate(field.value[depth:len(field.value) - 1:]):
        v = v.get(f, None)
        if isinstance(v, list):
            return depth + i + 1, v

    f = field.value.last()
    record[field.name] = v.get(f, None)
    return 0, None


def get_columns(data):
    return [{"name": n} for n in UNION(set(d.keys()) for d in data)]


def sort(data, fieldnames=None):
    """
    PASS A FIELD NAME, OR LIST OF FIELD NAMES, OR LIST OF STRUCTS WITH {"field":field_name, "sort":direction}
    """
    try:
        if data == None:
            return EmptyList

        if fieldnames == None:
            return wrap(sorted(data))

        fieldnames = struct.listwrap(fieldnames)
        if len(fieldnames) == 1:
            fieldnames = fieldnames[0]
            #SPECIAL CASE, ONLY ONE FIELD TO SORT BY
            if isinstance(fieldnames, basestring):
                def comparer(left, right):
                    return cmp(nvl(left, Struct())[fieldnames], nvl(right, Struct())[fieldnames])

                return wrap(sorted(data, cmp=comparer))
            else:
                #EXPECTING {"field":f, "sort":i} FORMAT
                def comparer(left, right):
                    return fieldnames["sort"] * cmp(nvl(left, Struct())[fieldnames["field"]], nvl(right, Struct())[fieldnames["field"]])

                return wrap(sorted(data, cmp=comparer))

        formal = query._normalize_sort(fieldnames)

        def comparer(left, right):
            left = nvl(left, Struct())
            right = nvl(right, Struct())
            for f in formal:
                try:
                    result = f["sort"] * cmp(left[f["field"]], right[f["field"]])
                    if result != 0:
                        return result
                except Exception, e:
                    Log.error("problem with compare", e)
            return 0

        if isinstance(data, list):
            output = wrap(sorted(data, cmp=comparer))
        elif hasattr(data, "__iter__"):
            output = wrap(sorted(list(data), cmp=comparer))
        else:
            Log.error("Do not know how to handle")

        return output
    except Exception, e:
        Log.error("Problem sorting\n{{data}}", {"data": data}, e)




def add(*values):
    total = Null
    for v in values:
        if total == None:
            total = v
        else:
            if v != None:
                total += v
    return total


def filter(data, where):
    """
    where  - a function that accepts (record, rownum, rows) and returns boolean
    """
    if isinstance(data, Cube):
        Log.error("Do not know how to handle")

    return drill_filter(where, data)


def drill_filter(esfilter, data):
    """
    PARTIAL EVALUATE THE FILTER BASED ON DATA GIVEN
    """
    esfilter = struct.unwrap(esfilter)
    primary_nested = []  #track if nested, changes if not
    primary_column = []  #only one path allowed
    primary_branch = []  #constantly changing as we dfs the tree

    def parse_field(fieldname, data, depth):
        """
        RETURN (first, rest) OF fieldname
        """
        col = split_field(fieldname)
        d = data[col[0]]
        if isinstance(d, list) and len(col) > 1:
            if len(primary_column) <= depth:
                primary_nested.append(True)
                primary_column.append(col[0])
                primary_branch.append(d)
            elif primary_nested[depth] and primary_column[depth] != col[0]:
                Log.error("only one branch of tree allowed")
            else:
                primary_nested[depth] = True
                primary_column[depth] = col[0]
                primary_branch[depth] = d
        else:
            if len(primary_column) <= depth:
                primary_nested.append(False)
                primary_column.append(col[0])
                primary_branch.append(d)

        if len(col) == 1:
            return col[0], None
        else:
            return col[0], ".".join(col[1:])

    def pe_filter(filter, data, depth):
        """
        PARTIAL EVALUATE THE filter BASED ON data GIVEN
        """
        if filter is TRUE_FILTER:
            return True
        if filter is FALSE_FILTER:
            return False

        if "and" in filter:
            result = True
            output = []
            for a in filter[u"and"]:
                f = pe_filter(a, data, depth)
                if f is False:
                    result = False
                elif f is not True:
                    output.append(f)
            if result and output:
                return {"and": output}
            else:
                return result
        elif "or" in filter:
            output = []
            for o in filter[u"or"]:
                f = pe_filter(o, data, depth)
                if f is True:
                    return True
                elif f is not False:
                    output.append(f)
            if output:
                return {"or": output}
            else:
                return False
        elif "not" in filter:
            f = pe_filter(filter[u"not"], data, depth)
            if f is True:
                return False
            elif f is False:
                return True
            else:
                return {"not": f}
        elif "term" in filter:
            result = True
            output = {}
            for col, val in filter["term"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    if d != val:
                        result = False
                else:
                    output[rest] = val
            if result and output:
                return {"term": output}
            else:
                return result
        elif "terms" in filter:
            result = True
            output = {}
            for col, vals in filter["terms"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    if d not in vals:
                        result = False
                else:
                    output[rest] = vals
            if result and output:
                return {"terms": output}
            else:
                return result

        elif "range" in filter:
            result = True
            output = {}
            for col, ranges in filter["range"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    for sign, val in ranges.items():
                        if sign in ("gt", ">") and d <= val:
                            result = False
                        if sign == "gte" and d < val:
                            result = False
                        if sign == "lte" and d > val:
                            result = False
                        if sign == "lt" and d >= val:
                            result = False
                else:
                    output[rest] = ranges
            if result and output:
                return {"range": output}
            else:
                return result
        elif "missing" in filter:
            if isinstance(filter.missing, basestring):
                field = filter["missing"]
            else:
                field = filter["missing"]["field"]

            first, rest = parse_field(field, data, depth)
            d = data[first]
            if not rest:
                if d == None:
                    return True
                return False
            else:
                return {"missing": rest}

        elif "exists" in filter:
            if isinstance(filter["exists"], basestring):
                field = filter["exists"]
            else:
                field = filter["exists"]["field"]

            first, rest = parse_field(field, data, depth)
            d = data[first]
            if not rest:
                if d != None:
                    return True
                return False
            else:
                return {"exists": rest}
        else:
            Log.error(u"Can not interpret esfilter: {{esfilter}}", {u"esfilter": filter})

    output = []  #A LIST OF OBJECTS MAKING THROUGH THE FILTER

    def main(sequence, esfilter, row, depth):
        """
        RETURN A SEQUENCE OF REFERENCES OF OBJECTS DOWN THE TREE
        SHORT SEQUENCES MEANS ALL NESTED OBJECTS ARE INCLUDED
        """
        new_filter = pe_filter(esfilter, row, depth)
        if new_filter is True:
            seq = list(sequence)
            seq.append(row)
            output.append(seq)
            return
        elif new_filter is False:
            return

        seq = list(sequence)
        seq.append(row)
        for d in primary_branch[depth]:
            main(seq, new_filter, d, depth + 1)

    # OUTPUT
    for d in data:
        main([], esfilter, d, 0)

    # AT THIS POINT THE primary_column[] IS DETERMINED
    # USE IT TO EXPAND output TO ALL NESTED OBJECTS
    max = 0
    for i, n in enumerate(primary_nested):
        if n:
            max = i + 1

    uniform_output = []

    def recurse(row, depth):
        if depth == max:
            uniform_output.append(row)
        else:
            nested = row[-1][primary_column[depth]]
            if not nested:
                #PASSED FILTER, BUT NO CHILDREN, SO ADD NULL CHILDREN
                for i in range(depth, max):
                    row.append(None)
                uniform_output.append(row)
            else:
                for d in nested:
                    r = list(row)
                    r.append(d)
                    recurse(r, depth + 1)

    for o in output:
        recurse(o, len(o) - 1)

    if not max:
        #SIMPLE LIST AS RESULT
        return wrap([u[0] for u in uniform_output])

    return FlatList(primary_column[0:max], uniform_output)


def wrap_function(func):
    """
    RETURN A THREE-PARAMETER WINDOW FUNCTION TO MATCH
    """
    numarg = func.__code__.co_argcount
    if numarg == 0:
        def temp(row, rownum, rows):
            return func()

        return temp
    elif numarg == 1:
        def temp(row, rownum, rows):
            return func(row)

        return temp
    elif numarg == 2:
        def temp(row, rownum, rows):
            return func(row, rownum)

        return temp
    elif numarg == 3:
        return func


def window(data, param):
    """
    MAYBE WE CAN DO THIS WITH NUMPY (no, the edges of windows are not graceful with numpy)
    data - list of records
    """
    name = param.name            # column to assign window function result
    edges = param.edges          # columns to gourp by
    sortColumns = param.sort            # columns to sort by
    calc_value = wrap_function(param.value) # function that takes a record and returns a value (for aggregation)
    aggregate = param.aggregate  # WindowFunction to apply
    _range = param.range          # of form {"min":-10, "max":0} to specify the size and relative position of window

    if not aggregate and not edges:
        #SIMPLE CALCULATED VALUE
        for rownum, r in enumerate(data):
            r[name] = calc_value(r, rownum, data)
        return

    for rownum, r in enumerate(data):
        r["__temp__"] = calc_value(r, rownum, data)

    for keys, values in groupby(data, edges):
        if not values:
            continue     # CAN DO NOTHING WITH THIS ZERO-SAMPLE

        sequence = sort(values, sortColumns)
        head = nvl(_range.max, _range.stop)
        tail = nvl(_range.min, _range.start)

        #PRELOAD total
        total = aggregate()
        for i in range(tail, head):
            total.add(sequence[i].__temp__)

        #WINDOW FUNCTION APPLICATION
        for i, r in enumerate(sequence):
            r[name] = total.end()
            total.add(sequence[i + head].__temp__)
            total.sub(sequence[i + tail].__temp__)

    for r in data:
        r["__temp__"] = None  #CLEANUP







def intervals(_min, _max=None, size=1):
    """
    RETURN (min, max) PAIRS OF GIVEN SIZE, WHICH COVER THE _min, _max RANGE
    THE LAST PAIR MAY BE SMALLER
    """
    if _max == None:
        _max = _min
        _min = 0
    _max = int(Math.ceiling(_max))

    output = ((x, min(x + size, _max)) for x in __builtin__.range(_min, _max, size))
    return output



