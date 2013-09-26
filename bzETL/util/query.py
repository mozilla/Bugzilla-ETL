################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

import sys
from .logs import Log
from .basic import nvl
import struct
from .strings import indent, expand_template
from .struct import StructList, Struct, Null
from .multiset import multiset


# A COLLECTION OF DATABASE OPERATORS (RELATIONAL ALGEBRA OPERATORS)
class Q:
    def __init__(self, query):
        pass


    @staticmethod
    def run(query):
        query = struct.wrap(query)
        if isinstance(query["from"], list):
            _from = query["from"]
        else:
            _from = Q.run(query["from"])

        if query.edges != Null:
            Log.error("not implemented yet")

        if query.filter != Null:
            Log.error("not implemented yet")

        if query.window != Null:
            w = query.window
            if not isinstance(w, list):
                w = [w]

            for param in w:
                Q.window(_from, param)

        if query.where != Null:
            w = query.where
            _from = Q.filter(_from, w)

        if query.select != Null:
            _from = Q.select(_from, query.select)

        return _from


    @staticmethod
    def groupby(data, keys=Null, size=Null, min_size=Null, max_size=Null):
    #return list of (keys, values) pairs where
    #group by the set of set of keys
    #values IS LIST OF ALL data that has those keys
        if size != Null or min_size != Null or max_size != Null:
            if size != Null: max_size = size
            return groupby_min_max_size(data, min_size=min_size, max_size=max_size)

        try:
            def keys2string(x):
                #REACH INTO dict TO GET PROPERTY VALUE
                return "|".join([unicode(x[k]) for k in keys])

            def get_keys(d):
                return struct.wrap({k: d[k] for k in keys})

            agg = {}
            for d in data:
                key = keys2string(d)
                if key in agg:
                    pair = agg[key]
                else:
                    pair = (get_keys(d), StructList())
                    agg[key] = pair
                pair[1].append(d)

            return agg.values()
        except Exception, e:
            Log.error("Problem grouping", e)


    @staticmethod
    def index(data, keys=Null):
    #return dict that uses keys to index data
        if not isinstance(keys, list): keys = [keys]

        output = dict()
        for d in data:
            o = output
            for k in keys[:-1]:
                v = d[k]
                if v not in o: o[v] = dict()
                o = o[v]
            v = d[keys[-1]]
            if v not in o: o[v] = list()
            o = o[v]
            o.append(d)
        return output


    @staticmethod
    def unique_index(data, keys=Null):
        """
        RETURN dict THAT USES KEYS TO INDEX DATA
        ONLY ONE VALUE ALLOWED PER UNIQUE KEY
        """
        if not isinstance(keys, list): keys = [keys]
        o = Index(keys)

        for d in data:
            try:
                o.add(d)
            except Exception, e:
                Log.error("index {{index}} is not unique {{key}} maps to both {{value1}} and {{value2}}", {
                    "index": keys,
                    "key": Q.select([d], keys)[0],
                    "value1": o[d],
                    "value2": d
                }, e)
        return o


    @staticmethod
    def select(data, field_name):
    #return list with values from field_name
        if isinstance(data, Cube): Log.error("Do not know how to deal with cubes yet")
        if isinstance(field_name, basestring):
            return [d[field_name] for d in data]

        return [dict([(k, v) for k, v in x.items() if k in field_name]) for x in data]


    @staticmethod
    def get_columns(data):
        output = {}
        for d in data:
            for k, v in d.items():
                c = output[k]
                if c == Null:
                    c = {"name": k, "domain": Null}
                    output[k] = c

                    # IT WOULD BE NICE TO ADD DOMAIN ANALYSIS HERE

        return [{"name": n} for n in output]


    @staticmethod
    def stack(data, name=Null, value_column=Null, columns=Null):
        """
        STACK ALL CUBE DATA TO A SINGLE COLUMN, WITH ONE COLUMN PER DIMENSION
        >>> s
              a   b
         one  1   2
         two  3   4

        >>> Q.stack(s)
         one a    1
         one b    2
         two a    3
         two b    4

        STACK LIST OF HASHES, OR 'MERGE' SEPARATE CUBES
        data - expected to be a list of dicts
        name - give a name to the new column
        value_column - Name given to the new, single value column
        columns - explicitly list the value columns (USE SELECT INSTEAD)
        """

        assert value_column != Null
        if isinstance(data, Cube): Log.error("Do not know how to deal with cubes yet")

        if columns == Null:
            columns = data.get_columns()
        data = data.select(columns)

        name = nvl(name, data.name)

        output = []

        parts = set()
        for r in data:
            for c in columns:
                v = r[c]
                parts.add(c)
                output.append({"name": c, "value": v})

        edge = struct.wrap({"domain": {"type": "set", "partitions": parts}})


    #UNSTACKING CUBES WILL BE SIMPLER BECAUSE THE keys ARE IMPLIED (edges-column)
    @staticmethod
    def unstack(data, keys=Null, column=Null, value=Null):
        assert keys != Null
        assert column != Null
        assert value != Null
        if isinstance(data, Cube): Log.error("Do not know how to deal with cubes yet")

        output = []
        for key, values in Q.groupby(data, keys):
            for v in values:
                key[v[column]] = v[value]
            output.append(key)

        return StructList(output)


    @staticmethod
    def normalize_sort(fieldnames):
        """
        CONVERT SORT PARAMETERS TO A NORMAL FORM SO EASIER TO USE
        """
        if fieldnames == Null:
            return []

        if not isinstance(fieldnames, list):
            fieldnames = [fieldnames]

        formal = []
        for f in fieldnames:
            if isinstance(f, basestring):
                f = {"field": f, "sort": 1}
            formal.append(f)

        return formal


    @staticmethod
    def sort(data, fieldnames=Null):
        """
        PASS A FIELD NAME, OR LIST OF FIELD NAMES, OR LIST OF STRUCTS WITH {"field":field_name, "sort":direction}
        """
        try:
            if data == Null:
                return Null

            if fieldnames == Null:
                return sorted(data)

            if not isinstance(fieldnames, list):
                #SPECIAL CASE, ONLY ONE FIELD TO SORT BY
                if isinstance(fieldnames, basestring):
                    def comparer(left, right):
                        return cmp(nvl(left, Struct())[fieldnames], nvl(right, Struct())[fieldnames])

                    return sorted(data, cmp=comparer)
                else:
                    #EXPECTING {"field":f, "sort":i} FORMAT
                    def comparer(left, right):
                        return fieldnames["sort"] * cmp(nvl(left, Struct())[fieldnames["field"]],
                                                        nvl(right, Struct())[fieldnames["field"]])

                    return sorted(data, cmp=comparer)

            formal = Q.normalize_sort(fieldnames)

            def comparer(left, right):
                left = nvl(left, Struct())
                right = nvl(right, Struct())
                for f in formal:
                    result = f["sort"] * cmp(left[f["field"]], right[f["field"]])
                    if result != 0: return result
                return 0

            output = sorted(data, cmp=comparer)
            return output
        except Exception, e:
            Log.error("Problem sorting\n{{data}}", {"data": data}, e)


    @staticmethod
    def add(*values):
        total = Null
        for v in values:
            if total == Null:
                total = v
            else:
                if v != Null and v != Null:
                    total += v
        return total

    @staticmethod
    def filter(data, where):
        """
        where  - a function that accepts (record, rownum, rows) and return s boolean
        """
        where = Q.wrap_function(where)
        return [d for i, d in enumerate(data) if where(d, i, data)]

    @staticmethod
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


    @staticmethod
    def window(data, param):
        """
        MAYBE WE CAN DO THIS WITH NUMPY??
        data - list of records
        """
        name = param.name            # column to assign window function result
        edges = param.edges          # columns to gourp by
        sort = param.sort            # columns to sort by
        value = Q.wrap_function(param.value) # function that takes a record and returns a value (for aggregation)
        aggregate = param.aggregate  # WindowFunction to apply
        _range = param.range          # of form {"min":-10, "max":0} to specify the size and relative position of window

        if aggregate == Null and sort == Null and edges == Null:
            #SIMPLE CALCULATED VALUE
            for rownum, r in enumerate(data):
                r[name] = value(r, rownum, data)

            return

        for rownum, r in enumerate(data):
            r["__temp__"] = value(r, rownum, data)

        for keys, values in Q.groupby(data, edges):
            if len(values) == 0:
                continue     # CAN DO NOTHING WITH THIS ONE SAMPLE

            sequence = struct.wrap(Q.sort(values, sort))
            head = nvl(_range.max, _range.stop)
            tail = nvl(_range.min, _range.start)

            #PRELOAD total
            total = aggregate()
            for i in range(head):
                total += sequence[i].__temp__

            #WINDOW FUNCTION APPLICATION
            for i, r in enumerate(sequence):
                r[name] = total.end()
                total.add(sequence[i + head].__temp__)
                total.sub(sequence[i + tail].__temp__)

        for r in data:
            r["__temp__"] = Null  #CLEANUP


def groupby_size(data, size):
    if hasattr(data, "next"):
        iterator = data
    elif hasattr(data, "__iter__"):
        iterator = data.__iter__()
    else:
        Log.error("do not know how to handle this type")

    done = []

    def more():
        output = []
        for i in range(size):
            try:
                output.append(iterator.next())
            except StopIteration:
                done.append(True)
                break
        return output

    #THIS IS LAZY
    i = 0
    while True:
        output = more()
        yield (i, output)
        if len(done) > 0: break
        i += 1


def groupby_multiset(data, min_size, max_size):
    # GROUP multiset BASED ON POPULATION OF EACH KEY, TRYING TO STAY IN min/max LIMITS
    if min_size == Null: min_size = 0

    total = 0
    i = 0
    g = list()
    for k, c in data.items():
        if total < min_size or total + c < max_size:
            total += c
            g.append(k)
        elif total < max_size:
            yield (i, g)
            i += 1
            total = c
            g = [k]

        if total >= max_size:
            Log.error("({{min}}, {{max}}) range is too strict given step of {{increment}}", {
                "min": min_size, "max": max_size, "increment": c
            })

    if len(g) > 0:
        yield (i, g)


def groupby_min_max_size(data, min_size=0, max_size=Null, ):
    if max_size == Null: max_size = sys.maxint

    if isinstance(data, list):
        return [(i, data[i:i + max_size]) for i in range(0, len(data), max_size)]
    elif not isinstance(data, multiset):
        return groupby_size(data, max_size)
    else:
        return groupby_multiset(data, min_size, max_size)


class Cube():
    def __init__(self, data=Null, edges=Null, name=Null):
        if isinstance(data, Cube): Log.error("do not know how to handle cubes yet")

        columns = Q.get_columns(data)

        if edges == Null:
            self.edges = [{"name": "index", "domain": {"type": "numeric", "min": 0, "max": len(data), "interval": 1}}]
            self.data = data
            self.select = columns
            return

        self.name = name
        self.edges = edges
        self.select = Null


    def get_columns(self):
        return self.columns


class Domain():
    def __init__(self):
        pass


    def part2key(self, part):
        pass


    def part2label(self, part):
        pass


    def part2value(self, part):
        pass


# SIMPLE TUPLE-OF-STRINGS LOOKUP TO LIST
class Index(object):
    def __init__(self, keys):
        self._data = {}
        self._keys = keys
        self.count = 0

        #THIS ONLY DEPENDS ON THE len(keys), SO WE COULD SHARED lookup
        #BETWEEN ALL n-key INDEXES.  FOR NOW, JUST MAKE lookup()
        code = "def lookup(d0):\n"
        for i, k in enumerate(self._keys):
            code = code + indent(expand_template(
                "for k{{next}}, d{{next}} in d{{curr}}.items():\n", {
                    "next": i + 1,
                    "curr": i
                }), prefix="    ", indent=i + 1)
        i = len(self._keys)
        code = code + indent(expand_template(
            "yield d{{curr}}", {"curr": i}), prefix="    ", indent=i + 1)
        exec code
        self.lookup = lookup


    def __getitem__(self, key):
        try:
            if not isinstance(key, dict):
                #WE WILL BE FORGIVING IF THE KEY IS NOT IN A LIST
                if len(self._keys) > 1:
                    Log.error("Must be given an array of keys")
                key = {self._keys[0]: key}

            d = self._data
            for k in self._keys:
                v = key[k]
                if v == Null:
                    Log.error("can not handle when {{key}} == Null", {"key": k})
                if v not in d:
                    return Null
                d = d[v]

            if len(key) != len(self._keys):
                #NOT A COMPLETE INDEXING, SO RETURN THE PARTIAL INDEX
                output = Index(self._keys[-len(key):])
                output._data = d
                return output
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        if not isinstance(val, dict): val = {(self._keys[0], val)}
        d = self._data
        for k in self._keys[0:-1]:
            v = val[k]
            if v == Null:
                Log.error("can not handle when {{key}} == Null", {"key": k})
            if v not in d:
                e = {}
                d[v] = e
            d = d[v]
        v = val[self._keys[-1]]
        if v in d:
            Log.error("key already filled")
        d[v] = val
        self.count += 1


    def __contains__(self, key):
        return self[key] != Null

    def __iter__(self):
        return self.lookup(self._data)

    def __sub__(self, other):
        output = Index(self._keys)
        for v in self:
            if v not in other: output.add(v)
        return output

    def __and__(self, other):
        output = Index(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = Index(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)




