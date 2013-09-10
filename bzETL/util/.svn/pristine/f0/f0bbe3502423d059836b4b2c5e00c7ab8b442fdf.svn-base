################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

import sys
from .debug import D
from .basic import nvl
import struct
from .strings import indent, expand_template
from .struct import StructList, Struct
from .multiset import multiset

# A COLLECTION OF DATABASE OPERATORS (RELATIONAL ALGEBRA OPERATORS)
class Q:

    def __init__(self, query):
        pass
        

    
    @staticmethod
    def groupby(data, keys=None, size=None, min_size=None, max_size=None):
    #return list of (keys, values) pairs where
    #group by the set of set of keys
    #values IS LIST OF ALL data that has those keys
        if size is not None or min_size is not None or max_size is not None:
            if size is not None: max_size=size
            return groupby_min_max_size(data, min_size=min_size, max_size=max_size)
        
        try:
            def keys2string(x):
                #REACH INTO dict TO GET PROPERTY VALUE
                return "|".join([unicode(x[k]) for k in keys])
            def get_keys(d): return struct.wrap({k:d[k] for k in keys})

            agg={}
            for d in data:
                key=keys2string(d)
                if key in agg:
                    pair=agg[key]
                else:
                    pair=(get_keys(d), StructList())
                    agg[key]=pair
                pair[1].append(d)

            return agg.values()
        except Exception, e:
            D.error("Problem grouping", e)


    @staticmethod
    def index(data, keys=None):
    #return dict that uses keys to index data
        if not isinstance(keys, list): keys=[keys]

        output=dict()
        for d in data:
            o=output
            for k in keys[:-1]:
                v=d[k]
                if v not in o: o[v]=dict()
                o=o[v]
            v=d[keys[-1]]
            if v not in o: o[v]=list()
            o=o[v]
            o.append(d)
        return output


    @staticmethod
    def unique_index(data, keys=None):
        """
        RETURN dict THAT USES KEYS TO INDEX DATA
        ONLY ONE VALUE ALLOWED PER UNIQUE KEY
        """
        if not isinstance(keys, list): keys=[keys]
        o=Index(keys)

        for d in data:
            try:
                o.add(d)
            except Exception, e:
                D.error("index {{index}} is not unique {{key}} maps to both {{value1}} and {{value2}}", {
                    "index":keys,
                    "key":Q.select([d], keys)[0],
                    "value1":o[d],
                    "value2":d
                }, e)
        return o



    @staticmethod
    def select(data, field_name):
    #return list with values from field_name
        if isinstance(data, Cube): D.error("Do not know how to deal with cubes yet")
        if isinstance(field_name, basestring):
            return [d[field_name] for d in data]

        return [dict([(k, v) for k, v in x.items() if k in field_name]) for x in data]


    @staticmethod
    def get_columns(data):
        output={}
        for d in data:
            for k, v in d.items():
                c=output[k]
                if c is None:
                    c={"name":k, "domain":None}
                    output[k]=c

                # IT WOULD BE NICE TO ADD DOMAIN ANALYSIS HERE

            

        return [{"name":n} for n in output]


    @staticmethod
    def stack(data, name=None, value_column=None, columns=None):
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
        data - expected to be a list of hashes
        name - give a name to the new column
        value_column - Name given to the new, single value column
        columns - explicitly list the value columns (USE SELECT INSTEAD)
        """

        assert value_column is not None
        if isinstance(data, Cube): D.error("Do not know how to deal with cubes yet")

        if columns is None:
            columns=data.get_columns()
        data=data.select(columns)

        name=nvl(name, data.name)


        output=[]

        parts=set()
        for r in data:
            for c in columns:
                v=r[c]
                parts.add(c)
                output.append({"name":c, "value":v})



        edge=struct.wrap({"domain":{"type":"set", "partitions":parts}})


    #UNSTACKING CUBES WILL BE SIMPLER BECAUSE THE keys ARE IMPLIED (edges-column)
    @staticmethod
    def unstack(data, keys=None, column=None, value=None):
        assert keys is not None
        assert column is not None
        assert value is not None
        if isinstance(data, Cube): D.error("Do not know how to deal with cubes yet")

        output=[]
        for key, values in Q.groupby(data, keys):
            for v in values:
                key[v[column]]=v[value]
            output.append(key)
            
        return StructList(output)

    
    # PASS A FIELD NAME, OR LIST OF FIELD NAMES, OR LIST OF STRUCTS WITH {"field":field_name, "sort":direction}
    @staticmethod
    def sort(data, fieldnames=None):
        try:
            if data is None: return None

            if fieldnames is None:
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
                        return fieldnames["sort"]*cmp(nvl(left, Struct())[fieldnames["field"]],  nvl(right, Struct())[fieldnames["field"]])
                    return sorted(data, cmp=comparer)

            formal=[]
            for f in fieldnames:
                if isinstance(f, basestring):
                    f={"field":f, "sort":1}
                formal.append(f)

            #DEFINITLY NOT FAST IN PYPY
    #        code="def comparer(left, right):\n"
    #        first=True
    #        for col in formal:
    #            if not first: code+="    if result!=0: return result\n"
    #            first=False
    #            code+="    result="+unicode(col["sort"])+" * cmp(left["+CNV.string2quote(col["field"])+"], right["+CNV.string2quote(col["field"])+"])\n"
    #        code+="    return result"
    #        exec(code)

            def comparer(left, right):
                left=nvl(left, Struct())
                right=nvl(right, Struct())
                for f in formal:
                    result=f["sort"]*cmp(left[f["field"]], right[f["field"]])
                    if result!=0: return result
                return 0

            return sorted(data, cmp=comparer)
        except Exception, e:
            D.error("Problem sorting", e)


def groupby_size(data, size):
    iterator=data.__iter__()
    done=[]
    def more():
        output=[]
        for i in range(size):
            try:
                output.append(iterator.next())
            except StopIteration:
                done.append(True)
                break
        return output

    #THIS IS LAZY
    i=0
    while True:
        output=more()
        yield (i, output)
        if len(done)>0: break
        i+=1


def groupby_multiset(data, min_size, max_size):
    # GROUP multiset BASED ON POPULATION OF EACH KEY, TRYING TO STAY IN min/max LIMITS
    if min_size is None: min_size=0

    total = 0
    i = 0
    g = list()
    for k, c in data.items():
        if total<min_size or total + c < max_size:
            total += c
            g.append(k)
        elif total<max_size:
            yield (i, g)
            i += 1
            total = c
            g = [k]

        if total >= max_size:
            D.error("({{min}}, {{max}}) range is too strict given step of {{increment}}", {
                "min": min_size, "max": max_size, "increment":c
            })

    if len(g) > 0:
        yield(i, g)


def groupby_min_max_size(data, min_size=0, max_size=None,):
    if max_size is None: max_size=sys.maxint

    if isinstance(data, list):
        return [(i, data[i:i+max_size]) for i in range(0, len(data), max_size)]
    elif not isinstance(data, multiset):
        return groupby_size(data, max_size)
    else:
        return groupby_multiset(data, min_size, max_size)



class Cube():
    def __init__(self, data=None, edges=None, name=None):
        if isinstance(data, Cube): D.error("do not know how to handle cubes yet")

        columns=Q.get_columns(data)

        if edges==None:
            self.edges=[{"name":"index", "domain":{"type":"numeric", "min":0, "max":len(data), "interval":1}}]
            self.data=data
            self.select=columns
            return





        self.name=name
        self.edges=edges
        self.select=None




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
        self._keys=keys

        #THIS ONLY DEPENDS ON THE len(keys), SO WE COULD SHARED lookup
        #BETWEEN ALL n-key INDEXES.  FOR NOW, JUST MAKE lookup()
        code="def lookup(d0):\n"
        for i, k in enumerate(self._keys):
            code=code+indent(expand_template(
                "for k{{next}}, d{{next}} in d{{curr}}.items():\n",{
                    "next":i+1,
                    "curr":1
                }), prefix="    ", indent=i+1)
        i=len(self._keys)
        code=code+indent(expand_template(
            "yield d{{curr}}", {"curr":i}), prefix="    ", indent=i+1)
        exec(code)
        self.lookup=lookup


    def __getitem__(self, key):
        try:
            if not isinstance(key, dict):
                #WE WILL BE FORGIVING IF THE KEY IS NOT IN A LIST
                if len(self._keys)>1:
                    D.error("Must be given an array of keys")
                key={self._keys[0]: key}

            d=self._data
            for k in self._keys:
                v=key[k]
                if v is None:
                    D.error("can not handle when {{key}} is None", {"key":k})
                if v not in d:
                    return None
                d=d[v]

            if len(key)!=len(self._keys):
                #NOT A COMPLETE INDEXING, SO RETURN THE PARTIAL INDEX
                output=Index(self._keys[-len(key):])
                output._data=d
                return output

                return struct.wrap(d)
        except Exception, e:
            D.error("something went wrong", e)
    
    def __setitem__(self, key, value):
        D.error("Not implemented")




    def add(self, val):
        if not isinstance(val, dict): val={(self._keys[0], val)}
        d=self._data
        for k in self._keys[0:-1]:
            v=val[k]
            if v is None:
                D.error("can not handle when {{key}} is None", {"key":k})
            if v not in d:
                e={}
                d[v]=e
            d=d[v]
        v=val[self._keys[-1]]
        if v in d:
            D.error("key already filled")
        d[v]=val



    def __contains__(self, key):
        return self[key] is not None

    def __iter__(self):
        return self.lookup(self._data)

    def __sub__(self, other):
        output=Index(self._keys)
        for v in self:
            if v not in other: output.add(v)
        return output

    def __and__(self, other):
        output=Index(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output=Index(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)


