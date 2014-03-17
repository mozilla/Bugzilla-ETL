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
from .. import struct
from ..collections.matrix import Matrix
from ..collections import MAX, OR
from ..queries.query import _normalize_edge
from ..struct import StructList, wrap
from ..env.logs import Log


class Cube(object):
    """
    A CUBE IS LIKE A NUMPY ARRAY, ONLY WITH THE DIMENSIONS TYPED AND NAMED.
    CUBES ARE BETTER THAN PANDAS BECAUSE THEY DEAL WITH NULLS GRACEFULLY
    """

    def __init__(self, select, edges, data, frum=None):
        """
        data IS EXPECTED TO BE A dict TO MATRICES, BUT OTHER COLLECTIONS ARE
        ALLOWED, USING THE select AND edges TO DESCRIBE THE data
        """

        self.is_value = False if isinstance(select, list) else True
        self.select = select

        #ENSURE frum IS PROPER FORM
        if isinstance(select, list):
            if OR(not isinstance(v, Matrix) for v in data.values()):
                Log.error("Expecting data to be a dict with Matrix values")

        if not edges:
            if isinstance(data, dict):
                # EXPECTING NO MORE THAN ONE rownum EDGE IN THE DATA
                length = MAX([len(v) for v in data.values()])
                if length >= 1:
                    self.edges = [{"name": "rownum", "domain": {"type": "index"}}]
                else:
                    self.edges = []
            elif isinstance(data, list):
                if isinstance(select, list):
                    Log.error("not expecting a list of records")

                data = {select.name: Matrix.wrap(data)}
                self.edges = [{"name": "rownum", "domain": {"type": "index"}}]
            elif isinstance(data, Matrix):
                if isinstance(select, list):
                    Log.error("not expecting a list of records")

                data = {select.name: data}
            else:
                if isinstance(select, list):
                    Log.error("not expecting a list of records")

                data = {select.name: Matrix(value=data)}
                self.edges = []
        else:
            self.edges = edges

        self.data = data

    def __len__(self):
        """
        RETURN DATA VOLUME
        """
        if not self.edges:
            return 1

        return len(self.data.values()[0])

    def __iter__(self):
        if self.is_value:
            return self.data[self.select.name].__iter__()

        if not self.edges:
            return list.__iter__([])

        if len(self.edges) == 1 and wrap(self.edges[0]).domain.type == "index":
            # ITERATE AS LIST OF RECORDS
            keys = list(self.data.keys())
            output = (struct.zip(keys, r) for r in zip(*self.data.values()))
            return output

        Log.error("This is a multicube")

    @property
    def value(self):
        if self.edges:
            Log.error("can not get value of with dimension")
        if isinstance(self.select, list):
            Log.error("can not get value of multi-valued cubes")
        return self.data[self.select.name].cube

    def __float__(self):
        return self.value

    def __lt__(self, other):
        return self.value < other

    def __gt__(self, other):
        return self.value > other

    def __eq__(self, other):
        if other == None:
            if self.value == None:
                return True
            return False
        return self.value == other

    def __ne__(self, other):
        return not Cube.__eq__(self, other)

    def __add__(self, other):
        return self.value + other

    def __radd__(self, other):
        return other + self.value

    def __sub__(self, other):
        return self.value - other

    def __rsub__(self, other):
        return other - self.value

    def __mul__(self, other):
        return self.value * other

    def __rmul__(self, other):
        return other * self.value

    def __div__(self, other):
        return self.value / other

    def __rdiv__(self, other):
        return other / self.value

    def __getitem__(self, item):
        return self.data[item]

    def __getattr__(self, item):
        return self.data[item]

    def get_columns(self):
        return self.edges + struct.listwrap(self.select)

    def _select(self, select):
        selects = struct.listwrap(select)
        is_aggregate = OR(s.aggregate != None and s.aggregate != "none" for s in selects)
        if is_aggregate:
            values = {s.name: Matrix(value=self.data[s.value].aggregate(s.aggregate)) for s in selects}
            return Cube(select, [], values)
        else:
            values = {s.name: self.data[s.value] for s in selects}
            return Cube(select, self.edges, values)

    def groupby(self, edges):
        """
        SLICE THIS CUBE IN TO ONES WITH LESS DIMENSIONALITY
        simple==True WILL HAVE GROUPS BASED ON PARTITION VALUE, NOT PARTITION OBJECTS
        """
        edges = StructList([_normalize_edge(e) for e in edges])

        stacked = [e for e in self.edges if e.name in edges.name]
        remainder = [e for e in self.edges if e.name not in edges.name]
        selector = [1 if e.name in edges.name else 0 for e in self.edges]

        if len(stacked) + len(remainder) != len(self.edges):
            Log.error("can not find some edges to group by")

        selects = struct.listwrap(self.select)
        index, v = zip(*self.data[selects[0].name].groupby(selector))

        coord = wrap([{e.name: e.domain.getKey(e.domain.partitions[c[i]]) for i, e in enumerate(self.edges) if c[i] != -1} for c in index])

        if isinstance(self.select, list):
            values = [v]
            for s in selects[1::]:
                i, v = zip(*self.data[s.name].group_by(selector))
                values.append(v)

            output = zip(coord, [Cube(self.select, remainder, {s.name: v[i] for i, s in enumerate(selects)}) for v in zip(*values)])
        else:
            output = zip(coord, [Cube(self.select, remainder, vv) for vv in v])

        return output

    def __str__(self):
        if self.is_value:
            return str(self.data)
        else:
            return str(self.data)

