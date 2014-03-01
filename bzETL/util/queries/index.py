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
from ..env.logs import Log
from ..strings import indent, expand_template
from ..struct import Null, wrap


class UniqueIndex(object):
    """
    DEFINE A SET OF ATTRIBUTES THAT UNIQUELY IDENTIFIES EACH OBJECT IN A list.
    THIS ALLOWS set-LIKE COMPARISIONS (UNION, INTERSECTION, DIFFERENCE, ETC) WHILE
    STILL MAINTAINING list-LIKE FEATURES
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = struct.unwrap(keys)
        self.count = 0
        self.lookup = lookup_method(len(keys), True)


    def __getitem__(self, key):
        try:
            if isinstance(key, dict):
                key = wrap(key)
                key = [key[k] for k in self._keys]
            elif not isinstance(key, (list, tuple)):
                key = [key]

            d = self._data
            for k in key:
                if k is None:
                    for i, k in enumerate(key):
                        if k == None:
                            Log.error("can not handle when {{key}} == None", {"key": self._keys[i]})
                d = d.get(k, Null)

            if len(key) < len(self._keys):
                # RETURN ANOTHER Index
                output = UniqueIndex(self._keys[len(key):])
                output._data = d
                return output
            else:
                return d
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        if not isinstance(val, dict):
            val = {self._keys[0]: val}
        val = wrap(val)
        d = self._data
        for k in self._keys[0:-1]:
            v = val[k]
            if v == None:
                Log.error("can not handle when {{key}} == None", {"key": k})
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
        return self[key] != None

    def __iter__(self):
        return self.lookup(self._data)

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)


def lookup_method(depth, is_unique):
    code = "def lookup(d0):\n"
    for i in range(depth):
        code = code + indent(expand_template(
            "for k{{next}}, d{{next}} in d{{curr}}.items():\n", {
                "next": i + 1,
                "curr": i
            }), prefix="    ", indent=i + 1)
    if not is_unique:
        code = code + indent(expand_template(
            "for d{{next}} in d{{curr}}:\n", {
                "next": depth + 1,
                "curr": depth
            }), prefix="    ", indent=depth + 1)
        depth += 1

    code = code + indent(expand_template("yield wrap(d{{curr}})", {"curr": depth}), prefix="    ", indent=depth + 1)
    lookup = None
    exec code
    return lookup


class Index(object):
    """
    USING DATABASE TERMINOLOGY, THIS IS A NON-UNIQUE INDEX
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = struct.unwrap(keys)
        self.count = 0
        self.lookup = lookup_method(len(keys), False)


    def __getitem__(self, key):
        try:
            if isinstance(key, dict):
                key = struct.unwrap(key)
                key = [key.get(k, None) for k in self._keys]
            elif isinstance(key, tuple):
                pass
            elif not isinstance(key, list):
                key = [key]

            d = self._data
            for k in key:
                if k is None:
                    for i, k in enumerate(key):
                        if k == None:
                            Log.error("can not handle when {{key}} == None", {"key": self._keys[i]})
                d = d.get(k, Null)
                if d == None:
                    return Null

            if len(key) < len(self._keys):
                # RETURN ANOTHER Index
                output = Index(self._keys[len(key):])
                output._data = d
                return output
            else:
                return list(d)
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        if not isinstance(val, dict):
            val = {self._keys[0]: val}
        val = wrap(val)
        d = self._data
        for k in self._keys[0:-1]:
            v = val[k]
            if v == None:
                Log.error("can not handle when {{key}} == None", {"key": k})
            if v not in d:
                e = {}
                d[v] = e
            d = d[v]
        v = val[self._keys[-1]]
        if v not in d:
            d[v] = list()
        d[v].append(val)
        self.count += 1


    # def __contains__(self, key):
    #     return self[key] != None

    def __iter__(self):
        return self.lookup(self._data)

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)
