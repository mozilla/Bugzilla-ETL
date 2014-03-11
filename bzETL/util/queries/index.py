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
from ..env.logs import Log
from ..struct import unwrap, wrap


class UniqueIndex(object):
    """
    DEFINE A SET OF ATTRIBUTES THAT UNIQUELY IDENTIFIES EACH OBJECT IN A list.
    THIS ALLOWS set-LIKE COMPARISIONS (UNION, INTERSECTION, DIFFERENCE, ETC) WHILE
    STILL MAINTAINING list-LIKE FEATURES
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = unwrap(keys)
        self.count = 0

    def __getitem__(self, key):
        try:
            key = value2key(self._keys, key)
            d = self._data.get(key, None)
            return wrap(d)
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        try:
            key = value2key(self._keys, key)
            d = self._data.get(key, None)
            if d != None:
                Log.error("key already filled")

            self._data[key] = unwrap(value)
            self.count += 1

        except Exception, e:
            Log.error("something went wrong", e)


    def add(self, val):
        key = value2key(self._keys, val)
        d = self._data.get(key, None)
        if d != None:
            Log.error("key already filled")

        self._data[key] = unwrap(val)
        self.count += 1

    def __contains__(self, key):
        return self[key] != None

    def __iter__(self):
        return (wrap(v) for v in self._data.itervalues())

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


class Index(object):
    """
    USING DATABASE TERMINOLOGY, THIS IS A NON-UNIQUE INDEX
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = unwrap(keys)
        self.count = 0

    def __getitem__(self, key):
        try:
            if isinstance(key, (list, tuple)) and len(key)<len(self._keys):
                # RETURN ANOTHER Index
                filter_key = self._keys[0:len(key):]
                filter_value = value2key(filter_key, key)
                new_key = self._keys[len(key)::]
                output = Index(new_key)
                for d in self:
                    if value2key(filter_key, d) == filter_value:
                        output.add(d)
                return output

            key = value2key(self._keys, key)
            d = self._data.get(key, None)
            return wrap(list(d))
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        key = value2key(self._keys, wrap(val))

        d = self._data.get(key, None)
        if d == None:
            d = list()
            self._data[key] = d
        d.append(unwrap(val))
        self.count += 1


    # def __contains__(self, key):
    #     return self[key] != None

    def __iter__(self):
        def itr():
            for v in self._data.values():
                for vv in v:
                    yield wrap(vv)
        return itr()

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
        for v in self:
            output.add(v)
        for v in other:
            output.add(v)
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


def value2key(keys, val):
    if len(keys)==1:
        if isinstance(val, dict):
            return val[keys[0]]
        return val
    else:
        if isinstance(val, dict):
            return wrap({k: val[k] for k in keys})
        elif isinstance(val, (list, tuple)):
            return wrap(dict(zip(keys, val)))
        else:
            Log.error("do not know what to do here")
