# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with self file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
import re
from .. import struct
from ..cnv import CNV
from ..env.logs import Log
from ..struct import Struct, nvl, wrap


ALGEBRAIC = ["time", "duration", "numeric", "count", "datetime"]  # DOMAINS THAT HAVE ALGEBRAIC OPERATIONS DEFINED
KNOWN = ["set", "boolean", "duration", "time", "numeric"]    # DOMAINS THAT HAVE A KNOWN NUMBER FOR PARTS AT QUERY TIME
PARTITION = ["set", "boolean"]    # DIMENSIONS WITH CLEAR PARTS


class Domain(object):
    def __new__(cls, **desc):
        desc = wrap(desc)
        if desc.type == "value":
            return ValueDomain(**struct.unwrap(desc))
        elif desc.type == "default":
            return DefaultDomain(**struct.unwrap(desc))
        elif desc.type == "set":
            if isinstance(desc.key, (list, tuple)):
                Log.error("multi key not supported yet")
            return SetDomain(**struct.unwrap(desc))
        else:
            Log.error("Do not know domain of type {{type}}", {"type": desc.type})

    def __init__(self, **desc):
        desc = wrap(desc)
        self.name = nvl(desc.name, desc.type)
        self.type = desc.type
        self.min = desc.min
        self.max = desc.max
        self.interval = desc.interval
        self.value = desc.value,
        self.key = desc.key,
        self.label = desc.label,
        self.end = desc.end,
        self.isFacet = nvl(desc.isFacet, False)
        self.dimension = desc.dimension

    @property
    def dict(self):
        return Struct(
            type=self.type,
            name=self.name,
            partitions=self.partitions,
            min=self.min,
            max=self.max,
            interval=self.interval,
            value=self.value,
            key=self.key,
            label=self.label,
            end=self.end,
            isFacet=self.isFacet
        )

    def __json__(self):
        return CNV.object2JSON(self.dict)


class ValueDomain(Domain):
    def __new__(cls, **desc):
        return object.__new__(ValueDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        self.NULL = None

    def compare(self, a, b):
        return value_compare(a, b)

    def getCanonicalPart(self, part):
        return part

    def getPartByKey(self, key):
        return key

    def getKey(self, part):
        return part

    def getEnd(self, value):
        return value


class DefaultDomain(Domain):
    """
    DOMAIN IS A LIST OF OBJECTS, EACH WITH A value PROPERTY
    """

    def __new__(cls, **desc):
        return object.__new__(DefaultDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)

        self.NULL = Struct(value=None)
        self.partitions = []
        self.map = dict()
        self.map[None] = self.NULL

    def compare(self, a, b):
        return value_compare(a.value, b.value)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getPartByKey(self, key):
        canonical = self.map.get(key, None)
        if canonical:
            return canonical

        canonical = Struct(name=key, value=key)

        self.partitions.append(canonical)
        self.map[key] = canonical
        return canonical

    def getKey(self, part):
        return part.value

    def getEnd(self, part):
        return part.value

    def getLabel(self, part):
        return part.value


class SetDomain(Domain):
    """
    DOMAIN IS A LIST OF OBJECTS, EACH WITH A value PROPERTY
    """

    def __new__(cls, **desc):
        return object.__new__(SetDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        desc = wrap(desc)

        self.NULL = Struct(value=None)
        self.partitions = []
        self.map = dict()
        self.map[None] = self.NULL
        self.label = nvl(self.label, "name")

        if not isinstance(desc.partitions, list):
            Log.error("expecting a list of partitions")

        if isinstance(desc.partitions[0], basestring):
            for p in desc.partitions:
                part = {"name": p, "value": p}
                self.partitions.append(part)
                self.map[p] = part
            self.key = ("value", )
        else:
            if desc.key == None:
                Log.error("Domains must have keys")
            if not is_keyword(desc.key):
                Log.error("scripts not supported yet")
            self.key = desc.key

            self.partitions = desc.partitions.copy()
            for p in desc.partitions:
                self.map[p[self.key]] = p

    def compare(self, a, b):
        return value_compare(self.getKey(a), self.getKey(b))

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getPartByKey(self, key):
        canonical = self.map.get(key, None)
        if not canonical:
            return self.NULL
        return canonical

    def getKey(self, part):
        return part[self.key]

    def getEnd(self, part):
        if self.value:
            return part[self.value]
        else:
            return part

    def getLabel(self, part):
        return part[self.label]



def value_compare(a, b):
    if a == None:
        if b == None:
            return 0
        return -1
    elif b == None:
        return 1

    if a > b:
        return 1
    elif a < b:
        return -1
    else:
        return 0



keyword_pattern = re.compile(r"\w+(?:\.\w+)*")
def is_keyword(value):
    if value == None:
        return False
    return True if keyword_pattern.match(value) else False
