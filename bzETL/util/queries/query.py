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
from .dimensions import Dimension
from .domains import Domain
from ..queries.filters import TRUE_FILTER
from ..struct import nvl, Struct, EmptyList


class Query(object):

    def __new__(cls, query, schema=None):
        if isinstance(query, Query):
            return query
        return object.__new__(cls)

    def __init__(self, query, schema=None):
        """
        NORMALIZE QUERY SO IT CAN STILL BE JSON
        """
        if isinstance(query, Query):
            return

        object.__init__(self)
        query = struct.wrap(query)

        self.name = query.name

        select = query.select
        if isinstance(select, list):
            select = [_normalize_select(s, schema=schema) for s in select]
        elif select:
            select = _normalize_select(select, schema=schema)
        else:
            select = []

        self.select2index = {}  # MAP FROM NAME TO data INDEX
        for i, s in enumerate(struct.listwrap(select)):
            self.select2index[s.name] = i
        self.select = select

        self.edges = [_normalize_edge(e, schema=schema) for e in struct.listwrap(query.edges)]
        self.frum = _normalize_from(query["from"], schema=schema)
        self.where = nvl(query.where, TRUE_FILTER)

        self.window = [_normalize_window(w) for w in struct.listwrap(query.window)]

        self.sort = _normalize_sort(query.sort)
        self.limit = query.limit
        self.isLean = query.isLean


    @property
    def columns(self):
        return self.select + self.edges

    def __getitem__(self, item):
        if item == "from":
            return self.frum
        return Struct.__getitem__(self, item)

    def copy(self):
        output = object.__new__(Query)
        source = object.__getattribute__(self, "__dict__")
        dest = object.__getattribute__(output, "__dict__")
        struct.set_default(dest, source)
        return output

def _normalize_selects(selects, schema=None):
    if isinstance(selects, list):
        return struct.wrap([_normalize_select(s, schema=schema) for s in selects])
    else:
        return _normalize_select(selects, schema=schema)


def _normalize_select(select, schema=None):
    if isinstance(select, basestring):
        if schema:
            s = schema[select]
            if s:
                return s.getSelect()
        return Struct(name=select, value=select, aggregate="none")
    else:
        if not select.name:
            select = select.copy()
            select.name = select.value

        select.aggregate = nvl(select.aggregate, "none")
        return select


def _normalize_edge(edge, schema=None):
    if isinstance(edge, basestring):
        if schema:
            e = schema[edge]
            if e:
                return Struct(
                    name=edge,
                    domain=e.getDomain()
                )
        return Struct(
            name=edge,
            value=edge,
            domain=_normalize_domain(schema=schema)
        )
    else:
        return Struct(
            name=nvl(edge.name, edge.value),
            value=edge.value,
            range=edge.range,
            allowNulls=False if edge.allowNulls is False else True,
            domain=_normalize_domain(edge.domain, schema=schema)
        )
def _normalize_from(frum, schema=None):
    if isinstance(frum, basestring):
        return Struct(name=frum)
    elif isinstance(frum, dict) and frum["from"]:
        return Query(frum, schema=schema)
    else:
        return struct.wrap(frum)

def _normalize_domain(domain=None, schema=None):
    if not domain:
        return Domain(type="default")
    elif isinstance(domain, Dimension):
        return domain.getDomain()
    elif schema and isinstance(domain, basestring) and schema[domain]:
        return schema[domain].getDomain()
    elif isinstance(domain, Domain):
        return domain

    if not domain.name:
        domain = domain.copy()
        domain.name = domain.type
    return Domain(**struct.unwrap(domain))

def _normalize_window(window, schema=None):
    return Struct(
        name=nvl(window.name, window.value),
        value=window.value,
        edges=[_normalize_edge(e, schema) for e in struct.listwrap(window.edges)],
        sort=_normalize_sort(window.sort),
        aggregate=window.aggregate,
        range=_normalize_range(window.range)
    )

def _normalize_range(range):
    if range == None:
        return None

    return Struct(
        min=range.min,
        max=range.max
    )


def _normalize_sort(sort=None):
    """
    CONVERT SORT PARAMETERS TO A NORMAL FORM SO EASIER TO USE
    """

    if not sort:
        return EmptyList

    output = []
    for s in struct.listwrap(sort):
        if isinstance(s, basestring):
            output.append({"field": s, "sort": 1})
        else:
            output.append({"field": nvl(s.field, s.value), "sort": nvl(sort_direction[s.sort], 1)})
    return struct.wrap(output)


sort_direction = {
    "asc": 1,
    "desc": -1,
    "none": 0,
    1: 1,
    0: 0,
    -1: -1
}
