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
from ..collections import AND, reverse
from ..env.logs import Log
from ..queries import MVEL
from ..queries.filters import TRUE_FILTER, simplify
from ..struct import nvl, Struct, EmptyList, wrap, split_field, join_field, StructList, unwrap
from .es_query_util import INDEX_CACHE


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
        query = wrap(query)

        self.name = query.name

        select = query.select
        if isinstance(select, list):
            select = wrap([unwrap(_normalize_select(s, schema=schema)) for s in select])
        elif select:
            select = _normalize_select(select, schema=schema)
        else:
            select = StructList()
        self.select2index = {}  # MAP FROM NAME TO data INDEX
        for i, s in enumerate(struct.listwrap(select)):
            self.select2index[s.name] = i
        self.select = select

        self.edges = [_normalize_edge(e, schema=schema) for e in struct.listwrap(query.edges)]
        self.frum = _normalize_from(query["from"], schema=schema)
        self.where = _normalize_where(query.where, schema=schema)

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
        return wrap([_normalize_select(s, schema=schema) for s in selects])
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
            select.name = nvl(select.value, select.aggregate)

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
        return wrap(frum)


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
        range=_normalize_range(window.range),
        where=_normalize_where(window.where, schema=schema)
    )


def _normalize_range(range):
    if range == None:
        return None

    return Struct(
        min=range.min,
        max=range.max
    )


def _normalize_where(where, schema=None):
    if where == None:
        return TRUE_FILTER
    if schema == None:
        return where
    where = simplify(_where_terms(where, where, schema))
    return where


def _map_term_using_schema(master, where, schema):
    """
    IF THE WHERE CLAUSE REFERS TO FIELDS IN THE SCHEMA, THEN EXPAND THEM
    """
    output = StructList()
    for k, v in where.term.items():
        dimension = schema.edges[k]
        if dimension:
            domain = schema.edges[k].getDomain()
            if dimension.fields:
                if isinstance(dimension.fields, dict):
                    # EXPECTING A TUPLE
                    for local_field, es_field in dimension.fields.items():
                        local_value = v[local_field]
                        if local_value == None:
                            output.append({"missing": {"field": es_field}})
                        else:
                            output.append({"term": {es_field: local_value}})
                    continue

                if len(dimension.fields) == 1 and MVEL.isKeyword(dimension.fields[0]):
                    # SIMPLE SINGLE-VALUED FIELD
                    if domain.getPartByKey(v) is domain.NULL:
                        output.append({"missing": {"field": dimension.fields[0]}})
                    else:
                        output.append({"term": {dimension.fields[0]: v}})
                    continue

                if AND(MVEL.isKeyword(f) for f in dimension.fields):
                    # EXPECTING A TUPLE
                    if not isinstance(v, tuple):
                        Log.error("expecing {{name}}={{value}} to be a tuple", {"name": k, "value": v})
                    for i, f in enumerate(dimension.fields):
                        vv = v[i]
                        if vv == None:
                            output.append({"missing": {"field": f}})
                        else:
                            output.append({"term": {f: vv}})
                    continue
            if len(dimension.fields) == 1 and MVEL.isKeyword(dimension.fields[0]):
                if domain.getPartByKey(v) is domain.NULL:
                    output.append({"missing": {"field": dimension.fields[0]}})
                else:
                    output.append({"term": {dimension.fields[0]: v}})
                continue
            if domain.partitions:
                part = domain.getPartByKey(v)
                if part is domain.NULL or not part.esfilter:
                    Log.error("not expected to get NULL")
                output.append(part.esfilter)
                continue
            else:
                Log.error("not expected")
        output.append({"term": {k: v}})
    return {"and": output}

def _move_nested_term(master, where, schema):
    """
    THE WHERE CLAUSE CAN CONTAIN NESTED PROPERTY REFERENCES, THESE MUST BE MOVED
    TO A NESTED FILTER
    """
    items = where.term.items()
    if len(items) != 1:
        Log.error("Expecting only one term")
    k, v = items[0]
    nested_path = _get_nested_path(k, schema)
    if nested_path:
        return {"nested": {
            "path": nested_path,
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"term": {k: v}}
                ]}
            }}
        }}
    return where

def _get_nested_path(field, schema):
    if MVEL.isKeyword(field):
        field = join_field([schema.es.alias]+split_field(field))
        for i, f in reverse(enumerate(split_field(field))):
            path = join_field(split_field(field)[0:i+1:])
            if path in INDEX_CACHE:
                return join_field(split_field(path)[1::])
    return None

def _where_terms(master, where, schema):
    """
    USE THE SCHEMA TO CONVERT DIMENSION NAMES TO ES FILTERS
    master - TOP LEVEL WHERE (FOR PLACING NESTED FILTERS)
    """
    if isinstance(where, dict):
        if where.term:
            #MAP TERM
            output = _map_term_using_schema(master, where, schema)
            return output
        elif where.terms:
            #MAP TERM
            output = StructList()
            for k, v in where.terms.items():
                if schema.edges[k]:
                    domain = schema.edges[k].getDomain()
                    fields = domain.dimension.fields
                    if isinstance(fields, dict):
                        for local_field, es_field in fields.items():
                            vv = v[local_field]
                            if vv == None:
                                output.append({"missing": {"field": es_field}})
                            else:
                                output.append({"term": {es_field: vv}})
                        continue
                    if isinstance(fields, list) and len(fields) == 1 and MVEL.isKeyword(fields[0]):
                        if domain.getPartByKey(v) is domain.NULL:
                            output.append({"missing": {"field": fields[0]}})
                        else:
                            output.append({"term": {fields[0]: v}})
                        continue
                    if domain.partitions:
                        output.append({"or": [domain.getPartByKey(vv).esfilter for vv in v]})
                        continue
                output.append({"terms": {k: v}})
            return {"and": output}
        elif where["and"] or where["or"]:
            return {k: [unwrap(_where_terms(master, vv, schema)) for vv in v] for k, v in where.items()}
    return where


def _normalize_sort(sort=None):
    """
    CONVERT SORT PARAMETERS TO A NORMAL FORM SO EASIER TO USE
    """

    if not sort:
        return EmptyList

    output = StructList()
    for s in struct.listwrap(sort):
        if isinstance(s, basestring):
            output.append({"field": s, "sort": 1})
        else:
            output.append({"field": nvl(s.field, s.value), "sort": nvl(sort_direction[s.sort], 1)})
    return wrap(output)


sort_direction = {
    "asc": 1,
    "desc": -1,
    "none": 0,
    1: 1,
    0: 0,
    -1: -1
}
