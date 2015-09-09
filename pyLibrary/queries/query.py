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
from __future__ import division
from __future__ import absolute_import

from pyLibrary.debugs.logs import Log
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import coalesce
from pyLibrary.dot import wrap, listwrap
from pyLibrary.maths import Math
from pyLibrary.queries import expressions
from pyLibrary.queries.containers import Container
from pyLibrary.queries.normalize import _normalize_groupby, _normalize_edges, _normalize_where, _normalize_window, _normalize_sort, DEFAULT_LIMIT, _normalize_selects


class Query(object):
    __slots__ = ["frum", "select", "edges", "groupby", "where", "window", "sort", "limit", "format", "isLean"]

    def __new__(cls, query, frum):
        if isinstance(query, Query):
            return query
        return object.__new__(cls)

    def __init__(self, query, frum):
        """
        NORMALIZE QUERY SO IT CAN STILL BE JSON
        """
        object.__init__(self)
        if isinstance(query, Query):
            return

        query = wrap(query)

        self.frum = frum
        if not isinstance(self.frum, Container):
            Log.error('Expecting from clause to be a Container')

        self.format = query.format

        if query.select:
            self.select = _normalize_selects(query.select, frum.schema)
        else:
            if query.edges or query.groupby:
                self.select = {"name": "count", "value": ".", "aggregate": "count"}
            else:
                self.select = {"name": "__all__", "value": "*", "aggregate": "none"}

        if query.groupby and query.edges:
            Log.error("You can not use both the `groupby` and `edges` clauses in the same query!")
        elif query.edges:
            self.edges = _normalize_edges(query.edges, schema=self.frum.schema)
            self.groupby = None
        elif query.groupby:
            self.edges = None
            self.groupby = _normalize_groupby(query.groupby, schema=self.frum.schema)
        else:
            self.edges = []
            self.groupby = None

        self.where = _normalize_where(query.where, schema=self.frum.schema)
        self.window = [_normalize_window(w) for w in listwrap(query.window)]
        self.sort = _normalize_sort(query.sort)
        self.limit = coalesce(query.limit, DEFAULT_LIMIT)
        if not Math.is_integer(self.limit) or self.limit < 0:
            Log.error("Expecting limit >= 0")

        self.isLean = query.isLean


        # DEPTH ANALYSIS - LOOK FOR COLUMN REFERENCES THAT MAY BE DEEPER THAN
        # THE from SOURCE IS.
        vars = get_all_vars(self, exclude_where=True)  # WE WILL EXCLUDE where VARIABLES
        for c in self.columns:
            if c.name in vars and c.depth:
                Log.error("This query, with variable {{var_name}} is too deep", var_name=c.name)

    @property
    def columns(self):
        return listwrap(self.select) + coalesce(self.edges, self.groupby)

    def __getitem__(self, item):
        if item == "from":
            return self.frum
        return Dict.__getitem__(self, item)

    def copy(self):
        output = object.__new__(Query)
        for s in Query.__slots__:
            setattr(output, s, getattr(self, s))
        return output

    def as_dict(self):
        output = wrap({s: getattr(self, s) for s in Query.__slots__})
        return output


def get_all_vars(query, exclude_where=False):
    """
    :param query:
    :param exclude_where: Sometimes we do not what to look at the where clause
    :return: all variables in use by query
    """
    output = []
    for s in listwrap(query.select):
        output.extend(select_get_all_vars(s))
    for s in listwrap(query.edges):
        output.extend(edges_get_all_vars(s))
    for s in listwrap(query.groupby):
        output.extend(edges_get_all_vars(s))
    if not exclude_where:
        output.extend(expressions.get_all_vars(query.where))
    return output


def select_get_all_vars(s):
    if isinstance(s.value, list):
        return set(s.value)
    elif isinstance(s.value, basestring):
        return set([s.value])
    elif s.value == None or s.value == ".":
        return set()
    else:
        if s.value == "*":
            return set(["*"])
        return expressions.get_all_vars(s.value)


def edges_get_all_vars(e):
    output = []
    if isinstance(e.value, basestring):
        output.append(e.value)
    if e.domain.key:
        output.append(e.domain.key)
    if e.domain.where:
        output.extend(expressions.get_all_vars(e.domain.where))
    if e.domain.partitions:
        for p in e.domain.partitions:
            if p.where:
                output.extend(expressions.get_all_vars(p.where))
    return output

