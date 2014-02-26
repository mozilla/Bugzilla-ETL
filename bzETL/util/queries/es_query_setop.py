# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from .. import struct
from ..collections.matrix import Matrix
from ..collections import AND, SUM, OR
from ..queries.es_query_util import aggregates
from ..queries import domains, es_query_util
from ..queries.filters import simplify, TRUE_FILTER
from ..env.logs import Log
from ..queries import MVEL, filters
from ..queries.cube import Cube
from ..struct import split_field, unwrap, nvl


def is_fieldop(query):
    # THESE SMOOTH EDGES REQUIRE ALL DATA (SETOP)

    select = struct.listwrap(query.select)
    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        isSimple = AND([s.value != None and MVEL.isKeyword(s.value) for s in select])
        noAgg = AND([s.aggregate == "none" for s in select])

        if not isDeep and isSimple and noAgg:
            return True
    else:
        isSmooth = AND((e.domain.type in domains.ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False

def es_fieldop(es, query):
    esQuery = es_query_util.buildESQuery(query)
    select = struct.listwrap(query.select)
    esQuery.query = {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": filters.simplify(query.where)
        }
    }
    esQuery.size = query.limit
    esQuery.fields = select.value
    esQuery.sort = [{s.field: "asc" if s.sort >= 0 else "desc"} for s in query.sort]

    data = es_query_util.post(es, esQuery, query.limit)

    T = data.hits.hits
    matricies = {}
    for s in select:
        if s.value == "*":
            matricies[s.name] = Matrix.wrap([t._source for t in T])
        elif not s.value:
            matricies[s.name] = Matrix.wrap([unwrap(t.fields)[s.value] for t in T])
        else:
            matricies[s.name] = Matrix.wrap([unwrap(t.fields)[s.value] for t in T])

    cube = Cube(query.select, query.edges, matricies, frum=query)
    cube.frum = query
    return cube


def is_setop(query):
    select = struct.listwrap(query.select)

    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND([s.aggregate in ("count", "none") for s in select])   # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

        # NO EDGES IMPLIES SIMPLER QUERIES: EITHER A SET OPERATION, OR RETURN SINGLE AGGREGATE
        if simpleAgg or isDeep:
            return True
    else:
        isSmooth = AND((e.domain.type in domains.ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False


def es_setop(es, mvel, query):
    esQuery = es_query_util.buildESQuery(query)
    select = struct.listwrap(query.select)

    isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
    isComplex = OR([s.value == None and s.aggregate not in ("count", "none") for s in select])   # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

    if not isDeep and not isComplex and len(select)==1 and MVEL.isKeyword(select[0].value):
        esQuery.facets.mvel = {
            "terms": {
                "field": select[0].value,
                "size": nvl(query.limit, 200000)
            },
            "facet_filter": simplify(query.where)
        }
        if query.sort:
            s = query.sort
            if len(s) > 1:
                Log.error("can not sort by more than one field")

            s0 = s[0]
            if s0.field != select[0].value:
                Log.error("can not sort by anything other than count, or term")

            esQuery.facets.mvel.terms.order = "term" if s0.sort >= 0 else "reverse_term"
    elif not isDeep:
        simple_query = query.copy()
        simple_query.where = TRUE_FILTER  #THE FACET FILTER IS FASTER
        esQuery.facets.mvel = {
            "terms": {
                "script_field": mvel.code(simple_query),
                "size": nvl(simple_query.limit, 200000)
            },
            "facet_filter": simplify(query.where)
        }
    else:
        esQuery.facets.mvel = {
            "terms": {
                "script_field": mvel.code(query),
                "size": nvl(query.limit, 200000)
            },
            "facet_filter": simplify(query.where)
        }

    data = es_query_util.post(es, esQuery, query.limit)

    if len(select) == 1 and MVEL.isKeyword(select[0].value):
        # SPECIAL CASE FOR SINGLE TERM
        T = data.facets.mvel.terms
        output = Matrix.wrap([t.term for t in T])
        cube = Cube(query.select, [], {select[0].name: output})
    else:
        data_list = MVEL.unpack_terms(data.facets.mvel, select)
        if not data_list:
            cube = Cube(select, [], {s.name: Matrix.wrap([]) for s in select})
        else:
            output = zip(*data_list)
            cube = Cube(select, [], {s.name: Matrix(list=output[i]) for i, s in enumerate(select)})

    cube.frum = query
    return cube



def is_deep(query):
    select = struct.listwrap(query.select)
    if len(select) > 1:
        return False

    if aggregates[select[0].aggregate] not in ("none", "count"):
        return False

    if len(query.edges)<=1:
        return False

    isDeep = len(split_field(query["from"].name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
    if not isDeep:
        return False   # BETTER TO USE TERM QUERY

    return True


def es_deepop(es, mvel, query):
    esQuery = es_query_util.buildESQuery(query)

    select = query.edges

    temp_query = query.copy()
    temp_query.select = select
    temp_query.edges = []

    esQuery.facets.mvel = {
        "terms": {
            "script_field": mvel.code(temp_query),
            "size": query.limit
        },
        "facet_filter": simplify(query.where)
    }

    data = es_query_util.post(es, esQuery, query.limit)

    rows = MVEL.unpack_terms(data.facets.mvel, query.edges)
    terms = zip(*rows)

    # NUMBER ALL EDGES FOR Qb INDEXING
    edges = query.edges
    for f, e in enumerate(edges):
        for r in terms[f]:
            e.domain.getPartByKey(r)

        e.index = f
        for p, part in enumerate(e.domain.partitions):
            part.dataIndex = p
        e.domain.NULL.dataIndex = len(e.domain.partitions)

    # MAKE CUBE
    dims = [len(e.domain.partitions) for e in query.edges]
    output = Matrix(*dims)

    # FILL CUBE
    for r in rows:
        term_coord = [e.domain.getPartByKey(r[i]).dataIndex for i, e in enumerate(edges)]
        output[term_coord] = SUM(output[term_coord], r[-1])

    cube = Cube(query.select, query.edges, {query.select.name: output})
    cube.frum = query
    return cube
