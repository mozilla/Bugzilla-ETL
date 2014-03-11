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
from ..collections import AND
from ..queries import es_query_util
from ..queries.es_query_util import aggregates, buildESQuery, compileEdges2Term
from ..queries.filters import simplify
from ..queries.cube import Cube
from ..struct import wrap


def is_terms(query):
    select = struct.listwrap(query.select)

    isSimple = not query.select or AND(aggregates[s.aggregate] in ("none", "count") for s in select)
    if isSimple:
        return True
    return False


def es_terms(es, mvel, query):
    """
    RETURN LIST OF ALL EDGE QUERIES

    EVERY FACET IS NAMED <select.name>, <c1>, ... <cN> WHERE <ci> ARE THE ELEMENT COORDINATES
    WE TRY TO PACK DIMENSIONS INTO THE TERMS TO MINIMIZE THE CROSS-PRODUCT EXPLOSION
    """
    select = struct.listwrap(query.select)
    esQuery = buildESQuery(query)
    packed_term = compileEdges2Term(mvel, query.edges, wrap([]))
    for s in select:

        esQuery.facets[s.name] = {
            "terms": {
                "field": packed_term.field,
                "script_field": packed_term.expression,
                "size": query.limit
            },
            "facet_filter": simplify(query.where)
        }

    term2Parts = packed_term.term2parts

    data = es_query_util.post(es, esQuery, query.limit)

    # GETTING ALL PARTS WILL EXPAND THE EDGES' DOMAINS
    # BUT HOW TO UNPACK IT FROM THE term FASTER IS UNKNOWN
    for k, f in data.facets.items():
        for t in f.terms:
            term2Parts(t.term)

    # NUMBER ALL EDGES FOR Qb INDEXING
    for f, e in enumerate(query.edges):
        e.index = f
        if e.domain.type == "default":
            # e.domain.partitions = Q.sort(e.domain.partitions, "value")
            for p, part in enumerate(e.domain.partitions):
                part.dataIndex = p
            e.domain.NULL.dataIndex = len(e.domain.partitions)

    # MAKE CUBE
    output = {}
    dims = [len(e.domain.partitions) + (1 if e.allowNulls else 0) for e in query.edges]
    for s in select:
        output[s.name] = Matrix(*dims)

    # FILL CUBE
    # EXPECTING ONLY SELECT CLAUSE FACETS
    for facetName, facet in data.facets.items():
        for term in facet.terms:
            term_coord = term2Parts(term.term).dataIndex
            for s in select:
                try:
                    output[s.name][term_coord] = term[aggregates[s.aggregate]]
                except Exception, e:
                    #USUALLY CAUSED BY output[s.name] NOT BEING BIG ENOUGH TO HANDLE NULL COUNTS
                    pass
    cube = Cube(query.select, query.edges, output)
    cube.query = query
    return cube

