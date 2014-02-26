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
from ..collections import SUM, COUNT
from ..queries import es_query_util
from ..queries.cube import Cube
from ..queries.es_query_util import aggregates, buildESQuery, compileEdges2Term
from ..queries.filters import simplify
from ..env.logs import Log
from ..queries import domains, MVEL, filters
from ..queries.MVEL import UID
from ..struct import nvl, StructList


def is_terms_stats(query):
    #ONLY ALLOWED ONE UNKNOWN DOMAIN
    num_unknown = COUNT(1 for e in query.edges if e.domain.type not in domains.KNOWN)

    if num_unknown <= 1:
        if query.sort:
            Log.error("terms_stats can not be sorted")

        return True
    return False


def es_terms_stats(es, mvel, query):
    select = struct.listwrap(query.select)
    facetEdges = []    # EDGES THAT WILL REQUIRE A FACET FOR EACH PART
    termsEdges = []
    specialEdge = None
    special_index = -1

    # A SPECIAL EDGE IS ONE THAT HAS AN UNDEFINED NUMBER OF PARTITIONS AT QUERY TIME
    # FIND THE specialEdge, IF ONE
    for f, tedge in enumerate(query.edges):
        if tedge.domain.type in domains.KNOWN:
            for p, part in enumerate(tedge.domain.partitions):
                part.dataIndex = p

            # FACETS ARE ONLY REQUIRED IF SQL JOIN ON DOMAIN IS REQUIRED (RANGE QUERY)
            # OR IF WE ARE NOT SIMPLY COUNTING
            # OR IF NO SCRIPTING IS ALLOWED (SOME OTHER CODE IS RESPONSIBLE FOR SETTING isFacet)
            # OR IF WE JUST WANT TO FORCE IT :)
            # OF COURSE THE default EDGE IS NOT EXPLICIT, SO MUST BE A TERM

            facetEdges.append(tedge)
        else:
            if specialEdge:
                Log.error("There is more than one open-ended edge: self can not be handled")
            specialEdge = tedge
            special_index = f
            termsEdges.append(tedge)

    if not specialEdge:
        # WE SERIOUSLY WANT A SPECIAL EDGE, OTHERWISE WE WILL HAVE TOO MANY FACETS
        #THE BIGGEST EDGE MAY BE COLLAPSED TO A TERM, MAYBE?
        num_parts = 0
        special_index = -1
        for i, e in enumerate(facetEdges):
            l = len(e.domain.partitions)
            if ((e.value and MVEL.isKeyword(e.value)) or len(e.domain.dimension.fields) == 1) and l > num_parts:
                num_parts = l
                specialEdge = e
                special_index = i

        facetEdges.pop(special_index)
        termsEdges.append(specialEdge)

    esQuery = buildESQuery(query)

    calcTerm = compileEdges2Term(mvel, termsEdges, StructList())
    term2parts = calcTerm.term2parts

    esFacets = getAllEdges(facetEdges)
    for s in select:
        for parts in esFacets:
            condition = []
            constants = []
            name = [s.name]
            for f, fedge in enumerate(facetEdges):
                name.append(str(parts[f].dataIndex))
                condition.append(buildCondition(mvel, fedge, parts[f]))
                constants.append({"name": fedge.domain.name, "value": parts[f]})
            condition.append(query.where)
            name = ",".join(name)

            esQuery.facets[name] = {
                "terms_stats": {
                    "key_field": calcTerm.field,
                    "value_field": s.value if MVEL.isKeyword(s.value) else None,
                    "value_script": mvel.compile_expression(s.value) if not MVEL.isKeyword(s.value) else None,
                    "size": nvl(query.limit, 200000)
                }
            }
            if condition:
                esQuery.facets[name].facet_filter = simplify({"and": condition})

    data = es_query_util.post(es, esQuery, query.limit)

    if specialEdge.domain.type not in domains.KNOWN:
        #WE BUILD THE PARTS BASED ON THE RESULTS WE RECEIVED
        partitions = []
        map = {}
        for facetName, parts in data.facets.items():
            for stats in parts.terms:
                if not map[stats]:
                    part = {"value": stats, "name": stats}
                    partitions.append(part)
                    map[stats] = part

        partitions.sort(specialEdge.domain.compare)
        for p, part in enumerate(partitions):
            part.dataIndex = p

        specialEdge.domain.map = map
        specialEdge.domain.partitions = partitions

    # MAKE CUBE
    matricies = {}
    dims = [len(e.domain.partitions) + (1 if e.allowNulls else 0) for e in query.edges]
    for s in select:
        matricies[s.name] = Matrix(*dims)

    name2agg = {s.name: aggregates[s.aggregate] for s in select}

    # FILL CUBE
    for edgeName, parts in data.facets.items():
        temp = edgeName.split(",")
        pre_coord = tuple(int(c) for c in temp[1:])
        sname = temp[0]

        for stats in parts.terms:
            if specialEdge:
                special = term2parts(stats.term)[0]
                coord = pre_coord[:special_index]+(special.dataIndex, )+pre_coord[special_index:]
            else:
                coord = pre_coord
            matricies[sname][coord] = stats[name2agg[sname]]

    cube = Cube(query.select, query.edges, matricies)
    cube.frum = query
    return cube


def register_script_field(esQuery, code):
    if not esQuery.script_fields:
        esQuery.script_fields = {}

    #IF CODE IS IDENTICAL, THEN USE THE EXISTING SCRIPT
    for n, c in esQuery.script_fields.items():
        if c.script == code:
            return n

    name = "script" + UID()
    esQuery.script_fields[name].script = code
    return name


def getAllEdges(facetEdges):
    if not facetEdges:
        return [()]
    return _getAllEdges(facetEdges, 0)


def _getAllEdges(facetEdges, edgeDepth):
    """
    RETURN ALL PARTITION COMBINATIONS:  A LIST OF ORDERED TUPLES
    """
    if edgeDepth == len(facetEdges):
        return [()]
    edge = facetEdges[edgeDepth]

    deeper = _getAllEdges(facetEdges, edgeDepth + 1)

    output = []
    partitions = edge.domain.partitions
    for part in partitions:
        for deep in deeper:
            output.append((part,) + deep)
    return output


def buildCondition(mvel, edge, partition):
    """
    RETURN AN ES FILTER OBJECT
    """
    output = {}

    if edge.domain.isFacet:
        # MUST USE THIS' esFacet
        condition = struct.wrap(nvl(partition.where, {"and": []}))

        if partition.min and partition.max and MVEL.isKeyword(edge.value):
            condition["and"].append({
                "range": {edge.value: {"gte": partition.min, "lt": partition.max}}
            })

        # ES WILL FREAK OUT IF WE SEND {"not":{"and":x}} (OR SOMETHING LIKE THAT)
        return filters.simplify(condition)
    elif edge.range:
        # THESE REALLY NEED FACETS TO PERFORM THE JOIN-TO-DOMAIN
        # USE MVEL CODE
        if edge.domain.type in domains.ALGEBRAIC:
            output = {"and": []}

            if edge.range.mode and edge.range.mode == "inclusive":
                # IF THE range AND THE partition OVERLAP, THEN MATCH IS MADE
                if MVEL.isKeyword(edge.range.min):
                    output["and"].append({"range": {edge.range.min: {"lt": MVEL.value2value(partition.max)}}})
                else:
                    # WHOA!! SUPER SLOW!!
                    output["and"].append({"script": {"script": mvel.compile_expression(
                        edge.range.min + " < " + MVEL.value2MVEL(partition.max)
                    )}})

                if MVEL.isKeyword(edge.range.max):
                    output["and"].append({"or": [
                        {"missing": {"field": edge.range.max}},
                        {"range": {edge.range.max, {"gt": MVEL.value2value(partition.min)}}}
                    ]})
                else:
                    # WHOA!! SUPER SLOW!!
                    output["and"].append({"script": {"script": mvel.compile_expression(
                        edge.range.max + " > " + MVEL.value2MVEL(partition.min))}})

            else:
                # SNAPSHOT - IF range INCLUDES partition.min, THEN MATCH IS MADE
                if MVEL.isKeyword(edge.range.min):
                    output["and"].append({"range": {edge.range.min: {"lte": MVEL.value2value(partition.min)}}})
                else:
                    # WHOA!! SUPER SLOW!!
                    output["and"].append({"script": {"script": mvel.compile_expression(
                        edge.range.min + "<=" + MVEL.value2MVEL(partition.min)
                    )}})

                if MVEL.isKeyword(edge.range.max):
                    output["and"].append({"or": [
                        {"missing": {"field": edge.range.max}},
                        {"range": {edge.range.max, {"gte": MVEL.value2value(partition.min)}}}
                    ]})
                else:
                    # WHOA!! SUPER SLOW!!
                    output["and"].append({"script": {"script": mvel.compile_expression(
                        MVEL.value2MVEL(partition.min) + " <= " + edge.range.max
                    )}})
            return output
        else:
            Log.error("Do not know how to handle range query on non-continuous domain")

    elif not edge.value:
        # MUST USE THIS' esFacet, AND NOT(ALL THOSE ABOVE)
        return partition.esfilter
    elif MVEL.isKeyword(edge.value):
        # USE FAST ES SYNTAX
        if edge.domain.type in domains.ALGEBRAIC:
            output.range = {}
            output.range[edge.value] = {"gte": MVEL.value2query(partition.min), "lt": MVEL.value2query(partition.max)}
        elif edge.domain.type == "set":
            if partition.value:
                if partition.value != edge.domain.getKey(partition):
                    Log.error("please ensure the key attribute of the domain matches the value attribute of all partitions, if only because we are now using the former")
                    # DEFAULT TO USING THE .value ATTRIBUTE, IF ONLY BECAUSE OF LEGACY REASONS
                output.term = {edge.value: partition.value}
            else:
                output.term = {edge.value: edge.domain.getKey(partition)}

        elif edge.domain.type == "default":
            output.term = dict()
            output.term[edge.value] = partition.value
        else:
            Log.error("Edge \"" + edge.name + "\" is not supported")

        return output
    else:
        # USE MVEL CODE
        if edge.domain.type in domains.ALGEBRAIC:
            output.script = {"script": edge.value + ">=" + MVEL.value2MVEL(partition.min) + " and " + edge.value + "<" + MVEL.value2MVEL(partition.max)}
        else:
            output.script = {"script": "( " + edge.value + " ) ==" + MVEL.value2MVEL(partition.value)}

        code = MVEL.addFunctions(output.script.script)
        output.script.script = code.head + code.body
        return output

