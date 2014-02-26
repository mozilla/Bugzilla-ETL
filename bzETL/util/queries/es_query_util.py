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
from datetime import datetime

from .. import struct
from ..cnv import CNV
from .. import strings
from dzAlerts.util.collections import COUNT
from ..maths import stats
from ..env.elasticsearch import ElasticSearch
from ..env.logs import Log
from ..maths import Math
from ..queries import domains, MVEL, filters
from ..struct import nvl, StructList, Struct, split_field, join_field
from ..times import durations


TrueFilter = {"match_all": {}}
DEBUG = False

INDEX_CACHE = {}  # MATCH NAMES TO FULL CONNECTION INFO


def loadColumns(es, frum):
    """
    ENSURE COLUMNS FOR GIVEN INDEX/QUERY ARE LOADED, AND MVEL COMPILATION WORKS BETTER
    """
    if isinstance(frum, basestring):
        if frum in INDEX_CACHE:
            return INDEX_CACHE[frum]
        frum = Struct(
            name=frum
        )
    else:
        if not frum.name:
            Log.error("Expecting name")

        if frum.name in INDEX_CACHE:
            return INDEX_CACHE[frum.name]

    # FILL frum WITH DEFAULTS FROM es.settings
    struct.set_default(frum, default=es.settings)

    if not frum.host:
        Log.error("must have host defined")

    #DETERMINE IF THE es IS FUNCTIONALLY DIFFERENT
    diff = False
    for k, v in es.settings.items():
        if k != "name" and v != frum[k]:
            diff = True
    if diff:
        es = ElasticSearch(frum)

    output = struct.wrap(frum).copy()
    schema = es.get_schema()
    properties = schema.properties
    output.es = es

    root = split_field(frum.name)[0]
    if root != frum.name:
        INDEX_CACHE[frum.name] = output
        loadColumns(es, root)
    else:
        INDEX_CACHE[root] = output
        output.columns = parseColumns(frum.index, root, properties)

    return output


def post(es, esQuery, limit):
    if not esQuery.facets and esQuery.size == 0:
        Log.error("ESQuery is sending no facets")
        # DO NOT KNOW WHY THIS WAS HERE
    # if isinstance(query.select, list) or len(query.edges) and not esQuery.facets.keys and esQuery.size == 0:
    #     Log.error("ESQuery is sending no facets")

    postResult = None
    try:
        postResult = es.search(esQuery)

        for facetName, f in postResult.facets:
            if f._type == "statistical":
                return None
            if not f.terms:
                return None

            if not DEBUG and not limit and len(f.terms) == limit:
                Log.error("Not all data delivered (" + str(len(f.terms)) + "/" + str(f.total) + ") try smaller range")
    except Exception, e:
        Log.error("Error with ESQuery", e)

    return postResult


def buildESQuery(query):
    output = struct.wrap({
        "query": {"match_all": {}},
        "from": 0,
        "size": 100 if DEBUG else 0,
        "sort": [],
        "facets": {
        }
    })

    if DEBUG:
        # TO LIMIT RECORDS TO WHAT'S IN FACETS
        output.query = {
            "filtered": {
                "query": {
                    "match_all": {}
                },
                "filter": filters.simplify(query.where)
            }
        }

    return output


def parseColumns(index_name, parent_path, esProperties):
    """
    RETURN THE COLUMN DEFINITIONS IN THE GIVEN esProperties OBJECT
    """
    columns = []
    for name, property in esProperties.items():
        if parent_path:
            path = join_field(split_field(parent_path) + [name])
        else:
            path = name

        childColumns = None

        if property.properties:
            childColumns = parseColumns(index_name, path, property.properties)
            columns.extend(childColumns)
            columns.append({
                "name": join_field(split_field(path)[1::]),
                "type": "object",
                "useSource": True
            })

        if property.type == "nested" and property.properties:
            # NESTED TYPE IS A NEW TYPE DEFINITION
            if path not in INDEX_CACHE:
                INDEX_CACHE[path] = INDEX_CACHE[parent_path].copy()
                INDEX_CACHE[path].name = path
            INDEX_CACHE[path].columns = childColumns
            continue

        if property.dynamic:
            continue
        if not property.type:
            continue
        if property.type == "multi_field":
            property.type = property.fields[name].type  # PULL DEFAULT TYPE
            for i, n, p in enumerate(property.fields):
                if n == name:
                    # DEFAULT
                    columns.append({"name": struct.join_field(split_field(path)[1::]), "type": p.type, "useSource": p.index == "no"})
                else:
                    columns.append({"name": struct.join_field(split_field(path)[1::]) + "\\." + n, "type": p.type, "useSource": p.index == "no"})
            continue

        if property.type in ["string", "boolean", "integer", "date", "long", "double"]:
            columns.append({
                "name": struct.join_field(split_field(path)[1::]),
                "type": property.type,
                "useSource": property.index == "no"
            })
            if property.index_name and name != property.index_name:
                columns.append({
                    "name": property.index_name,
                    "type": property.type,
                    "useSource": property.index == "no"
                })
        else:
            Log.warning("unknown type {{type}} for property {{path}}", {"type": property.type, "path": path})

    # SPECIAL CASE FOR PROPERTIES THAT WILL CAUSE OutOfMemory EXCEPTIONS
    for c in columns:
        if name == "bugs" and (c.name == "dependson" or c.name == "blocked"):
            c.useSource = True

    return columns


def compileTime2Term(edge):
    """
    RETURN MVEL CODE THAT MAPS TIME AND DURATION DOMAINS DOWN TO AN INTEGER AND
    AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
    """
    if edge.esscript:
        Log.error("edge script not supported yet")

    # IS THERE A LIMIT ON THE DOMAIN?
    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if MVEL.isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    nullTest = compileNullTest(edge)
    ref = nvl(edge.domain.min, edge.domain.max, datetime(2000, 1, 1))

    if edge.domain.interval.month > 0:
        offset = ref.subtract(ref.floorMonth(), durations.DAY).milli
        if offset > durations.DAY.milli * 28:
            offset = ref.subtract(ref.ceilingMonth(), durations.DAY).milli
        partition2int = "milli2Month(" + value + ", " + MVEL.value2MVEL(offset) + ")"
        partition2int = "((" + nullTest + ") ? 0 : " + partition2int + ")"

        def int2Partition(value):
            if Math.round(value) == 0:
                return edge.domain.NULL

            d = datetime(str(value)[:4:], str(value).right(2), 1)
            d = d.addMilli(offset)
            return edge.domain.getPartByKey(d)
    else:
        partition2int = "Math.floor((" + value + "-" + MVEL.value2MVEL(ref) + ")/" + edge.domain.interval.milli + ")"
        partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"

        def int2Partition(value):
            if Math.round(value) == numPartitions:
                return edge.domain.NULL
            return edge.domain.getPartByKey(ref.add(edge.domain.interval.multiply(value)))

    return Struct(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


# RETURN MVEL CODE THAT MAPS DURATION DOMAINS DOWN TO AN INTEGER AND
# AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
def compileDuration2Term(edge):
    if edge.esscript:
        Log.error("edge script not supported yet")

    # IS THERE A LIMIT ON THE DOMAIN?
    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if MVEL.isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    ref = nvl(edge.domain.min, edge.domain.max, durations.ZERO)
    nullTest = compileNullTest(edge)

    ms = edge.domain.interval.milli
    if edge.domain.interval.month > 0:
        ms = durations.YEAR.milli / 12 * edge.domain.interval.month

    partition2int = "Math.floor((" + value + "-" + MVEL.value2MVEL(ref) + ")/" + ms + ")"
    partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"

    def int2Partition(value):
        if Math.round(value) == numPartitions:
            return edge.domain.NULL
        return edge.domain.getPartByKey(ref.add(edge.domain.interval.multiply(value)))

    return Struct(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


# RETURN MVEL CODE THAT MAPS THE numeric DOMAIN DOWN TO AN INTEGER AND
# AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
def compileNumeric2Term(edge):
    if edge.script:
        Log.error("edge script not supported yet")

    if edge.domain.type != "numeric" and edge.domain.type != "count":
        Log.error("can only translate numeric domains")

    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if MVEL.isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    if not edge.domain.max:
        if not edge.domain.min:
            ref = 0
            partition2int = "Math.floor(" + value + ")/" + MVEL.value2MVEL(edge.domain.interval) + ")"
            nullTest = "false"
        else:
            ref = MVEL.value2MVEL(edge.domain.min)
            partition2int = "Math.floor((" + value + "-" + ref + ")/" + MVEL.value2MVEL(edge.domain.interval) + ")"
            nullTest = "" + value + "<" + ref
    elif not edge.domain.min:
        ref = MVEL.value2MVEL(edge.domain.max)
        partition2int = "Math.floor((" + value + "-" + ref + ")/" + MVEL.value2MVEL(edge.domain.interval) + ")"
        nullTest = "" + value + ">=" + ref
    else:
        top = MVEL.value2MVEL(edge.domain.max)
        ref = MVEL.value2MVEL(edge.domain.min)
        partition2int = "Math.floor((" + value + "-" + ref + ")/" + MVEL.value2MVEL(edge.domain.interval) + ")"
        nullTest = "(" + value + "<" + ref + ") or (" + value + ">=" + top + ")"

    partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"
    offset = CNV.value2int(ref)

    def int2Partition(value):
        if Math.round(value) == numPartitions:
            return edge.domain.NULL
        return edge.domain.getPartByKey((value * edge.domain.interval) + offset)

    return Struct(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


def compileString2Term(edge):
    if edge.esscript:
        Log.error("edge script not supported yet")

    value = edge.value
    if MVEL.isKeyword(value):
        value = strings.expand_template("getDocValue({{path}})", {"path": CNV.string2quote(value)})
        # DO NOT DO THIS, PARENT DOCS MAY NOT BE IN THE doc[]
        # path = split_field(value)
        # value = "getDocValue(\"" + path[0] + "\")"
        # for p in path[1::]:
        #     value = "get("+value+", \""+p+"\")"

    def fromTerm(value):
        return edge.domain.getPartByKey(CNV.pipe2value(value))

    return Struct(
        toTerm={"head": "", "body": 'Value2Pipe(' + value + ')'},
        fromTerm=fromTerm
    )


def compileNullTest(edge):
    """
    RETURN A MVEL EXPRESSION THAT WILL EVALUATE TO true FOR OUT-OF-BOUNDS
    """
    if edge.domain.type not in domains.ALGEBRAIC:
        Log.error("can only translate time and duration domains")

    # IS THERE A LIMIT ON THE DOMAIN?
    value = edge.value
    if MVEL.isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    if not edge.domain.max:
        if not edge.domain.min:
            return False
        bot = MVEL.value2MVEL(edge.domain.min)
        nullTest = "" + value + "<" + bot
    elif not edge.domain.min:
        top = MVEL.value2MVEL(edge.domain.max)
        nullTest = "" + value + ">=" + top
    else:
        top = MVEL.value2MVEL(edge.domain.max)
        bot = MVEL.value2MVEL(edge.domain.min)
        nullTest = "(" + value + "<" + bot + ") or (" + value + ">=" + top + ")"

    return nullTest


def compileEdges2Term(mvel_compiler, edges, constants):
    """
    GIVE MVEL CODE THAT REDUCES A UNIQUE TUPLE OF PARTITIONS DOWN TO A UNIQUE TERM
    GIVE LAMBDA THAT WILL CONVERT THE TERM BACK INTO THE TUPLE
    RETURNS TUPLE OBJECT WITH "type" and "value" ATTRIBUTES.
    "type" CAN HAVE A VALUE OF "script", "field" OR "count"
    CAN USE THE constants (name, value pairs)
    """

    # IF THE QUERY IS SIMPLE ENOUGH, THEN DO NOT USE TERM PACKING
    edge0 = edges[0]

    if len(edges) == 1 and edge0.domain.type in ["set", "default"]:
        # THE TERM RETURNED WILL BE A MEMBER OF THE GIVEN SET
        def temp(term):
            return StructList([edge0.domain.getPartByKey(term)])

        if edge0.value and MVEL.isKeyword(edge0.value):
            return Struct(
                field=edge0.value,
                term2parts=temp
            )
        elif COUNT(edge0.domain.dimension.fields) == 1:
            return Struct(
                field=edge0.domain.dimension.fields[0],
                term2parts=temp
            )
        elif not edge0.value and edge0.domain.partitions:
            script = mvel_compiler.Parts2TermScript(edge0.domain)
            return Struct(
                expression=script,
                term2parts=temp
            )
        else:
            return Struct(
                expression=mvel_compiler.compile_expression(edge0.value, constants),
                term2parts=temp
            )

    mvel_terms = []     # FUNCTION TO PACK TERMS
    fromTerm2Part = []   # UNPACK TERMS BACK TO PARTS
    for e in edges:
        if not e.value and e.domain.field:
            Log.error("not expected")

        if e.domain.type == "time":
            t = compileTime2Term(e)
        elif e.domain.type == "duration":
            t = compileDuration2Term(e)
        elif e.domain.type in domains.ALGEBRAIC:
            t = compileNumeric2Term(e)
        elif e.domain.type == "set" and not e.domain.field:
            def fromTerm(term):
                return e.domain.getPartByKey(term)

            t = Struct(
                toTerm=mvel_terms.Parts2Term(
                    query.frum,
                    e.domain
                ),
                fromTerm=fromTerm
            )
        else:
            t = compileString2Term(e)

        if not t.toTerm.body:
            Log.error("")

        fromTerm2Part.append(t.fromTerm)
        mvel_terms.append(t.toTerm.body)

    # REGISTER THE DECODE FUNCTION
    def temp(term):
        terms = term.split('|')
        output = StructList([fromTerm2Part[i](t) for i, t in enumerate(terms)])
        return output

    return Struct(
        expression=mvel_compiler.compile_expression("+'|'+".join(mvel_terms), constants),
        term2parts=temp
    )


def fix_es_stats(s):
    """
    ES RETURNS BAD DEFAULT VALUES FOR STATS
    """
    if s.count == 0:
        return stats.zero
    return s


#MAP NAME TO SQL FUNCTION
aggregates = {
    "none": "none",
    "one": "count",
    "sum": "total",
    "add": "total",
    "count": "count",
    "maximum": "max",
    "minimum": "min",
    "max": "max",
    "min": "min",
    "mean": "mean",
    "average": "mean",
    "avg": "mean",
    "N": "count",
    "X0": "count",
    "X1": "total",
    "X2": "sum_of_squares",
    "std": "std_deviation",
    "stddev": "std_deviation",
    "var": "variance",
    "variance": "variance"
}

