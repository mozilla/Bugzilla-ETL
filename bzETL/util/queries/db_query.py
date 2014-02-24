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
from ..cnv import CNV
from ..collections.matrix import Matrix
from .query import Query
from ..sql.db import int_list_packer, SQL, DB
from ..env.logs import Log
from ..strings import indent, expand_template
from ..struct import nvl


class DBQuery(object):
    """
    Qb to MySQL DATABASE QUERIES
    """
    def __init__(self, db):
        object.__init__(self)
        if isinstance(db, DB):
            self.db = db
        else:
            self.db = DB(db)

    def query(self, query, stacked=False):
        """
        TRANSLATE Qb QUERY ON SINGLE TABLE TO SQL QUERY
        """
        query = Query(query)

        sql, post = self._subquery(query, isolate=False, stacked=stacked)
        query.data = post(sql)
        return query.data

    def update(self, query):
        self.db.execute("""
            UPDATE {{table_name}}
            SET {{assignment}}
            {{where}}
        """, {
            "table_name": query["from"],
            "assignment": ",".join(self.db.quote_column(k) + "=" + self.db.quote_value(v) for k, v in query.set),
            "where": self._where2sql(query.where)
        })


    def _subquery(self, query, isolate=True, stacked=False):
        if isinstance(query, basestring):
            return self.db.quote_column(query), None
        if query.name:  # IT WOULD BE SAFER TO WRAP TABLE REFERENCES IN A TYPED OBJECT (Cube, MAYBE?)
            return self.db.quote_column(query.name), None

        if query.edges:
            # RETURN A CUBE
            sql, post = self._grouped(query, stacked)
        else:
            select = struct.listwrap(query.select)
            if select[0].aggregate != "none":
                sql, post = self._aggop(query)
            else:
                sql, post = self._setop(query)

        if isolate:
            return "(\n"+sql+"\n) a\n", post
        else:
            return sql, post

    def _grouped(self, query, stacked=False):
        select = struct.listwrap(query.select)

        # RETURN SINGLE OBJECT WITH AGGREGATES
        for s in select:
            if s.aggregate not in aggregates:
                Log.error("Expecting all columns to have an aggregate: {{select}}", {"select": s})

        selects = []
        groups = []

        edges = query.edges
        for e in edges:
            if e.domain.type != "default":
                Log.error("domain of type {{type}} not supported, yet", {"type": e.domain.type})
            groups.append(e.value)
            selects.append(e.value + " AS " + self.db.quote_column(e.name))

        for s in select:
            selects.append(aggregates[s.aggregate].replace("{{code}}", s.value) + " AS " + self.db.quote_column(s.name))

        sql = expand_template("""
            SELECT
                {{selects}}
            FROM
                {{table}}
            {{where}}
            GROUP BY
                {{groups}}
        """, {
            "selects": SQL(",\n".join(selects)),
            "groups": SQL(",\n".join(groups)),
            "table": self._subquery(query["from"])[0],
            "where": self._where2sql(query.where)
        })

        def post_stacked(sql):
            # RETURN IN THE USUAL DATABASE RESULT SET FORMAT
            return self.db.query(sql)

        def post(sql):
            # FIND OUT THE default DOMAIN SIZES
            result = self.db.column_query(sql)
            num_edges = len(edges)
            for e, edge in enumerate(edges):
                domain = edge.domain
                if domain.type == "default":
                    domain.type = "set"
                    parts = set(result[e])
                    domain.partitions = [{"index": i, "value": p} for i, p in enumerate(parts)]
                    domain.map = {p: i for i, p in enumerate(parts)}
                else:
                    Log.error("Do not know what to do here, yet")

            # FILL THE DATA CUBE
            maps = [(struct.unwrap(e.domain.map), result[i]) for i, e in enumerate(edges)]
            cubes = []
            for c, s in enumerate(select):
                data = Matrix(*[len(e.domain.partitions) + (1 if e.allow_nulls else 0) for e in edges])
                for rownum, value in enumerate(result[c + num_edges]):
                    coord = [m[r[rownum]] for m, r in maps]
                    data[coord] = value
                cubes.append(data)

            if isinstance(query.select, list):
                return cubes
            else:
                return cubes[0]

        return sql, post if not stacked else post_stacked

    def _aggop(self, query):
        """
        SINGLE ROW RETURNED WITH AGGREGATES
        """
        if isinstance(query.select, list):
            # RETURN SINGLE OBJECT WITH AGGREGATES
            for s in query.select:
                if s.aggregate not in aggregates:
                    Log.error("Expecting all columns to have an aggregate: {{select}}", {"select": s})

            selects = []
            for s in query.select:
                selects.append(aggregates[s.aggregate].replace("{{code}}", s.value) + " AS " + self.db.quote_column(s.name))

            sql = expand_template("""
                SELECT
                    {{selects}}
                FROM
                    {{table}}
                {{where}}
            """, {
                "selects": SQL(",\n".join(selects)),
                "table": self._subquery(query["from"])[0],
                "where": self._where2sql(query.filter)
            })

            return sql, lambda sql: self.db.column(sql)[0]  # RETURNING SINGLE OBJECT WITH AGGREGATE VALUES
        else:
            # RETURN SINGLE VALUE
            s0 = query.select
            if s0.aggregate not in aggregates:
                Log.error("Expecting all columns to have an aggregate: {{select}}", {"select": s0})

            select = aggregates[s0.aggregate].replace("{{code}}", s0.value) + " AS " + self.db.quote_column(s0.name)

            sql = expand_template("""
                SELECT
                    {{selects}}
                FROM
                    {{table}}
                {{where}}
            """, {
                "selects": SQL(select),
                "table": self._subquery(query["from"])[0],
                "where": self._where2sql(query.where)
            })

            def post(sql):
                result = self.db.column_query(sql)
                return result[0][0]

            return sql, post  # RETURN SINGLE VALUE

    def _setop(self, query):
        """
        NO AGGREGATION, SIMPLE LIST COMPREHENSION
        """
        if isinstance(query.select, list):
            # RETURN BORING RESULT SET
            selects = []
            for s in query.select:
                selects.append(s.value + " AS " + self.db.quote_column(s.name))

            sql = expand_template("""
                SELECT
                    {{selects}}
                FROM
                    {{table}}
                {{where}}
                {{limit}}
                {{sort}}
            """, {
                "selects": SQL(",\n".join(selects)),
                "table": self._subquery(query["from"])[0],
                "where": self._where2sql(query.where),
                "limit": self._limit2sql(query.limit),
                "sort": self._sort2sql(query.sort)
            })

            return sql, lambda sql: self.db.query(sql)  # RETURN BORING RESULT SET
        else:
            # RETURN LIST OF VALUES
            name = query.select.name
            select = query.select.value + " AS " + self.db.quote_column(name)

            sql = expand_template("""
                SELECT
                    {{selects}}
                FROM
                    {{table}}
                {{where}}
                {{limit}}
                {{sort}}
            """, {
                "selects": SQL(select),
                "table": self._subquery(query["from"])[0],
                "where": self._where2sql(query.where),
                "limit": self._limit2sql(query.limit),
                "sort": self._sort2sql(query.sort)
            })

            return sql, lambda sql: [r[name] for r in self.db.query(sql)]  # RETURNING LIST OF VALUES

    def _sort2sql(self, sort):
        """
        RETURN ORDER BY CLAUSE
        """
        if not sort:
            return ""
        return SQL("ORDER BY "+",\n".join([self.db.quote_column(o.field)+(" DESC" if o.sort==-1 else "") for o in sort]))

    def _limit2sql(self, limit):
        return SQL("" if not limit else "LIMIT "+str(limit))


    def _where2sql(self, where):
        if where == None:
            return ""
        return SQL("WHERE "+_esfilter2sqlwhere(self.db, where))


def _isolate(separator, list):
    if len(list) > 1:
        return "(\n" + indent((" " + separator + "\n").join(list)) + "\n)"
    else:
        return list[0]


def esfilter2sqlwhere(db, esfilter):
    return SQL(_esfilter2sqlwhere(db, esfilter))

def _esfilter2sqlwhere(db, esfilter):
    """
    CONVERT ElassticSearch FILTER TO SQL FILTER
    db - REQUIRED TO PROPERLY QUOTE VALUES AND COLUMN NAMES
    """
    esfilter = struct.wrap(esfilter)

    if esfilter["and"]:
        return _isolate("AND", [esfilter2sqlwhere(db, a) for a in esfilter["and"]])
    elif esfilter["or"]:
        return _isolate("OR", [esfilter2sqlwhere(db, a) for a in esfilter["or"]])
    elif esfilter["not"]:
        return "NOT (" + esfilter2sqlwhere(db, esfilter["not"]) + ")"
    elif esfilter.term:
        return _isolate("AND", [db.quote_column(col) + "=" + db.quote_value(val) for col, val in esfilter.term.items()])
    elif esfilter.terms:
        for col, v in esfilter.terms.items():
            if len(v) == 0:
                return "FALSE"

            try:
                int_list = CNV.value2intlist(v)
                has_null = False
                for vv in v:
                    if vv == None:
                        has_null = True
                        break
                if int_list:
                    filter = int_list_packer(col, int_list)
                    if has_null:
                        return esfilter2sqlwhere(db, {"or": [{"missing": col}, filter]})
                    else:
                        return esfilter2sqlwhere(db, filter)
                else:
                    if has_null:
                        return esfilter2sqlwhere(db, {"missing": col})
                    else:
                        return "false"
            except Exception, e:
                pass
            return db.quote_column(col) + " in (" + ", ".join([db.quote_value(val) for val in v]) + ")"
    elif esfilter.script:
        return "(" + esfilter.script + ")"
    elif esfilter.range:
        name2sign = {
            "gt": ">",
            "gte": ">=",
            "lte": "<=",
            "lt": "<"
        }

        def single(col, r):
            min = nvl(r["gte"], r[">="])
            max = nvl(r["lte"], r["<="])
            if min and max:
                #SPECIAL CASE (BETWEEN)
                return db.quote_column(col) + " BETWEEN " + db.quote_value(min) + " AND " + db.quote_value(max)
            else:
                return " AND ".join(
                    db.quote_column(col) + name2sign[sign] + db.quote_value(value)
                        for sign, value in r.items()
                )

        output = _isolate("AND", [single(col, ranges) for col, ranges in esfilter.range.items()])
        return output
    elif esfilter.missing:
        if isinstance(esfilter.missing, basestring):
            return "(" + db.quote_column(esfilter.missing) + " IS Null)"
        else:
            return "(" + db.quote_column(esfilter.missing.field) + " IS Null)"
    elif esfilter.exists:
        if isinstance(esfilter.exists, basestring):
            return "(" + db.quote_column(esfilter.exists) + " IS NOT Null)"
        else:
            return "(" + db.quote_column(esfilter.exists.field) + " IS NOT Null)"
    elif esfilter.match_all:
        return "1=1"
    else:
        Log.error("Can not convert esfilter to SQL: {{esfilter}}", {"esfilter": esfilter})


#MAP NAME TO SQL FUNCTION
aggregates = {
    "one": "COUNT({{code}})",
    "sum": "SUM({{code}})",
    "add": "SUM({{code}})",
    "count": "COUNT({{code}})",
    "maximum": "MAX({{code}})",
    "minimum": "MIN({{code}})",
    "max": "MAX({{code}})",
    "min": "MIN({{code}})",
    "mean": "AVG({{code}})",
    "average": "AVG({{code}})",
    "avg": "AVG({{code}})",
    "N": "COUNT({{code}})",
    "X0": "COUNT({{code}})",
    "X1": "SUM({{code}})",
    "X2": "SUM(POWER({{code}}, 2))",
    "std": "STDDEV({{code}})",
    "stddev": "STDDEV({{code}})",
    "var": "POWER(STDDEV({{code}}), 2)",
    "variance": "POWER(STDDEV({{code}}), 2)"
}
